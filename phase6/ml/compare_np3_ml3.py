from pathlib import Path
import json

import joblib
import pandas as pd
from sqlalchemy import text

from phase4.api.config import DATABASE_URL, NP3_ALERT_PATH
from phase4.api.db.database import engine


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

THRESHOLD = 2000

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ARTIFACT_DIR = PROJECT_ROOT / "phase6" / "ml" / "artifacts"

MODEL_PATH = ARTIFACT_DIR / "logistic_regression.joblib"
SCALER_PATH = ARTIFACT_DIR / "feature_scaler.joblib"

COMPARISON_PATH = ARTIFACT_DIR / "ml3_np3_comparison.csv"
SUMMARY_PATH = ARTIFACT_DIR / "ml3_np3_comparison_summary.json"


FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]


# ---------------------------------------------------------
# Load ML3 modeling data
# ---------------------------------------------------------

def load_ml3_data() -> pd.DataFrame:
    query = text(
        """
        SELECT
            mf.grid_id,
            mf.feature_timestamp,
            mf.avg_activity,
            mf.activity_growth,
            mf.active_hours,
            mf.peak_ratio,
            mf.variability,
            mf.internet_share,

            CASE
                WHEN f.total_activity >= :threshold
                THEN 1
                ELSE 0
            END AS high_activity_target

        FROM ml_grid_features mf

        INNER JOIN dim_grid dg
            ON dg.grid_id = mf.grid_id

        INNER JOIN dim_time future_time
            ON future_time.timestamp =
               DATE_ADD(mf.feature_timestamp, INTERVAL 1 HOUR)

        INNER JOIN fact_network_activity f
            ON f.grid_key = dg.grid_key
           AND f.time_key = future_time.time_key

        ORDER BY
            mf.feature_timestamp,
            mf.grid_id
        """
    )

    with engine.connect() as connection:
        df = pd.read_sql(
            query,
            connection,
            params={"threshold": THRESHOLD},
        )

    df["feature_timestamp"] = pd.to_datetime(df["feature_timestamp"])

    return df


# ---------------------------------------------------------
# Recreate the same chronological ML3 test split
# ---------------------------------------------------------

def chronological_test_split(df: pd.DataFrame):
    timestamps = sorted(df["feature_timestamp"].unique())

    split_index = int(len(timestamps) * 0.8)

    train_timestamps = timestamps[:split_index]
    test_timestamps = timestamps[split_index:]

    train_end = train_timestamps[-1]
    test_start = test_timestamps[0]

    train_df = df[df["feature_timestamp"] <= train_end].copy()

    test_df = df[df["feature_timestamp"] >= test_start].copy()

    return train_df, test_df


# ---------------------------------------------------------
# Generate ML3 predictions using saved model
# ---------------------------------------------------------

def generate_ml3_predictions(test_df: pd.DataFrame) -> pd.DataFrame:
    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)

    X_test = test_df[FEATURE_COLUMNS]

    X_test_scaled = scaler.transform(X_test)

    test_df["ml3_prediction"] = model.predict(X_test_scaled)

    test_df["ml3_probability"] = model.predict_proba(
        X_test_scaled
    )[:, 1]

    test_df["ml3_prediction"] = test_df["ml3_prediction"].astype(int)

    test_df["ml3_prediction_label"] = test_df["ml3_prediction"].map(
        {
            0: "NOT_HIGH",
            1: "HIGH",
        }
    )

    return test_df


# ---------------------------------------------------------
# Load and prepare NP3 alerts
# ---------------------------------------------------------

def load_np3_alerts() -> pd.DataFrame:
    if not NP3_ALERT_PATH.exists():
        raise FileNotFoundError(
            f"NP3 alert file not found: {NP3_ALERT_PATH}"
        )

    np3 = pd.read_csv(NP3_ALERT_PATH)

    required_columns = {
        "grid_id",
        "timestamp",
        "alert_type",
    }

    missing = required_columns - set(np3.columns)

    if missing:
        raise ValueError(
            f"NP3 alert file is missing columns: {sorted(missing)}"
        )

    np3["timestamp"] = pd.to_datetime(
        np3["timestamp"],
        utc=True,
    ).dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)

    # One row per grid + timestamp.
    # Multiple NP3 alert types at the same hour are collapsed.
    np3_grouped = (
        np3.groupby(
            ["grid_id", "timestamp"],
            as_index=False,
        )
        .agg(
            np3_alert=("alert_type", lambda values: True),
            np3_alert_types=(
                "alert_type",
                lambda values: ",".join(
                    sorted(set(values.astype(str)))
                ),
            ),
            np3_alert_count=("alert_type", "count"),
        )
    )

    return np3_grouped


# ---------------------------------------------------------
# Compare NP3 and ML3
# ---------------------------------------------------------

def build_comparison(
    ml3_test: pd.DataFrame,
    np3: pd.DataFrame,
) -> pd.DataFrame:

    comparison = ml3_test.merge(
        np3,
        how="left",
        left_on=["grid_id", "feature_timestamp"],
        right_on=["grid_id", "timestamp"],
    )

    comparison["np3_alert"] = (
        comparison["np3_alert"]
        .fillna(False)
        .astype(bool)
    )

    comparison["np3_alert_types"] = (
        comparison["np3_alert_types"]
        .fillna("")
    )

    comparison["np3_alert_count"] = (
        comparison["np3_alert_count"]
        .fillna(0)
        .astype(int)
    )

    comparison["ml3_positive"] = (
        comparison["ml3_prediction"] == 1
    )

    comparison["actual_next_hour_high"] = (
        comparison["high_activity_target"] == 1
    )

    # Four-way comparison
    comparison["comparison_group"] = "NEITHER"

    comparison.loc[
        comparison["np3_alert"] & comparison["ml3_positive"],
        "comparison_group",
    ] = "BOTH"

    comparison.loc[
        comparison["np3_alert"] & ~comparison["ml3_positive"],
        "comparison_group",
    ] = "NP3_ONLY"

    comparison.loc[
        ~comparison["np3_alert"] & comparison["ml3_positive"],
        "comparison_group",
    ] = "ML3_ONLY"

    return comparison


# ---------------------------------------------------------
# Summary calculations
# ---------------------------------------------------------

def calculate_summary(comparison: pd.DataFrame) -> dict:
    total_rows = len(comparison)

    group_counts = (
        comparison["comparison_group"]
        .value_counts()
        .to_dict()
    )

    def pct(value):
        if total_rows == 0:
            return 0.0
        return round((value / total_rows) * 100, 4)

    np3_alert_rows = int(comparison["np3_alert"].sum())
    ml3_positive_rows = int(comparison["ml3_positive"].sum())

    both_rows = int(
        (comparison["comparison_group"] == "BOTH").sum()
    )

    np3_only_rows = int(
        (comparison["comparison_group"] == "NP3_ONLY").sum()
    )

    ml3_only_rows = int(
        (comparison["comparison_group"] == "ML3_ONLY").sum()
    )

    neither_rows = int(
        (comparison["comparison_group"] == "NEITHER").sum()
    )

    # Actual next-hour high-activity rate among each detector's alerts.
    np3_next_hour_rate = (
        comparison.loc[
            comparison["np3_alert"],
            "actual_next_hour_high",
        ].mean()
        if np3_alert_rows
        else 0.0
    )

    ml3_next_hour_rate = (
        comparison.loc[
            comparison["ml3_positive"],
            "actual_next_hour_high",
        ].mean()
        if ml3_positive_rows
        else 0.0
    )

    # Agreement means both systems say positive
    # or both say negative.
    agreement_rows = both_rows + neither_rows

    agreement_rate = (
        agreement_rows / total_rows
        if total_rows
        else 0.0
    )

    summary = {
        "threshold": THRESHOLD,
        "total_comparison_rows": total_rows,

        "test_start": str(
            comparison["feature_timestamp"].min()
        ),
        "test_end": str(
            comparison["feature_timestamp"].max()
        ),

        "np3_alert_rows": np3_alert_rows,
        "np3_alert_rate_percent": pct(np3_alert_rows),

        "ml3_positive_rows": ml3_positive_rows,
        "ml3_positive_rate_percent": pct(ml3_positive_rows),

        "both_rows": both_rows,
        "np3_only_rows": np3_only_rows,
        "ml3_only_rows": ml3_only_rows,
        "neither_rows": neither_rows,

        "agreement_rows": agreement_rows,
        "agreement_rate_percent": round(
            agreement_rate * 100,
            4,
        ),

        "np3_alert_next_hour_high_rate_percent": round(
            np3_next_hour_rate * 100,
            4,
        ),

        "ml3_positive_next_hour_high_rate_percent": round(
            ml3_next_hour_rate * 100,
            4,
        ),

        "np3_alert_types": {},
    }

    # Breakdown by NP3 alert type
    alert_type_rows = comparison[
        comparison["np3_alert"]
    ].copy()

    if not alert_type_rows.empty:
        for alert_type in sorted(
            {
                alert_type
                for value in alert_type_rows["np3_alert_types"]
                for alert_type in value.split(",")
                if alert_type
            }
        ):
            mask = alert_type_rows[
                "np3_alert_types"
            ].str.contains(
                alert_type,
                regex=False,
            )

            subset = alert_type_rows[mask]

            count = len(subset)

            ml3_overlap = int(
                subset["ml3_positive"].sum()
            )

            next_hour_high = float(
                subset["actual_next_hour_high"].mean()
            )

            summary["np3_alert_types"][alert_type] = {
                "rows": count,
                "percent_of_np3_alert_rows": round(
                    (count / np3_alert_rows) * 100,
                    4,
                )
                if np3_alert_rows
                else 0.0,
                "ml3_positive_overlap_rows": ml3_overlap,
                "ml3_positive_overlap_percent": round(
                    (ml3_overlap / count) * 100,
                    4,
                )
                if count
                else 0.0,
                "next_hour_high_rate_percent": round(
                    next_hour_high * 100,
                    4,
                ),
            }

    # Add the four-way counts in a predictable order.
    summary["comparison_groups"] = {
        "BOTH": group_counts.get("BOTH", 0),
        "NP3_ONLY": group_counts.get("NP3_ONLY", 0),
        "ML3_ONLY": group_counts.get("ML3_ONLY", 0),
        "NEITHER": group_counts.get("NEITHER", 0),
    }

    return summary


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    print("Loading ML3 modeling data...")

    ml3_data = load_ml3_data()

    print(f"Usable ML3 rows: {len(ml3_data):,}")

    train_df, test_df = chronological_test_split(
        ml3_data
    )

    print()
    print("--- ML3 Test Period ---")
    print(
        f"Test start: {test_df['feature_timestamp'].min()}"
    )
    print(
        f"Test end:   {test_df['feature_timestamp'].max()}"
    )
    print(f"Test rows:   {len(test_df):,}")

    print()
    print("Generating ML3 predictions using saved model...")

    test_df = generate_ml3_predictions(test_df)

    print(
        f"ML3 positive predictions: "
        f"{test_df['ml3_prediction'].sum():,}"
    )

    print()
    print("Loading NP3 alerts...")

    np3 = load_np3_alerts()

    print(f"NP3 alert rows after grouping: {len(np3):,}")

    # Restrict NP3 to the ML3 test period.
    test_start = test_df["feature_timestamp"].min()
    test_end = test_df["feature_timestamp"].max()

    np3 = np3[
        (np3["timestamp"] >= test_start)
        & (np3["timestamp"] <= test_end)
    ].copy()

    print(
        f"NP3 alerts inside ML3 test period: "
        f"{len(np3):,}"
    )

    print()
    print("Building NP3 vs ML3 comparison...")

    comparison = build_comparison(
        test_df,
        np3,
    )

    summary = calculate_summary(comparison)

    # -----------------------------------------------------
    # Print results
    # -----------------------------------------------------

    print()
    print("=" * 60)
    print("ML3 vs NP3 COMPARISON")
    print("=" * 60)

    print()
    print(
        f"Comparison rows: {summary['total_comparison_rows']:,}"
    )

    print(
        f"NP3 alert rows:   "
        f"{summary['np3_alert_rows']:,} "
        f"({summary['np3_alert_rate_percent']:.2f}%)"
    )

    print(
        f"ML3 positive:     "
        f"{summary['ml3_positive_rows']:,} "
        f"({summary['ml3_positive_rate_percent']:.2f}%)"
    )

    print()
    print("--- Four-Way Comparison ---")

    groups = summary["comparison_groups"]

    print(f"BOTH:     {groups['BOTH']:,}")
    print(f"NP3_ONLY: {groups['NP3_ONLY']:,}")
    print(f"ML3_ONLY: {groups['ML3_ONLY']:,}")
    print(f"NEITHER:  {groups['NEITHER']:,}")

    print()
    print(
        f"Overall agreement: "
        f"{summary['agreement_rate_percent']:.2f}%"
    )

    print()
    print("--- Next-Hour High Activity ---")

    print(
        f"NP3 alert -> next-hour high: "
        f"{summary['np3_alert_next_hour_high_rate_percent']:.2f}%"
    )

    print(
        f"ML3 positive -> next-hour high: "
        f"{summary['ml3_positive_next_hour_high_rate_percent']:.2f}%"
    )

    print()
    print("--- NP3 Alert Type Breakdown ---")

    for alert_type, values in summary[
        "np3_alert_types"
    ].items():

        print()
        print(alert_type)
        print(f"  rows: {values['rows']:,}")
        print(
            "  ML3 positive overlap: "
            f"{values['ml3_positive_overlap_percent']:.2f}%"
        )
        print(
            "  next-hour high rate: "
            f"{values['next_hour_high_rate_percent']:.2f}%"
        )

    # -----------------------------------------------------
    # Save detailed comparison
    # -----------------------------------------------------

    comparison_columns = [
        "grid_id",
        "feature_timestamp",
        "avg_activity",
        "activity_growth",
        "active_hours",
        "peak_ratio",
        "variability",
        "internet_share",
        "ml3_probability",
        "ml3_prediction",
        "ml3_prediction_label",
        "np3_alert",
        "np3_alert_types",
        "np3_alert_count",
        "high_activity_target",
        "actual_next_hour_high",
        "comparison_group",
    ]

    comparison[
        comparison_columns
    ].to_csv(
        COMPARISON_PATH,
        index=False,
    )

    with open(
        SUMMARY_PATH,
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            summary,
            file,
            indent=2,
        )

    print()
    print("Artifacts saved:")
    print(f"  {COMPARISON_PATH}")
    print(f"  {SUMMARY_PATH}")


if __name__ == "__main__":
    main()
from __future__ import annotations

import json
from pathlib import Path
import sys
import joblib
import numpy as np
import pandas as pd


# ============================================================
# Paths
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PHASE4_DIR = PROJECT_ROOT / "phase4"
sys.path.insert(0, str(PHASE4_DIR))
DATA_DIR = PROJECT_ROOT / "data"
LANDING_DIR = DATA_DIR / "landing"
ARTIFACT_DIR = PROJECT_ROOT / "phase6" / "ml" / "artifacts"

ANOMALY_FILE = LANDING_DIR / "network_anomaly_scores.csv"
ANALYTICS_FILE = LANDING_DIR / "hourly_grid_summary.csv"
NP3_FILE = LANDING_DIR / "network_alerts.csv"

MODEL_FILE = ARTIFACT_DIR / "logistic_regression.joblib"
SCALER_FILE = ARTIFACT_DIR / "feature_scaler.joblib"
ML3_EVALUATION_FILE = ARTIFACT_DIR / "ml3_evaluation.json"

OUTPUT_COMPARISON_FILE = (
    LANDING_DIR / "ml4_three_way_comparison.csv"
)

OUTPUT_REPORT_FILE = (
    LANDING_DIR / "ml4_validation_report.txt"
)

print("PROJECT_ROOT:", PROJECT_ROOT)
print("PHASE4_DIR:", PHASE4_DIR)
print("PHASE4 exists:", PHASE4_DIR.exists())
print("API exists:", (PHASE4_DIR / "api").exists())
# ============================================================
# ML3 feature configuration
# ============================================================

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]


# ============================================================
# Utility functions
# ============================================================

def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(
            f"Required file not found: {path}"
        )


def normalize_timestamps(
    series: pd.Series,
) -> pd.Series:
    """
    Normalize timestamps to naive Asia/Kolkata local time.

    NP3 output is timezone-aware (+05:30), while ML3 feature
    timestamps are naive local timestamps.
    """

    parsed = pd.to_datetime(
        series,
        errors="coerce",
    )

    if parsed.isna().any():
        raise ValueError(
            "Invalid timestamps found during normalization."
        )

    # If timezone-aware, convert to Asia/Kolkata first.
    if getattr(parsed.dt, "tz", None) is not None:
        return (
            parsed
            .dt.tz_convert("Asia/Kolkata")
            .dt.tz_localize(None)
        )

    return parsed


# ============================================================
# 1. Load ML4 anomaly scores
# ============================================================

def load_ml4() -> pd.DataFrame:
    require_file(ANOMALY_FILE)

    df = pd.read_csv(
        ANOMALY_FILE
    )

    required = {
        "grid_id",
        "timestamp",
        "hour_of_day",
        "current_activity",
        "baseline_activity",
        "history_count",
        "deviation",
        "anomaly_score",
        "direction",
        "is_anomaly",
        "reason",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            f"ML4 file is missing columns: {sorted(missing)}"
        )

    df["timestamp"] = normalize_timestamps(
        df["timestamp"]
    )

    df["grid_id"] = pd.to_numeric(
        df["grid_id"],
        errors="raise",
    ).astype(int)

    df["hour_of_day"] = pd.to_numeric(
        df["hour_of_day"],
        errors="raise",
    ).astype(int)

    df["current_activity"] = pd.to_numeric(
        df["current_activity"],
        errors="raise",
    )

    df["baseline_activity"] = pd.to_numeric(
        df["baseline_activity"],
        errors="raise",
    )

    df["history_count"] = pd.to_numeric(
        df["history_count"],
        errors="raise",
    ).astype(int)

    df["anomaly_score"] = pd.to_numeric(
        df["anomaly_score"],
        errors="coerce",
    )

    df["is_anomaly"] = (
        df["is_anomaly"]
        .astype(str)
        .str.lower()
        .isin(["true", "1"])
    )

    return df


# ============================================================
# 2. Load underlying analytics
# ============================================================

def load_analytics() -> pd.DataFrame:
    require_file(ANALYTICS_FILE)

    df = pd.read_csv(
        ANALYTICS_FILE
    )

    required = {
        "grid_id",
        "timestamp",
        "total_activity",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            "Analytics file is missing columns: "
            f"{sorted(missing)}"
        )

    df["timestamp"] = normalize_timestamps(
        df["timestamp"]
    )

    df["grid_id"] = pd.to_numeric(
        df["grid_id"],
        errors="raise",
    ).astype(int)

    df["total_activity"] = pd.to_numeric(
        df["total_activity"],
        errors="raise",
    )

    df["hour_of_day"] = df["timestamp"].dt.hour

    return df


# ============================================================
# 3. Select representative anomaly cases
# ============================================================

def select_cases(
    ml4: pd.DataFrame,
) -> dict[str, pd.DataFrame]:
    """
    Select two HIGH and two LOW cases.

    We select strong anomalies, but avoid simply choosing four
    extreme rows from the same grid/hour pattern.

    Cases are spread across different grids where possible.
    """

    anomalies = ml4[
        ml4["is_anomaly"]
        & ml4["direction"].isin(["HIGH", "LOW"])
    ].copy()

    high = (
        anomalies[
            anomalies["direction"] == "HIGH"
        ]
        .sort_values(
            "anomaly_score",
            ascending=False,
        )
    )

    low = (
        anomalies[
            anomalies["direction"] == "LOW"
        ]
        .sort_values(
            "anomaly_score",
            ascending=False,
        )
    )

    def choose_diverse(
        frame: pd.DataFrame,
        count: int = 2,
    ) -> pd.DataFrame:

        selected = []

        used_grids = set()

        # First pass: prefer different grids.
        for _, row in frame.iterrows():

            if row["grid_id"] not in used_grids:

                selected.append(row)
                used_grids.add(
                    row["grid_id"]
                )

                if len(selected) == count:
                    break

        # Second pass if needed.
        if len(selected) < count:

            selected_indices = {
                row.name
                for row in selected
            }

            for _, row in frame.iterrows():

                if row.name not in selected_indices:

                    selected.append(row)

                    if len(selected) == count:
                        break

        return pd.DataFrame(selected)

    return {
        "HIGH": choose_diverse(high, 2),
        "LOW": choose_diverse(low, 2),
    }


# ============================================================
# 4. Inspect anomaly cases against underlying data
# ============================================================

def inspect_cases(
    cases: dict[str, pd.DataFrame],
    analytics: pd.DataFrame,
) -> list[str]:

    report_lines = []

    report_lines.append(
        "=" * 72
    )
    report_lines.append(
        "ML4 ANOMALY CASE VALIDATION"
    )
    report_lines.append(
        "=" * 72
    )

    for direction in ["HIGH", "LOW"]:

        report_lines.append("")
        report_lines.append(
            f"{direction} ANOMALY CASES"
        )
        report_lines.append(
            "-" * 72
        )

        selected = cases[direction]

        for case_number, (_, row) in enumerate(
            selected.iterrows(),
            start=1,
        ):

            grid_id = int(row["grid_id"])
            timestamp = row["timestamp"]
            hour = int(row["hour_of_day"])

            current = float(
                row["current_activity"]
            )

            baseline = float(
                row["baseline_activity"]
            )

            score = float(
                row["anomaly_score"]
            )

            history_count = int(
                row["history_count"]
            )

            # Same grid + same hour-of-day history.
            history = analytics[
                (analytics["grid_id"] == grid_id)
                &
                (analytics["hour_of_day"] == hour)
            ].copy()

            history = history.sort_values(
                "timestamp"
            )

            # Exclude the current observation.
            historical = history[
                history["timestamp"] != timestamp
            ].copy()

            historical_values = (
                historical[
                    "total_activity"
                ]
                .tolist()
            )

            historical_median = (
                float(
                    np.median(
                        historical_values
                    )
                )
                if historical_values
                else np.nan
            )

            report_lines.append("")
            report_lines.append(
                f"Case {case_number}"
            )

            report_lines.append(
                f"Grid: {grid_id}"
            )

            report_lines.append(
                f"Timestamp: {timestamp}"
            )

            report_lines.append(
                f"Hour of day: {hour:02d}:00"
            )

            report_lines.append(
                f"Current activity: {current:.4f}"
            )

            report_lines.append(
                f"ML4 baseline: {baseline:.4f}"
            )

            report_lines.append(
                f"Independent historical median: "
                f"{historical_median:.4f}"
            )

            report_lines.append(
                f"Deviation: "
                f"{float(row['deviation']):.4f}"
            )

            report_lines.append(
                f"Anomaly score: {score:.4f}"
            )

            report_lines.append(
                f"Direction: {row['direction']}"
            )

            report_lines.append(
                f"History count: {history_count}"
            )

            report_lines.append(
                f"Historical observations excluding current: "
                f"{len(historical_values)}"
            )

            report_lines.append(
                "Historical same-hour activity:"
            )

            for hist_row in historical.itertuples():

                report_lines.append(
                    f"  "
                    f"{hist_row.timestamp} -> "
                    f"{hist_row.total_activity:.4f}"
                )

            if historical_values:

                if direction == "HIGH":

                    explanation = (
                        "The current hour is unusually high "
                        "relative to this grid's historical "
                        "behavior at the same hour."
                    )

                else:

                    explanation = (
                        "The current hour is unusually low "
                        "relative to this grid's historical "
                        "behavior at the same hour."
                    )

            else:

                explanation = (
                    "Historical observations could not be "
                    "independently reconstructed."
                )

            report_lines.append(
                f"Interpretation: {explanation}"
            )

    return report_lines


# ============================================================
# 5. Load NP3 alerts
# ============================================================

def load_np3() -> pd.DataFrame:
    require_file(NP3_FILE)

    df = pd.read_csv(
        NP3_FILE
    )

    required = {
        "grid_id",
        "timestamp",
        "alert_type",
    }

    missing = required - set(df.columns)

    if missing:
        raise ValueError(
            f"NP3 file is missing columns: {sorted(missing)}"
        )

    df["timestamp"] = normalize_timestamps(
        df["timestamp"]
    )

    df["grid_id"] = pd.to_numeric(
        df["grid_id"],
        errors="raise",
    ).astype(int)

    return df


# ============================================================
# 6. Recreate ML3 test set
# ============================================================

def load_ml3_features() -> pd.DataFrame:
    """
    Load the ML2 feature table from MySQL using the existing
    Phase 4 SQLAlchemy model.

    The actual model is MLGridFeatures.
    """

    try:
        from api.db.database import SessionLocal
        from api.db.models import MLGridFeatures

    except ImportError as exc:
        raise RuntimeError(
            "Could not import the existing Phase 4 database "
            "configuration or MLGridFeatures model."
        ) from exc

    session = SessionLocal()

    try:
        rows = (
            session.query(
                MLGridFeatures.grid_id,
                MLGridFeatures.feature_timestamp,
                MLGridFeatures.avg_activity,
                MLGridFeatures.activity_growth,
                MLGridFeatures.active_hours,
                MLGridFeatures.peak_ratio,
                MLGridFeatures.variability,
                MLGridFeatures.internet_share,
            )
            .all()
        )

    finally:
        session.close()

    if not rows:
        raise ValueError(
            "No rows found in ml_grid_features."
        )

    df = pd.DataFrame(
        [
            {
                "grid_id": row.grid_id,
                "timestamp": row.feature_timestamp,
                "avg_activity": row.avg_activity,
                "activity_growth": row.activity_growth,
                "active_hours": row.active_hours,
                "peak_ratio": row.peak_ratio,
                "variability": row.variability,
                "internet_share": row.internet_share,
            }
            for row in rows
        ]
    )

    df["timestamp"] = normalize_timestamps(
        df["timestamp"]
    )

    return df

# def load_ml3_features() -> pd.DataFrame:
#     """
#     Load the ML3 feature table.

#     We use the same feature definitions and next-hour target
#     convention established during ML3.
#     """

#     try:
#         from features import FEATURE_COLUMNS as imported_features

#         feature_columns = imported_features

#     except (ImportError, AttributeError):
#         feature_columns = FEATURE_COLUMNS

#     # The feature data is stored in MySQL, so use SQLAlchemy
#     # through the existing Phase 4 database configuration.
#     try:
#         from api.db.database import SessionLocal
#         from api.db.models import MLGridFeature

#     except ImportError:
#         raise RuntimeError(
#             "Could not import the existing MLGridFeature "
#             "database model. Run this script from D:\\Project1."
#         )

#     session = SessionLocal()

#     try:

#         rows = (
#             session.query(
#                 MLGridFeature.grid_id,
#                 MLGridFeature.feature_timestamp,
#                 MLGridFeature.avg_activity,
#                 MLGridFeature.activity_growth,
#                 MLGridFeature.active_hours,
#                 MLGridFeature.peak_ratio,
#                 MLGridFeature.variability,
#                 MLGridFeature.internet_share,
#             )
#             .all()
#         )

#     finally:
#         session.close()

#     if not rows:
#         raise ValueError(
#             "No rows found in ml_grid_features."
#         )

#     df = pd.DataFrame(
#         [
#             {
#                 "grid_id": row.grid_id,
#                 "timestamp": row.feature_timestamp,
#                 "avg_activity": row.avg_activity,
#                 "activity_growth": row.activity_growth,
#                 "active_hours": row.active_hours,
#                 "peak_ratio": row.peak_ratio,
#                 "variability": row.variability,
#                 "internet_share": row.internet_share,
#             }
#             for row in rows
#         ]
#     )

#     df["timestamp"] = normalize_timestamps(
#         df["timestamp"]
#     )

#     return df


# ============================================================
# 7. Build exact ML3 target
# ============================================================

def build_ml3_test_set(
    features: pd.DataFrame,
    analytics: pd.DataFrame,
) -> pd.DataFrame:

    target_source = analytics[
        [
            "grid_id",
            "timestamp",
            "total_activity",
        ]
    ].copy()

    target_source = target_source.rename(
        columns={
            "timestamp": "target_timestamp",
            "total_activity": "next_hour_activity",
        }
    )

    # For each feature timestamp t, target is activity at t+1.
    target_source["feature_timestamp"] = (
        target_source["target_timestamp"]
        - pd.Timedelta(hours=1)
    )

    merged = features.merge(
        target_source[
            [
                "grid_id",
                "feature_timestamp",
                "next_hour_activity",
            ]
        ],
        left_on=[
            "grid_id",
            "timestamp",
        ],
        right_on=[
            "grid_id",
            "feature_timestamp",
        ],
        how="inner",
    )

    merged["target"] = (
        merged["next_hour_activity"] >= 2000
    ).astype(int)

    merged = merged.sort_values(
        "timestamp"
    ).reset_index(drop=True)

    # --------------------------------------------------------
    # Exact chronological 80/20 split used by ML3.
    # --------------------------------------------------------

    unique_timestamps = np.sort(
        merged["timestamp"]
        .unique()
    )

    split_index = int(
        len(unique_timestamps) * 0.8
    )

    split_timestamp = unique_timestamps[
        split_index
    ]

    test = merged[
        merged["timestamp"] >= split_timestamp
    ].copy()

    return test


# ============================================================
# 8. Load ML3 model and generate predictions
# ============================================================

def generate_ml3_predictions(
    test: pd.DataFrame,
) -> pd.DataFrame:

    require_file(MODEL_FILE)
    require_file(SCALER_FILE)

    model = joblib.load(
        MODEL_FILE
    )

    scaler = joblib.load(
        SCALER_FILE
    )

    X = test[
        FEATURE_COLUMNS
    ]

    X_scaled = scaler.transform(
        X
    )

    probabilities = model.predict_proba(
        X_scaled
    )[:, 1]

    predictions = (
        probabilities >= 0.5
    ).astype(int)

    result = test[
        [
            "grid_id",
            "timestamp",
            "next_hour_activity",
            "target",
        ]
    ].copy()

    result["ml3_probability"] = (
        probabilities
    )

    result["ml3_positive"] = (
        predictions == 1
    )

    return result


# ============================================================
# 9. Aggregate NP3 to grid/hour
# ============================================================

def aggregate_np3(
    np3: pd.DataFrame,
) -> pd.DataFrame:

    grouped = (
        np3.groupby(
            [
                "grid_id",
                "timestamp",
            ]
        )["alert_type"]
        .agg(
            list
        )
        .reset_index()
    )

    grouped["np3_alert"] = True

    grouped["np3_alert_types"] = (
        grouped["alert_type"]
        .apply(
            lambda values: "|".join(
                sorted(
                    set(values)
                )
            )
        )
    )

    return grouped[
        [
            "grid_id",
            "timestamp",
            "np3_alert",
            "np3_alert_types",
        ]
    ]


# ============================================================
# 10. Build ML4 grid/hour flags
# ============================================================

def aggregate_ml4(
    ml4: pd.DataFrame,
) -> pd.DataFrame:

    result = ml4[
        [
            "grid_id",
            "timestamp",
            "is_anomaly",
            "direction",
            "anomaly_score",
            "baseline_activity",
            "current_activity",
        ]
    ].copy()

    result = result.rename(
        columns={
            "is_anomaly": "ml4_anomaly",
            "direction": "ml4_direction",
        }
    )

    return result


# ============================================================
# 11. Three-way comparison
# ============================================================

def build_three_way_comparison(
    ml4: pd.DataFrame,
    np3: pd.DataFrame,
    ml3_test: pd.DataFrame,
) -> pd.DataFrame:

    np3_hourly = aggregate_np3(
        np3
    )

    ml4_hourly = aggregate_ml4(
        ml4
    )

    comparison = ml3_test[
        [
            "grid_id",
            "timestamp",
            "next_hour_activity",
            "target",
            "ml3_probability",
            "ml3_positive",
        ]
    ].copy()

    comparison = comparison.merge(
        np3_hourly,
        on=[
            "grid_id",
            "timestamp",
        ],
        how="left",
    )

    comparison = comparison.merge(
        ml4_hourly,
        on=[
            "grid_id",
            "timestamp",
        ],
        how="left",
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

    comparison["ml4_anomaly"] = (
        comparison["ml4_anomaly"]
        .fillna(False)
        .astype(bool)
    )

    comparison["ml4_direction"] = (
        comparison["ml4_direction"]
        .fillna("NONE")
    )

    comparison["ml4_only"] = (
        comparison["ml4_anomaly"]
        & ~comparison["np3_alert"]
        & ~comparison["ml3_positive"]
    )

    comparison["np3_only"] = (
        comparison["np3_alert"]
        & ~comparison["ml4_anomaly"]
        & ~comparison["ml3_positive"]
    )

    comparison["ml3_only"] = (
        comparison["ml3_positive"]
        & ~comparison["np3_alert"]
        & ~comparison["ml4_anomaly"]
    )

    comparison["all_three"] = (
        comparison["np3_alert"]
        & comparison["ml3_positive"]
        & comparison["ml4_anomaly"]
    )

    comparison["np3_ml4_both"] = (
        comparison["np3_alert"]
        & comparison["ml4_anomaly"]
        & ~comparison["ml3_positive"]
    )

    comparison["np3_ml3_both"] = (
        comparison["np3_alert"]
        & comparison["ml3_positive"]
        & ~comparison["ml4_anomaly"]
    )

    comparison["ml3_ml4_both"] = (
        comparison["ml3_positive"]
        & comparison["ml4_anomaly"]
        & ~comparison["np3_alert"]
    )

    comparison["none"] = (
        ~comparison["np3_alert"]
        & ~comparison["ml3_positive"]
        & ~comparison["ml4_anomaly"]
    )

    return comparison


# ============================================================
# 12. Comparison summary
# ============================================================

def comparison_summary(
    comparison: pd.DataFrame,
) -> list[str]:

    total = len(
        comparison
    )

    lines = []

    lines.append("")
    lines.append(
        "=" * 72
    )
    lines.append(
        "NP3 / ML3 / ML4 THREE-WAY COMPARISON"
    )
    lines.append(
        "=" * 72
    )

    lines.append(
        f"Comparison rows: {total:,}"
    )

    np3_count = int(
        comparison["np3_alert"].sum()
    )

    ml3_count = int(
        comparison["ml3_positive"].sum()
    )

    ml4_count = int(
        comparison["ml4_anomaly"].sum()
    )

    lines.append(
        f"NP3 alerts:      {np3_count:,} "
        f"({np3_count / total:.2%})"
    )

    lines.append(
        f"ML3 positive:    {ml3_count:,} "
        f"({ml3_count / total:.2%})"
    )

    lines.append(
        f"ML4 anomalies:   {ml4_count:,} "
        f"({ml4_count / total:.2%})"
    )

    lines.append("")
    lines.append(
        "Exact three-way combinations:"
    )

    combinations = {
        "ALL THREE": (
            comparison["all_three"]
        ),
        "NP3 + ML4 only": (
            comparison["np3_ml4_both"]
        ),
        "NP3 + ML3 only": (
            comparison["np3_ml3_both"]
        ),
        "ML3 + ML4 only": (
            comparison["ml3_ml4_both"]
        ),
        "NP3 ONLY": (
            comparison["np3_only"]
        ),
        "ML3 ONLY": (
            comparison["ml3_only"]
        ),
        "ML4 ONLY": (
            comparison["ml4_only"]
        ),
        "NONE": (
            comparison["none"]
        ),
    }

    for name, mask in combinations.items():

        count = int(
            mask.sum()
        )

        lines.append(
            f"  {name:<18} "
            f"{count:>8,} "
            f"({count / total:.2%})"
        )

    # --------------------------------------------------------
    # Outcome rates
    # --------------------------------------------------------

    lines.append("")
    lines.append(
        "Next-hour HIGH_ACTIVITY outcome rates:"
    )

    for name, mask in [
        (
            "NP3 alert",
            comparison["np3_alert"],
        ),
        (
            "ML3 positive",
            comparison["ml3_positive"],
        ),
        (
            "ML4 anomaly",
            comparison["ml4_anomaly"],
        ),
        (
            "ML4 HIGH",
            comparison["ml4_direction"] == "HIGH",
        ),
        (
            "ML4 LOW",
            comparison["ml4_direction"] == "LOW",
        ),
    ]:

        count = int(
            mask.sum()
        )

        if count == 0:
            rate_text = "n/a"

        else:
            rate = (
                comparison.loc[
                    mask,
                    "target",
                ]
                .mean()
            )

            rate_text = f"{rate:.2%}"

        lines.append(
            f"  {name:<18} "
            f"rows={count:>8,} "
            f"next-hour-high={rate_text}"
        )

    # --------------------------------------------------------
    # ML4 direction counts
    # --------------------------------------------------------

    lines.append("")
    lines.append(
        "ML4 direction:"
    )

    high_count = int(
        (
            comparison["ml4_direction"]
            == "HIGH"
        ).sum()
    )

    low_count = int(
        (
            comparison["ml4_direction"]
            == "LOW"
        ).sum()
    )

    lines.append(
        f"  HIGH: {high_count:,}"
    )

    lines.append(
        f"  LOW:  {low_count:,}"
    )

    return lines


# ============================================================
# 13. Genuine disagreement examples
# ============================================================

def disagreement_examples(
    comparison: pd.DataFrame,
) -> list[str]:

    lines = []

    lines.append("")
    lines.append(
        "=" * 72
    )
    lines.append(
        "GENUINE DISAGREEMENT EXAMPLES"
    )
    lines.append(
        "=" * 72
    )

    # --------------------------------------------------------
    # Example A:
    # ML4 anomaly but ML3 not positive.
    #
    # This demonstrates that anomaly detection and
    # next-hour prediction answer different questions.
    # --------------------------------------------------------

    ml4_not_ml3 = comparison[
        comparison["ml4_anomaly"]
        & ~comparison["ml3_positive"]
    ].copy()

    if not ml4_not_ml3.empty:

        ml4_not_ml3 = (
            ml4_not_ml3
            .sort_values(
                "anomaly_score",
                ascending=False,
            )
            .head(1)
        )

        row = ml4_not_ml3.iloc[0]

        lines.append("")
        lines.append(
            "Example 1: ML4 anomaly, ML3 negative"
        )

        lines.append(
            f"Grid: {int(row['grid_id'])}"
        )

        lines.append(
            f"Timestamp: {row['timestamp']}"
        )

        lines.append(
            f"ML4 direction: {row['ml4_direction']}"
        )

        lines.append(
            f"ML4 score: {row['anomaly_score']:.4f}"
        )

        lines.append(
            f"ML4 baseline: "
            f"{row['baseline_activity']:.4f}"
        )

        lines.append(
            f"Current activity: "
            f"{row['current_activity']:.4f}"
        )

        lines.append(
            f"ML3 probability: "
            f"{row['ml3_probability']:.4f}"
        )

        lines.append(
            f"Next-hour activity: "
            f"{row['next_hour_activity']:.4f}"
        )

        lines.append(
            "Explanation: ML4 identifies unusual behavior "
            "relative to this grid's historical same-hour "
            "pattern, while ML3 predicts whether the NEXT "
            "hour will cross the 2000 activity threshold. "
            "Therefore an ML4 anomaly does not necessarily "
            "imply an ML3 positive prediction."
        )

    # --------------------------------------------------------
    # Example B:
    # ML3 positive but ML4 not anomalous.
    # --------------------------------------------------------

    ml3_not_ml4 = comparison[
        comparison["ml3_positive"]
        & ~comparison["ml4_anomaly"]
    ].copy()

    if not ml3_not_ml4.empty:

        ml3_not_ml4 = (
            ml3_not_ml4
            .sort_values(
                "ml3_probability",
                ascending=False,
            )
            .head(1)
        )

        row = ml3_not_ml4.iloc[0]

        lines.append("")
        lines.append(
            "Example 2: ML3 positive, ML4 not anomalous"
        )

        lines.append(
            f"Grid: {int(row['grid_id'])}"
        )

        lines.append(
            f"Timestamp: {row['timestamp']}"
        )

        lines.append(
            f"ML3 probability: "
            f"{row['ml3_probability']:.4f}"
        )

        lines.append(
            f"ML4 direction: "
            f"{row['ml4_direction']}"
        )

        lines.append(
            f"ML4 score: "
            f"{row['anomaly_score']:.4f}"
        )

        lines.append(
            f"Current activity: "
            f"{row['current_activity']:.4f}"
        )

        lines.append(
            f"ML4 baseline: "
            f"{row['baseline_activity']:.4f}"
        )

        lines.append(
            f"Next-hour activity: "
            f"{row['next_hour_activity']:.4f}"
        )

        lines.append(
            "Explanation: the current hour can be "
            "historically normal while the feature pattern "
            "still gives ML3 a high probability that the "
            "NEXT hour will cross 2000."
        )

    # --------------------------------------------------------
    # Example C:
    # NP3 vs ML4 disagreement.
    # --------------------------------------------------------

    np3_ml4_difference = comparison[
        comparison["np3_alert"]
        != comparison["ml4_anomaly"]
    ].copy()

    if not np3_ml4_difference.empty:

        row = np3_ml4_difference.iloc[0]

        lines.append("")
        lines.append(
            "Example 3: NP3 and ML4 disagree"
        )

        lines.append(
            f"Grid: {int(row['grid_id'])}"
        )

        lines.append(
            f"Timestamp: {row['timestamp']}"
        )

        lines.append(
            f"NP3 alert: "
            f"{bool(row['np3_alert'])}"
        )

        lines.append(
            f"NP3 types: "
            f"{row['np3_alert_types']}"
        )

        lines.append(
            f"ML4 anomaly: "
            f"{bool(row['ml4_anomaly'])}"
        )

        lines.append(
            f"ML4 direction: "
            f"{row['ml4_direction']}"
        )

        lines.append(
            f"ML4 score: "
            f"{row['anomaly_score']:.4f}"
        )

        lines.append(
            "Explanation: NP3 uses its reactive within-grid "
            "baseline/rule system, whereas ML4 compares the "
            "current hour with historical observations for "
            "the same grid AND hour-of-day. A disagreement "
            "is therefore expected when a value is unusual "
            "relative to one baseline but not the other."
        )

    return lines


# ============================================================
# 14. Main validation pipeline
# ============================================================

def main() -> None:

    print(
        "Starting ML4 validation..."
    )

    # --------------------------------------------------------
    # Load data
    # --------------------------------------------------------

    ml4 = load_ml4()

    print(
        f"Loaded ML4 rows: {len(ml4):,}"
    )

    analytics = load_analytics()

    print(
        f"Loaded analytics rows: {len(analytics):,}"
    )

    np3 = load_np3()

    print(
        f"Loaded NP3 alert rows: {len(np3):,}"
    )

    # --------------------------------------------------------
    # ML4 basic validation
    # --------------------------------------------------------

    high_count = int(
        (
            ml4["is_anomaly"]
            & (ml4["direction"] == "HIGH")
        ).sum()
    )

    low_count = int(
        (
            ml4["is_anomaly"]
            & (ml4["direction"] == "LOW")
        ).sum()
    )

    print("")
    print(
        "ML4 validation:"
    )

    print(
        f"  HIGH anomalies: {high_count:,}"
    )

    print(
        f"  LOW anomalies:  {low_count:,}"
    )

    print(
        f"  Minimum history count: "
        f"{ml4['history_count'].min()}"
    )

    print(
        f"  Maximum history count: "
        f"{ml4['history_count'].max()}"
    )

    if high_count < 2:
        raise ValueError(
            "Fewer than 2 HIGH anomalies found."
        )

    if low_count < 2:
        raise ValueError(
            "Fewer than 2 LOW anomalies found."
        )

    if ml4["history_count"].min() < 2:
        raise ValueError(
            "Found an ML4 bucket with fewer than "
            "two historical observations."
        )

    # --------------------------------------------------------
    # Four case inspection
    # --------------------------------------------------------

    cases = select_cases(
        ml4
    )

    case_report = inspect_cases(
        cases,
        analytics,
    )

    # --------------------------------------------------------
    # ML3
    # --------------------------------------------------------

    print("")
    print(
        "Loading ML3 feature data..."
    )

    features = load_ml3_features()

    print(
        f"Loaded ML3 feature rows: "
        f"{len(features):,}"
    )

    ml3_test = build_ml3_test_set(
        features,
        analytics,
    )

    print(
        f"Recreated ML3 test rows: "
        f"{len(ml3_test):,}"
    )

    ml3_predictions = generate_ml3_predictions(
        ml3_test
    )

    print(
        "Generated ML3 predictions."
    )

    # --------------------------------------------------------
    # Three-way comparison
    # --------------------------------------------------------

    comparison = build_three_way_comparison(
        ml4,
        np3,
        ml3_predictions,
    )

    comparison.to_csv(
        OUTPUT_COMPARISON_FILE,
        index=False,
    )

    print(
        f"Three-way comparison exported to: "
        f"{OUTPUT_COMPARISON_FILE}"
    )

    summary_report = comparison_summary(
        comparison
    )

    disagreement_report = disagreement_examples(
        comparison
    )

    # --------------------------------------------------------
    # Final report
    # --------------------------------------------------------

    all_report_lines = (
        case_report
        + summary_report
        + disagreement_report
    )

    OUTPUT_REPORT_FILE.write_text(
        "\n".join(
            all_report_lines
        ),
        encoding="utf-8",
    )

    # --------------------------------------------------------
    # Print important results
    # --------------------------------------------------------

    print(
        "\n".join(summary_report)
    )

    print(
        "\n".join(disagreement_report)
    )

    print("")
    print(
        "=" * 72
    )
    print(
        "ML4 VALIDATION COMPLETE"
    )
    print(
        "=" * 72
    )

    print(
        f"Validation report: "
        f"{OUTPUT_REPORT_FILE}"
    )

    print(
        f"Comparison CSV: "
        f"{OUTPUT_COMPARISON_FILE}"
    )


if __name__ == "__main__":
    main()
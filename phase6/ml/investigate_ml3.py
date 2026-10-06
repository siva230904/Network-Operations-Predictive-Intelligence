from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text


# =========================================================
# Project path
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from phase4.api.db.database import engine


# =========================================================
# Configuration
# =========================================================

TARGET_THRESHOLD = 2000.0

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]


# =========================================================
# Load ML3 data
# =========================================================

def load_data() -> pd.DataFrame:
    """
    Load the same ML3 dataset used by train_model.py.

    Features are measured at t.
    Target is measured at t+1.
    """

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
                DATE_ADD(
                    mf.feature_timestamp,
                    INTERVAL 1 HOUR
                )

        INNER JOIN fact_network_activity f
            ON f.grid_key = dg.grid_key
            AND f.time_key = future_time.time_key

        ORDER BY
            mf.feature_timestamp,
            mf.grid_id
        """
    )

    print("Loading ML3 dataset...")

    with engine.connect() as connection:
        dataframe = pd.read_sql(
            query,
            connection,
            params={
                "threshold": TARGET_THRESHOLD,
            },
        )

    if dataframe.empty:
        raise RuntimeError(
            "No ML3 rows were returned."
        )

    return dataframe


# =========================================================
# Feature vs target investigation
# =========================================================

def investigate_feature_relationships(
    dataframe: pd.DataFrame,
):
    print("\n" + "=" * 60)
    print("ML3 LEAKAGE INVESTIGATION")
    print("=" * 60)

    print(
        f"\nUsable rows: {len(dataframe):,}"
    )

    print(
        f"Positive target rows: "
        f"{int(dataframe['high_activity_target'].sum()):,}"
    )

    print(
        f"Negative target rows: "
        f"{int((dataframe['high_activity_target'] == 0).sum()):,}"
    )

    print(
        f"Base rate: "
        f"{dataframe['high_activity_target'].mean():.4%}"
    )

    # -----------------------------------------------------
    # Compare feature distributions by target
    # -----------------------------------------------------

    print("\nFeature averages by target:")

    grouped = (
        dataframe
        .groupby("high_activity_target")[FEATURE_COLUMNS]
        .mean()
        .T
    )

    grouped.columns = [
        "Not High Activity",
        "High Activity",
    ]

    print(
        grouped.to_string()
    )

    # -----------------------------------------------------
    # Ratio of positive-target mean to negative-target mean
    # -----------------------------------------------------

    print(
        "\nPositive-target / negative-target mean ratio:"
    )

    negative_means = (
        dataframe[
            dataframe["high_activity_target"] == 0
        ][FEATURE_COLUMNS]
        .mean()
    )

    positive_means = (
        dataframe[
            dataframe["high_activity_target"] == 1
        ][FEATURE_COLUMNS]
        .mean()
    )

    ratios = (
        positive_means / negative_means
    )

    for feature in FEATURE_COLUMNS:
        print(
            f"  {feature:<20} "
            f"{ratios[feature]:.4f}"
        )

    # -----------------------------------------------------
    # Correlation with target
    # -----------------------------------------------------

    print(
        "\nPoint-biserial-style Pearson correlation "
        "with target:"
    )

    correlations = (
        dataframe[
            FEATURE_COLUMNS + ["high_activity_target"]
        ]
        .corr()["high_activity_target"]
        .drop("high_activity_target")
        .sort_values(
            ascending=False,
            key=lambda series: series.abs(),
        )
    )

    for feature, correlation in correlations.items():
        print(
            f"  {feature:<20} "
            f"{correlation:+.6f}"
        )

    # -----------------------------------------------------
    # Specifically inspect avg_activity
    # -----------------------------------------------------

    print(
        "\n" + "-" * 60
    )

    print(
        "avg_activity distribution by target:"
    )

    for target_value, label in [
        (0, "Not High Activity"),
        (1, "High Activity"),
    ]:

        subset = dataframe[
            dataframe["high_activity_target"]
            == target_value
        ]["avg_activity"]

        print(
            f"\n{label}:"
        )

        print(
            f"  count:  {len(subset):,}"
        )

        print(
            f"  mean:   {subset.mean():.4f}"
        )

        print(
            f"  median: {subset.median():.4f}"
        )

        print(
            f"  p90:    {subset.quantile(0.90):.4f}"
        )

        print(
            f"  p95:    {subset.quantile(0.95):.4f}"
        )

        print(
            f"  p99:    {subset.quantile(0.99):.4f}"
        )

        print(
            f"  max:    {subset.max():.4f}"
        )

    # -----------------------------------------------------
    # Check how many positives already have high
    # current-hour average activity
    # -----------------------------------------------------

    print(
        "\n" + "-" * 60
    )

    print(
        "Current feature vs next-hour target:"
    )

    thresholds = [
        500,
        1000,
        1500,
        2000,
        3000,
        5000,
    ]

    for threshold in thresholds:

        high_current = (
            dataframe["avg_activity"]
            >= threshold
        )

        positive_target = (
            dataframe["high_activity_target"]
            == 1
        )

        total_current_high = int(
            high_current.sum()
        )

        target_positive_with_high_current = int(
            (
                high_current
                & positive_target
            ).sum()
        )

        if total_current_high > 0:
            conditional_rate = (
                target_positive_with_high_current
                / total_current_high
            )
        else:
            conditional_rate = 0.0

        print(
            f"  avg_activity >= {threshold:>4}: "
            f"{total_current_high:>9,} rows, "
            f"next-hour positive rate = "
            f"{conditional_rate:.4%}"
        )


# =========================================================
# Main
# =========================================================

def main():

    dataframe = load_data()

    investigate_feature_relationships(
        dataframe
    )

    print(
        "\n" + "=" * 60
    )

    print(
        "Investigation completed."
    )


if __name__ == "__main__":
    main()
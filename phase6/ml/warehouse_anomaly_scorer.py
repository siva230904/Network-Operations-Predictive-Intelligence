from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker


# =========================================================
# Project path
# =========================================================

PROJECT_ROOT = Path(
    os.getenv(
        "PROJECT_ROOT",
        Path(__file__).resolve().parents[2],
    )
).resolve()

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# =========================================================
# Import project models
# =========================================================

from phase4.api.db.models import (
    DimGrid,
    DimTime,
    FactNetworkActivity,
)


# =========================================================
# Import the exact shared NP3 baseline implementation
# =========================================================

NP3_DIR = PROJECT_ROOT / "phase1" / "np3"

if str(NP3_DIR) not in sys.path:
    sys.path.insert(0, str(NP3_DIR))

from alert_detector import NetworkAlertGenerator


# =========================================================
# ML6 database configuration
# =========================================================

DATABASE_URL = os.getenv("ML6_DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError(
        "ML6_DATABASE_URL environment variable is required."
    )

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)


# =========================================================
# Configuration
# =========================================================

ANOMALY_THRESHOLD = 0.5
MIN_HISTORY_DAYS = 2


logger = logging.getLogger(__name__)


# =========================================================
# Load warehouse activity
# =========================================================

def load_activity_data(session) -> pd.DataFrame:
    """
    Load the exact warehouse activity fields required by ML4.

    ML4 baseline is calculated from:

        grid_id
        timestamp
        total_activity

    No ML2 feature approximation is used.
    """

    statement = (
        select(
            DimGrid.grid_id,
            DimTime.timestamp,
            FactNetworkActivity.total_activity,
        )
        .join(
            FactNetworkActivity,
            FactNetworkActivity.grid_key
            == DimGrid.grid_key,
        )
        .join(
            DimTime,
            DimTime.time_key
            == FactNetworkActivity.time_key,
        )
        .order_by(
            DimGrid.grid_id,
            DimTime.timestamp,
        )
    )

    rows = session.execute(statement).all()

    if not rows:
        raise RuntimeError(
            "No warehouse network activity was found."
        )

    dataframe = pd.DataFrame(
        rows,
        columns=[
            "grid_id",
            "timestamp",
            "total_activity",
        ],
    )

    logger.info(
        "Loaded %s warehouse activity rows.",
        f"{len(dataframe):,}",
    )

    return dataframe


# =========================================================
# Validate warehouse activity
# =========================================================

def validate_input(
    dataframe: pd.DataFrame,
) -> None:

    required_columns = {
        "grid_id",
        "timestamp",
        "total_activity",
    }

    missing = (
        required_columns
        - set(dataframe.columns)
    )

    if missing:
        raise ValueError(
            "Warehouse activity is missing required "
            f"columns: {sorted(missing)}"
        )

    dataframe["timestamp"] = pd.to_datetime(
        dataframe["timestamp"],
        errors="coerce",
    )

    if dataframe["timestamp"].isna().any():
        raise ValueError(
            "Invalid timestamp values found."
        )

    dataframe["total_activity"] = pd.to_numeric(
        dataframe["total_activity"],
        errors="coerce",
    )

    if dataframe["total_activity"].isna().any():
        raise ValueError(
            "Invalid total_activity values found."
        )

    if (
        dataframe["total_activity"] < 0
    ).any():
        raise ValueError(
            "Negative total_activity values found."
        )

    duplicate_count = int(
        dataframe.duplicated(
            subset=[
                "grid_id",
                "timestamp",
            ]
        ).sum()
    )

    if duplicate_count:
        raise ValueError(
            "Warehouse activity contains "
            f"{duplicate_count:,} duplicate grid/timestamp rows."
        )

    logger.info(
        "Warehouse ML4 input validation passed."
    )


# =========================================================
# Build exact ML4 baseline
# =========================================================

def build_baseline(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Reproduce the exact ML4 baseline methodology.

    Bucket:

        grid_id + hour_of_day

    Baseline:

        leave-one-out median

    The calculation itself is delegated to the shared
    NP3 NetworkAlertGenerator implementation.
    """

    df = dataframe.copy()

    df["hour_of_day"] = (
        df["timestamp"].dt.hour
    )

    generator = NetworkAlertGenerator(
        dataframe=df
    )

    generator.load_data()

    result = generator.build_baseline(
        bucket_columns=[
            "grid_id",
            "hour_of_day",
        ]
    )

    bucket_counts = (
        result.groupby(
            [
                "grid_id",
                "hour_of_day",
            ],
            sort=False,
        )["total_activity"]
        .transform("count")
    )

    result["history_count"] = bucket_counts

    logger.info(
        "Built exact ML4 leave-one-out median "
        "baseline using grid_id + hour_of_day."
    )

    return result


# =========================================================
# Calculate ML4 anomalies
# =========================================================

def calculate_anomalies(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Reproduce the exact ML4 anomaly calculation.
    """

    df = build_baseline(
        dataframe
    )

    eligible = (
        (df["history_count"] > MIN_HISTORY_DAYS - 1)
        & (df["baseline_activity"] > 0)
    )

    df["deviation"] = (
        df["total_activity"]
        - df["baseline_activity"]
    )

    df["anomaly_score"] = np.nan

    df.loc[
        eligible,
        "anomaly_score",
    ] = (
        df.loc[
            eligible,
            "deviation",
        ].abs()
        / df.loc[
            eligible,
            "baseline_activity",
        ]
    )

    df["direction"] = pd.Series(
        pd.NA,
        index=df.index,
        dtype="string",
    )

    df.loc[
        eligible
        & (df["deviation"] > 0),
        "direction",
    ] = "HIGH"

    df.loc[
        eligible
        & (df["deviation"] < 0),
        "direction",
    ] = "LOW"

    df.loc[
        eligible
        & (df["deviation"] == 0),
        "direction",
    ] = "NONE"

    df["is_anomaly"] = (
        eligible
        & (
            df["anomaly_score"]
            >= ANOMALY_THRESHOLD
        )
        & df["direction"].isin(
            ["HIGH", "LOW"]
        )
    )

    df["reason"] = ""

    anomalous = df["is_anomaly"]

    df.loc[
        anomalous,
        "reason",
    ] = (
        df.loc[
            anomalous,
            "direction",
        ]
        + " anomaly: activity is "
        + (
            df.loc[
                anomalous,
                "anomaly_score",
            ]
            * 100
        ).round(1).astype(str)
        + "% away from the historical median "
          "for this grid and hour-of-day."
    )

    normal = (
        eligible
        & ~anomalous
    )

    df.loc[
        normal,
        "reason",
    ] = (
        "Within expected historical range for "
        "this grid and hour-of-day."
    )

    insufficient_history = (
        df["history_count"]
        <= MIN_HISTORY_DAYS - 1
    )

    df.loc[
        insufficient_history,
        "reason",
    ] = (
        "Insufficient historical observations "
        "for hour-of-day baseline."
    )

    zero_baseline = (
        eligible.eq(False)
        & (
            df["history_count"]
            > MIN_HISTORY_DAYS - 1
        )
        & (
            df["baseline_activity"]
            <= 0
        )
    )

    df.loc[
        zero_baseline,
        "reason",
    ] = (
        "Historical baseline is zero; percentage "
        "deviation cannot be calculated."
    )

    return df


# =========================================================
# Create ML6 output
# =========================================================

def create_output(
    dataframe: pd.DataFrame,
) -> pd.DataFrame:

    output_columns = [
        "grid_id",
        "timestamp",
        "hour_of_day",
        "total_activity",
        "baseline_activity",
        "history_count",
        "deviation",
        "anomaly_score",
        "direction",
        "is_anomaly",
        "reason",
    ]

    output = dataframe[
        output_columns
    ].copy()

    output = output.rename(
        columns={
            "total_activity": "current_activity",
        }
    )

    return output


# =========================================================
# Summary
# =========================================================

def print_summary(
    output: pd.DataFrame,
) -> None:

    total_rows = len(output)

    anomaly_rows = output[
        output["is_anomaly"]
    ]

    high_count = int(
        (
            anomaly_rows["direction"]
            == "HIGH"
        ).sum()
    )

    low_count = int(
        (
            anomaly_rows["direction"]
            == "LOW"
        ).sum()
    )

    high_direction = int(
        (
            output["direction"]
            == "HIGH"
        ).sum()
    )

    low_direction = int(
        (
            output["direction"]
            == "LOW"
        ).sum()
    )

    print()
    print("=" * 60)
    print("ML6 ML4 WAREHOUSE ANOMALY SUMMARY")
    print("=" * 60)

    print(
        f"Total grid/hours: {total_rows:,}"
    )

    print(
        f"Anomalies: {len(anomaly_rows):,}"
    )

    print(
        f"Anomaly proportion: "
        f"{len(anomaly_rows) / total_rows:.2%}"
    )

    print()
    print("Anomalies by direction:")

    print(
        f"  HIGH: {high_count:,}"
    )

    print(
        f"  LOW:  {low_count:,}"
    )

    print()
    print("All eligible directions:")

    print(
        f"  HIGH: {high_direction:,}"
    )

    print(
        f"  LOW:  {low_direction:,}"
    )

    print()
    print("Baseline configuration:")

    print(
        f"  Threshold: {ANOMALY_THRESHOLD:.2f}"
    )

    print(
        f"  Minimum history days: "
        f"{MIN_HISTORY_DAYS}"
    )

    print("=" * 60)


# =========================================================
# Main workflow
# =========================================================

def run() -> pd.DataFrame:
    """
    Run ML6 warehouse-based ML4 anomaly scoring.

    IMPORTANT:
        This function does NOT modify network_risk_scores.
        It is a validation/scoring stage only.
    """

    logger.info(
        "Starting ML6 warehouse ML4 anomaly scoring."
    )

    session = SessionLocal()

    try:
        logger.info(
            "Loading warehouse activity from MySQL."
        )

        dataframe = load_activity_data(
            session
        )

        validate_input(
            dataframe
        )

    finally:
        session.close()

    logger.info(
        "Calculating exact ML4 anomaly scores."
    )

    scored = calculate_anomalies(
        dataframe
    )

    output = create_output(
        scored
    )

    print_summary(
        output
    )

    logger.info(
        "ML6 warehouse ML4 anomaly scoring completed."
    )

    return output


# =========================================================
# CLI
# =========================================================

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        ),
    )

    result = run()

    print()
    print(
        f"Rows scored: {len(result):,}"
    )
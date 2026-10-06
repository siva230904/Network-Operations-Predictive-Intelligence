from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------
# Import the shared NP3 baseline implementation.
# ---------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
NP3_DIR = PROJECT_ROOT / "phase1" / "np3"

if str(NP3_DIR) not in sys.path:
    sys.path.insert(0, str(NP3_DIR))

from alert_detector import NetworkAlertGenerator


logger = logging.getLogger(__name__)


class NetworkAnomalyScorer:
    """
    ML4 anomaly scorer.

    Uses the shared NP3 leave-one-out baseline implementation, but
    buckets observations by:

        grid_id + hour_of_day

    This means a grid is compared with its own historical behavior
    during the same hour of the day.

    Example:
        Grid 123 at 03:00 is compared against Grid 123's historical
        03:00 observations, rather than against all hours for Grid 123.
    """

    def __init__(
        self,
        file_path: str | Path,
        anomaly_threshold: float = 1.0,
        min_history_days: int = 2,
        log_dir: str | Path = "data/logs",
    ) -> None:
        self.file_path = Path(file_path)
        self.anomaly_threshold = anomaly_threshold
        self.min_history_days = min_history_days
        self.log_dir = Path(log_dir)

        self.data: pd.DataFrame | None = None
        self.anomaly_scores: pd.DataFrame | None = None

    # -----------------------------------------------------------------
    # Loading
    # -----------------------------------------------------------------

    def load_data(self) -> pd.DataFrame:
        logger.info("Loading analytics file: %s", self.file_path)

        df = pd.read_csv(self.file_path)

        logger.info("Loaded %s analytics rows.", f"{len(df):,}")

        self.data = df
        return df

    # -----------------------------------------------------------------
    # Validation
    # -----------------------------------------------------------------

    def validate_input(self) -> None:
        if self.data is None:
            raise RuntimeError("Data has not been loaded.")

        required_columns = {
            "grid_id",
            "timestamp",
            "total_activity",
        }

        missing = required_columns - set(self.data.columns)

        if missing:
            raise ValueError(
                f"Missing required columns: {sorted(missing)}"
            )

        self.data["timestamp"] = pd.to_datetime(
            self.data["timestamp"],
            errors="coerce",
        )

        if self.data["timestamp"].isna().any():
            raise ValueError("Invalid timestamp values found.")

        self.data["total_activity"] = pd.to_numeric(
            self.data["total_activity"],
            errors="coerce",
        )

        if self.data["total_activity"].isna().any():
            raise ValueError(
                "Invalid total_activity values found."
            )

        if (self.data["total_activity"] < 0).any():
            raise ValueError(
                "Negative total_activity values found."
            )

        logger.info("Input validation passed.")

    # -----------------------------------------------------------------
    # Baseline
    # -----------------------------------------------------------------

    def build_hour_of_day_baseline(self) -> pd.DataFrame:
        """
        Build a leave-one-out median baseline using:

            grid_id + hour_of_day

        The actual baseline calculation is delegated to the shared
        NP3 implementation.

        This ensures NP3 and ML4 use one baseline implementation.
        """

        if self.data is None:
            raise RuntimeError("Data has not been loaded.")

        df = self.data.copy()

        df["hour_of_day"] = df["timestamp"].dt.hour

        # Reuse the exact shared baseline implementation from NP3.
        generator = NetworkAlertGenerator(
            dataframe=df
        )

        # Populate the shared generator's analytics_data
        # using its normal loading path.
        generator.load_data()

        df = generator.build_baseline(
            bucket_columns=["grid_id", "hour_of_day"]
        )

        # Count observations in each grid/hour bucket.
        bucket_counts = (
            df.groupby(
                ["grid_id", "hour_of_day"],
                sort=False,
            )["total_activity"]
            .transform("count")
        )

        df["history_count"] = bucket_counts

        logger.info(
            "Built hour-of-day leave-one-out median baseline "
            "using buckets: ['grid_id', 'hour_of_day']"
        )

        return df

    # -----------------------------------------------------------------
    # Anomaly calculation
    # -----------------------------------------------------------------

    def calculate_anomalies(self) -> pd.DataFrame:
        """
        Calculate percentage-deviation anomaly scores.

        deviation:
            current_activity - baseline_activity

        anomaly_score:
            abs(deviation) / baseline_activity

        direction:
            HIGH if current > baseline
            LOW  if current < baseline
        """

        df = self.build_hour_of_day_baseline()

        # A bucket must contain at least two observations so that the
        # current observation has historical data to compare against.
        eligible = (
            (df["history_count"] > self.min_history_days - 1)
            & (df["baseline_activity"] > 0)
        )

        df["deviation"] = (
            df["total_activity"] - df["baseline_activity"]
        )

        df["anomaly_score"] = np.nan

        df.loc[eligible, "anomaly_score"] = (
            df.loc[eligible, "deviation"].abs()
            / df.loc[eligible, "baseline_activity"]
        )

        df["direction"] = pd.Series(
            pd.NA,
            index=df.index,
            dtype="string",
        )

        df.loc[
            eligible & (df["deviation"] > 0),
            "direction",
        ] = "HIGH"

        df.loc[
            eligible & (df["deviation"] < 0),
            "direction",
        ] = "LOW"

        # Exact equality is not anomalous.
        df.loc[
            eligible & (df["deviation"] == 0),
            "direction",
        ] = "NONE"

        df["is_anomaly"] = (
            eligible
            & (df["anomaly_score"] >= self.anomaly_threshold)
            & (df["direction"].isin(["HIGH", "LOW"]))
        )

        # Human-readable explanation.
        df["reason"] = ""

        anomalous = df["is_anomaly"]

        df.loc[
            anomalous,
            "reason",
        ] = (
            df.loc[anomalous, "direction"]
            + " anomaly: activity is "
            + (
                df.loc[anomalous, "anomaly_score"] * 100
            ).round(1).astype(str)
            + "% away from the historical median for this grid "
              "and hour-of-day."
        )

        # Non-anomalous rows get an explicit explanation too.
        normal = eligible & ~anomalous

        df.loc[
            normal,
            "reason",
        ] = (
            "Within expected historical range for this grid "
            "and hour-of-day."
        )

        # Insufficient-history rows.
        insufficient_history = (
            df["history_count"] <= self.min_history_days - 1
        )

        df.loc[
            insufficient_history,
            "reason",
        ] = (
            "Insufficient historical observations for "
            "hour-of-day baseline."
        )

        # Zero-baseline rows.
        zero_baseline = (
            eligible.eq(False)
            & (df["history_count"] > self.min_history_days - 1)
            & (df["baseline_activity"] <= 0)
        )

        df.loc[
            zero_baseline,
            "reason",
        ] = (
            "Historical baseline is zero; percentage deviation "
            "cannot be calculated."
        )

        self.anomaly_scores = df

        return df

    # -----------------------------------------------------------------
    # Output preparation
    # -----------------------------------------------------------------

    def create_output(self) -> pd.DataFrame:
        if self.anomaly_scores is None:
            raise RuntimeError(
                "Anomalies have not been calculated."
            )

        df = self.anomaly_scores.copy()

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

        output = df[output_columns].copy()

        output = output.rename(
            columns={
                "total_activity": "current_activity",
            }
        )

        return output

    # -----------------------------------------------------------------
    # Export
    # -----------------------------------------------------------------

    def export_scores(
        self,
        output_path: str | Path,
    ) -> pd.DataFrame:
        output = self.create_output()

        output_path = Path(output_path)
        output_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        output.to_csv(
            output_path,
            index=False,
        )

        logger.info(
            "Anomaly score file exported to: %s",
            output_path,
        )

        return output

    # -----------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------

    def create_summary(
        self,
        output: pd.DataFrame,
    ) -> dict:
        total_rows = len(output)

        anomaly_rows = output[
            output["is_anomaly"]
        ]

        high_count = int(
            (anomaly_rows["direction"] == "HIGH").sum()
        )

        low_count = int(
            (anomaly_rows["direction"] == "LOW").sum()
        )

        high_direction = int(
            (output["direction"] == "HIGH").sum()
        )

        low_direction = int(
            (output["direction"] == "LOW").sum()
        )

        history_counts = output[
            "history_count"
        ].dropna()

        summary = {
            "total_grid_hours": total_rows,
            "anomalies": len(anomaly_rows),
            "anomaly_proportion": (
                len(anomaly_rows) / total_rows
                if total_rows
                else 0
            ),
            "high_anomalies": high_count,
            "low_anomalies": low_count,
            "high_direction_rows": high_direction,
            "low_direction_rows": low_direction,
            "anomaly_threshold": self.anomaly_threshold,
            "min_history_days": self.min_history_days,
            "min_bucket_count": (
                int(history_counts.min())
                if len(history_counts)
                else 0
            ),
            "max_bucket_count": (
                int(history_counts.max())
                if len(history_counts)
                else 0
            ),
        }

        return summary

    def print_summary(
        self,
        summary: dict,
    ) -> None:
        print()
        print("=" * 60)
        print("NETWORK ANOMALY SCORE SUMMARY")
        print("=" * 60)

        print(
            f"Total grid/hours: "
            f"{summary['total_grid_hours']:,}"
        )

        print(
            f"Anomalies: "
            f"{summary['anomalies']:,}"
        )

        print(
            f"Anomaly proportion: "
            f"{summary['anomaly_proportion']:.2%}"
        )

        print(
            f"Anomaly threshold: "
            f"{summary['anomaly_threshold']:.2f}"
        )

        print()
        print("Anomalies by direction:")

        print(
            f"  HIGH: "
            f"{summary['high_anomalies']:,}"
        )

        print(
            f"  LOW:  "
            f"{summary['low_anomalies']:,}"
        )

        print()
        print("Baseline bucket counts:")

        print(
            f"  Minimum: "
            f"{summary['min_bucket_count']}"
        )

        print(
            f"  Maximum: "
            f"{summary['max_bucket_count']}"
        )

        print("=" * 60)

    # -----------------------------------------------------------------
    # Pipeline
    # -----------------------------------------------------------------

    def process(
        self,
        output_path: str | Path,
    ) -> pd.DataFrame:

        logger.info(
            "Starting ML4 network anomaly pipeline."
        )

        self.load_data()
        self.validate_input()

        self.calculate_anomalies()

        output = self.export_scores(
            output_path
        )

        summary = self.create_summary(
            output
        )

        self.print_summary(summary)

        logger.info(
            "ML4 network anomaly pipeline completed successfully."
        )

        return output


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        ),
    )


if __name__ == "__main__":
    configure_logging()

    project_root = Path(__file__).resolve().parents[2]

    analytics_file = (
        project_root
        / "data"
        / "landing"
        / "hourly_grid_summary.csv"
    )

    output_file = (
        project_root
        / "data"
        / "landing"
        / "network_anomaly_scores.csv"
    )

    scorer = NetworkAnomalyScorer(
        file_path=analytics_file,
        anomaly_threshold=0.5,
        min_history_days=2,
    )

    scorer.process(
        output_path=output_file
    )
import logging
from pathlib import Path

import pandas as pd


# =========================================================
# Logging
# =========================================================

LOG_DIR = Path("../../data/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "usage_processor.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, mode="a"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


# =========================================================
# UsageProcessor
# =========================================================

class UsageProcessor:
    """
    Reusable processor for one daily Milan mobile-activity file.

    Pipeline:

        load
          ↓
        clean
          ↓
        derive time features
          ↓
        aggregate to grid/hour
          ↓
        derive activity features
          ↓
        compute KPIs
          ↓
        export summaries
    """

    # -----------------------------------------------------
    # Project schema
    # -----------------------------------------------------

    RAW_TO_CANONICAL = {
        "datetime": "timestamp",
        "CellID": "grid_id",
        "countrycode": "country_code",
        "smsin": "sms_in",
        "smsout": "sms_out",
        "callin": "call_in",
        "callout": "call_out",
        "internet": "internet_activity",
    }

    ACTIVITY_COLUMNS = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
    ]

    REQUIRED_RAW_COLUMNS = list(RAW_TO_CANONICAL.keys())

    REQUIRED_CANONICAL_COLUMNS = [
        "timestamp",
        "grid_id",
        "country_code",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
    ]

    # -----------------------------------------------------
    # Constructor
    # -----------------------------------------------------

    def __init__(self, file_path=None, dataframe=None):

        if file_path is None and dataframe is None:
            raise ValueError(
                "Provide either file_path or dataframe."
            )

        if file_path is not None and dataframe is not None:
            raise ValueError(
                "Provide either file_path or dataframe, not both."
            )

        self.file_path = file_path

        if dataframe is not None:
            self.dataframe = dataframe.copy()
        else:
            self.dataframe = None

        # Runtime state
        self.raw_data = None
        self.canonical_data = None
        self.analytics_data = None
        self.daily_summary = None
        self.grid_summary = None

        # Data-quality metrics
        self.nulls_handled = 0
        self.dropped_rows = 0

    # =====================================================
    # Internal state reset
    # =====================================================

    def _reset_state(self):
        """Reset all per-run processing state."""

        self.raw_data = None
        self.canonical_data = None
        self.analytics_data = None
        self.daily_summary = None
        self.grid_summary = None

        self.nulls_handled = 0
        self.dropped_rows = 0

    # =====================================================
    # 1. Load data
    # =====================================================

    def load_data(self):
        """
        Load the CSV or use the supplied DataFrame.

        The raw input is never modified.
        """

        if self.file_path is not None:

            path = Path(self.file_path)

            logger.info(
                "Loading file: %s",
                path
            )

            if not path.exists():
                raise FileNotFoundError(
                    f"File not found: {path}"
                )

            if path.suffix.lower() != ".csv":
                raise ValueError(
                    "UsageProcessor expects a CSV file."
                )

            self.raw_data = pd.read_csv(path)

        else:

            logger.info(
                "Using supplied DataFrame."
            )

            self.raw_data = self.dataframe.copy()

        logger.info(
            "Loaded %d rows and %d columns.",
            len(self.raw_data),
            len(self.raw_data.columns)
        )

        # Check required raw columns
        missing_columns = [
            column
            for column in self.REQUIRED_RAW_COLUMNS
            if column not in self.raw_data.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {missing_columns}"
            )

        # Create canonical copy.
        # raw_data remains untouched.
        self.canonical_data = (
            self.raw_data
            .rename(columns=self.RAW_TO_CANONICAL)
            .copy()
        )

        return self.canonical_data

    # =====================================================
    # 2. Clean data
    # =====================================================

    def clean_data(self):
        """
        Apply the curated-layer data-quality rules.

        Rules:
        - timestamp must exist
        - grid_id must exist
        - grid_id must be 1-10000
        - activity values cannot be negative
        - blank activity values become zero
        """

        if self.canonical_data is None:
            raise RuntimeError(
                "Run load_data() before clean_data()."
            )

        df = self.canonical_data.copy()

        # -------------------------------------------------
        # Timestamp
        # -------------------------------------------------

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce"
        )

        # -------------------------------------------------
        # Missing key fields
        # -------------------------------------------------

        missing_timestamp = df["timestamp"].isna()
        missing_grid = df["grid_id"].isna()

        missing_key_rows = (
            missing_timestamp |
            missing_grid
        )

        missing_key_count = int(
            missing_key_rows.sum()
        )

        if missing_key_count > 0:

            logger.error(
                "Found %d rows with missing timestamp/grid_id.",
                missing_key_count
            )

            self.dropped_rows += missing_key_count

            raise ValueError(
                "Missing timestamp or grid_id detected."
            )

        # -------------------------------------------------
        # Grid ID validation
        # -------------------------------------------------

        invalid_grid = df[
            (df["grid_id"] < 1) |
            (df["grid_id"] > 10000)
        ]

        if len(invalid_grid) > 0:

            logger.error(
                "Found %d invalid grid_id values.",
                len(invalid_grid)
            )

            raise ValueError(
                "grid_id contains values outside 1-10000."
            )

        # -------------------------------------------------
        # Negative activity validation
        # -------------------------------------------------

        for column in self.ACTIVITY_COLUMNS:

            negative_count = int(
                (df[column] < 0).sum()
            )

            if negative_count > 0:

                logger.error(
                    "%s contains %d negative values.",
                    column,
                    negative_count
                )

                raise ValueError(
                    f"{column} contains "
                    f"{negative_count} negative values."
                )

        # -------------------------------------------------
        # Curated null-to-zero rule
        # -------------------------------------------------

        null_count = int(
            df[self.ACTIVITY_COLUMNS]
            .isna()
            .sum()
            .sum()
        )

        self.nulls_handled = null_count

        if null_count > 0:

            logger.info(
                "Handling %d null activity values "
                "using the curated null-to-zero rule.",
                null_count
            )

            df[self.ACTIVITY_COLUMNS] = (
                df[self.ACTIVITY_COLUMNS]
                .fillna(0)
            )

        self.canonical_data = df

        logger.info(
            "Cleaning complete. "
            "Rows retained: %d. "
            "Rows dropped: %d.",
            len(df),
            self.dropped_rows
        )

        return self.canonical_data

    # =====================================================
    # 3. Derive time features
    # =====================================================

    def derive_time_features(self):
        """Derive date, hour and day_of_week."""

        if self.canonical_data is None:
            raise RuntimeError(
                "Run load_data() and clean_data() first."
            )

        df = self.canonical_data.copy()

        df["date"] = (
            df["timestamp"].dt.date
        )

        df["hour"] = (
            df["timestamp"].dt.hour
        )

        df["day_of_week"] = (
            df["timestamp"].dt.dayofweek
        )

        self.canonical_data = df

        logger.info(
            "Derived time features."
        )

        return self.canonical_data

    # =====================================================
    # 4. Aggregate to grid/hour
    # =====================================================

    def aggregate_to_grid_time(self):
        """
        Aggregate country-code rows to:

            timestamp + grid_id

        country_code is deliberately excluded from
        the operational analytics layer.
        """

        if self.canonical_data is None:
            raise RuntimeError(
                "Run the loading and cleaning steps first."
            )

        df = self.canonical_data

        self.analytics_data = (
            df.groupby(
                ["timestamp", "grid_id"],
                as_index=False
            )[self.ACTIVITY_COLUMNS]
            .sum()
        )

        logger.info(
            "Aggregated to grid/hour grain: %d rows.",
            len(self.analytics_data)
        )

        # Defensive duplicate check
        duplicates = self.analytics_data.duplicated(
            subset=["timestamp", "grid_id"]
        ).sum()

        if duplicates > 0:

            raise ValueError(
                "Aggregation produced duplicate "
                "(timestamp, grid_id) records."
            )

        return self.analytics_data

    # =====================================================
    # 5. Derive activity features
    # =====================================================

    def derive_activity_features(self):
        """
        Create project-defined activity measures.
        """

        if self.analytics_data is None:
            raise RuntimeError(
                "Run aggregate_to_grid_time() first."
            )

        df = self.analytics_data.copy()

        df["total_sms"] = (
            df["sms_in"] +
            df["sms_out"]
        )

        df["total_calls"] = (
            df["call_in"] +
            df["call_out"]
        )

        df["total_activity"] = (
            df["total_sms"] +
            df["total_calls"] +
            df["internet_activity"]
        )

        self.analytics_data = df

        logger.info(
            "Derived activity features."
        )

        return self.analytics_data

    # =====================================================
    # 6. Compute KPIs
    # =====================================================

    def compute_kpis(self):
        """
        Create daily and grid-level summary tables.

        These are calculated from the grid/hour analytics
        layer, not the country-code raw/canonical layer.
        """

        if self.analytics_data is None:
            raise RuntimeError(
                "Run derive_activity_features() first."
            )

        df = self.analytics_data.copy()

        # -------------------------------------------------
        # Daily summary
        # -------------------------------------------------

        daily = (
            df.assign(
                date=df["timestamp"].dt.date
            )
            .groupby("date")
            .agg(
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                internet_activity=(
                    "internet_activity",
                    "sum"
                ),
                total_activity=(
                    "total_activity",
                    "sum"
                ),
                active_grid_hours=(
                    "grid_id",
                    "count"
                )
            )
            .reset_index()
        )

        self.daily_summary = daily

        # -------------------------------------------------
        # Grid summary
        # -------------------------------------------------

        grid = (
            df.groupby("grid_id")
            .agg(
                total_sms=("total_sms", "sum"),
                total_calls=("total_calls", "sum"),
                internet_activity=(
                    "internet_activity",
                    "sum"
                ),
                total_activity=(
                    "total_activity",
                    "sum"
                ),
                active_hours=(
                    "timestamp",
                    "nunique"
                )
            )
            .reset_index()
        )

        self.grid_summary = grid

        logger.info(
            "Computed daily summary: %d rows.",
            len(self.daily_summary)
        )

        logger.info(
            "Computed grid summary: %d rows.",
            len(self.grid_summary)
        )

        return (
            self.daily_summary,
            self.grid_summary
        )

    # =====================================================
    # 7. Export summaries
    # =====================================================

    def export_summary(self, output_dir="data/processed"):
        """
        Export:
            - hourly grid analytics
            - daily summary
            - grid summary

        Output filenames are based on the input filename,
        preventing different daily files from overwriting
        one another.
        """

        if self.analytics_data is None:
            raise RuntimeError(
                "Run derive_activity_features() before "
                "export_summary()."
            )

        if (
            self.daily_summary is None
            or self.grid_summary is None
        ):
            raise RuntimeError(
                "Run compute_kpis() before export_summary()."
            )

        output_path = Path(output_dir)

        output_path.mkdir(
            parents=True,
            exist_ok=True
        )

        # -------------------------------------------------
        # Determine safe output prefix
        # -------------------------------------------------

        if self.file_path is not None:

            input_name = Path(
                self.file_path
            ).stem

        else:

            input_name = "dataframe_input"

        # -------------------------------------------------
        # Output paths
        # -------------------------------------------------

        analytics_path = (
            output_path /
            f"{input_name}_hourly_grid_summary.csv"
        )

        daily_path = (
            output_path /
            f"{input_name}_daily_summary.csv"
        )

        grid_path = (
            output_path /
            f"{input_name}_grid_summary.csv"
        )

        # -------------------------------------------------
        # Write hourly grid analytics
        # -------------------------------------------------

        analytics_export = self.analytics_data.copy()

        analytics_export["timestamp"] = (
            analytics_export["timestamp"]
            .dt.strftime("%Y-%m-%d %H:%M:%S")
        )

        analytics_export.to_csv(
            analytics_path,
            index=False
        )

        logger.info(
            "Hourly grid summary exported to: %s",
            analytics_path
        )

        logger.info(
            "Hourly grid summary exported to: %s",
            analytics_path
        )

        # -------------------------------------------------
        # Write daily summary
        # -------------------------------------------------

        self.daily_summary.to_csv(
            daily_path,
            index=False
        )

        logger.info(
            "Daily summary exported to: %s",
            daily_path
        )

        # -------------------------------------------------
        # Write grid summary
        # -------------------------------------------------

        self.grid_summary.to_csv(
            grid_path,
            index=False
        )

        logger.info(
            "Grid summary exported to: %s",
            grid_path
        )

        return {
            "analytics_path": analytics_path,
            "daily_path": daily_path,
            "grid_path": grid_path,
        }


    # =====================================================
    # 8. Full processing pipeline
    # =====================================================

    def process(self, output_dir="../../data/landing"):
        """
        Run the complete UsageProcessor pipeline.

        The individual methods remain independently testable,
        while this method provides a single entry point for
        processing one input file.
        """

        # Make repeated calls on the same processor safe.
        self._reset_state()

        logger.info(
            "Starting processing pipeline."
        )

        # -------------------------------------------------
        # Pipeline
        # -------------------------------------------------

        self.load_data()

        self.clean_data()

        self.derive_time_features()

        self.aggregate_to_grid_time()
        
        self.derive_activity_features()

        daily_summary, grid_summary = (
            self.compute_kpis()
        )

        exported_paths = self.export_summary(
            output_dir
        )

        logger.info(
            "Processing pipeline completed successfully."
        )

        # -------------------------------------------------
        # Return useful outputs
        # -------------------------------------------------

        return {
            "analytics": self.analytics_data,
            "daily_summary": daily_summary,
            "grid_summary": grid_summary,

            "analytics_path": (
                exported_paths["analytics_path"]
            ),

            "daily_path": (
                exported_paths["daily_path"]
            ),

            "grid_path": (
                exported_paths["grid_path"]
            ),

            "nulls_handled": self.nulls_handled,
            "dropped_rows": self.dropped_rows,
        }
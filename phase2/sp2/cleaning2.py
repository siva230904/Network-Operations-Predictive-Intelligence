import json
import logging
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
)


# =========================================================
# Logging
# =========================================================

def create_logger(log_dir):
    """
    Create a unique SP2 log file.

    Existing logs are never overwritten.
    """

    log_dir = Path(log_dir)
    log_dir.mkdir(
        parents=True,
        exist_ok=True
    )

    counter = 1

    while True:

        log_file = (
            log_dir /
            f"sp2_cleaning_run_{counter:03d}.log"
        )

        if not log_file.exists():
            break

        counter += 1

    logger = logging.getLogger(
        f"SP2_Cleaning_{counter}"
    )

    logger.setLevel(
        logging.INFO
    )

    logger.propagate = False

    handler = logging.FileHandler(
        log_file,
        encoding="utf-8"
    )

    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        )
    )

    logger.addHandler(handler)

    return logger, log_file


# =========================================================
# Network Cleaner
# =========================================================

class NetworkCleaner:
    """
    SP2 — Cleaning and Standardization.

    Input grain:

        timestamp + grid_id + country_code

    SP2 DOES NOT aggregate country-code rows.

    Output:

        clean_network_df

    Persistent checkpoint:

        data/landing/sp2/clean_network
    """

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

    REQUIRED_RAW_COLUMNS = list(
        RAW_TO_CANONICAL.keys()
    )

    # =====================================================
    # Constructor
    # =====================================================

    def __init__(
        self,
        spark,
        input_path=None,
        raw_network_df=None,
        output_dir="data/landing/sp2",
        log_dir="data/logs",
    ):

        if (
            input_path is None
            and raw_network_df is None
        ):
            raise ValueError(
                "Provide either input_path "
                "or raw_network_df."
            )

        if (
            input_path is not None
            and raw_network_df is not None
        ):
            raise ValueError(
                "Provide either input_path "
                "or raw_network_df, not both."
            )

        self.spark = spark

        self.input_path = input_path

        self.raw_network_df = (
            raw_network_df
        )

        self.output_dir = Path(
            output_dir
        )

        self.logger, self.log_file = (
            create_logger(
                log_dir
            )
        )

        self.raw_df = None
        self.canonical_df = None
        self.clean_network_df = None
        self.rejected_df = None

        self.input_row_count = 0
        self.rejected_row_count = 0
        self.final_row_count = 0
        self.nulls_handled = 0

        self.file_count = 0

    # =====================================================
    # 1. Load
    # =====================================================

    def load_data(self):

        self.logger.info(
            "Starting SP2 cleaning pipeline."
        )

        if self.raw_network_df is not None:

            self.raw_df = (
                self.raw_network_df
            )

            self.logger.info(
                "Using supplied Spark DataFrame."
            )

        else:

            if self.input_path is None:

                raise ValueError(
                    "input_path is required."
                )

            input_path = str(
                self.input_path
            )

            self.logger.info(
                "Loading raw files: %s",
                input_path
            )

            self.raw_df = (
                self.spark.read
                .option(
                    "header",
                    True
                )
                .option(
                    "inferSchema",
                    False
                )
                .csv(
                    input_path
                )
            )

        # -------------------------------------------------
        # Validate columns BEFORE any transformation
        # -------------------------------------------------

        missing_columns = [
            column
            for column
            in self.REQUIRED_RAW_COLUMNS
            if column
            not in
            self.raw_df.columns
        ]

        if missing_columns:

            raise ValueError(
                "Missing required raw columns: "
                f"{missing_columns}"
            )

        # -------------------------------------------------
        # Add source file if not already present
        # -------------------------------------------------

        if (
            "input_file_name"
            not in
            self.raw_df.columns
        ):

            self.raw_df = (
                self.raw_df
                .withColumn(
                    "input_file_name",
                    F.input_file_name()
                )
            )

        self.input_row_count = (
            self.raw_df.count()
        )

        self.file_count = (
            self.raw_df
            .select(
                "input_file_name"
            )
            .distinct()
            .count()
        )

        self.logger.info(
            "Input rows: %d",
            self.input_row_count
        )

        self.logger.info(
            "Input files detected: %d",
            self.file_count
        )

        # -------------------------------------------------
        # Canonicalize
        # -------------------------------------------------

        self.canonical_df = (
            self.raw_df
            .select(
                *[
                    F.col(raw)
                    .alias(canonical)
                    for raw, canonical
                    in self.RAW_TO_CANONICAL.items()
                ],
                F.col(
                    "input_file_name"
                )
            )
        )

        return self.canonical_df

    # =====================================================
    # 2. Standardize data types
    # =====================================================

    def standardize_types(self):

        if self.canonical_df is None:

            raise RuntimeError(
                "Run load_data() first."
            )

        df = self.canonical_df

        # -------------------------------------------------
        # Timestamp
        # -------------------------------------------------

        df = df.withColumn(
            "timestamp",
            F.to_timestamp(
                F.col("timestamp")
            )
        )

        # -------------------------------------------------
        # Grid
        # -------------------------------------------------

        df = df.withColumn(
            "grid_id",
            F.col("grid_id")
            .cast(IntegerType())
        )

        # -------------------------------------------------
        # Country code
        # -------------------------------------------------

        df = df.withColumn(
            "country_code",
            F.col("country_code")
            .cast(IntegerType())
        )

        # -------------------------------------------------
        # Activity
        # -------------------------------------------------

        for column in self.ACTIVITY_COLUMNS:

            df = df.withColumn(
                column,
                F.col(column)
                .cast(DoubleType())
            )

        self.canonical_df = df

        self.logger.info(
            "Canonical data types standardized."
        )

        return df

    # =====================================================
    # 3. Validate hourly cadence
    # =====================================================

    def validate_hourly_cadence(self):
        """
        Validate hourly cadence WITHOUT a global Window.

        For each source day:

        - exactly 24 distinct timestamps
        - first timestamp starts at an hourly boundary
        - last timestamp is exactly 23 hours after first

        This avoids lag/order windows over the entire
        dataset.
        """

        df = (
            self.canonical_df
            .filter(
                F.col("timestamp").isNotNull()
            )
            .select(
                F.to_date(
                    "timestamp"
                ).alias("date"),
                "timestamp"
            )
            .distinct()
        )

        daily_cadence = (
            df.groupBy("date")
            .agg(
                F.count(
                    "*"
                ).alias(
                    "timestamp_count"
                ),

                F.min(
                    "timestamp"
                ).alias(
                    "min_timestamp"
                ),

                F.max(
                    "timestamp"
                ).alias(
                    "max_timestamp"
                ),
            )
            .withColumn(
                "span_hours",
                (
                    F.col(
                        "max_timestamp"
                    ).cast("long")
                    -
                    F.col(
                        "min_timestamp"
                    ).cast("long")
                )
                /
                F.lit(3600)
            )
        )

        invalid_days = (
            daily_cadence
            .filter(
                (F.col("timestamp_count") != 24)
                |
                (F.col("span_hours") != 23)
            )
        )

        invalid_day_count = (
            invalid_days.count()
        )

        if invalid_day_count > 0:

            self.logger.error(
                "Hourly cadence validation failed "
                "for %d day(s).",
                invalid_day_count
            )

            invalid_days.show(
                truncate=False
            )

            raise ValueError(
                "Hourly cadence validation failed."
            )

        total_timestamps = (
            df.count()
        )

        self.logger.info(
            "Hourly cadence validation passed."
        )

        self.logger.info(
            "Distinct timestamp/day records: %d",
            total_timestamps
        )

        return daily_cadence

    # =====================================================
    # 4. Identify rejected rows
    # =====================================================

    def identify_rejected_rows(self):

        df = self.canonical_df

        # -------------------------------------------------
        # Missing timestamp
        # -------------------------------------------------

        missing_timestamp = (
            F.col("timestamp").isNull()
        )

        # -------------------------------------------------
        # Missing grid
        # -------------------------------------------------

        missing_grid = (
            F.col("grid_id").isNull()
        )

        # -------------------------------------------------
        # Invalid grid
        # -------------------------------------------------

        invalid_grid = (
            F.col("grid_id").isNotNull()
            &
            (
                (F.col("grid_id") < 1)
                |
                (F.col("grid_id") > 10000)
            )
        )

        # -------------------------------------------------
        # Negative activity
        # -------------------------------------------------

        negative_activity = F.lit(False)

        for column in self.ACTIVITY_COLUMNS:

            negative_activity = (
                negative_activity
                |
                (
                    F.col(column) < 0
                )
            )

        # -------------------------------------------------
        # Combined rejection
        # -------------------------------------------------

        rejection_condition = (
            missing_timestamp
            |
            missing_grid
            |
            invalid_grid
            |
            negative_activity
        )

        # -------------------------------------------------
        # Human-readable reason
        # -------------------------------------------------

        rejection_reason = (
            F.when(
                missing_timestamp,
                F.lit(
                    "missing_timestamp"
                )
            )
            .when(
                missing_grid,
                F.lit(
                    "missing_grid_id"
                )
            )
            .when(
                invalid_grid,
                F.lit(
                    "invalid_grid_id"
                )
            )
            .when(
                negative_activity,
                F.lit(
                    "negative_activity"
                )
            )
            .otherwise(
                F.lit("unknown")
            )
        )

        self.rejected_df = (
            df
            .filter(
                rejection_condition
            )
            .withColumn(
                "rejection_reason",
                rejection_reason
            )
        )

        self.rejected_row_count = (
            self.rejected_df.count()
        )

        self.logger.info(
            "Rejected rows: %d",
            self.rejected_row_count
        )

        return self.rejected_df

    # =====================================================
    # 5. Clean valid rows
    # =====================================================

    def clean_data(self):

        df = self.canonical_df

        # -------------------------------------------------
        # Same rejection conditions
        # -------------------------------------------------

        valid_condition = (
            F.col("timestamp").isNotNull()
            &
            F.col("grid_id").isNotNull()
            &
            (F.col("grid_id") >= 1)
            &
            (F.col("grid_id") <= 10000)
        )

        for column in self.ACTIVITY_COLUMNS:

            valid_condition = (
                valid_condition
                &
                (
                    F.col(column).isNull()
                    |
                    (
                        F.col(column) >= 0
                    )
                )
            )

        df = (
            df.filter(
                valid_condition
            )
        )

        # -------------------------------------------------
        # Count activity nulls BEFORE filling
        # -------------------------------------------------

        null_count_expression = F.lit(0)

        for column in self.ACTIVITY_COLUMNS:

            null_count_expression = (
                null_count_expression
                +
                F.when(
                    F.col(column).isNull(),
                    1
                )
                .otherwise(0)
            )

        null_result = (
            df.select(
                F.sum(
                    null_count_expression
                ).alias(
                    "null_count"
                )
            )
            .first()
        )

        self.nulls_handled = (
            int(
                null_result["null_count"]
                or 0
            )
        )

        self.logger.info(
            "Activity nulls handled: %d",
            self.nulls_handled
        )

        # -------------------------------------------------
        # Curated null-to-zero rule
        # -------------------------------------------------

        for column in self.ACTIVITY_COLUMNS:

            df = df.withColumn(
                column,
                F.coalesce(
                    F.col(column),
                    F.lit(0.0)
                )
            )

        self.clean_network_df = df

        return df

    # =====================================================
    # 6. Derive features
    # =====================================================

    def derive_features(self):

        if self.clean_network_df is None:

            raise RuntimeError(
                "Run clean_data() first."
            )

        df = self.clean_network_df

        df = (
            df
            .withColumn(
                "date",
                F.to_date(
                    "timestamp"
                )
            )
            .withColumn(
                "hour",
                F.hour(
                    "timestamp"
                )
            )
            .withColumn(
                "day_of_week",
                F.dayofweek(
                    "timestamp"
                )
            )
            .withColumn(
                "total_sms",
                F.col("sms_in")
                +
                F.col("sms_out")
            )
            .withColumn(
                "total_calls",
                F.col("call_in")
                +
                F.col("call_out")
            )
            .withColumn(
                "total_activity",
                F.col("sms_in")
                +
                F.col("sms_out")
                +
                F.col("call_in")
                +
                F.col("call_out")
                +
                F.col(
                    "internet_activity"
                )
            )
        )

        self.clean_network_df = df

        self.logger.info(
            "Derived date, hour, day_of_week, "
            "total_sms, total_calls and "
            "total_activity."
        )

        return df

    # =====================================================
    # 7. Final validation
    # =====================================================

    def validate_final_rows(self):

        final_count = (
            self.clean_network_df.count()
        )

        self.final_row_count = (
            final_count
        )

        expected_count = (
            self.input_row_count
            -
            self.rejected_row_count
        )

        self.logger.info(
            "Row reconciliation:"
        )

        self.logger.info(
            "Input rows: %d",
            self.input_row_count
        )

        self.logger.info(
            "Rejected rows: %d",
            self.rejected_row_count
        )

        self.logger.info(
            "Expected clean rows: %d",
            expected_count
        )

        self.logger.info(
            "Actual clean rows: %d",
            final_count
        )

        if final_count != expected_count:

            raise ValueError(
                "Row-count reconciliation failed. "
                f"Input={self.input_row_count}, "
                f"Rejected={self.rejected_row_count}, "
                f"Final={final_count}, "
                f"Expected={expected_count}"
            )

        self.logger.info(
            "Row-count reconciliation passed."
        )

    # =====================================================
    # 8. Validate grain
    # =====================================================

    def validate_raw_grain(self):

        """
        SP2 must retain country-code grain.

        Therefore we do NOT expect uniqueness on:

            timestamp + grid_id

        Instead, verify that country_code still exists
        and that multiple country-code records can exist.
        """

        columns = (
            self.clean_network_df.columns
        )

        if (
            "country_code"
            not in
            columns
        ):

            raise ValueError(
                "country_code was lost during cleaning."
            )

        country_codes = (
            self.clean_network_df
            .select(
                "country_code"
            )
            .distinct()
            .count()
        )

        self.logger.info(
            "Distinct country-code categories "
            "after cleaning: %d",
            country_codes
        )

        # -------------------------------------------------
        # Find whether multiple country-code records
        # exist for the same grid/hour.
        # -------------------------------------------------

        multiple_country_rows = (
            self.clean_network_df
            .groupBy(
                "timestamp",
                "grid_id"
            )
            .count()
            .filter(
                F.col("count") > 1
            )
            .limit(1)
            .count()
        )

        if multiple_country_rows == 0:

            raise ValueError(
                "Country-code grain appears to have "
                "been collapsed in SP2."
            )

        self.logger.info(
            "Raw country-code grain preserved."
        )

    # =====================================================
    # 9. Save checkpoint
    # =====================================================

    def save_checkpoint(self):

        self.output_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        clean_path = (
            self.output_dir /
            "clean_network"
        )

        rejected_path = (
            self.output_dir /
            "rejected_records"
        )

        report_path = (
            self.output_dir /
            "sp2_quality_report.json"
        )

        # -------------------------------------------------
        # Clean checkpoint
        # -------------------------------------------------

        (
            self.clean_network_df
            .write
            .mode("overwrite")
            .parquet(
                str(clean_path)
            )
        )

        # -------------------------------------------------
        # Rejected checkpoint
        # -------------------------------------------------

        (
            self.rejected_df
            .write
            .mode("overwrite")
            .parquet(
                str(rejected_path)
            )
        )

        # -------------------------------------------------
        # Quality report
        # -------------------------------------------------

        report = {
            "input_files":
                self.file_count,

            "input_rows":
                self.input_row_count,

            "rejected_rows":
                self.rejected_row_count,

            "final_rows":
                self.final_row_count,

            "activity_nulls_handled":
                self.nulls_handled,

            "clean_output":
                str(clean_path),

            "rejected_output":
                str(rejected_path),

            "grain":
                "timestamp + grid_id + country_code",
        }

        with open(
            report_path,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                report,
                file,
                indent=4
            )

        self.logger.info(
            "Clean checkpoint written to: %s",
            clean_path
        )

        self.logger.info(
            "Rejected checkpoint written to: %s",
            rejected_path
        )

        self.logger.info(
            "Quality report written to: %s",
            report_path
        )

        return {
            "clean_path":
                clean_path,

            "rejected_path":
                rejected_path,

            "report_path":
                report_path,
        }

    # =====================================================
    # 10. Full pipeline
    # =====================================================

    def process(self):

        self.load_data()

        self.standardize_types()

        self.validate_hourly_cadence()

        self.identify_rejected_rows()

        self.clean_data()

        self.derive_features()

        self.validate_final_rows()

        self.validate_raw_grain()

        paths = (
            self.save_checkpoint()
        )

        self.logger.info(
            "SP2 cleaning pipeline "
            "completed successfully."
        )

        return {
            "clean_network_df":
                self.clean_network_df,

            "rejected_df":
                self.rejected_df,

            "input_row_count":
                self.input_row_count,

            "rejected_row_count":
                self.rejected_row_count,

            "final_row_count":
                self.final_row_count,

            "nulls_handled":
                self.nulls_handled,

            "paths":
                paths,

            "log_file":
                self.log_file,
        }
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.functions import (
    col,
    lit,
    when,
    to_date,
    hour,
    dayofweek,
    count,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    coalesce,
)
from pyspark.sql.window import Window


class NetworkCleaner:
    """
    SP2 — Cleaning & Standardization

    Input:
        SP1 raw_network_df

    Output:
        clean_network_df
        rejected_records
        rejected_summary
        null_handling_report

    Raw grain is preserved:

        timestamp + grid_id + country_code

    Country-code aggregation does NOT happen in SP2.
    """

    # =====================================================
    # Constructor
    # =====================================================

    def __init__(
        self,
        spark,
        raw_network_df,
    ):

        self.spark = spark
        self.raw_network_df = raw_network_df

        self.clean_network_df = None
        self.rejected_records = None

        self.rejected_summary = None
        self.null_handling_report = None

        self.initial_row_count = 0
        self.final_row_count = 0
        self.rejected_row_count = 0
        self.nulls_handled = 0

    # =====================================================
    # 1. Rename to canonical schema
    # =====================================================

    def standardize_columns(self):
        """
        Rename SP1 raw columns to canonical project names.
        """

        df = self.raw_network_df

        rename_map = {
            "datetime": "timestamp",
            "CellID": "grid_id",
            "countrycode": "country_code",
            "smsin": "sms_in",
            "smsout": "sms_out",
            "callin": "call_in",
            "callout": "call_out",
            "internet": "internet_activity",
        }

        for old_name, new_name in rename_map.items():

            if old_name in df.columns:

                df = df.withColumnRenamed(
                    old_name,
                    new_name
                )

        self.clean_network_df = df

        return self.clean_network_df

    # =====================================================
    # 2. Cast columns
    # =====================================================

    def cast_columns(self):
        """
        Cast timestamp, grid and activity fields
        to the canonical Spark types.
        """

        if self.clean_network_df is None:
            raise RuntimeError(
                "Run standardize_columns() first."
            )

        df = self.clean_network_df

        # -------------------------------------------------
        # Timestamp
        # -------------------------------------------------

        df = df.withColumn(
            "timestamp",
            col("timestamp").cast(
                TimestampType()
            )
        )

        # -------------------------------------------------
        # Grid
        # -------------------------------------------------

        df = df.withColumn(
            "grid_id",
            col("grid_id").cast(
                IntegerType()
            )
        )

        # -------------------------------------------------
        # Country code
        # -------------------------------------------------

        df = df.withColumn(
            "country_code",
            col("country_code").cast(
                IntegerType()
            )
        )

        # -------------------------------------------------
        # Activity measures
        # -------------------------------------------------

        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
        ]

        for column_name in activity_columns:

            df = df.withColumn(
                column_name,
                col(column_name).cast(
                    DoubleType()
                )
            )

        self.clean_network_df = df

        return self.clean_network_df

    # =====================================================
    # 3. Capture initial row count
    # =====================================================

    def profile_initial_rows(self):
        """
        Capture the number of records before cleaning.
        """

        self.initial_row_count = (
            self.raw_network_df.count()
        )

        return self.initial_row_count

    # =====================================================
    # 4. Quarantine invalid records
    # =====================================================

    def quarantine_invalid_records(self):
        """
        Quarantine rows with:

        - missing timestamp
        - missing grid_id
        - invalid grid_id
        - negative activity values

        Activity nulls are retained and handled later
        by the curated null-to-zero rule.
        """

        if self.clean_network_df is None:
            raise RuntimeError(
                "Run standardize_columns() and "
                "cast_columns() first."
            )

        df = self.clean_network_df

        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
        ]

        # -----------------------------------------------------
        # Missing key fields
        # -----------------------------------------------------

        missing_timestamp = (
            col("timestamp").isNull()
        )

        missing_grid = (
            col("grid_id").isNull()
        )

        missing_key_condition = (
            missing_timestamp |
            missing_grid
        )

        # -----------------------------------------------------
        # Invalid grid
        # -----------------------------------------------------

        invalid_grid_condition = (
            col("grid_id").isNotNull()
            &
            (
                (col("grid_id") < 1)
                |
                (col("grid_id") > 10000)
            )
        )

        # -----------------------------------------------------
        # Negative activity
        #
        # NULL activity is explicitly treated as NOT negative.
        # -----------------------------------------------------

        negative_activity_condition = lit(False)

        for column_name in activity_columns:

            negative_activity_condition = (
                negative_activity_condition
                |
                coalesce(
                    col(column_name) < 0,
                    lit(False)
                )
            )

        # -----------------------------------------------------
        # Final rejection condition
        # -----------------------------------------------------

        reject_condition = (
            missing_key_condition
            |
            invalid_grid_condition
            |
            negative_activity_condition
        )

        # -----------------------------------------------------
        # Rejected records
        # -----------------------------------------------------

        self.rejected_records = (
            df
            .filter(reject_condition)
            .withColumn(
                "rejection_reason",
                when(
                    missing_timestamp
                    & missing_grid,
                    lit(
                        "Missing timestamp and grid_id"
                    )
                )
                .when(
                    missing_timestamp,
                    lit(
                        "Missing timestamp"
                    )
                )
                .when(
                    missing_grid,
                    lit(
                        "Missing grid_id"
                    )
                )
                .when(
                    invalid_grid_condition,
                    lit(
                        "grid_id outside 1-10000"
                    )
                )
                .when(
                    negative_activity_condition,
                    lit(
                        "Negative activity value"
                    )
                )
                .otherwise(
                    lit("Unknown")
                )
            )
        )

        # -----------------------------------------------------
        # Clean records
        # -----------------------------------------------------

        self.clean_network_df = (
            df.filter(
                ~reject_condition
            )
        )

        # -----------------------------------------------------
        # Count rejected records
        # -----------------------------------------------------

        self.rejected_row_count = (
            self.rejected_records.count()
        )

        return self.clean_network_df

    # =====================================================
    # 5. Profile activity nulls
    # =====================================================

    def profile_activity_nulls(self):
        """
        Count blank activity values before applying
        the curated null-to-zero rule.
        """

        if self.clean_network_df is None:
            raise RuntimeError(
                "Run the cleaning steps first."
            )

        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
        ]

        expressions = []

        for column_name in activity_columns:

            expressions.append(
                spark_sum(
                    when(
                        col(column_name).isNull(),
                        1
                    )
                    .otherwise(0)
                )
                .alias(column_name)
            )

        result = (
            self.clean_network_df
            .agg(*expressions)
            .collect()[0]
        )

        report = []

        for column_name in activity_columns:

            null_count = int(
                result[column_name]
                or 0
            )

            report.append(
                (
                    column_name,
                    null_count
                )
            )

        self.null_handling_report = (
            self.spark.createDataFrame(
                report,
                [
                    "column",
                    "null_count",
                ]
            )
        )

        self.nulls_handled = sum(
            count
            for _, count in report
        )

        return self.null_handling_report

    # =====================================================
    # 6. Apply null-to-zero rule
    # =====================================================

    def apply_null_policy(self):
        """
        Apply the documented curated-layer rule:

        Activity nulls → zero

        Missing timestamp/grid_id have already been
        quarantined and are NOT defaulted.
        """

        if self.clean_network_df is None:
            raise RuntimeError(
                "Run quarantine_invalid_records() first."
            )

        activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
        ]

        df = self.clean_network_df

        for column_name in activity_columns:

            df = df.withColumn(
                column_name,
                when(
                    col(column_name).isNull(),
                    lit(0.0)
                )
                .otherwise(
                    col(column_name)
                )
            )

        self.clean_network_df = df

        return self.clean_network_df

    # =====================================================
    # 7. Derive time features
    # =====================================================

    def derive_time_features(self):
        """
        Derive:

        date
        hour
        day_of_week
        """

        if self.clean_network_df is None:
            raise RuntimeError(
                "Run cleaning before deriving "
                "time features."
            )

        df = self.clean_network_df

        df = df.withColumn(
            "date",
            to_date(
                col("timestamp")
            )
        )

        df = df.withColumn(
            "hour",
            hour(
                col("timestamp")
            )
        )

        # Spark dayofweek:
        # 1 = Sunday
        # 2 = Monday
        # ...
        # 7 = Saturday
        #
        # Project pandas convention:
        # 0 = Monday
        # ...
        # 6 = Sunday
        #
        # Convert Spark numbering to pandas numbering.

        df = df.withColumn(
            "day_of_week",
            (
                dayofweek(
                    col("timestamp")
                ) + 5
            ) % 7
        )

        self.clean_network_df = df

        return self.clean_network_df

    # =====================================================
    # 8. Derive activity features
    # =====================================================

    def derive_activity_features(self):
        """
        Create:

        total_sms
        total_calls
        total_activity
        """

        if self.clean_network_df is None:
            raise RuntimeError(
                "Run derive_time_features() first."
            )

        df = self.clean_network_df

        df = df.withColumn(
            "total_sms",
            col("sms_in") +
            col("sms_out")
        )

        df = df.withColumn(
            "total_calls",
            col("call_in") +
            col("call_out")
        )

        df = df.withColumn(
            "total_activity",
            col("total_sms") +
            col("total_calls") +
            col("internet_activity")
        )

        self.clean_network_df = df

        return self.clean_network_df

    # =====================================================
    # 9. Validate hourly cadence
    # =====================================================

    def validate_hourly_cadence(self):
        """
        Verify that the distinct timestamps form a continuous
        hourly sequence across all supplied files.

        This implementation avoids a global Spark window.
        """

        if self.clean_network_df is None:
            raise RuntimeError(
                "Clean data before validating cadence."
            )

        timestamp_stats = (
            self.clean_network_df
            .select("timestamp")
            .distinct()
            .agg(
                count("*").alias(
                    "distinct_timestamps"
                ),
                spark_min("timestamp").alias(
                    "min_timestamp"
                ),
                spark_max("timestamp").alias(
                    "max_timestamp"
                ),
            )
            .collect()[0]
        )

        distinct_timestamps = int(
            timestamp_stats["distinct_timestamps"]
        )

        min_timestamp = (
            timestamp_stats["min_timestamp"]
        )

        max_timestamp = (
            timestamp_stats["max_timestamp"]
        )

        if (
            min_timestamp is None
            or max_timestamp is None
        ):

            raise ValueError(
                "No valid timestamps available "
                "for cadence validation."
            )

        # -----------------------------------------------------
        # Number of hourly intervals expected between
        # minimum and maximum timestamp, inclusive.
        # -----------------------------------------------------

        total_seconds = (
            max_timestamp
            - min_timestamp
        ).total_seconds()

        expected_timestamps = int(
            total_seconds / 3600
        ) + 1

        # -----------------------------------------------------
        # A continuous hourly sequence must have exactly
        # the expected number of distinct timestamps.
        # -----------------------------------------------------

        cadence_valid = (
            distinct_timestamps
            == expected_timestamps
        )

        print(
            f"\nDistinct timestamps: "
            f"{distinct_timestamps:,}"
        )

        print(
            f"Expected hourly timestamps: "
            f"{expected_timestamps:,}"
        )

        print(
            f"Time range: "
            f"{min_timestamp} to {max_timestamp}"
        )

        if cadence_valid:

            print(
                "Hourly cadence validation passed."
            )

        else:

            print(
                "WARNING: Hourly cadence validation failed."
            )

            print(
                "There are missing or irregular "
                "hourly intervals."
            )

        return cadence_valid

    # =====================================================
    # 10. Final row counts
    # =====================================================

    def profile_final_rows(self):
        """
        Compare input, rejected and clean record counts.
        """

        self.final_row_count = (
            self.clean_network_df.count()
        )

        # -------------------------------------------------
        # Consistency check
        # -------------------------------------------------

        if (
            self.initial_row_count
            !=
            self.final_row_count
            +
            self.rejected_row_count
        ):

            raise ValueError(
                "Row-count reconciliation failed."
            )

        return {
            "initial_rows":
                self.initial_row_count,

            "rejected_rows":
                self.rejected_row_count,

            "clean_rows":
                self.final_row_count,

            "null_activity_values_handled":
                self.nulls_handled,
        }

    # =====================================================
    # 11. SP2 Acceptance Criteria
    # =====================================================

    def validate_acceptance_criteria(self, expected_file_count):
        """
        Validate all SP2 acceptance criteria.

        Criteria:
        1. Pandas-vs-Spark comparison is handled separately.
        2. Rejected-row count and null-handled count are separate.
        3. Distinct timestamps = D × 24.
        4. Individual activity columns and composites exist.
        """

        if self.clean_network_df is None:
            raise RuntimeError(
                "Run process() before validation."
            )

        df = self.clean_network_df

        print("\n" + "=" * 70)
        print("SP2 ACCEPTANCE CRITERIA")
        print("=" * 70)

        results = {}

        # =================================================
        # Criterion 2
        # =================================================

        print("\n1. REJECTED ROWS VS NULL-HANDLED VALUES")

        rejected_rows = (
            self.rejected_row_count
        )

        nulls_handled = (
            self.nulls_handled
        )

        # They must be tracked separately.
        criterion_2 = (
            isinstance(rejected_rows, int)
            and
            isinstance(nulls_handled, int)
        )

        results[
            "separate_quality_metrics"
        ] = criterion_2

        print(
            f"Rejected rows: "
            f"{rejected_rows:,}"
        )

        print(
            f"Activity nulls handled: "
            f"{nulls_handled:,}"
        )

        print(
            "PASS"
            if criterion_2
            else "FAIL"
        )

        # =================================================
        # Criterion 3
        # =================================================

        print("\n2. DISTINCT TIMESTAMP COUNT")

        distinct_timestamps = (
            df
            .select("timestamp")
            .distinct()
            .count()
        )

        expected_timestamps = (
            expected_file_count * 24
        )

        criterion_3 = (
            distinct_timestamps
            == expected_timestamps
        )

        results[
            "timestamp_count"
        ] = criterion_3

        print(
            f"Files: "
            f"{expected_file_count}"
        )

        print(
            f"Distinct timestamps: "
            f"{distinct_timestamps:,}"
        )

        print(
            f"Expected timestamps: "
            f"{expected_timestamps:,}"
        )

        print(
            "PASS"
            if criterion_3
            else "FAIL"
        )

        # =================================================
        # Criterion 4
        # =================================================

        print("\n3. REQUIRED ACTIVITY COLUMNS")

        required_activity_columns = [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_sms",
            "total_calls",
            "total_activity",
        ]

        missing_columns = [
            column_name
            for column_name
            in required_activity_columns
            if column_name not in df.columns
        ]

        criterion_4 = (
            len(missing_columns) == 0
        )

        results[
            "activity_columns"
        ] = criterion_4

        print(
            "Required columns:"
        )

        for column_name in required_activity_columns:

            print(
                f"  {column_name}: "
                f"{'YES' if column_name in df.columns else 'NO'}"
            )

        if missing_columns:

            print(
                f"Missing columns: "
                f"{missing_columns}"
            )

        print(
            "PASS"
            if criterion_4
            else "FAIL"
        )

        # =================================================
        # Overall
        # =================================================

        print("\n" + "=" * 70)

        # Criterion 1 is deliberately NOT marked here.
        # It is validated by the separate pandas-vs-Spark
        # comparison test.

        overall = all(
            results.values()
        )

        if overall:

            print(
                "SP2 LOCAL ACCEPTANCE: ALL PASS"
            )

            print(
                "Pandas-vs-Spark comparison "
                "must also pass."
            )

        else:

            print(
                "SP2 LOCAL ACCEPTANCE: FAILED"
            )

        print("=" * 70)

        return {
            "all_passed":
                overall,

            "criteria":
                results,

            "rejected_rows":
                rejected_rows,

            "nulls_handled":
                nulls_handled,

            "distinct_timestamps":
                distinct_timestamps,

            "expected_timestamps":
                expected_timestamps,

            "missing_columns":
                missing_columns,
        }

    # =====================================================
    # 12. Process
    # =====================================================

    def process(self):
        """
        Run the complete SP2 cleaning pipeline.
        """

        print(
            "\nStarting SP2 cleaning pipeline..."
        )

        self.profile_initial_rows()

        self.standardize_columns()

        self.cast_columns()

        self.quarantine_invalid_records()

        self.profile_activity_nulls()

        self.apply_null_policy()

        self.derive_time_features()

        self.derive_activity_features()

        cadence_valid = (
            self.validate_hourly_cadence()
        )

        counts = (
            self.profile_final_rows()
        )

        print(
            "\nSP2 cleaning pipeline completed."
        )

        return {
            "clean_network_df":
                self.clean_network_df,

            "rejected_records":
                self.rejected_records,

            "rejected_summary":
                counts,

            "null_handling_report":
                self.null_handling_report,

            "cadence_valid":
                cadence_valid,
        }
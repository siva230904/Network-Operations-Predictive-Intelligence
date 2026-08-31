# =========================================================
# SP3 — Network Activity Aggregations
# File: phase2/sp3/aggregation2.py
# =========================================================

import logging
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# =========================================================
# Logging
# =========================================================

def create_logger(log_dir="../../data/logs"):
    """
    Create a unique log file for every SP3 run.

    Existing logs are never overwritten.
    """

    log_path = Path(log_dir)
    log_path.mkdir(
        parents=True,
        exist_ok=True
    )

    run_number = 1

    while True:

        filename = (
            log_path
            / f"sp3_aggregation_run_{run_number:03d}.log"
        )

        if not filename.exists():
            break

        run_number += 1

    logger = logging.getLogger(
        f"SP3_{run_number}"
    )

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    )

    file_handler = logging.FileHandler(
        filename,
        encoding="utf-8"
    )

    file_handler.setFormatter(formatter)

    logger.addHandler(file_handler)

    return logger, filename


# =========================================================
# Network Aggregator
# =========================================================

class NetworkAggregator:
    """
    SP3 distributed network activity aggregation.

    Input grain:

        timestamp + grid_id + country_code

    Output grain:

        timestamp + grid_id

    The country_code dimension is removed from the
    operational analytics layer.
    """

    ACTIVITY_COLUMNS = [
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
    ]

    REQUIRED_COLUMNS = [
        "timestamp",
        "grid_id",
        "country_code",
        "sms_in",
        "sms_out",
        "call_in",
        "call_out",
        "internet_activity",
    ]

    # =====================================================
    # Constructor
    # =====================================================

    def __init__(
        self,
        clean_network_df: DataFrame,
        log_dir="../../data/logs"
    ):

        if clean_network_df is None:
            raise ValueError(
                "clean_network_df cannot be None."
            )

        self.clean_network_df = clean_network_df

        # -------------------------------------------------
        # Outputs
        # -------------------------------------------------

        self.hourly_grid_summary = None
        self.daily_traffic_summary = None
        self.hotspot_ranking = None
        self.peak_activity_hour = None

        # -------------------------------------------------
        # Counts
        # -------------------------------------------------

        self.clean_row_count = None
        self.hourly_row_count = None

        # -------------------------------------------------
        # Logging
        # -------------------------------------------------

        self.logger, self.log_file = create_logger(
            log_dir
        )

    # =====================================================
    # 1. Validate input
    # =====================================================

    def validate_input(self):
        """
        Validate that the SP2 checkpoint contains the
        expected canonical country-code-level columns.
        """

        missing_columns = [
            column
            for column in self.REQUIRED_COLUMNS
            if column not in self.clean_network_df.columns
        ]

        if missing_columns:

            self.logger.error(
                "Missing required columns: %s",
                missing_columns
            )

            raise ValueError(
                "Missing required columns: "
                f"{missing_columns}"
            )

        self.clean_row_count = (
            self.clean_network_df.count()
        )

        if self.clean_row_count == 0:

            raise ValueError(
                "clean_network_df contains zero rows."
            )

        self.logger.info(
            "Input validation passed."
        )

        self.logger.info(
            "Clean network rows: %d",
            self.clean_row_count
        )

    # =====================================================
    # 2. Aggregate country-code grain
    # =====================================================

    def aggregate_to_grid_hour(self):
        """
        Collapse:

            timestamp + grid_id + country_code

        into:

            timestamp + grid_id

        by summing the five activity measures.
        """

        aggregation_expressions = [
            F.sum(column).alias(column)
            for column in self.ACTIVITY_COLUMNS
        ]

        self.hourly_grid_summary = (
            self.clean_network_df
            .groupBy(
                "timestamp",
                "grid_id"
            )
            .agg(
                *aggregation_expressions
            )
        )

        self.logger.info(
            "Country-code rows aggregated "
            "to timestamp + grid_id grain."
        )

    # =====================================================
    # 3. Derive activity measures
    # =====================================================

    def derive_activity_features(self):
        """
        Create:

            total_sms
            total_calls
            total_activity
            internet_share
        """

        if self.hourly_grid_summary is None:

            raise RuntimeError(
                "Run aggregate_to_grid_hour() first."
            )

        df = self.hourly_grid_summary

        df = df.withColumn(
            "total_sms",
            F.col("sms_in") +
            F.col("sms_out")
        )

        df = df.withColumn(
            "total_calls",
            F.col("call_in") +
            F.col("call_out")
        )

        df = df.withColumn(
            "total_activity",
            F.col("total_sms") +
            F.col("total_calls") +
            F.col("internet_activity")
        )

        # Avoid division by zero.
        df = df.withColumn(
            "internet_share",
            F.when(
                F.col("total_activity") != 0,
                F.round(
                    F.col("internet_activity")
                    /
                    F.col("total_activity"),
                    6
                )
            ).otherwise(0.0)
        )

        self.hourly_grid_summary = df

        self.logger.info(
            "Derived total_sms, total_calls, "
            "total_activity and internet_share."
        )

    # =====================================================
    # 4. Add date/time fields
    # =====================================================

    def derive_time_features(self):
        """
        Derive date, hour and day_of_week.

        These are derived from timestamp rather than
        relying on potentially stale upstream fields.
        """

        if self.hourly_grid_summary is None:

            raise RuntimeError(
                "Run aggregate_to_grid_hour() first."
            )

        df = self.hourly_grid_summary

        df = df.withColumn(
            "date",
            F.to_date("timestamp")
        )

        df = df.withColumn(
            "hour",
            F.hour("timestamp")
        )

        df = df.withColumn(
            "day_of_week",
            F.dayofweek("timestamp")
        )

        self.hourly_grid_summary = df

        self.logger.info(
            "Derived date, hour and day_of_week."
        )

    # =====================================================
    # 5. Validate hourly grain
    # =====================================================

    def validate_grain(self):
        """
        Validate that hourly_grid_summary has exactly
        one row per grid_id + timestamp.
        """

        if self.hourly_grid_summary is None:

            raise RuntimeError(
                "Hourly grid summary has not been created."
            )

        df = self.hourly_grid_summary

        duplicate_groups = (
            df
            .groupBy(
                "grid_id",
                "timestamp"
            )
            .count()
            .filter(
                F.col("count") > 1
            )
        )

        duplicate_count = (
            duplicate_groups.count()
        )

        if duplicate_count > 0:

            self.logger.error(
                "Found %d duplicate "
                "(grid_id, timestamp) groups.",
                duplicate_count
            )

            raise ValueError(
                "hourly_grid_summary contains "
                "duplicate (grid_id, timestamp) records."
            )

        self.hourly_row_count = (
            df.count()
        )

        # -------------------------------------------------
        # Acceptance criterion:
        #
        # output must be strictly smaller than input.
        # -------------------------------------------------

        if (
            self.hourly_row_count
            >=
            self.clean_row_count
        ):

            self.logger.error(
                "Aggregation did not reduce row count. "
                "Input=%d, Output=%d",
                self.clean_row_count,
                self.hourly_row_count
            )

            raise ValueError(
                "Aggregation did not reduce the row count."
            )

        self.logger.info(
            "Grain validation passed."
        )

        self.logger.info(
            "Hourly grid rows: %d",
            self.hourly_row_count
        )

        self.logger.info(
            "Duplicate grid/hour groups: 0"
        )

    # =====================================================
    # 6. Daily traffic summary
    # =====================================================

    def compute_daily_traffic(self):
        """
        Calculate daily traffic across all grids.
        """

        if self.hourly_grid_summary is None:

            raise RuntimeError(
                "Hourly grid summary has not been created."
            )

        self.daily_traffic_summary = (
            self.hourly_grid_summary
            .groupBy("date")
            .agg(

                F.sum(
                    "total_sms"
                ).alias(
                    "total_sms"
                ),

                F.sum(
                    "total_calls"
                ).alias(
                    "total_calls"
                ),

                F.sum(
                    "internet_activity"
                ).alias(
                    "internet_activity"
                ),

                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                ),

                F.countDistinct(
                    "grid_id"
                ).alias(
                    "active_grids"
                )
            )
            .orderBy("date")
        )

        self.logger.info(
            "Daily traffic summary calculated."
        )

        return self.daily_traffic_summary

    # =====================================================
    # 7. Top 10 hotspots
    # =====================================================

    def compute_hotspots(self):
        """
        Calculate the top ten grids by total activity
        across the complete loaded period.
        """

        if self.hourly_grid_summary is None:

            raise RuntimeError(
                "Hourly grid summary has not been created."
            )

        self.hotspot_ranking = (
            self.hourly_grid_summary
            .groupBy("grid_id")
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
            .orderBy(
                F.desc("total_activity")
            )
            .limit(10)
        )

        self.logger.info(
            "Top 10 high-activity grids calculated."
        )

        return self.hotspot_ranking

    # =====================================================
    # 8. Peak activity hour
    # =====================================================

    def compute_peak_hour(self):
        """
        Calculate the single peak hourly interval across
        all grids and all loaded dates.
        """

        if self.hourly_grid_summary is None:

            raise RuntimeError(
                "Hourly grid summary has not been created."
            )

        hourly_activity = (
            self.hourly_grid_summary
            .groupBy("timestamp")
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
        )

        self.peak_activity_hour = (
            hourly_activity
            .orderBy(
                F.desc("total_activity"),
                F.asc("timestamp")
            )
            .limit(1)
        )

        self.logger.info(
            "Peak activity hour calculated."
        )

        return self.peak_activity_hour

    # =====================================================
    # 9. Additional statistics
    # =====================================================

    def compute_summary_statistics(self):
        """
        Calculate useful SP3-level statistics.
        """

        df = self.hourly_grid_summary

        summary = (
            df
            .agg(
                F.sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                ),

                F.avg(
                    "total_activity"
                ).alias(
                    "average_activity"
                ),

                F.max(
                    "total_activity"
                ).alias(
                    "maximum_grid_hour_activity"
                ),

                F.countDistinct(
                    "grid_id"
                ).alias(
                    "unique_grids"
                ),

                F.countDistinct(
                    "timestamp"
                ).alias(
                    "distinct_timestamps"
                )
            )
        )

        self.summary_statistics = summary

        self.logger.info(
            "SP3 summary statistics calculated."
        )

        return summary

    # =====================================================
    # 10. Acceptance criteria
    # =====================================================

    def validate_acceptance_criteria(
        self,
        expected_file_count
    ):
        """
        Validate all SP3 acceptance criteria.
        """

        df = self.hourly_grid_summary

        print("\n" + "=" * 70)
        print("SP3 ACCEPTANCE CRITERIA")
        print("=" * 70)

        results = {}

        # -------------------------------------------------
        # Criterion 1
        # -------------------------------------------------

        print(
            "\n1. ZERO DUPLICATES ON "
            "(grid_id, timestamp)"
        )

        duplicates = (
            df
            .groupBy(
                "grid_id",
                "timestamp"
            )
            .count()
            .filter(
                F.col("count") > 1
            )
            .count()
        )

        criterion_1 = (
            duplicates == 0
        )

        results[
            "zero_duplicates"
        ] = criterion_1

        print(
            f"Duplicate groups: "
            f"{duplicates}"
        )

        print(
            "PASS"
            if criterion_1
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 2
        # -------------------------------------------------

        print(
            "\n2. HOURLY OUTPUT < CLEAN INPUT"
        )

        clean_count = self.clean_row_count
        hourly_count = self.hourly_row_count

        criterion_2 = (
            hourly_count
            <
            clean_count
        )

        results[
            "row_reduction"
        ] = criterion_2

        print(
            f"clean_network_df: "
            f"{clean_count:,}"
        )

        print(
            f"hourly_grid_summary: "
            f"{hourly_count:,}"
        )

        print(
            "PASS"
            if criterion_2
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 3
        # -------------------------------------------------

        print(
            "\n3. MAXIMUM ROW COUNT"
        )

        maximum_allowed = (
            expected_file_count
            * 24
            * 10000
        )

        criterion_3 = (
            hourly_count
            <=
            maximum_allowed
        )

        results[
            "maximum_row_count"
        ] = criterion_3

        print(
            f"Actual: "
            f"{hourly_count:,}"
        )

        print(
            f"Maximum allowed: "
            f"{maximum_allowed:,}"
        )

        print(
            "PASS"
            if criterion_3
            else "FAIL"
        )

        # -------------------------------------------------
        # Criterion 4
        # -------------------------------------------------

        print(
            "\n4. COUNTRY_CODE REMOVED"
        )

        criterion_4 = (
            "country_code"
            not in
            df.columns
        )

        results[
            "country_code_removed"
        ] = criterion_4

        print(
            "country_code present: "
            f"{'YES' if not criterion_4 else 'NO'}"
        )

        print(
            "PASS"
            if criterion_4
            else "FAIL"
        )

        # -------------------------------------------------
        # Additional grid validation
        # -------------------------------------------------

        print(
            "\n5. GRID IDs WITHIN 1-10000"
        )

        invalid_grids = (
            df
            .filter(
                (F.col("grid_id") < 1)
                |
                (F.col("grid_id") > 10000)
            )
            .count()
        )

        criterion_5 = (
            invalid_grids == 0
        )

        results[
            "valid_grid_ids"
        ] = criterion_5

        print(
            f"Invalid grid rows: "
            f"{invalid_grids:,}"
        )

        print(
            "PASS"
            if criterion_5
            else "FAIL"
        )

        # -------------------------------------------------
        # Overall
        # -------------------------------------------------

        all_passed = all(
            results.values()
        )

        print(
            "\n" + "=" * 70
        )

        if all_passed:

            print(
                "SP3 AUTOMATED ACCEPTANCE: ALL PASS"
            )

        else:

            print(
                "SP3 AUTOMATED ACCEPTANCE: FAILED"
            )

        print(
            "=" * 70
        )

        self.logger.info(
            "SP3 acceptance criteria: %s",
            "ALL PASS"
            if all_passed
            else "FAILED"
        )

        return {
            "all_passed":
                all_passed,

            "criteria":
                results,

            "clean_rows":
                clean_count,

            "hourly_rows":
                hourly_count,

            "maximum_allowed":
                maximum_allowed,
        }

    # =====================================================
    # 11. Export outputs
    # =====================================================

    def export_outputs(
        self,
        output_dir="../../data/landing"
    ):
        """
        Export all SP3 outputs.

        Spark writes directories containing part files.
        Existing SP3 output directories are replaced.
        """

        output_path = Path(
            output_dir
        )

        output_path.mkdir(
            parents=True,
            exist_ok=True
        )

        # -------------------------------------------------
        # Output directories
        # -------------------------------------------------

        hourly_output = (
            output_path /
            "hourly_grid_summary"
        )

        daily_output = (
            output_path /
            "daily_traffic_summary"
        )

        hotspot_output = (
            output_path /
            "hotspot_ranking"
        )

        peak_output = (
            output_path /
            "peak_activity_hour"
        )

        # -------------------------------------------------
        # Hourly summary
        # -------------------------------------------------

        (
            self.hourly_grid_summary
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(hourly_output)
            )
        )

        # -------------------------------------------------
        # Daily summary
        # -------------------------------------------------

        (
            self.daily_traffic_summary
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(daily_output)
            )
        )

        # -------------------------------------------------
        # Hotspots
        # -------------------------------------------------

        (
            self.hotspot_ranking
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(hotspot_output)
            )
        )

        # -------------------------------------------------
        # Peak hour
        # -------------------------------------------------

        (
            self.peak_activity_hour
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(peak_output)
            )
        )

        self.logger.info(
            "Hourly summary exported to: %s",
            hourly_output
        )

        self.logger.info(
            "Daily summary exported to: %s",
            daily_output
        )

        self.logger.info(
            "Hotspot ranking exported to: %s",
            hotspot_output
        )

        self.logger.info(
            "Peak activity hour exported to: %s",
            peak_output
        )

        return {
            "hourly":
                hourly_output,

            "daily":
                daily_output,

            "hotspots":
                hotspot_output,

            "peak":
                peak_output,
        }

    # =====================================================
    # 12. Full pipeline
    # =====================================================

    def process(
        self,
        expected_file_count,
        output_dir="../../data/landing"
    ):
        """
        Run the complete SP3 aggregation pipeline.

        Flow:

            SP2 clean checkpoint
                    ↓
            country-code aggregation
                    ↓
            grid/hour analytics
                    ↓
            activity features
                    ↓
            daily summary
                    ↓
            hotspots
                    ↓
            peak hour
                    ↓
            validation
                    ↓
            output checkpoints
        """

        self.logger.info(
            "Starting SP3 aggregation pipeline."
        )

        # -------------------------------------------------
        # Input validation
        # -------------------------------------------------

        self.validate_input()

        # -------------------------------------------------
        # Grain transition
        # -------------------------------------------------

        self.aggregate_to_grid_hour()

        # -------------------------------------------------
        # Derived analytics
        # -------------------------------------------------

        self.derive_activity_features()

        self.derive_time_features()

        # -------------------------------------------------
        # Grain validation
        # -------------------------------------------------

        self.validate_grain()

        # -------------------------------------------------
        # Summary calculations
        # -------------------------------------------------

        self.compute_daily_traffic()

        self.compute_hotspots()

        self.compute_peak_hour()

        self.compute_summary_statistics()

        # -------------------------------------------------
        # Acceptance criteria
        # -------------------------------------------------

        acceptance = (
            self.validate_acceptance_criteria(
                expected_file_count
            )
        )

        if not acceptance["all_passed"]:

            self.logger.error(
                "SP3 acceptance criteria failed."
            )

            raise ValueError(
                "SP3 acceptance criteria failed."
            )

        # -------------------------------------------------
        # Export
        # -------------------------------------------------

        output_paths = (
            self.export_outputs(
                output_dir
            )
        )

        # -------------------------------------------------
        # Completion
        # -------------------------------------------------

        self.logger.info(
            "SP3 aggregation pipeline "
            "completed successfully."
        )

        self.logger.info(
            "Log file: %s",
            self.log_file
        )

        return {
            "hourly_grid_summary":
                self.hourly_grid_summary,

            "daily_traffic_summary":
                self.daily_traffic_summary,

            "hotspot_ranking":
                self.hotspot_ranking,

            "peak_activity_hour":
                self.peak_activity_hour,

            "summary_statistics":
                self.summary_statistics,

            "acceptance":
                acceptance,

            "output_paths":
                output_paths,

            "log_file":
                self.log_file,
        }
import logging
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    count,
    countDistinct,
    to_date,
    hour,
    round as spark_round,
    desc,
    max as spark_max,
)


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

    # Find next available log filename
    run_number = 1

    while True:

        filename = (
            log_path /
            f"sp3_aggregation_run_{run_number:03d}.log"
        )

        if not filename.exists():
            break

        run_number += 1

    logger = logging.getLogger(
        f"SP3_{run_number}"
    )

    logger.setLevel(
        logging.INFO
    )

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

    file_handler.setFormatter(
        formatter
    )

    logger.addHandler(
        file_handler
    )

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

    The country_code dimension is intentionally removed
    from the operational analytics layer.
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

    # -----------------------------------------------------
    # Constructor
    # -----------------------------------------------------

    def __init__(
        self,
        clean_network_df: DataFrame,
        log_dir="../../data/logs"
    ):

        if clean_network_df is None:
            raise ValueError(
                "clean_network_df cannot be None."
            )

        self.clean_network_df = (
            clean_network_df
        )

        self.hourly_grid_summary = None
        self.daily_traffic_summary = None
        self.hotspot_ranking = None
        self.peak_activity_hour = None

        self.clean_row_count = None
        self.hourly_row_count = None

        self.logger, self.log_file = (
            create_logger(log_dir)
        )

    # =====================================================
    # 1. Validate input
    # =====================================================

    def validate_input(self):

        missing_columns = [
            column
            for column in self.REQUIRED_COLUMNS
            if column not in self.clean_network_df.columns
        ]

        if missing_columns:

            raise ValueError(
                "Missing required columns: "
                f"{missing_columns}"
            )

        self.clean_row_count = (
            self.clean_network_df.count()
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
            spark_sum(
                column
            ).alias(column)
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

        df = (
            self.hourly_grid_summary
        )

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

        df = df.withColumn(
            "internet_share",
            spark_round(
                col("internet_activity")
                /
                col("total_activity"),
                6
            )
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

        df = (
            self.hourly_grid_summary
        )

        df = df.withColumn(
            "date",
            to_date("timestamp")
        )

        df = df.withColumn(
            "hour",
            hour("timestamp")
        )

        self.hourly_grid_summary = df

        self.logger.info(
            "Derived date and hour fields."
        )

    # =====================================================
    # 5. Validate hourly grain
    # =====================================================

    def validate_grain(self):
        """
        The canonical SP3 output must contain exactly
        one row per timestamp + grid_id.
        """

        df = (
            self.hourly_grid_summary
        )

        duplicate_groups = (
            df
            .groupBy(
                "grid_id",
                "timestamp"
            )
            .count()
            .filter(
                col("count") > 1
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
        # output must be strictly smaller than input.
        # -------------------------------------------------

        if (
            self.hourly_row_count
            >=
            self.clean_row_count
        ):

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

        self.daily_traffic_summary = (
            self.hourly_grid_summary
            .groupBy("date")
            .agg(
                spark_sum(
                    "total_sms"
                ).alias(
                    "total_sms"
                ),

                spark_sum(
                    "total_calls"
                ).alias(
                    "total_calls"
                ),

                spark_sum(
                    "internet_activity"
                ).alias(
                    "internet_activity"
                ),

                spark_sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                ),

                countDistinct(
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

        return (
            self.daily_traffic_summary
        )

    # =====================================================
    # 7. Top 10 hotspots
    # =====================================================

    def compute_hotspots(self):

        self.hotspot_ranking = (
            self.hourly_grid_summary
            .groupBy("grid_id")
            .agg(
                spark_sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
            .orderBy(
                desc("total_activity")
            )
            .limit(10)
        )

        self.logger.info(
            "Top 10 high-activity grids calculated."
        )

        return (
            self.hotspot_ranking
        )

    # =====================================================
    # 8. Peak activity hour
    # =====================================================

    def compute_peak_hour(self):

        hourly_activity = (
            self.hourly_grid_summary
            .groupBy("timestamp")
            .agg(
                spark_sum(
                    "total_activity"
                ).alias(
                    "total_activity"
                )
            )
        )

        self.peak_activity_hour = (
            hourly_activity
            .orderBy(
                desc("total_activity")
            )
            .limit(1)
        )

        self.logger.info(
            "Peak activity hour calculated."
        )

        return (
            self.peak_activity_hour
        )

    # =====================================================
    # 9. Acceptance criteria
    # =====================================================

    def validate_acceptance_criteria(
        self,
        expected_file_count
    ):
        """
        Validate all SP3 acceptance criteria.
        """

        df = (
            self.hourly_grid_summary
        )

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
                col("count") > 1
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

        clean_count = (
            self.clean_row_count
        )

        hourly_count = (
            df.count()
        )

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
        # Criterion 5
        # -------------------------------------------------

        print(
            "\n4. COUNTRY_CODE REMOVED"
        )

        criterion_5 = (
            "country_code"
            not in
            df.columns
        )

        results[
            "country_code_removed"
        ] = criterion_5

        print(
            "country_code present: "
            f"{'YES' if not criterion_5 else 'NO'}"
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
    # 10. Export
    # =====================================================

    def export_outputs(
        self,
        output_dir="../../data/landing"
    ):

        output_path = Path(
            output_dir
        )

        output_path.mkdir(
            parents=True,
            exist_ok=True
        )

        hourly_path = (
            output_path /
            "hourly_grid_summary.csv"
        )

        daily_path = (
            output_path /
            "daily_traffic_summary.csv"
        )

        hotspot_path = (
            output_path /
            "hotspot_ranking.csv"
        )

        peak_path = (
            output_path /
            "peak_activity_hour.csv"
        )

        (
            self.hourly_grid_summary
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(
                    output_path /
                    "hourly_grid_summary"
                )
            )
        )

        (
            self.daily_traffic_summary
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(
                    output_path /
                    "daily_traffic_summary"
                )
            )
        )

        (
            self.hotspot_ranking
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(
                    output_path /
                    "hotspot_ranking"
                )
            )
        )

        (
            self.peak_activity_hour
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(
                str(
                    output_path /
                    "peak_activity_hour"
                )
            )
        )

        self.logger.info(
            "SP3 outputs exported to %s",
            output_path
        )

        return {
            "hourly":
                output_path /
                "hourly_grid_summary",

            "daily":
                output_path /
                "daily_traffic_summary",

            "hotspots":
                output_path /
                "hotspot_ranking",

            "peak":
                output_path /
                "peak_activity_hour",
        }

    # =====================================================
    # 11. Full pipeline
    # =====================================================

    def process(
        self,
        expected_file_count,
        output_dir="../../data/landing"
    ):
        """
        Run the complete SP3 pipeline.
        """

        self.logger.info(
            "Starting SP3 aggregation pipeline."
        )

        self.validate_input()

        self.aggregate_to_grid_hour()

        self.derive_activity_features()

        self.derive_time_features()

        self.validate_grain()

        self.compute_daily_traffic()

        self.compute_hotspots()

        self.compute_peak_hour()

        acceptance = (
            self.validate_acceptance_criteria(
                expected_file_count
            )
        )

        if not acceptance["all_passed"]:

            raise ValueError(
                "SP3 acceptance criteria failed."
            )

        output_paths = (
            self.export_outputs(
                output_dir
            )
        )

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

            "acceptance":
                acceptance,

            "output_paths":
                output_paths,

            "log_file":
                self.log_file,
        }
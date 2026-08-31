# =========================================================
# SP6 — Write Processed & Analytics Data
# File: phase2/sp6/storage.py
# =========================================================

import json
import logging
import shutil
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# =========================================================
# Logging
# =========================================================

def create_logger(log_dir="../../data/logs"):
    """
    Create a unique SP6 log file.

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
            / f"sp6_storage_run_{run_number:03d}.log"
        )

        if not filename.exists():
            break

        run_number += 1

    logger = logging.getLogger(
        f"SP6_{run_number}"
    )

    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s | "
        "%(levelname)s | "
        "%(message)s"
    )

    handler = logging.FileHandler(
        filename,
        encoding="utf-8"
    )

    handler.setFormatter(formatter)

    logger.addHandler(handler)

    return logger, filename


# =========================================================
# SP6 Storage Manager
# =========================================================

class NetworkStorageManager:

    """
    SP6 — Persist processed and analytics data.

    Inputs:

        SP2 clean Parquet checkpoint
        SP3 hourly_grid_summary Parquet checkpoint

    Outputs:

        data/processed/activity/
        data/analytics/hourly_grid_summary/
        data/analytics/dashboard_summary.csv

    Geometry remains in:

        data/reference/milano-grid.geojson

    Full polygon geometry is intentionally NOT written
    into hourly_grid_summary.
    """

    # -----------------------------------------------------
    # Controlled local-write partition count.
    #
    # This avoids creating excessively large partitions
    # during local Parquet writes.
    # -----------------------------------------------------

    WRITE_PARTITIONS = 64

    # =====================================================
    # Constructor
    # =====================================================

    def __init__(
        self,
        spark,
        clean_network_df: DataFrame,
        hourly_grid_summary: DataFrame,
        output_dir="../../data",
        log_dir="../../data/logs"
    ):

        if spark is None:
            raise ValueError(
                "spark cannot be None."
            )

        if clean_network_df is None:
            raise ValueError(
                "clean_network_df cannot be None."
            )

        if hourly_grid_summary is None:
            raise ValueError(
                "hourly_grid_summary cannot be None."
            )

        self.spark = spark

        self.clean_network_df = clean_network_df

        self.hourly_grid_summary = (
            hourly_grid_summary
        )

        self.base_dir = Path(output_dir)

        self.processed_dir = (
            self.base_dir / "processed"
        )

        self.analytics_dir = (
            self.base_dir / "analytics"
        )

        self.reference_dir = (
            self.base_dir / "reference"
        )

        self.activity_output = (
            self.processed_dir
            / "activity"
        )

        self.hourly_output = (
            self.analytics_dir
            / "hourly_grid_summary"
        )

        self.dashboard_output = (
            self.analytics_dir
            / "dashboard_summary.csv"
        )

        self.logger, self.log_file = (
            create_logger(log_dir)
        )

        self.clean_input_count = None
        self.hourly_input_count = None

        self.activity_roundtrip_count = None
        self.hourly_roundtrip_count = None

        self.csv_size_bytes = None
        self.parquet_size_bytes = None

        self.dashboard_summary = None
        self.acceptance = None

    # =====================================================
    # 1. Validate inputs
    # =====================================================

    def validate_inputs(self):

        required_clean_columns = [
            "timestamp",
            "grid_id",
            "country_code",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_sms",
            "total_calls",
            "total_activity",
            "date",
        ]

        missing_clean = [
            column
            for column in required_clean_columns
            if column not in self.clean_network_df.columns
        ]

        if missing_clean:

            raise ValueError(
                "SP2 checkpoint is missing columns: "
                f"{missing_clean}"
            )

        required_hourly_columns = [
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_sms",
            "total_calls",
            "total_activity",
        ]

        missing_hourly = [
            column
            for column in required_hourly_columns
            if column not in self.hourly_grid_summary.columns
        ]

        if missing_hourly:

            raise ValueError(
                "SP3 checkpoint is missing columns: "
                f"{missing_hourly}"
            )

        # -------------------------------------------------
        # Geometry must NOT be present in SP3 analytics.
        # -------------------------------------------------

        if "geometry" in self.hourly_grid_summary.columns:

            raise ValueError(
                "hourly_grid_summary contains geometry. "
                "SP6 requires geometry to remain in the "
                "static Milan grid reference."
            )

        # -------------------------------------------------
        # Counts are deliberately performed once here.
        # -------------------------------------------------

        self.clean_input_count = (
            self.clean_network_df.count()
        )

        self.hourly_input_count = (
            self.hourly_grid_summary.count()
        )

        if self.clean_input_count == 0:

            raise ValueError(
                "SP2 clean checkpoint contains zero rows."
            )

        if self.hourly_input_count == 0:

            raise ValueError(
                "SP3 hourly checkpoint contains zero rows."
            )

        self.logger.info(
            "SP6 input validation passed."
        )

        self.logger.info(
            "SP2 clean rows: %d",
            self.clean_input_count
        )

        self.logger.info(
            "SP3 hourly rows: %d",
            self.hourly_input_count
        )

    # =====================================================
    # 2. Validate hourly grain
    # =====================================================

    def validate_hourly_grain(self):

        duplicates = (
            self.hourly_grid_summary
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

        if duplicates > 0:

            raise ValueError(
                "SP3 hourly_grid_summary contains "
                f"{duplicates} duplicate "
                "(grid_id, timestamp) groups."
            )

        self.logger.info(
            "Hourly grid grain validation passed."
        )

    # =====================================================
    # 3. Write cleaned activity
    # =====================================================

    def write_clean_activity(self):

        """
        Write cleaned country-code-level activity
        as Parquet partitioned by date.

        The data is repartitioned by date before the write
        so that the local writer does not have to handle
        excessively large partitions.

        This does NOT change the logical data or grain.
        """

        self.logger.info(
            "Writing cleaned activity to: %s",
            self.activity_output
        )

        # -------------------------------------------------
        # Select the canonical SP2 columns explicitly.
        #
        # This avoids carrying unnecessary columns through
        # the final write.
        # -------------------------------------------------

        output_df = (
            self.clean_network_df
            .select(
                "timestamp",
                "grid_id",
                "country_code",
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet_activity",
                "input_file_name",
                "date",
                "hour",
                "day_of_week",
                "total_sms",
                "total_calls",
                "total_activity",
            )
        )

        # -------------------------------------------------
        # Repartition by date.
        #
        # The output still uses partitionBy("date"), while
        # the upstream shuffle produces manageable partitions.
        # -------------------------------------------------

        output_df = (
            output_df
            .repartition(
                self.WRITE_PARTITIONS,
                "date"
            )
        )

        (
            output_df
            .write
            .mode("overwrite")
            .partitionBy("date")
            .parquet(
                str(self.activity_output)
            )
        )

        self.logger.info(
            "Clean activity Parquet written."
        )

        return self.activity_output

    # =====================================================
    # 4. Write hourly analytics
    # =====================================================

    def write_hourly_grid_summary(self):

        """
        Write canonical grid/hour analytics.

        Geometry is explicitly excluded.
        """

        output_df = (
            self.hourly_grid_summary
            .select(
                "timestamp",
                "grid_id",
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet_activity",
                "total_sms",
                "total_calls",
                "total_activity",
            )
        )

        self.logger.info(
            "Writing hourly_grid_summary to: %s",
            self.hourly_output
        )

        (
            output_df
            .write
            .mode("overwrite")
            .parquet(
                str(self.hourly_output)
            )
        )

        self.logger.info(
            "Hourly grid summary Parquet written."
        )

        return self.hourly_output

    # =====================================================
    # 5. Dashboard summary
    # =====================================================

    def create_dashboard_summary(self):

        """
        Create a small CSV suitable for inspection.

        One row per timestamp with network-wide KPIs.
        """

        self.dashboard_summary = (
            self.hourly_grid_summary
            .groupBy("timestamp")
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
            .withColumn(
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
            .orderBy("timestamp")
        )

        return self.dashboard_summary

    # =====================================================
    # 6. Write dashboard CSV
    # =====================================================

    def write_dashboard_summary(self):

        """
        Write the dashboard summary as one CSV file.
        """

        temp_output = (
            self.analytics_dir
            / "_dashboard_summary_tmp"
        )

        if temp_output.exists():
            shutil.rmtree(temp_output)

        self.analytics_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        (
            self.dashboard_summary
            .coalesce(1)
            .write
            .mode("overwrite")
            .option(
                "header",
                True
            )
            .csv(
                str(temp_output)
            )
        )

        csv_files = list(
            temp_output.glob("part-*.csv")
        )

        if len(csv_files) != 1:

            raise RuntimeError(
                "Expected exactly one dashboard CSV "
                f"part file, found {len(csv_files)}."
            )

        if self.dashboard_output.exists():
            self.dashboard_output.unlink()

        shutil.move(
            str(csv_files[0]),
            str(self.dashboard_output)
        )

        shutil.rmtree(temp_output)

        self.logger.info(
            "Dashboard summary written to: %s",
            self.dashboard_output
        )

        return self.dashboard_output


    # =====================================================
    # 7. Round-trip validation — activity
    # =====================================================

    def validate_activity_roundtrip(self):

        """
        Read the partitioned activity Parquet back and
        validate row count and schema.

        Because 'date' is written as a partition column,
        Spark reconstructs it at the end of the schema when
        reading the Parquet dataset.

        Therefore schema validation compares column names
        and data types independently of column order.
        """

        loaded = (
            self.spark.read
            .parquet(
                str(self.activity_output)
            )
        )

        count_after = (
            loaded.count()
        )

        # -------------------------------------------------
        # Row count
        # -------------------------------------------------

        if count_after != self.clean_input_count:

            raise ValueError(
                "Activity Parquet round-trip row count "
                "mismatch. "
                f"Before={self.clean_input_count}, "
                f"After={count_after}"
            )

        # -------------------------------------------------
        # Schema comparison
        # -------------------------------------------------

        original_schema = {
            field.name: field.dataType.simpleString()
            for field
            in self.clean_network_df.schema.fields
        }

        loaded_schema = {
            field.name: field.dataType.simpleString()
            for field
            in loaded.schema.fields
        }

        # -------------------------------------------------
        # Check column names
        # -------------------------------------------------

        original_columns = set(
            original_schema.keys()
        )

        loaded_columns = set(
            loaded_schema.keys()
        )

        if original_columns != loaded_columns:

            missing_columns = (
                original_columns
                -
                loaded_columns
            )

            unexpected_columns = (
                loaded_columns
                -
                original_columns
            )

            print("\n" + "=" * 70)
            print("ACTIVITY ROUND-TRIP SCHEMA MISMATCH")
            print("=" * 70)

            print(
                f"Missing columns: "
                f"{sorted(missing_columns)}"
            )

            print(
                f"Unexpected columns: "
                f"{sorted(unexpected_columns)}"
            )

            raise ValueError(
                "Activity Parquet round-trip column "
                "set mismatch."
            )

        # -------------------------------------------------
        # Check data types
        # -------------------------------------------------

        type_mismatches = []

        for column_name in original_columns:

            original_type = (
                original_schema[column_name]
            )

            loaded_type = (
                loaded_schema[column_name]
            )

            if original_type != loaded_type:

                type_mismatches.append(
                    (
                        column_name,
                        original_type,
                        loaded_type
                    )
                )

        if type_mismatches:

            print("\n" + "=" * 70)
            print("ACTIVITY ROUND-TRIP TYPE MISMATCH")
            print("=" * 70)

            for (
                column_name,
                original_type,
                loaded_type
            ) in type_mismatches:

                print(
                    f"{column_name}: "
                    f"original={original_type}, "
                    f"loaded={loaded_type}"
                )

            raise ValueError(
                "Activity Parquet round-trip data "
                "type mismatch."
            )

        # -------------------------------------------------
        # Confirm date partition column
        # -------------------------------------------------

        if "date" not in loaded.columns:

            raise ValueError(
                "Activity Parquet round-trip is missing "
                "the date partition column."
            )

        # -------------------------------------------------
        # Confirm partition directories
        # -------------------------------------------------

        partition_dirs = [
            path
            for path in self.activity_output.glob(
                "date=*"
            )
            if path.is_dir()
        ]

        if not partition_dirs:

            raise ValueError(
                "Activity Parquet output does not contain "
                "date=... partition directories."
            )

        # -------------------------------------------------
        # Report the expected partition-column reorder
        # -------------------------------------------------

        original_order = [
            field.name
            for field
            in self.clean_network_df.schema.fields
        ]

        loaded_order = [
            field.name
            for field
            in loaded.schema.fields
        ]

        if original_order != loaded_order:

            self.logger.info(
                "Activity schema column order changed "
                "during partitioned Parquet round trip."
            )

            self.logger.info(
                "Original column order: %s",
                original_order
            )

            self.logger.info(
                "Loaded column order: %s",
                loaded_order
            )

        self.activity_roundtrip_count = (
            count_after
        )

        self.logger.info(
            "Activity round-trip validation passed."
        )

        self.logger.info(
            "Activity rows before: %d",
            self.clean_input_count
        )

        self.logger.info(
            "Activity rows after: %d",
            count_after
        )

        self.logger.info(
            "Activity schema names and data types "
            "match successfully."
        )

        self.logger.info(
            "Date partitions found: %d",
            len(partition_dirs)
        )

        return True

    # =====================================================
    # 8. Round-trip validation — hourly
    # =====================================================

    def validate_hourly_roundtrip(self):

        loaded = (
            self.spark.read
            .parquet(
                str(self.hourly_output)
            )
        )

        count_after = loaded.count()

        if count_after != self.hourly_input_count:

            raise ValueError(
                "Hourly Parquet round-trip row count "
                "mismatch. "
                f"Before={self.hourly_input_count}, "
                f"After={count_after}"
            )

        expected_columns = [
            "timestamp",
            "grid_id",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_sms",
            "total_calls",
            "total_activity",
        ]

        original_schema = (
            self.hourly_grid_summary
            .select(*expected_columns)
            .schema
        )

        loaded_schema = loaded.schema

        if original_schema != loaded_schema:

            raise ValueError(
                "Hourly Parquet round-trip schema mismatch."
            )

        duplicates = (
            loaded
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

        if duplicates != 0:

            raise ValueError(
                "Hourly Parquet round-trip contains "
                f"{duplicates} duplicate grid/hour groups."
            )

        if "geometry" in loaded.columns:

            raise ValueError(
                "Geometry exists in hourly_grid_summary "
                "after round trip."
            )

        self.hourly_roundtrip_count = count_after

        self.logger.info(
            "Hourly round-trip validation passed."
        )

        return True

    # =====================================================
    # 9. File-size helper
    # =====================================================

    @staticmethod
    def calculate_directory_size(path):

        path = Path(path)

        if not path.exists():
            return 0

        total = 0

        for file in path.rglob("*"):

            if file.is_file():

                total += file.stat().st_size

        return total

    # =====================================================
    # 10. File-size comparison
    # =====================================================

    def compare_csv_and_parquet_sizes(self):

        """
        Compare a CSV representation of hourly analytics
        against the Parquet representation.
        """

        comparison_dir = (
            self.analytics_dir
            / "_hourly_csv_comparison"
        )

        if comparison_dir.exists():
            shutil.rmtree(comparison_dir)

        hourly_for_csv = (
            self.hourly_grid_summary
            .select(
                "timestamp",
                "grid_id",
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet_activity",
                "total_sms",
                "total_calls",
                "total_activity",
            )
        )

        (
            hourly_for_csv
            .write
            .mode("overwrite")
            .option(
                "header",
                True
            )
            .csv(
                str(comparison_dir)
            )
        )

        csv_size = (
            self.calculate_directory_size(
                comparison_dir
            )
        )

        parquet_size = (
            self.calculate_directory_size(
                self.hourly_output
            )
        )

        self.csv_size_bytes = csv_size
        self.parquet_size_bytes = parquet_size

        ratio = (
            csv_size / parquet_size
            if parquet_size > 0
            else None
        )

        print("\n" + "=" * 70)
        print("FILE SIZE COMPARISON")
        print("=" * 70)

        print(
            f"CSV representation    : "
            f"{csv_size:,} bytes"
        )

        print(
            f"Parquet representation: "
            f"{parquet_size:,} bytes"
        )

        if ratio is not None:

            print(
                f"CSV / Parquet ratio   : "
                f"{ratio:.2f}x"
            )

        shutil.rmtree(comparison_dir)

        self.logger.info(
            "CSV/Parquet size comparison completed. "
            "CSV=%d bytes, Parquet=%d bytes",
            csv_size,
            parquet_size
        )

        return {
            "csv_bytes": csv_size,
            "parquet_bytes": parquet_size,
            "csv_to_parquet_ratio": ratio,
        }

    # =====================================================
    # 11. Validate processed partitioning
    # =====================================================

    def validate_date_partitioning(self):

        if not self.activity_output.exists():

            raise FileNotFoundError(
                "Activity output does not exist."
            )

        partition_dirs = sorted(
            path
            for path in self.activity_output.glob("date=*")
            if path.is_dir()
        )

        if len(partition_dirs) == 0:

            raise ValueError(
                "No date=... partition directories found "
                "under processed/activity."
            )

        partition_names = [
            path.name
            for path in partition_dirs
        ]

        print("\n" + "=" * 70)
        print("DATE PARTITION VALIDATION")
        print("=" * 70)

        print(
            f"Date partitions found: "
            f"{len(partition_names)}"
        )

        for name in partition_names:
            print(f"  {name}")

        self.logger.info(
            "Date partition validation passed. "
            "Partitions=%d",
            len(partition_names)
        )

        return partition_names

    # =====================================================
    # 12. Validate reference GeoJSON
    # =====================================================

    def validate_reference_geojson(self):

        geojson_path = (
            self.reference_dir
            / "milano-grid.geojson"
        )

        if not geojson_path.exists():

            raise FileNotFoundError(
                "Static Milan grid reference not found: "
                f"{geojson_path}"
            )

        self.logger.info(
            "Static GeoJSON reference found: %s",
            geojson_path
        )

        return geojson_path

    # =====================================================
    # 13. Acceptance criteria
    # =====================================================

    def validate_acceptance_criteria(self):

        print("\n" + "=" * 70)
        print("SP6 ACCEPTANCE CRITERIA")
        print("=" * 70)

        results = {}

        # -------------------------------------------------
        # 1. Round-trip
        # -------------------------------------------------

        print(
            "\n1. ROUND-TRIP ROW COUNT + SCHEMA"
        )

        criterion_1 = (
            self.activity_roundtrip_count
            ==
            self.clean_input_count
            and
            self.hourly_roundtrip_count
            ==
            self.hourly_input_count
        )

        results[
            "roundtrip_schema_and_counts"
        ] = criterion_1

        print(
            f"SP2 before : "
            f"{self.clean_input_count:,}"
        )

        print(
            f"SP2 after  : "
            f"{self.activity_roundtrip_count:,}"
        )

        print(
            f"SP3 before : "
            f"{self.hourly_input_count:,}"
        )

        print(
            f"SP3 after  : "
            f"{self.hourly_roundtrip_count:,}"
        )

        print(
            "PASS"
            if criterion_1
            else "FAIL"
        )

        # -------------------------------------------------
        # 2. Duplicates
        # -------------------------------------------------

        print(
            "\n2. ZERO DUPLICATES AFTER ROUND TRIP"
        )

        loaded_hourly = (
            self.spark.read
            .parquet(
                str(self.hourly_output)
            )
        )

        duplicates = (
            loaded_hourly
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

        criterion_2 = (
            duplicates == 0
        )

        results[
            "zero_duplicates"
        ] = criterion_2

        print(
            f"Duplicate groups: "
            f"{duplicates}"
        )

        print(
            "PASS"
            if criterion_2
            else "FAIL"
        )

        # -------------------------------------------------
        # 3. Date partitioning
        # -------------------------------------------------

        print(
            "\n3. DATE PARTITIONING"
        )

        partition_dirs = [
            path
            for path in self.activity_output.glob("date=*")
            if path.is_dir()
        ]

        criterion_3 = (
            len(partition_dirs) > 0
        )

        results[
            "date_partitioning"
        ] = criterion_3

        print(
            f"Date partitions: "
            f"{len(partition_dirs)}"
        )

        print(
            "PASS"
            if criterion_3
            else "FAIL"
        )

        # -------------------------------------------------
        # 4. No geometry
        # -------------------------------------------------

        print(
            "\n4. NO GEOMETRY IN HOURLY ANALYTICS"
        )

        criterion_4 = (
            "geometry"
            not in loaded_hourly.columns
        )

        results[
            "no_geometry"
        ] = criterion_4

        print(
            f"Geometry present: "
            f"{'YES' if not criterion_4 else 'NO'}"
        )

        print(
            "PASS"
            if criterion_4
            else "FAIL"
        )

        # -------------------------------------------------
        # 5. File-size comparison
        # -------------------------------------------------

        print(
            "\n5. CSV / PARQUET SIZE COMPARISON"
        )

        criterion_5 = (
            self.csv_size_bytes is not None
            and
            self.parquet_size_bytes is not None
            and
            self.csv_size_bytes > 0
            and
            self.parquet_size_bytes > 0
        )

        results[
            "file_size_comparison"
        ] = criterion_5

        print(
            f"CSV bytes    : "
            f"{self.csv_size_bytes:,}"
        )

        print(
            f"Parquet bytes: "
            f"{self.parquet_size_bytes:,}"
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

        print("\n" + "=" * 70)

        if all_passed:

            print(
                "SP6 AUTOMATED ACCEPTANCE: ALL PASS"
            )

        else:

            print(
                "SP6 AUTOMATED ACCEPTANCE: FAILED"
            )

        print("=" * 70)

        self.logger.info(
            "SP6 acceptance: %s",
            "ALL PASS"
            if all_passed
            else "FAILED"
        )

        return {
            "all_passed": all_passed,
            "criteria": results,
        }

    # =====================================================
    # 14. Export SP6 report
    # =====================================================

    def export_report(
        self,
        size_comparison,
        partition_names
    ):

        report = {

            "input": {
                "sp2_clean_rows":
                    self.clean_input_count,

                "sp3_hourly_rows":
                    self.hourly_input_count
            },

            "outputs": {
                "activity":
                    str(self.activity_output),

                "hourly_grid_summary":
                    str(self.hourly_output),

                "dashboard_summary":
                    str(self.dashboard_output),

                "static_reference":
                    str(
                        self.reference_dir
                        /
                        "milano-grid.geojson"
                    )
            },

            "roundtrip": {
                "activity_rows":
                    self.activity_roundtrip_count,

                "hourly_rows":
                    self.hourly_roundtrip_count
            },

            "date_partitions":
                partition_names,

            "file_size_comparison":
                size_comparison,

            "acceptance":
                self.acceptance
        }

        report_path = (
            self.analytics_dir
            / "sp6_storage_report.json"
        )

        self.analytics_dir.mkdir(
            parents=True,
            exist_ok=True
        )

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
            "SP6 report written to: %s",
            report_path
        )

        return report_path

    # =====================================================
    # 15. Full process
    # =====================================================

    def process(self):

        self.logger.info(
            "Starting SP6 storage pipeline."
        )

        # -------------------------------------------------
        # Validation
        # -------------------------------------------------

        self.validate_inputs()

        self.validate_hourly_grain()

        self.validate_reference_geojson()

        # -------------------------------------------------
        # Writes
        # -------------------------------------------------

        self.write_clean_activity()

        self.write_hourly_grid_summary()

        self.create_dashboard_summary()

        self.write_dashboard_summary()

        # -------------------------------------------------
        # Round trips
        # -------------------------------------------------

        self.validate_activity_roundtrip()

        self.validate_hourly_roundtrip()

        # -------------------------------------------------
        # Disk validation
        # -------------------------------------------------

        partition_names = (
            self.validate_date_partitioning()
        )

        # -------------------------------------------------
        # File-size comparison
        # -------------------------------------------------

        size_comparison = (
            self.compare_csv_and_parquet_sizes()
        )

        # -------------------------------------------------
        # Acceptance
        # -------------------------------------------------

        self.acceptance = (
            self.validate_acceptance_criteria()
        )

        if not self.acceptance["all_passed"]:

            self.logger.error(
                "SP6 acceptance criteria failed."
            )

            raise ValueError(
                "SP6 acceptance criteria failed."
            )

        # -------------------------------------------------
        # Report
        # -------------------------------------------------

        report_path = (
            self.export_report(
                size_comparison,
                partition_names
            )
        )

        self.logger.info(
            "SP6 storage pipeline completed successfully."
        )

        self.logger.info(
            "Log file: %s",
            self.log_file
        )

        return {

            "activity_output":
                self.activity_output,

            "hourly_output":
                self.hourly_output,

            "dashboard_output":
                self.dashboard_output,

            "report":
                report_path,

            "size_comparison":
                size_comparison,

            "acceptance":
                self.acceptance,

            "log_file":
                self.log_file
        }
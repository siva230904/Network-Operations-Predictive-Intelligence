
# =========================================================
# SP7 — Reusable Spark ETL Job
# File: spark/telecom_pipeline.py
# =========================================================

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
# ---------------------------------------------------------
# Reuse existing SP2 / SP3 / SP4 implementations.
#
# Adjust these imports only if your project filenames differ.
# ---------------------------------------------------------

# =========================================================
# Reuse SP2 / SP3 / SP4 implementations
# =========================================================

SPARK_DIR = (
    Path(__file__)
    .resolve()
    .parent
)

PROJECT_ROOT = (
    SPARK_DIR
    .parents[1]
)

PHASE2_DIR = (
    PROJECT_ROOT
    / "phase2"
)

SP2_DIR = (
    PHASE2_DIR
    / "sp2"
)

SP3_DIR = (
    PHASE2_DIR
    / "sp3"
)

SP4_DIR = (
    PHASE2_DIR
    / "sp4"
)

for directory in [
    SP2_DIR,
    SP3_DIR,
    SP4_DIR,
]:

    directory_string = str(
        directory
    )

    if directory_string not in sys.path:

        sys.path.insert(
            0,
            directory_string
        )


from cleaning2 import NetworkCleaner
from aggregation2 import NetworkAggregator
from enrichment3 import NetworkGeoEnricher


# =========================================================
# Logging
# =========================================================

def create_logger(log_dir):
    """
    Create a unique SP7 execution log.

    Existing logs are never overwritten.
    """

    log_path = Path(log_dir)

    log_path.mkdir(
        parents=True,
        exist_ok=True
    )

    counter = 1

    while True:

        log_file = (
            log_path
            /
            f"sp7_telecom_pipeline_run_{counter:03d}.log"
        )

        if not log_file.exists():
            break

        counter += 1

    logger = logging.getLogger(
        f"SP7_TelecomPipeline_{counter}"
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

    logger.addHandler(
        handler
    )

    return logger, log_file


# =========================================================
# Reusable Telecom ETL Job
# =========================================================

class TelecomPipeline:
    """
    SP7 — Reusable production-style Spark ETL job.

    Pipeline:

        raw daily CSV files
                |
                v
            read_raw()
                |
                v
              clean()
                |
                v
            aggregate()
                |
                v
             enrich()
                |
                v
          write_outputs()

    Existing SP2, SP3 and SP4 implementations are reused.

    IMPORTANT:

        SP2 retains:

            timestamp + grid_id + country_code

        SP3 performs the country-code aggregation BEFORE
        operational grid/hour analytics.

        SP4 uses:

            milano-grid.geojson

        as the static grid reference.

    The main reusable job intentionally avoids expensive
    SP4 laboratory diagnostics such as:

        explain_join_strategies()
        geographic_spot_check()
        compute_top_grids()

    unless explicitly requested elsewhere.

    This keeps the production-style job smaller and safer
    for a 16 GB RAM local Spark environment.
    """

    # =====================================================
    # Constructor
    # =====================================================

    def __init__(
        self,
        spark,
        input_path,
        output_path,
        reference_path,
        log_dir
    ):

        if spark is None:
            raise ValueError(
                "spark cannot be None."
            )

        if input_path is None:
            raise ValueError(
                "input_path is required."
            )

        if output_path is None:
            raise ValueError(
                "output_path is required."
            )

        if reference_path is None:
            raise ValueError(
                "reference_path is required."
            )

        self.spark = spark

        self.input_path = Path(
            input_path
        )

        self.output_path = Path(
            output_path
        )

        self.reference_path = Path(
            reference_path
        )

        self.log_dir = Path(
            log_dir
        )

        self.logger, self.log_file = (
            create_logger(
                self.log_dir
            )
        )

        # -------------------------------------------------
        # DataFrames
        # -------------------------------------------------

        self.raw_df = None
        self.clean_df = None
        self.hourly_df = None
        self.enriched_df = None

        # -------------------------------------------------
        # SP2 / SP3 / SP4 objects
        # -------------------------------------------------

        self.cleaner = None
        self.aggregator = None
        self.enricher = None

        # -------------------------------------------------
        # Metrics
        # -------------------------------------------------

        self.input_rows = 0
        self.rejected_rows = 0
        self.nulls_handled = 0
        self.clean_rows = 0
        self.output_rows = 0

        self.start_time = None
        self.end_time = None

        self.status = "NOT_STARTED"

    # =====================================================
    # 1. Input validation
    # =====================================================

    def validate_input_path(self):
        """
        Fail fast if the input path does not exist or
        contains no daily CSV files.
        """

        if not self.input_path.exists():

            raise FileNotFoundError(
                "Input path does not exist: "
                f"{self.input_path}"
            )

        if not self.input_path.is_dir():

            raise ValueError(
                "Input path must be a directory: "
                f"{self.input_path}"
            )

        csv_files = sorted(
            self.input_path.glob("*.csv")
        )

        if len(csv_files) == 0:

            raise FileNotFoundError(
                "No daily activity CSV files are present "
                f"in input folder: {self.input_path}"
            )

        self.logger.info(
            "Input validation passed."
        )

        self.logger.info(
            "Input folder: %s",
            self.input_path
        )

        self.logger.info(
            "Daily CSV files detected: %d",
            len(csv_files)
        )

        for csv_file in csv_files:

            self.logger.info(
                "Input file: %s",
                csv_file.name
            )

        return csv_files

    # =====================================================
    # 2. read_raw()
    # =====================================================

    def read_raw(self):
        """
        Read all supplied daily CSV files.

        No transformation is performed here.

        Returns:
            raw Spark DataFrame
        """

        csv_files = (
            self.validate_input_path()
        )

        self.logger.info(
            "READ_RAW_START"
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
                str(self.input_path)
            )
        )

        # -------------------------------------------------
        # Count only once at this stage.
        # -------------------------------------------------

        self.input_rows = (
            self.raw_df.count()
        )

        if self.input_rows == 0:

            raise ValueError(
                "Input files were found, but they contain "
                "zero data rows."
            )

        self.logger.info(
            "input_rows=%d",
            self.input_rows
        )

        self.logger.info(
            "input_files=%d",
            len(csv_files)
        )

        self.logger.info(
            "READ_RAW_END"
        )

        return self.raw_df

    # =====================================================
    # 3. clean()
    # =====================================================

    def clean(self):
        """
        Reuse the existing SP2 NetworkCleaner.

        No cleaning rules are duplicated here.
        """

        if self.raw_df is None:

            raise RuntimeError(
                "Run read_raw() before clean()."
            )

        self.logger.info(
            "CLEAN_START"
        )

        self.cleaner = NetworkCleaner(
            spark=self.spark,
            raw_network_df=self.raw_df,
            output_dir=(
                self.output_path
                /
                "checkpoints"
                /
                "sp2"
            ),
            log_dir=self.log_dir
        )

        # -------------------------------------------------
        # Reuse SP2 transformation sequence.
        # -------------------------------------------------

        self.cleaner.load_data()

        self.cleaner.standardize_types()

        self.cleaner.validate_hourly_cadence()

        self.cleaner.identify_rejected_rows()

        self.cleaner.clean_data()

        self.cleaner.derive_features()

        self.cleaner.validate_final_rows()

        self.cleaner.validate_raw_grain()

        self.clean_df = (
            self.cleaner.clean_network_df
        )

        self.rejected_rows = (
            self.cleaner.rejected_row_count
        )

        self.nulls_handled = (
            self.cleaner.nulls_handled
        )

        self.clean_rows = (
            self.cleaner.final_row_count
        )

        self.logger.info(
            "rejected_rows=%d",
            self.rejected_rows
        )

        self.logger.info(
            "nulls_handled=%d",
            self.nulls_handled
        )

        self.logger.info(
            "clean_rows=%d",
            self.clean_rows
        )

        self.logger.info(
            "CLEAN_END"
        )

        return self.clean_df

    # =====================================================
    # 4. aggregate()
    # =====================================================

    def aggregate(self):
        """
        Reuse the existing SP3 NetworkAggregator.

        CRITICAL PROJECT RULE:

            country-code aggregation happens FIRST.

        The transformation is:

            timestamp + grid_id + country_code

                    ↓

            timestamp + grid_id

        No country-code aggregation is reimplemented here.
        """

        if self.clean_df is None:

            raise RuntimeError(
                "Run clean() before aggregate()."
            )

        self.logger.info(
            "AGGREGATE_START"
        )

        self.aggregator = NetworkAggregator(
            clean_network_df=self.clean_df,
            log_dir=self.log_dir
        )

        # -------------------------------------------------
        # Reuse SP3 transformation sequence.
        # -------------------------------------------------

        self.aggregator.validate_input()

        self.aggregator.aggregate_to_grid_hour()

        self.aggregator.derive_activity_features()

        self.aggregator.derive_time_features()

        self.aggregator.validate_grain()

        self.hourly_df = (
            self.aggregator.hourly_grid_summary
        )

        self.logger.info(
            "output_rows_after_aggregation=%d",
            self.aggregator.hourly_row_count
        )

        self.logger.info(
            "AGGREGATE_END"
        )

        return self.hourly_df

    # =====================================================
    # 5. enrich()
    # =====================================================

    def enrich(self):
        """
        Reuse the existing SP4 NetworkGeoEnricher.

        The static Milan grid is loaded from the configurable
        reference path.

        A broadcast join is used because the reference
        contains only 10,000 grid records.
        """

        if self.hourly_df is None:

            raise RuntimeError(
                "Run aggregate() before enrich()."
            )

        if not self.reference_path.exists():

            raise FileNotFoundError(
                "Reference GeoJSON not found: "
                f"{self.reference_path}"
            )

        self.logger.info(
            "ENRICH_START"
        )

        self.enricher = NetworkGeoEnricher(
            spark=self.spark,
            hourly_grid_summary=self.hourly_df,
            geojson_path=self.reference_path,
            log_dir=self.log_dir
        )

        # -------------------------------------------------
        # Reuse SP4 validation and lookup creation.
        # -------------------------------------------------

        self.enricher.validate_inputs()

        self.enricher.build_grid_lookup()

        # -------------------------------------------------
        # Do NOT call explain_join_strategies().
        #
        # It is a teaching/inspection operation, not part
        # of the reusable production ETL.
        # -------------------------------------------------

        self.enricher.enrich()

        self.enricher.validate_coverage()

        self.enricher.validate_final_schema()

        self.enriched_df = (
            self.enricher.grid_activity_geo_df
        )

        self.output_rows = (
            self.enricher.output_row_count
        )

        self.logger.info(
            "output_rows=%d",
            self.output_rows
        )

        self.logger.info(
            "ENRICH_END"
        )

        return self.enriched_df

    # =====================================================
    # 6. write_outputs()
    # =====================================================

    def write_outputs(self):
        """
        Write the reusable ETL outputs.

        Outputs:

            output/
                processed/
                    activity/
                analytics/
                    hourly_grid_summary/
                    dashboard_summary/
                reference/
                    milano-grid.geojson

        The static reference itself is not copied or rewritten.

        Geometry is retained in the enriched operational
        output, while the hourly analytics output remains
        geometry-free.
        """

        if self.clean_df is None:

            raise RuntimeError(
                "Clean output does not exist."
            )

        if self.hourly_df is None:

            raise RuntimeError(
                "Hourly aggregation output does not exist."
            )

        if self.enriched_df is None:

            raise RuntimeError(
                "Enriched output does not exist."
            )

        self.logger.info(
            "WRITE_OUTPUTS_START"
        )

        processed_dir = (
            self.output_path
            /
            "processed"
        )

        analytics_dir = (
            self.output_path
            /
            "analytics"
        )

        activity_output = (
            processed_dir
            /
            "activity"
        )

        hourly_output = (
            analytics_dir
            /
            "hourly_grid_summary"
        )

        enriched_output = (
            processed_dir
            /
            "grid_activity_geo"
        )

        dashboard_output = (
            analytics_dir
            /
            "dashboard_summary"
        )

        processed_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        analytics_dir.mkdir(
            parents=True,
            exist_ok=True
        )

        # -------------------------------------------------
        # 1. Clean country-code-level activity
        #
        # Partition by date to keep downstream access
        # manageable.
        # -------------------------------------------------

        (
            self.clean_df
            .write
            .mode("overwrite")
            .partitionBy("date")
            .parquet(
                str(activity_output)
            )
        )

        self.logger.info(
            "Wrote clean activity: %s",
            activity_output
        )

        # -------------------------------------------------
        # 2. Geometry-free hourly analytics
        #
        # This is the operational analytics checkpoint.
        # -------------------------------------------------

        hourly_output_df = (
            self.hourly_df
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
                "internet_share",
                "date",
                "hour",
                "day_of_week"
            )
        )

        (
            hourly_output_df
            .write
            .mode("overwrite")
            .parquet(
                str(hourly_output)
            )
        )

        self.logger.info(
            "Wrote hourly analytics: %s",
            hourly_output
        )

        # -------------------------------------------------
        # 3. Enriched geospatial output
        #
        # Geometry is kept here because this is the
        # dedicated geospatial product.
        #
        # Avoid coalesce(1) and CSV for the full 1.6M-row
        # enriched dataset.
        # -------------------------------------------------

        (
            self.enriched_df
            .write
            .mode("overwrite")
            .parquet(
                str(enriched_output)
            )
        )

        self.logger.info(
            "Wrote geospatial output: %s",
            enriched_output
        )

        # -------------------------------------------------
        # 4. Dashboard summary
        #
        # Build this from geometry-free hourly data.
        # -------------------------------------------------

        dashboard_df = (
            self.hourly_df
            .groupBy("timestamp")
            .agg(
                F.sum("total_sms").alias("total_sms"),
                F.sum("total_calls").alias("total_calls"),
                F.sum("internet_activity").alias(
                    "internet_activity"
                ),
                F.sum("total_activity").alias(
                    "total_activity"
                ),
                F.countDistinct("grid_id").alias(
                    "active_grids"
                )
            )
            .orderBy("timestamp")
        )

        (
            dashboard_df
            .write
            .mode("overwrite")
            .option(
                "header",
                True
            )
            .csv(
                str(dashboard_output)
            )
        )

        self.logger.info(
            "Wrote dashboard summary: %s",
            dashboard_output
        )

        self.logger.info(
            "WRITE_OUTPUTS_END"
        )

        return {
            "activity":
                activity_output,

            "hourly_grid_summary":
                hourly_output,

            "grid_activity_geo":
                enriched_output,

            "dashboard_summary":
                dashboard_output
        }

    # =====================================================
    # 7. Final reconciliation
    # =====================================================

    def validate_reconciliation(self):
        """
        Validate the main row-count contract.

        Input:

            raw rows

        Clean:

            input - rejected

        Aggregated:

            <= clean rows

        Enriched:

            same row count as hourly aggregation
        """

        expected_clean = (
            self.input_rows
            -
            self.rejected_rows
        )

        if self.clean_rows != expected_clean:

            raise ValueError(
                "Clean row reconciliation failed. "
                f"Input={self.input_rows}, "
                f"Rejected={self.rejected_rows}, "
                f"Clean={self.clean_rows}, "
                f"Expected={expected_clean}"
            )

        if self.output_rows != (
            self.aggregator.hourly_row_count
        ):

            raise ValueError(
                "Enrichment row reconciliation failed. "
                f"Hourly={self.aggregator.hourly_row_count}, "
                f"Enriched={self.output_rows}"
            )

        self.logger.info(
            "Row reconciliation passed."
        )

        return True

    # =====================================================
    # 8. run()
    # =====================================================

    def run(self):
        """
        Execute the complete reusable ETL job.
        """

        self.start_time = (
            datetime.now()
        )

        self.status = "RUNNING"

        self.logger.info(
            "=================================================="
        )

        self.logger.info(
            "SP7 TELECOM PIPELINE START"
        )

        self.logger.info(
            "start_time=%s",
            self.start_time.isoformat()
        )

        self.logger.info(
            "input_path=%s",
            self.input_path
        )

        self.logger.info(
            "output_path=%s",
            self.output_path
        )

        self.logger.info(
            "reference_path=%s",
            self.reference_path
        )

        try:

            # -------------------------------------------------
            # ETL
            # -------------------------------------------------

            self.read_raw()

            self.clean()

            self.aggregate()

            self.enrich()

            self.validate_reconciliation()

            outputs = (
                self.write_outputs()
            )

            # -------------------------------------------------
            # Completion
            # -------------------------------------------------

            self.end_time = (
                datetime.now()
            )

            self.status = "SUCCESS"

            self.logger.info(
                "output_rows=%d",
                self.output_rows
            )

            self.logger.info(
                "end_time=%s",
                self.end_time.isoformat()
            )

            self.logger.info(
                "status=SUCCESS"
            )

            self.logger.info(
                "SP7 TELECOM PIPELINE COMPLETED"
            )

            self.logger.info(
                "=================================================="
            )

            return {
                "status":
                    self.status,

                "input_rows":
                    self.input_rows,

                "rejected_rows":
                    self.rejected_rows,

                "nulls_handled":
                    self.nulls_handled,

                "clean_rows":
                    self.clean_rows,

                "output_rows":
                    self.output_rows,

                "outputs":
                    outputs,

                "start_time":
                    self.start_time,

                "end_time":
                    self.end_time,

                "log_file":
                    self.log_file
            }

        except Exception as exc:

            self.end_time = (
                datetime.now()
            )

            self.status = "FAILED"

            self.logger.exception(
                "SP7 pipeline failed."
            )

            self.logger.error(
                "input_rows=%d",
                self.input_rows
            )

            self.logger.error(
                "rejected_rows=%d",
                self.rejected_rows
            )

            self.logger.error(
                "nulls_handled=%d",
                self.nulls_handled
            )

            self.logger.error(
                "output_rows=%d",
                self.output_rows
            )

            self.logger.error(
                "end_time=%s",
                self.end_time.isoformat()
            )

            self.logger.error(
                "status=FAILED"
            )

            raise


# =========================================================
# 9. main()
# =========================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "SP7 reusable Spark telecom ETL job."
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help=(
            "Directory containing daily activity CSV files."
        )
    )

    parser.add_argument(
        "--output",
        required=True,
        help=(
            "Root output directory."
        )
    )

    parser.add_argument(
        "--reference",
        required=True,
        help=(
            "Path to milano-grid.geojson."
        )
    )

    parser.add_argument(
        "--log-dir",
        required=True,
        help=(
            "Directory for the SP7 execution log."
        )
    )

    args = parser.parse_args()

    # -----------------------------------------------------
    # Python worker configuration
    # -----------------------------------------------------

    python_executable = sys.executable

    import os

    os.environ[
        "PYSPARK_PYTHON"
    ] = python_executable

    os.environ[
        "PYSPARK_DRIVER_PYTHON"
    ] = python_executable

    # -----------------------------------------------------
    # Spark
    # -----------------------------------------------------

    spark = (
        SparkSession
        .builder
        .appName(
            "SP7-Reusable-Telecom-ETL"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )
        .config(
            "spark.default.parallelism",
            "8"
        )
        .config(
            "spark.driver.memory",
            "4g"
        )
        .config(
            "spark.executor.memory",
            "4g"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    pipeline = None

    try:

        pipeline = TelecomPipeline(
            spark=spark,
            input_path=args.input,
            output_path=args.output,
            reference_path=args.reference,
            log_dir=args.log_dir
        )

        result = (
            pipeline.run()
        )

        print("\n" + "=" * 70)
        print("SP7 TELECOM PIPELINE")
        print("=" * 70)

        print(
            f"Status          : {result['status']}"
        )

        print(
            f"Input rows      : "
            f"{result['input_rows']:,}"
        )

        print(
            f"Rejected rows   : "
            f"{result['rejected_rows']:,}"
        )

        print(
            f"Nulls handled   : "
            f"{result['nulls_handled']:,}"
        )

        print(
            f"Clean rows      : "
            f"{result['clean_rows']:,}"
        )

        print(
            f"Output rows     : "
            f"{result['output_rows']:,}"
        )

        print(
            f"Start time      : "
            f"{result['start_time']}"
        )

        print(
            f"End time        : "
            f"{result['end_time']}"
        )

        print(
            "\nOutputs:"
        )

        for name, path in result[
            "outputs"
        ].items():

            print(
                f"  {name}: {path}"
            )

        print(
            f"\nExecution log:\n"
            f"  {result['log_file']}"
        )

        print(
            "\n" + "=" * 70
        )
        print(
            "SP7 RUN FINISHED SUCCESSFULLY"
        )
        print(
            "=" * 70
        )

        return 0

    except Exception as exc:

        print(
            "\n" + "=" * 70,
            file=sys.stderr
        )

        print(
            "SP7 RUN FAILED",
            file=sys.stderr
        )

        print(
            f"Reason: {exc}",
            file=sys.stderr
        )

        if pipeline is not None:

            print(
                f"Log: {pipeline.log_file}",
                file=sys.stderr
            )

        print(
            "=" * 70,
            file=sys.stderr
        )

        return 1

    finally:

        spark.stop()


# =========================================================
# Script entry point
# =========================================================

if __name__ == "__main__":

    sys.exit(
        main()
    )


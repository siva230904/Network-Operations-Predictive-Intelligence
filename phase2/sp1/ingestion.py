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
    count,
    countDistinct,
    input_file_name,
    to_timestamp,
)


class NetworkIngestion:
    """
    PySpark ingestion module for the Milan mobile activity data.

    Input grain:
        timestamp + grid_id + country_code

    The raw country-code-level grain is preserved.
    """

    # =====================================================
    # Manual schema
    # =====================================================

    RAW_SCHEMA = StructType([
        StructField(
            "datetime",
            TimestampType(),
            True
        ),

        StructField(
            "CellID",
            IntegerType(),
            True
        ),

        StructField(
            "countrycode",
            IntegerType(),
            True
        ),

        StructField(
            "smsin",
            DoubleType(),
            True
        ),

        StructField(
            "smsout",
            DoubleType(),
            True
        ),

        StructField(
            "callin",
            DoubleType(),
            True
        ),

        StructField(
            "callout",
            DoubleType(),
            True
        ),

        StructField(
            "internet",
            DoubleType(),
            True
        ),
    ])

    # =====================================================
    # Constructor
    # =====================================================

    def __init__(
        self,
        input_dir,
        app_name="SP1_Network_Ingestion",
    ):

        self.input_dir = Path(
            input_dir
        )

        self.app_name = app_name

        self.spark = None
        self.raw_network_df = None

    # =====================================================
    # 1. Create SparkSession
    # =====================================================

    def create_spark_session(self):
        """
        Create the SparkSession used by SP1.
        """

        self.spark = (
            SparkSession.builder
            .appName(self.app_name)
            .getOrCreate()
        )

        return self.spark

    # =====================================================
    # 2. Read raw files
    # =====================================================

    def read_data(self):
        """
        Read all Milan daily CSV files.

        IMPORTANT:
            The -mi- portion of the filename pattern
            is intentional and must not be removed.
        """

        if self.spark is None:
            raise RuntimeError(
                "Run create_spark_session() first."
            )

        if not self.input_dir.exists():
            raise FileNotFoundError(
                f"Input directory not found: "
                f"{self.input_dir}"
            )

        # -------------------------------------------------
        # Required Milan-only pattern
        # -------------------------------------------------

        file_pattern = str(
            self.input_dir /
            "sms-call-internet-mi-*.csv"
        )

        print(
            f"Reading files using pattern:\n"
            f"{file_pattern}"
        )

        self.raw_network_df = (
            self.spark.read
            .option("header", True)
            .schema(self.RAW_SCHEMA)
            .csv(file_pattern)
        )

        return self.raw_network_df

    # =====================================================
    # 3. Add traceability
    # =====================================================

    def add_traceability(self):
        """
        Add the source filename to every raw record.
        """

        if self.raw_network_df is None:
            raise RuntimeError(
                "Run read_data() first."
            )

        self.raw_network_df = (
            self.raw_network_df
            .withColumn(
                "source_file",
                input_file_name()
            )
        )

        return self.raw_network_df

    # =====================================================
    # 4. Canonicalize columns
    # =====================================================

    def canonicalize(self):
        """
        Rename raw columns to the canonical project schema.

        Country-code grain is preserved.
        """

        if self.raw_network_df is None:
            raise RuntimeError(
                "Run read_data() first."
            )

        self.raw_network_df = (
            self.raw_network_df
            .withColumnRenamed(
                "datetime",
                "timestamp"
            )
            .withColumnRenamed(
                "CellID",
                "grid_id"
            )
            .withColumnRenamed(
                "countrycode",
                "country_code"
            )
            .withColumnRenamed(
                "smsin",
                "sms_in"
            )
            .withColumnRenamed(
                "smsout",
                "sms_out"
            )
            .withColumnRenamed(
                "callin",
                "call_in"
            )
            .withColumnRenamed(
                "callout",
                "call_out"
            )
            .withColumnRenamed(
                "internet",
                "internet_activity"
            )
        )

        return self.raw_network_df

    # =====================================================
    # 5. Validate schema
    # =====================================================

    def validate_schema(self):
        """
        Check that the loaded DataFrame has the expected
        canonical fields and Spark data types.
        """

        if self.raw_network_df is None:
            raise RuntimeError(
                "Load the data before validating."
            )

        expected_columns = [
            "timestamp",
            "grid_id",
            "country_code",
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "source_file",
        ]

        actual_columns = (
            self.raw_network_df.columns
        )

        missing_columns = [
            column
            for column in expected_columns
            if column not in actual_columns
        ]

        if missing_columns:
            raise ValueError(
                f"Missing columns: "
                f"{missing_columns}"
            )

        print("\nValidated schema:")
        self.raw_network_df.printSchema()

        return True

    # =====================================================
    # 6. Profile input
    # =====================================================

    def profile(self):
        """
        Produce the SP1 ingestion/profile metrics.
        """

        if self.raw_network_df is None:
            raise RuntimeError(
                "Load the data before profiling."
            )

        df = self.raw_network_df

        # -------------------------------------------------
        # Row count
        # -------------------------------------------------

        row_count = df.count()

        # -------------------------------------------------
        # Source file count
        # -------------------------------------------------

        file_count = (
            df.select("source_file")
            .distinct()
            .count()
        )

        # -------------------------------------------------
        # Unique grids
        # -------------------------------------------------

        unique_grids = (
            df.select("grid_id")
            .distinct()
            .count()
        )

        # -------------------------------------------------
        # Country-code categories
        # -------------------------------------------------

        country_codes = (
            df.select("country_code")
            .distinct()
            .count()
        )

        # -------------------------------------------------
        # Distinct hourly intervals
        # -------------------------------------------------

        hourly_intervals = (
            df.select("timestamp")
            .distinct()
            .count()
        )

        # -------------------------------------------------
        # Partition count
        # -------------------------------------------------

        partition_count = (
            df.rdd.getNumPartitions()
        )

        metrics = {
            "row_count": row_count,
            "file_count": file_count,
            "unique_grids": unique_grids,
            "country_code_categories": country_codes,
            "distinct_hourly_intervals": (
                hourly_intervals
            ),
            "partition_count": partition_count,
        }

        return metrics

    # =====================================================
    # 7. File-level row count
    # =====================================================

    def file_report(self):
        """
        Produce row counts for each source file.
        """

        if self.raw_network_df is None:
            raise RuntimeError(
                "Load the data before creating "
                "the file report."
            )

        report = (
            self.raw_network_df
            .groupBy("source_file")
            .agg(
                count("*").alias("row_count")
            )
            .orderBy("source_file")
        )

        return report

    # =====================================================
    # 8. Grain validation
    # =====================================================

    def validate_raw_grain(self):
        """
        Verify that country_code remains part of the raw grain.

        The raw grain is:

            timestamp + grid_id + country_code
        """

        if self.raw_network_df is None:
            raise RuntimeError(
                "Load the data first."
            )

        duplicate_count = (
            self.raw_network_df
            .groupBy(
                "timestamp",
                "grid_id",
                "country_code"
            )
            .count()
            .filter(
                col("count") > 1
            )
            .count()
        )

        if duplicate_count > 0:
            raise ValueError(
                f"Found {duplicate_count} "
                "duplicate raw-grain groups."
            )

        print(
            "\nRaw grain validation passed:"
        )

        print(
            "timestamp + grid_id + country_code"
        )

        return True


    # =====================================================
    # 9. SP1 Acceptance Criteria
    # =====================================================

    def validate_acceptance_criteria(self):
        """
        Validate all SP1 acceptance criteria.

        1. Distinct timestamps = D × 24
        2. File count matches files physically present
        3. grid_id values are 1-10000
        4. Raw row count > eventual grid/hour count
        5. Every row has populated source_file
        """

        if self.raw_network_df is None:
            raise RuntimeError(
                "Run the ingestion pipeline before "
                "validating acceptance criteria."
            )

        df = self.raw_network_df

        print("\n" + "=" * 70)
        print("SP1 ACCEPTANCE CRITERIA")
        print("=" * 70)

        results = {}

        # =================================================
        # Criterion 1
        # =================================================

        print("\n1. DISTINCT TIMESTAMP COUNT")

        # -------------------------------------------------
        # Count actual Milan files in the folder
        # -------------------------------------------------

        actual_files = sorted(
            self.input_dir.glob(
                "sms-call-internet-mi-*.csv"
            )
        )

        actual_file_count = len(
            actual_files
        )

        distinct_timestamps = (
            df.select("timestamp")
            .distinct()
            .count()
        )

        expected_timestamps = (
            actual_file_count * 24
        )

        criterion_1 = (
            distinct_timestamps
            == expected_timestamps
        )

        results[
            "timestamps"
        ] = criterion_1

        print(
            f"Files loaded: {actual_file_count}"
        )

        print(
            f"Distinct timestamps: "
            f"{distinct_timestamps}"
        )

        print(
            f"Expected timestamps: "
            f"{expected_timestamps}"
        )

        print(
            "PASS" if criterion_1 else "FAIL"
        )

        # =================================================
        # Criterion 2
        # =================================================

        print("\n2. FILE COUNT")

        spark_file_count = (
            df.select("source_file")
            .distinct()
            .count()
        )

        criterion_2 = (
            spark_file_count
            == actual_file_count
        )

        results[
            "file_count"
        ] = criterion_2

        print(
            f"Files physically in folder: "
            f"{actual_file_count}"
        )

        print(
            f"Distinct source files in Spark: "
            f"{spark_file_count}"
        )

        print(
            "PASS" if criterion_2 else "FAIL"
        )

        # =================================================
        # Criterion 3
        # =================================================

        print("\n3. GRID ID RANGE")

        invalid_grid_count = (
            df.filter(
                (col("grid_id") < 1)
                |
                (col("grid_id") > 10000)
                |
                col("grid_id").isNull()
            )
            .count()
        )

        criterion_3 = (
            invalid_grid_count == 0
        )

        results[
            "grid_range"
        ] = criterion_3

        print(
            f"Invalid grid_id rows: "
            f"{invalid_grid_count}"
        )

        print(
            "PASS" if criterion_3 else "FAIL"
        )

        # =================================================
        # Criterion 4
        # =================================================

        print("\n4. RAW GRAIN VS EVENTUAL GRID/HOUR GRAIN")

        raw_row_count = df.count()

        eventual_hourly_count = (
            df.select(
                "timestamp",
                "grid_id"
            )
            .distinct()
            .count()
        )

        criterion_4 = (
            raw_row_count
            > eventual_hourly_count
        )

        results[
            "raw_grain"
        ] = criterion_4

        print(
            f"Raw country-code rows: "
            f"{raw_row_count:,}"
        )

        print(
            f"Distinct grid/hour records: "
            f"{eventual_hourly_count:,}"
        )

        print(
            "PASS" if criterion_4 else "FAIL"
        )

        # =================================================
        # Criterion 5
        # =================================================

        print("\n5. INPUT FILE TRACEABILITY")

        missing_source_file_count = (
            df.filter(
                col("source_file").isNull()
                |
                (col("source_file") == "")
            )
            .count()
        )

        criterion_5 = (
            missing_source_file_count == 0
        )

        results[
            "source_file"
        ] = criterion_5

        print(
            f"Rows with missing source_file: "
            f"{missing_source_file_count:,}"
        )

        print(
            "PASS" if criterion_5 else "FAIL"
        )

        # =================================================
        # Overall result
        # =================================================

        all_passed = all(
            results.values()
        )

        print("\n" + "=" * 70)

        if all_passed:
            print(
                "SP1 ACCEPTANCE: ALL PASS"
            )
        else:
            print(
                "SP1 ACCEPTANCE: FAILED"
            )

            failed = [
                name
                for name, passed
                in results.items()
                if not passed
            ]

            print(
                "Failed criteria:",
                failed
            )

        print("=" * 70)

        return {
            "all_passed": all_passed,
            "criteria": results,
            "actual_file_count":
                actual_file_count,
            "spark_file_count":
                spark_file_count,
            "distinct_timestamps":
                distinct_timestamps,
            "expected_timestamps":
                expected_timestamps,
            "raw_row_count":
                raw_row_count,
            "eventual_hourly_count":
                eventual_hourly_count,
            "invalid_grid_count":
                invalid_grid_count,
            "missing_source_file_count":
                missing_source_file_count,
        }
    # =====================================================
    # 9. Full SP1 pipeline
    # =====================================================

    def process(self):
        """
        Run the complete SP1 ingestion pipeline.
        """

        self.create_spark_session()

        self.read_data()

        self.add_traceability()

        self.canonicalize()

        self.validate_schema()

        metrics = self.profile()

        self.validate_raw_grain()

        acceptance = (
            self.validate_acceptance_criteria()
        )

        return {
            "raw_network_df":
                self.raw_network_df,

            "metrics":
                metrics,

            "file_report":
                self.file_report(),

            "acceptance":
                acceptance,
        }

    # =====================================================
    # 10. Stop Spark
    # =====================================================

    def stop(self):
        """
        Stop the SparkSession.
        """

        if self.spark is not None:
            self.spark.stop()
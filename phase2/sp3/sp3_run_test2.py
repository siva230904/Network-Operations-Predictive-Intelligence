
import os

import sys
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# =========================================================
# SP3 Runner
# File: phase2/sp3/sp3_run_test2.py
# =========================================================

from pathlib import Path

from pyspark.sql import SparkSession

from aggregation2 import NetworkAggregator


# =========================================================
# Paths
# =========================================================

BASE_DIR = Path(__file__).resolve().parents[2]

CLEAN_CHECKPOINT = (
    BASE_DIR
    / "data"
    / "landing"
    / "sp2"
    / "clean_network"
)

OUTPUT_DIR = (
    BASE_DIR
    / "data"
    / "landing"
)

LOG_DIR = (
    BASE_DIR
    / "data"
    / "logs"
)


# =========================================================
# Expected number of daily files
# =========================================================

EXPECTED_FILE_COUNT = 7


# =========================================================
# Spark
# =========================================================

spark = (
    SparkSession
    .builder
    .appName("SP3-Network-Aggregation")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


try:

    # -----------------------------------------------------
    # Load SP2 checkpoint
    # -----------------------------------------------------

    print(
        f"Loading SP2 checkpoint:\n"
        f"{CLEAN_CHECKPOINT}"
    )

    clean_network_df = (
        spark.read
        .parquet(
            str(CLEAN_CHECKPOINT)
        )
    )

    print(
        f"SP2 checkpoint rows: "
        f"{clean_network_df.count():,}"
    )

    # -----------------------------------------------------
    # SP3
    # -----------------------------------------------------

    aggregator = NetworkAggregator(
        clean_network_df=clean_network_df,
        log_dir=str(LOG_DIR)
    )

    result = aggregator.process(
        expected_file_count=EXPECTED_FILE_COUNT,
        output_dir=str(OUTPUT_DIR)
    )

    # -----------------------------------------------------
    # Results
    # -----------------------------------------------------

    print("\n" + "=" * 70)
    print("SP3 COMPLETED")
    print("=" * 70)

    print(
        f"Clean input rows : "
        f"{result['acceptance']['clean_rows']:,}"
    )

    print(
        f"Hourly rows      : "
        f"{result['acceptance']['hourly_rows']:,}"
    )

    print(
        f"Maximum allowed  : "
        f"{result['acceptance']['maximum_allowed']:,}"
    )

    print(
        "\nHourly summary:"
    )

    result[
        "hourly_grid_summary"
    ].show(5, truncate=False)

    print(
        "\nDaily traffic:"
    )

    result[
        "daily_traffic_summary"
    ].show(20, truncate=False)

    print(
        "\nTop 10 hotspots:"
    )

    result[
        "hotspot_ranking"
    ].show(10, truncate=False)

    print(
        "\nPeak activity hour:"
    )

    result[
        "peak_activity_hour"
    ].show(5, truncate=False)

    print(
        "\nOutputs:"
    )

    for name, path in result[
        "output_paths"
    ].items():

        print(
            f"  {name}: {path}"
        )

    print(
        f"\nLog file: "
        f"{result['log_file']}"
    )

finally:

    spark.stop()
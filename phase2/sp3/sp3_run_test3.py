# =========================================================
# SP3 Runner
# File: phase2/sp3/sp3_run_test2.py
# =========================================================

import os
import sys

# =========================================================
# PySpark Python configuration
# =========================================================

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


from pathlib import Path

from pyspark.sql import SparkSession

from aggregation3 import NetworkAggregator


# =========================================================
# Project paths
# =========================================================

BASE_DIR = (
    Path(__file__)
    .resolve()
    .parents[2]
)

# ---------------------------------------------------------
# SP2 Parquet checkpoint
# ---------------------------------------------------------

CLEAN_CHECKPOINT = (
    BASE_DIR
    / "data"
    / "landing"
    / "sp2"
    / "clean_network"
)

# ---------------------------------------------------------
# SP3 Parquet checkpoint directory
# ---------------------------------------------------------

SP3_PARQUET_DIR = (
    BASE_DIR
    / "data"
    / "landing"
    / "sp3"
)

# ---------------------------------------------------------
# SP3 CSV deliverables
# ---------------------------------------------------------

CSV_OUTPUT_DIR = (
    BASE_DIR
    / "data"
    / "landing"
)

# ---------------------------------------------------------
# Logs
# ---------------------------------------------------------

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
    .appName(
        "SP3-Network-Aggregation"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)


try:

    # =====================================================
    # 1. Verify SP2 checkpoint
    # =====================================================

    if not CLEAN_CHECKPOINT.exists():

        raise FileNotFoundError(
            "SP2 Parquet checkpoint not found:\n"
            f"{CLEAN_CHECKPOINT}"
        )

    print(
        "\n" + "=" * 70
    )

    print(
        "SP3 INPUT"
    )

    print(
        "=" * 70
    )

    print(
        "Loading SP2 Parquet checkpoint:"
    )

    print(
        CLEAN_CHECKPOINT
    )

    # =====================================================
    # 2. Load SP2 Parquet
    # =====================================================

    clean_network_df = (
        spark.read
        .parquet(
            str(CLEAN_CHECKPOINT)
        )
    )

    input_count = (
        clean_network_df.count()
    )

    print(
        f"SP2 checkpoint rows: "
        f"{input_count:,}"
    )

    # =====================================================
    # 3. Create SP3 aggregator
    # =====================================================

    aggregator = NetworkAggregator(
        clean_network_df=
            clean_network_df,

        log_dir=
            str(LOG_DIR)
    )

    # =====================================================
    # 4. Run SP3
    # =====================================================

    result = aggregator.process(

        expected_file_count=
            EXPECTED_FILE_COUNT,

        parquet_output_dir=
            str(SP3_PARQUET_DIR),

        csv_output_dir=
            str(CSV_OUTPUT_DIR)
    )

    # =====================================================
    # 5. Results
    # =====================================================

    print(
        "\n" + "=" * 70
    )

    print(
        "SP3 COMPLETED"
    )

    print(
        "=" * 70
    )

    acceptance = (
        result[
            "acceptance"
        ]
    )

    print(
        f"Clean input rows : "
        f"{acceptance['clean_rows']:,}"
    )

    print(
        f"Hourly rows      : "
        f"{acceptance['hourly_rows']:,}"
    )

    print(
        f"Maximum allowed  : "
        f"{acceptance['maximum_allowed']:,}"
    )

    # =====================================================
    # 6. Show hourly sample
    # =====================================================

    print(
        "\nHourly grid summary:"
    )

    result[
        "hourly_grid_summary"
    ].show(
        5,
        truncate=False
    )

    # =====================================================
    # 7. Show daily summary
    # =====================================================

    print(
        "\nDaily traffic summary:"
    )

    result[
        "daily_traffic_summary"
    ].show(
        20,
        truncate=False
    )

    # =====================================================
    # 8. Show hotspots
    # =====================================================

    print(
        "\nTop 10 hotspots:"
    )

    result[
        "hotspot_ranking"
    ].show(
        10,
        truncate=False
    )

    # =====================================================
    # 9. Show peak hour
    # =====================================================

    print(
        "\nPeak activity hour:"
    )

    result[
        "peak_activity_hour"
    ].show(
        5,
        truncate=False
    )

    # =====================================================
    # 10. Output locations
    # =====================================================

    print(
        "\n" + "=" * 70
    )

    print(
        "SP3 OUTPUTS"
    )

    print(
        "=" * 70
    )

    output_paths = (
        result[
            "output_paths"
        ]
    )

    print(
        "\nParquet checkpoints:"
    )

    print(
        f"  Hourly  : "
        f"{output_paths['hourly_parquet']}"
    )

    print(
        f"  Daily   : "
        f"{output_paths['daily_parquet']}"
    )

    print(
        f"  Hotspots: "
        f"{output_paths['hotspots_parquet']}"
    )

    print(
        f"  Peak    : "
        f"{output_paths['peak_parquet']}"
    )

    print(
        "\nCSV deliverables:"
    )

    print(
        f"  Hourly  : "
        f"{output_paths['hourly_csv']}"
    )

    print(
        f"  Daily   : "
        f"{output_paths['daily_csv']}"
    )

    print(
        f"  Hotspots: "
        f"{output_paths['hotspots_csv']}"
    )

    print(
        f"  Peak    : "
        f"{output_paths['peak_csv']}"
    )

    # =====================================================
    # 11. Log
    # =====================================================

    print(
        "\nLog file:"
    )

    print(
        result[
            "log_file"
        ]
    )

    print(
        "\n" + "=" * 70
    )

finally:

    spark.stop()
# =========================================================
# SP6 Runner
# File: phase2/sp6/sp6_run_test.py
# =========================================================

import os
import sys

# =========================================================
# Python configuration
# =========================================================

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


# =========================================================
# Imports
# =========================================================

from pathlib import Path

from pyspark.sql import SparkSession

from storage import NetworkStorageManager


# =========================================================
# Project paths
# =========================================================

BASE_DIR = (
    Path(__file__)
    .resolve()
    .parents[2]
)

SP2_CHECKPOINT = (
    BASE_DIR
    / "data"
    / "landing"
    / "sp2"
    / "clean_network"
)

SP3_CHECKPOINT = (
    BASE_DIR
    / "data"
    / "landing"
    / "sp3"
    / "hourly_grid_summary"
)

DATA_DIR = (
    BASE_DIR
    / "data"
)

LOG_DIR = (
    BASE_DIR
    / "data"
    / "logs"
)


# =========================================================
# Spark configuration
# =========================================================

# ---------------------------------------------------------
# These settings are intentionally conservative for a
# local Windows training environment.
#
# The important change is that the JVM receives an explicit
# heap size and Spark does not rely entirely on defaults.
# ---------------------------------------------------------

spark = (
    SparkSession
    .builder
    .appName("SP6-Network-Storage")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.sql.shuffle.partitions", "64")
    .config("spark.default.parallelism", "64")
    .config("spark.sql.adaptive.enabled", "true")
    .config(
        "spark.sql.adaptive.coalescePartitions.enabled",
        "true"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)


try:

    # =====================================================
    # 1. Check checkpoints
    # =====================================================

    print("\n" + "=" * 70)
    print("SP6 INPUT CHECKPOINTS")
    print("=" * 70)

    print(
        f"SP2:\n"
        f"{SP2_CHECKPOINT}"
    )

    print(
        f"\nSP3:\n"
        f"{SP3_CHECKPOINT}"
    )

    if not SP2_CHECKPOINT.exists():

        raise FileNotFoundError(
            "SP2 Parquet checkpoint not found:\n"
            f"{SP2_CHECKPOINT}"
        )

    if not SP3_CHECKPOINT.exists():

        raise FileNotFoundError(
            "SP3 Parquet checkpoint not found:\n"
            f"{SP3_CHECKPOINT}"
        )

    # =====================================================
    # 2. Load SP2
    # =====================================================

    print("\n" + "=" * 70)
    print("LOADING SP2 CLEAN PARQUET")
    print("=" * 70)

    clean_network_df = (
        spark.read
        .parquet(
            str(SP2_CHECKPOINT)
        )
    )

    print(
        f"SP2 rows: "
        f"{clean_network_df.count():,}"
    )

    # =====================================================
    # 3. Load SP3
    # =====================================================

    print("\n" + "=" * 70)
    print("LOADING SP3 HOURLY PARQUET")
    print("=" * 70)

    hourly_grid_summary = (
        spark.read
        .parquet(
            str(SP3_CHECKPOINT)
        )
    )

    print(
        f"SP3 rows: "
        f"{hourly_grid_summary.count():,}"
    )

    # =====================================================
    # 4. Show schemas
    # =====================================================

    print("\n" + "=" * 70)
    print("SP2 SCHEMA")
    print("=" * 70)

    clean_network_df.printSchema()

    print("\n" + "=" * 70)
    print("SP3 SCHEMA")
    print("=" * 70)

    hourly_grid_summary.printSchema()

    # =====================================================
    # 5. Create storage manager
    # =====================================================

    storage_manager = (
        NetworkStorageManager(
            spark=spark,
            clean_network_df=clean_network_df,
            hourly_grid_summary=hourly_grid_summary,
            output_dir=str(DATA_DIR),
            log_dir=str(LOG_DIR)
        )
    )

    # =====================================================
    # 6. Run SP6
    # =====================================================

    result = (
        storage_manager.process()
    )

    # =====================================================
    # 7. Final output
    # =====================================================

    print("\n" + "=" * 70)
    print("SP6 COMPLETED")
    print("=" * 70)

    print(
        "\nProcessed activity:"
    )

    print(
        f"  {result['activity_output']}"
    )

    print(
        "\nHourly analytics:"
    )

    print(
        f"  {result['hourly_output']}"
    )

    print(
        "\nDashboard CSV:"
    )

    print(
        f"  {result['dashboard_output']}"
    )

    print(
        "\nSP6 report:"
    )

    print(
        f"  {result['report']}"
    )

    # =====================================================
    # 8. File-size comparison
    # =====================================================

    print(
        "\nFile-size comparison:"
    )

    size_comparison = (
        result["size_comparison"]
    )

    print(
        f"  CSV: "
        f"{size_comparison['csv_bytes']:,} bytes"
    )

    print(
        f"  Parquet: "
        f"{size_comparison['parquet_bytes']:,} bytes"
    )

    ratio = (
        size_comparison[
            "csv_to_parquet_ratio"
        ]
    )

    if ratio is not None:

        print(
            f"  CSV / Parquet: "
            f"{ratio:.2f}x"
        )

    # =====================================================
    # 9. Acceptance
    # =====================================================

    print(
        "\n" + "=" * 70
    )

    if (
        result["acceptance"]["all_passed"]
    ):

        print(
            "SP6 ACCEPTANCE: ALL PASS"
        )

    else:

        print(
            "SP6 ACCEPTANCE: FAILED"
        )

    print(
        "=" * 70
    )

    # =====================================================
    # 10. Log
    # =====================================================

    print(
        f"\nLog file:\n"
        f"  {result['log_file']}"
    )

    print(
        "\n" + "=" * 70
    )

    print(
        "SP6 RUN FINISHED SUCCESSFULLY"
    )

    print(
        "=" * 70
    )


finally:

    # -----------------------------------------------------
    # Important:
    #
    # This releases the Spark JVM when the run finishes.
    # No SP6 cache is intentionally left behind.
    # -----------------------------------------------------

    spark.stop()
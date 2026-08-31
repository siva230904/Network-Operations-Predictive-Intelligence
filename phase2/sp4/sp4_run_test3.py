# =========================================================
# SP4 Runner
# File: phase2/sp4/sp4_run_test3.py
# =========================================================

import os
import sys

# ---------------------------------------------------------
# Ensure Spark uses the same Python interpreter
# ---------------------------------------------------------

python_path = sys.executable

os.environ[
    "PYSPARK_PYTHON"
] = python_path

os.environ[
    "PYSPARK_DRIVER_PYTHON"
] = python_path


# =========================================================
# Imports
# =========================================================

from pathlib import Path

from pyspark.sql import SparkSession

from enrichment3 import NetworkGeoEnricher


# =========================================================
# Project paths
# =========================================================

BASE_DIR = (
    Path(__file__)
    .resolve()
    .parents[2]
)


# =========================================================
# SP3 Parquet checkpoint
# =========================================================

SP3_CHECKPOINT = (
    BASE_DIR
    / "data"
    / "landing"
    / "sp3"
    / "hourly_grid_summary"
)


# =========================================================
# GeoJSON reference
# =========================================================

GEOJSON_PATH = (
    BASE_DIR
    / "data"
    / "reference"
    / "milano-grid.geojson"
)


# =========================================================
# SP4 output
# =========================================================

OUTPUT_DIR = (
    BASE_DIR
    / "data"
    / "landing"
    / "sp4"
)


# =========================================================
# Log directory
# =========================================================

LOG_DIR = (
    BASE_DIR
    / "data"
    / "logs"
)


# =========================================================
# Spark
# =========================================================

spark = (
    SparkSession
    .builder
    .appName(
        "SP4-Geospatial-Enrichment"
    )
    .config(
        "spark.sql.execution.pyspark.udf.faulthandler.enabled",
        "true"
    )
    .config(
        "spark.python.worker.faulthandler.enabled",
        "true"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)


try:

    # =====================================================
    # 1. Validate paths
    # =====================================================

    print("\n" + "=" * 70)
    print("SP4 INPUT VALIDATION")
    print("=" * 70)

    print(
        f"SP3 checkpoint:\n"
        f"{SP3_CHECKPOINT}"
    )

    print(
        f"\nGeoJSON:\n"
        f"{GEOJSON_PATH}"
    )

    print(
        f"\nSP4 output:\n"
        f"{OUTPUT_DIR}"
    )

    if not SP3_CHECKPOINT.exists():

        raise FileNotFoundError(
            "SP3 Parquet checkpoint not found:\n"
            f"{SP3_CHECKPOINT}"
        )

    if not GEOJSON_PATH.exists():

        raise FileNotFoundError(
            "GeoJSON reference not found:\n"
            f"{GEOJSON_PATH}"
        )

    # =====================================================
    # 2. Load SP3 Parquet
    # =====================================================

    print("\n" + "=" * 70)
    print("LOADING SP3 PARQUET CHECKPOINT")
    print("=" * 70)

    print(
        f"Loading:\n"
        f"{SP3_CHECKPOINT}"
    )

    hourly_grid_summary = (
        spark.read
        .parquet(
            str(SP3_CHECKPOINT)
        )
    )

    sp3_row_count = (
        hourly_grid_summary.count()
    )

    print(
        f"SP3 checkpoint rows: "
        f"{sp3_row_count:,}"
    )

    # =====================================================
    # 3. Show SP3 schema
    # =====================================================

    print("\n" + "=" * 70)
    print("SP3 INPUT SCHEMA")
    print("=" * 70)

    hourly_grid_summary.printSchema()

    print("\nSP3 columns:")

    for column in (
        hourly_grid_summary.columns
    ):

        print(
            f"  {column}"
        )

    # =====================================================
    # 4. SP4 enricher
    # =====================================================

    enricher = NetworkGeoEnricher(
        spark=spark,
        hourly_grid_summary=hourly_grid_summary,
        geojson_path=GEOJSON_PATH,
        log_dir=LOG_DIR
    )

    # =====================================================
    # 5. Process
    # =====================================================

    result = enricher.process(
        output_dir=OUTPUT_DIR
    )

    # =====================================================
    # 6. Final results
    # =====================================================

    print("\n" + "=" * 70)
    print("SP4 COMPLETED")
    print("=" * 70)

    print(
        f"SP3 input rows : "
        f"{result['acceptance']['input_rows']:,}"
    )

    print(
        f"SP4 output rows: "
        f"{result['acceptance']['output_rows']:,}"
    )

    print(
        "\nCoverage:"
    )

    coverage = (
        result[
            "coverage_report"
        ]
    )

    print(
        f"  Activity grids : "
        f"{coverage['activity_grids']:,}"
    )

    print(
        f"  Matched grids  : "
        f"{coverage['matched_grids']:,}"
    )

    print(
        f"  Unmatched grids: "
        f"{coverage['unmatched_grids']:,}"
    )

    print(
        f"  Coverage       : "
        f"{coverage['coverage_percentage']:.2f}%"
    )

    # =====================================================
    # 7. Top grids
    # =====================================================

    print(
        "\nTop 10 grids:"
    )

    result[
        "top_grids"
    ].show(
        10,
        truncate=False
    )

    # =====================================================
    # 8. Acceptance
    # =====================================================

    print(
        "\nAcceptance:"
    )

    print(
        "  "
        + (
            "ALL PASS"
            if result[
                "acceptance"
            ]["all_passed"]
            else
            "FAILED"
        )
    )

    # =====================================================
    # 9. Outputs
    # =====================================================

    print(
        "\nOutputs:"
    )

    for name, path in (
        result[
            "output_paths"
        ].items()
    ):

        print(
            f"  {name}: {path}"
        )

    # =====================================================
    # 10. Log
    # =====================================================

    print(
        f"\nLog file:\n"
        f"  {result['log_file']}"
    )

    print("\n" + "=" * 70)
    print("SP4 RUN FINISHED SUCCESSFULLY")
    print("=" * 70)


finally:

    spark.stop()
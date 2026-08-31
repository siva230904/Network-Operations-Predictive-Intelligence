import os
import sys
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
# =========================================================
# SP4 Runner
# File: phase2/sp4/sp4_run_test.py
# =========================================================

from pathlib import Path

from pyspark.sql import SparkSession

from enrichment2 import NetworkGeoEnricher


# =========================================================
# Paths
# =========================================================

BASE_DIR = Path(
    __file__
).resolve().parents[2]

SP3_CHECKPOINT = (
    BASE_DIR
    / "data"
    / "landing"
    / "hourly_grid_summary"
)

GEOJSON_PATH = (
    BASE_DIR
    / "data"
    / "reference"
    / "milano-grid.geojson"
)

OUTPUT_DIR = (
    BASE_DIR
    / "data"
    / "landing"
    / "sp4"
)

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
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)


try:

    # -----------------------------------------------------
    # Load SP3 checkpoint
    # -----------------------------------------------------

    print(
        "\nLoading SP3 hourly checkpoint:"
    )

    print(
        SP3_CHECKPOINT
    )

    hourly_grid_summary = (
        spark.read
        .option(
            "header",
            True
        )
        .option(
            "inferSchema",
            True
        )
        .csv(
            str(
                SP3_CHECKPOINT
            )
        )
    )

    print(
        f"SP3 rows: "
        f"{hourly_grid_summary.count():,}"
    )

    # -----------------------------------------------------
    # SP4
    # -----------------------------------------------------

    enricher = NetworkGeoEnricher(
        spark=spark,
        hourly_grid_summary=hourly_grid_summary,
        geojson_path=GEOJSON_PATH,
        log_dir=LOG_DIR
    )

    result = enricher.process(
        output_dir=OUTPUT_DIR
    )

    # -----------------------------------------------------
    # Results
    # -----------------------------------------------------

    print(
        "\n" + "=" * 70
    )

    print(
        "SP4 COMPLETED"
    )

    print(
        "=" * 70
    )

    coverage = (
        result[
            "coverage_report"
        ]
    )

    print(
        "\nGeographic enrichment:"
    )

    print(
        f"Activity grids : "
        f"{coverage['activity_grids']:,}"
    )

    print(
        f"Matched grids  : "
        f"{coverage['matched_grids']:,}"
    )

    print(
        f"Unmatched grids: "
        f"{coverage['unmatched_grids']:,}"
    )

    print(
        f"Coverage       : "
        f"{coverage['coverage_percentage']:.2f}%"
    )

    print(
        "\nTop grids with geometry:"
    )

    result[
        "top_grids"
    ].show(
        10,
        truncate=False
    )

    print(
        "\nOutput paths:"
    )

    for name, path in (
        result["output_paths"]
        .items()
    ):

        print(
            f"  {name}: {path}"
        )

    print(
        f"\nLog file: "
        f"{result['log_file']}"
    )

finally:

    spark.stop()
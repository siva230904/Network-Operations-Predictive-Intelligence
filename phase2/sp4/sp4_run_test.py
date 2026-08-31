import sys
from pathlib import Path

import os
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
# =========================================================
# Make sibling phase folders importable
# =========================================================

PHASE2_DIR = (
    Path(__file__)
    .resolve()
    .parent
    .parent
)

sys.path.insert(
    0,
    str(PHASE2_DIR)
)


# =========================================================
# Imports
# =========================================================

from pyspark.sql import SparkSession

from sp2.cleaning import NetworkCleaner
from sp3.aggregation import NetworkAggregator
from sp4.enrichment import NetworkGeoEnricher


# =========================================================
# Paths
# =========================================================

RAW_DIR = (
    PHASE2_DIR.parent /
    "data" /
    "raw"
)

REFERENCE_DIR = (
    PHASE2_DIR.parent /
    "data" /
    "reference"
)

GEOJSON_PATH = (
    REFERENCE_DIR /
    "milano-grid.geojson"
)

LOG_DIR = (
    PHASE2_DIR.parent /
    "data" /
    "logs"
)

OUTPUT_DIR = (
    PHASE2_DIR.parent /
    "data" /
    "landing" /
    "sp4"
)


# =========================================================
# Count Milan files
# =========================================================

daily_files = sorted(
    RAW_DIR.glob(
        "sms-call-internet-mi-*.csv"
    )
)

expected_file_count = len(
    daily_files
)

if expected_file_count == 0:

    raise FileNotFoundError(
        "No Milan files found in "
        f"{RAW_DIR}"
    )


print(
    f"Found {expected_file_count} Milan daily files."
)


# =========================================================
# Spark
# =========================================================

spark = (
    SparkSession.builder
    .appName(
        "SP4_Network_GeoEnrichment"
    )
    .getOrCreate()
)


# =========================================================
# Read raw data
# =========================================================

print(
    "\nLoading raw Milan activity files..."
)

raw_network_df = (
    spark.read
    .option("header", True)
    .csv(
        str(
            RAW_DIR /
            "sms-call-internet-mi-*.csv"
        )
    )
)


# =========================================================
# SP2 — Cleaning
# =========================================================

print(
    "\nRunning SP2 cleaning..."
)

cleaner = NetworkCleaner(
    spark=spark,
    raw_network_df=raw_network_df
)

sp2_result = (
    cleaner.process()
)

clean_network_df = (
    sp2_result[
        "clean_network_df"
    ]
)


# =========================================================
# SP3 — Aggregation
# =========================================================

print(
    "\nRunning SP3 aggregation..."
)

aggregator = NetworkAggregator(
    clean_network_df=clean_network_df,
    log_dir=str(LOG_DIR)
)

sp3_result = (
    aggregator.process(
        expected_file_count=
            expected_file_count,

        output_dir=
            str(
                PHASE2_DIR.parent /
                "data" /
                "landing" /
                "sp3"
            )
    )
)

hourly_grid_summary = (
    sp3_result[
        "hourly_grid_summary"
    ]
)


# =========================================================
# SP4 — Geospatial enrichment
# =========================================================

print(
    "\nRunning SP4 geospatial enrichment..."
)

enricher = NetworkGeoEnricher(
    spark=spark,

    hourly_grid_summary=
        hourly_grid_summary,

    geojson_path=
        GEOJSON_PATH,

    log_dir=
        str(LOG_DIR)
)

result = (
    enricher.process(
        expected_file_count=
            expected_file_count,

        output_dir=
            str(OUTPUT_DIR)
    )
)


# =========================================================
# Final results
# =========================================================

print(
    "\n" + "=" * 70
)

print(
    "SP4 COMPLETED SUCCESSFULLY"
)

print(
    "=" * 70
)


print(
    "\nEnrichment coverage:"
)

print(
    result[
        "coverage_report"
    ]
)


print(
    "\nTop high-activity grids:"
)

result[
    "top_grids"
].show(
    10,
    truncate=False
)


print(
    "\nLog file:"
)

print(
    result[
        "log_file"
    ]
)


# =========================================================
# Stop Spark
# =========================================================

spark.stop()
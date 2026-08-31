import sys
from pathlib import Path

PHASE2_DIR = Path(__file__).resolve().parent.parent

sys.path.insert(
    0,
    str(PHASE2_DIR)
)
import os
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
from sp2.cleaning import NetworkCleaner
from aggregation import NetworkAggregator

from pathlib import Path

from pyspark.sql import SparkSession

# =========================================================
# Paths
# =========================================================

RAW_DIR = Path(
    r"..\..\data\raw"
)

LOG_DIR = Path(
    r"..\..\data\logs"
)

OUTPUT_DIR = Path(
    r"..\..\data\landing"
)


# =========================================================
# Count supplied Milan files
# =========================================================

daily_files = list(
    RAW_DIR.glob(
        "sms-call-internet-mi-*.csv"
    )
)

expected_file_count = len(
    daily_files
)

if expected_file_count == 0:
    raise FileNotFoundError(
        "No files found using "
        "sms-call-internet-mi-*.csv"
    )


print(
    f"Found {expected_file_count} Milan daily files."
)


# =========================================================
# Spark
# =========================================================

spark = (
    SparkSession.builder
    .appName("SP3_Network_Aggregation")
    .getOrCreate()
)


# =========================================================
# Read raw data
# =========================================================

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

cleaner = NetworkCleaner(
    spark=spark,
    raw_network_df=raw_network_df
)

clean_result = cleaner.process()

clean_network_df = (
    clean_result[
        "clean_network_df"
    ]
)


# =========================================================
# SP3 — Aggregation
# =========================================================

aggregator = NetworkAggregator(
    clean_network_df=clean_network_df,
    log_dir=str(LOG_DIR)
)

result = aggregator.process(
    expected_file_count=expected_file_count,
    output_dir=str(OUTPUT_DIR)
)


# =========================================================
# Display results
# =========================================================

print("\n" + "=" * 70)
print("SP3 COMPLETED")
print("=" * 70)

print(
    "\nHourly grid summary:"
)

result[
    "hourly_grid_summary"
].show(
    10,
    truncate=False
)

print(
    "\nDaily traffic summary:"
)

result[
    "daily_traffic_summary"
].show(
    truncate=False
)

print(
    "\nTop 10 hotspots:"
)

result[
    "hotspot_ranking"
].show(
    truncate=False
)

print(
    "\nPeak activity hour:"
)

result[
    "peak_activity_hour"
].show(
    truncate=False
)

print(
    "\nLog file:"
)

print(
    result["log_file"]
)


# =========================================================
# Stop Spark
# =========================================================

spark.stop()
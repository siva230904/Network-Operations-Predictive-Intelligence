import os
import sys
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path
from pathlib import Path

from pyspark.sql import SparkSession

from cleaning2 import NetworkCleaner


# =========================================================
# Project paths
# =========================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

RAW_DIR = (
    PROJECT_ROOT /
    "data" /
    "raw"
)

OUTPUT_DIR = (
    PROJECT_ROOT /
    "data" /
    "landing" /
    "sp2"
)

LOG_DIR = (
    PROJECT_ROOT /
    "data" /
    "logs"
)


# =========================================================
# Verify input files before Spark starts
# =========================================================

daily_files = sorted(
    RAW_DIR.glob(
        "sms-call-internet-mi-*.csv"
    )
)

if not daily_files:

    raise FileNotFoundError(
        "No files found using "
        "sms-call-internet-mi-*.csv "
        f"in {RAW_DIR}"
    )

print(
    f"Found {len(daily_files)} Milan daily files."
)

for file in daily_files:

    print(
        f"  {file.name}"
    )


# =========================================================
# Spark
# =========================================================

spark = (
    SparkSession.builder
    .appName(
        "SP2_Cleaning"
    )
    .getOrCreate()
)


# =========================================================
# Input pattern
# =========================================================

input_pattern = str(
    RAW_DIR /
    "sms-call-internet-mi-*.csv"
)

print(
    "\nInput pattern:"
)

print(
    input_pattern
)


# =========================================================
# Run SP2
# =========================================================

cleaner = NetworkCleaner(
    spark=spark,

    input_path=
        input_pattern,

    output_dir=
        str(
            OUTPUT_DIR
        ),

    log_dir=
        str(
            LOG_DIR
        )
)

result = (
    cleaner.process()
)


# =========================================================
# Summary
# =========================================================

print(
    "\n" + "=" * 70
)

print(
    "SP2 COMPLETED"
)

print(
    "=" * 70
)

print(
    f"Input files   : "
    f"{len(daily_files)}"
)

print(
    f"Input rows    : "
    f"{result['input_row_count']:,}"
)

print(
    f"Rejected rows : "
    f"{result['rejected_row_count']:,}"
)

print(
    f"Final rows    : "
    f"{result['final_row_count']:,}"
)

print(
    f"Nulls handled : "
    f"{result['nulls_handled']:,}"
)

print(
    "\nClean checkpoint:"
)

print(
    result["paths"]["clean_path"]
)

print(
    "\nRejected checkpoint:"
)

print(
    result["paths"]["rejected_path"]
)

print(
    "\nQuality report:"
)

print(
    result["paths"]["report_path"]
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
from pathlib import Path

import pandas as pd

from pyspark.sql import SparkSession

from pyspark.sql.functions import col

from cleaning import NetworkCleaner


# =========================================================
# Configuration
# =========================================================

INPUT_FILE = (
    Path(r"..\..\data\raw")
    / "sms-call-internet-mi-2013-11-01.csv"
)


# =========================================================
# Spark
# =========================================================

spark = (
    SparkSession.builder
    .appName("SP2_Pandas_Spark_Comparison")
    .getOrCreate()
)


# =========================================================
# 1. Pandas
# =========================================================

print("\n" + "=" * 70)
print("PANDAS VS SPARK COMPARISON")
print("=" * 70)

print(
    f"\nInput file:\n{INPUT_FILE}"
)


pandas_df = pd.read_csv(
    INPUT_FILE
)


# ---------------------------------------------------------
# Canonical names
# ---------------------------------------------------------

pandas_df = pandas_df.rename(
    columns={
        "datetime": "timestamp",
        "CellID": "grid_id",
        "countrycode": "country_code",
        "smsin": "sms_in",
        "smsout": "sms_out",
        "callin": "call_in",
        "callout": "call_out",
        "internet": "internet_activity",
    }
)


# ---------------------------------------------------------
# Types
# ---------------------------------------------------------

pandas_df["timestamp"] = pd.to_datetime(
    pandas_df["timestamp"]
)

pandas_df["grid_id"] = (
    pandas_df["grid_id"]
    .astype("int64")
)

activity_columns = [
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
]

for column_name in activity_columns:

    pandas_df[column_name] = pd.to_numeric(
        pandas_df[column_name],
        errors="coerce"
    )


# ---------------------------------------------------------
# Reject invalid rows
# ---------------------------------------------------------

invalid = (
    pandas_df["timestamp"].isna()
    |
    pandas_df["grid_id"].isna()
    |
    (pandas_df["grid_id"] < 1)
    |
    (pandas_df["grid_id"] > 10000)
)

for column_name in activity_columns:

    invalid = (
        invalid
        |
        (
            pandas_df[column_name]
            .fillna(0)
            < 0
        )
    )


pandas_clean = (
    pandas_df.loc[
        ~invalid
    ]
    .copy()
)


# ---------------------------------------------------------
# Curated null-to-zero
# ---------------------------------------------------------

pandas_clean[
    activity_columns
] = (
    pandas_clean[
        activity_columns
    ]
    .fillna(0)
)


# ---------------------------------------------------------
# Time features
# ---------------------------------------------------------

pandas_clean["date"] = (
    pandas_clean["timestamp"]
    .dt.date
)

pandas_clean["hour"] = (
    pandas_clean["timestamp"]
    .dt.hour
)

pandas_clean["day_of_week"] = (
    pandas_clean["timestamp"]
    .dt.dayofweek
)


# ---------------------------------------------------------
# Activity features
# ---------------------------------------------------------

pandas_clean["total_sms"] = (
    pandas_clean["sms_in"]
    +
    pandas_clean["sms_out"]
)

pandas_clean["total_calls"] = (
    pandas_clean["call_in"]
    +
    pandas_clean["call_out"]
)

pandas_clean["total_activity"] = (
    pandas_clean["total_sms"]
    +
    pandas_clean["total_calls"]
    +
    pandas_clean["internet_activity"]
)


# =========================================================
# 2. Spark
# =========================================================

spark_raw = (
    spark.read
    .option("header", True)
    .csv(str(INPUT_FILE))
)


cleaner = NetworkCleaner(
    spark=spark,
    raw_network_df=spark_raw
)


spark_result = cleaner.process()

spark_df = (
    spark_result[
        "clean_network_df"
    ]
)


# =========================================================
# 3. Compare row counts
# =========================================================

pandas_count = len(
    pandas_clean
)

spark_count = (
    spark_df.count()
)


print("\n" + "-" * 70)
print("ROW COUNT")
print("-" * 70)

print(
    f"Pandas: {pandas_count:,}"
)

print(
    f"Spark : {spark_count:,}"
)


if pandas_count != spark_count:

    raise AssertionError(
        "Pandas and Spark row counts do not match."
    )


print(
    "PASS"
)


# =========================================================
# 4. Compare timestamp counts
# =========================================================

pandas_timestamps = (
    pandas_clean[
        "timestamp"
    ]
    .nunique()
)

spark_timestamps = (
    spark_df
    .select("timestamp")
    .distinct()
    .count()
)


print("\n" + "-" * 70)
print("DISTINCT TIMESTAMPS")
print("-" * 70)

print(
    f"Pandas: {pandas_timestamps}"
)

print(
    f"Spark : {spark_timestamps}"
)


if (
    pandas_timestamps
    !=
    spark_timestamps
):

    raise AssertionError(
        "Timestamp counts do not match."
    )


print(
    "PASS"
)


# =========================================================
# 5. Compare activity totals
# =========================================================

pandas_totals = (
    pandas_clean[
        [
            "sms_in",
            "sms_out",
            "call_in",
            "call_out",
            "internet_activity",
            "total_sms",
            "total_calls",
            "total_activity",
        ]
    ]
    .sum()
)


spark_totals = (
    spark_df
    .select(
        *[
            col_name
            for col_name
            in [
                "sms_in",
                "sms_out",
                "call_in",
                "call_out",
                "internet_activity",
                "total_sms",
                "total_calls",
                "total_activity",
            ]
        ]
    )
    .groupBy()
    .sum()
    .collect()[0]
)


print("\n" + "-" * 70)
print("ACTIVITY TOTALS")
print("-" * 70)


for column_name in [
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_sms",
    "total_calls",
    "total_activity",
]:

    pandas_value = float(
        pandas_totals[
            column_name
        ]
    )

    spark_value = float(
        spark_totals[
            f"sum({column_name})"
        ]
    )

    difference = abs(
        pandas_value
        -
        spark_value
    )

    print(
        f"{column_name}: "
        f"Pandas={pandas_value:.6f}, "
        f"Spark={spark_value:.6f}, "
        f"difference={difference:.10f}"
    )

    if difference > 1e-6:

        raise AssertionError(
            f"{column_name} does not match."
        )


print(
    "\nPASS"
)


# =========================================================
# Final
# =========================================================

print("\n" + "=" * 70)
print("PANDAS VS SPARK: PASS")
print("=" * 70)


spark.stop()
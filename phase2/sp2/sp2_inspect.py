from pathlib import Path

from pyspark.sql import SparkSession


# =========================================================
# Paths
# =========================================================

SP2_CHECKPOINT = Path(
    "../../data/landing/sp2/clean_network"
)


# =========================================================
# Spark
# =========================================================

spark = (
    SparkSession.builder
    .appName("SP2_Checkpoint_Inspection")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================================================
# Load SP2 checkpoint
# =========================================================

print("=" * 70)
print("SP2 CHECKPOINT INSPECTION")
print("=" * 70)

print()
print(
    f"Checkpoint: {SP2_CHECKPOINT.resolve()}"
)

if not SP2_CHECKPOINT.exists():

    raise FileNotFoundError(
        f"SP2 checkpoint not found: "
        f"{SP2_CHECKPOINT.resolve()}"
    )


print()
print("Loading Parquet checkpoint...")

df = (
    spark.read
    .parquet(
        str(SP2_CHECKPOINT)
    )
)


# =========================================================
# Schema
# =========================================================

print()
print("=" * 70)
print("SP2 CHECKPOINT SCHEMA")
print("=" * 70)

df.printSchema()


# =========================================================
# Columns
# =========================================================

print()
print("=" * 70)
print("COLUMNS")
print("=" * 70)

for column in df.columns:
    print(column)


# =========================================================
# Row count
# =========================================================

print()
print("=" * 70)
print("ROW COUNT")
print("=" * 70)

row_count = df.count()

print(
    f"Rows: {row_count:,}"
)


# =========================================================
# Sample
# =========================================================

print()
print("=" * 70)
print("FIRST 5 ROWS")
print("=" * 70)

df.show(
    5,
    truncate=False
)


# =========================================================
# Required SP2 columns
# =========================================================

required_columns = [
    "timestamp",
    "grid_id",
    "country_code",
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
    "total_sms",
    "total_calls",
    "total_activity",
    "date",
    "hour",
    "day_of_week",
]


print()
print("=" * 70)
print("REQUIRED COLUMN CHECK")
print("=" * 70)

missing_columns = [
    column
    for column in required_columns
    if column not in df.columns
]

if missing_columns:

    print(
        "MISSING:"
    )

    for column in missing_columns:
        print(
            f"  - {column}"
        )

else:

    print(
        "All required SP2 columns are present."
    )


# =========================================================
# Timestamp inspection
# =========================================================

print()
print("=" * 70)
print("TIMESTAMP INSPECTION")
print("=" * 70)

if "timestamp" in df.columns:

    timestamp_count = (
        df
        .select("timestamp")
        .distinct()
        .count()
    )

    print(
        f"Distinct timestamps: "
        f"{timestamp_count:,}"
    )

    print()
    print("Timestamp sample:")

    (
        df
        .select("timestamp")
        .distinct()
        .orderBy("timestamp")
        .show(
            30,
            truncate=False
        )
    )


# =========================================================
# Grid inspection
# =========================================================

print()
print("=" * 70)
print("GRID INSPECTION")
print("=" * 70)

if "grid_id" in df.columns:

    grid_count = (
        df
        .select("grid_id")
        .distinct()
        .count()
    )

    print(
        f"Unique grids: "
        f"{grid_count:,}"
    )

    print()
    print("Grid range:")

    df.selectExpr(
        "min(grid_id) AS min_grid_id",
        "max(grid_id) AS max_grid_id"
    ).show()


# =========================================================
# Country-code inspection
# =========================================================

print()
print("=" * 70)
print("COUNTRY-CODE INSPECTION")
print("=" * 70)

if "country_code" in df.columns:

    country_count = (
        df
        .select("country_code")
        .distinct()
        .count()
    )

    print(
        f"Country-code categories: "
        f"{country_count:,}"
    )


# =========================================================
# Activity null inspection
# =========================================================

activity_columns = [
    "sms_in",
    "sms_out",
    "call_in",
    "call_out",
    "internet_activity",
]


print()
print("=" * 70)
print("ACTIVITY NULL INSPECTION")
print("=" * 70)

available_activity_columns = [
    column
    for column in activity_columns
    if column in df.columns
]

if available_activity_columns:

    null_expressions = [
        (
            df[column]
            .isNull()
            .cast("long")
            .alias(column)
        )
        for column in available_activity_columns
    ]

    null_row = (
        df
        .select(*null_expressions)
        .groupBy()
        .sum()
        .collect()[0]
    )

    for index, column in enumerate(
        available_activity_columns
    ):

        value = (
            null_row[index]
            if null_row[index] is not None
            else 0
        )

        print(
            f"{column}: {value:,}"
        )


# =========================================================
# Final summary
# =========================================================

print()
print("=" * 70)
print("SP2 CHECKPOINT SUMMARY")
print("=" * 70)

print(
    f"Rows              : {row_count:,}"
)

if "grid_id" in df.columns:

    print(
        f"Unique grids      : {grid_count:,}"
    )

if "timestamp" in df.columns:

    print(
        f"Distinct timestamps: "
        f"{timestamp_count:,}"
    )

if "country_code" in df.columns:

    print(
        f"Country categories: "
        f"{country_count:,}"
    )

print(
    f"Missing columns   : "
    f"{len(missing_columns)}"
)

print()
print(
    "SP2 checkpoint inspection completed."
)

print("=" * 70)


# =========================================================
# Stop Spark
# =========================================================

spark.stop()
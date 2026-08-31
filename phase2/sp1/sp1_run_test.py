from ingestion import NetworkIngestion
import os
import sys
python_path = sys.executable
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# =========================================================
# SP1 Runner
# =========================================================

INPUT_DIR = r"..\..\data\raw"


ingestion = NetworkIngestion(
    input_dir=INPUT_DIR
)


result = ingestion.process()


# =========================================================
# Results
# =========================================================

print("\n" + "=" * 60)
print("SP1 INGESTION RESULTS")
print("=" * 60)

for key, value in result["metrics"].items():
    print(
        f"{key}: {value}"
    )


print("\n" + "=" * 60)
print("FILE-LEVEL ROW COUNTS")
print("=" * 60)

result["file_report"].show(
    truncate=False
)


print("\n" + "=" * 60)
print("RAW NETWORK DATA")
print("=" * 60)

result["raw_network_df"].show(
    10,
    truncate=False
)


print("\n" + "=" * 60)
print("PARTITION COUNT")
print("=" * 60)

print(
    result["raw_network_df"]
    .rdd
    .getNumPartitions()
)

print("\n" + "=" * 60)
print("FINAL SP1 ACCEPTANCE RESULT")
print("=" * 60)

if result["acceptance"]["all_passed"]:
    print("ALL SP1 ACCEPTANCE CRITERIA PASSED")
else:
    print("SP1 ACCEPTANCE CRITERIA FAILED")

# =========================================================
# Stop Spark
# =========================================================

ingestion.stop()
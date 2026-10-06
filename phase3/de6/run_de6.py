# =========================================================
# DE6 — Run Warehouse Build + Validation
# File: phase3/de6/run_de6.py
# =========================================================


from pathlib import Path
import os
import sys

# =========================================================
# Python configuration
# =========================================================

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# =========================================================
# Make the DE6 directory importable
# =========================================================

DE6_DIR = Path(
    __file__
).resolve().parent

sys.path.insert(
    0,
    str(DE6_DIR)
)


from pyspark.sql import SparkSession

from warehouse_loader import (
    create_logger,
    WarehouseLoader,
)

from validate_warehouse import (
    validate,
)


# =========================================================
# Main
# =========================================================

def main():

    logger, log_file = (
        create_logger()
    )

    print()
    print("=" * 70)
    print("DE6 — MYSQL ANALYTICS WAREHOUSE")
    print("=" * 70)
    print(
        f"Log: {log_file}"
    )
    print("=" * 70)

    spark = None

    try:

        logger.info(
            "DE6 pipeline starting."
        )

        spark = (
            SparkSession.builder
            .master("local[*]")
            .config("spark.python.worker.reuse", "true")
            .appName(
                "DE6_MySQL_Analytics_Warehouse"
            )
            .getOrCreate()
        )
        
        loader = WarehouseLoader(
            spark=spark,
            logger=logger
        )

        counts = loader.run()

        logger.info(
            "Warehouse load completed."
        )

        print()
        print(
            "Warehouse load completed."
        )

        print(
            f"dim_time: "
            f"{counts['dim_time']:,}"
        )

        print(
            f"dim_grid: "
            f"{counts['dim_grid']:,}"
        )

        print(
            f"fact_network_activity: "
            f"{counts['fact']:,}"
        )

        # -------------------------------------------------
        # Validation
        # -------------------------------------------------

        print()
        print(
            "Running DE6 MySQL validation..."
        )

        validation_passed = (
            validate()
        )

        if not validation_passed:

            logger.error(
                "DE6 validation FAILED."
            )

            return 1

        logger.info(
            "DE6 pipeline completed successfully."
        )

        print()
        print("=" * 70)
        print("DE6 COMPLETE — ALL PASS")
        print("=" * 70)
        print(
            f"Log: {log_file}"
        )
        print("=" * 70)

        return 0

    except Exception as exc:

        logger.exception(
            "DE6 pipeline failed: %s",
            exc
        )

        print()
        print("=" * 70)
        print("DE6 FAILED")
        print("=" * 70)
        print(
            f"Reason: {exc}"
        )
        print(
            f"Log: {log_file}"
        )
        print("=" * 70)

        return 1

    finally:

        if spark is not None:

            spark.stop()


# =========================================================
# Entry point
# =========================================================

if __name__ == "__main__":

    raise SystemExit(
        main()
    )
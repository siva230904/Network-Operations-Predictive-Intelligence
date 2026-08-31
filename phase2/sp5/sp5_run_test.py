# =========================================================
# SP5 Runner
# File: phase2/sp5/sp5_run_test.py
# =========================================================

import os
import sys

# =========================================================
# Python environment
# =========================================================

python_path = sys.executable

os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path


# =========================================================
# Imports
# =========================================================

from pathlib import Path

from pyspark.sql import SparkSession

from performance import PerformanceAnalyzer


# =========================================================
# Paths
# =========================================================

BASE_DIR = (
    Path(__file__)
    .resolve()
    .parents[2]
)


# ---------------------------------------------------------
# SP2
# ---------------------------------------------------------

SP2_CHECKPOINT = (
    BASE_DIR
    / "data"
    / "landing"
    / "sp2"
    / "clean_network"
)


# ---------------------------------------------------------
# SP3
# ---------------------------------------------------------

SP3_CHECKPOINT = (
    BASE_DIR
    / "data"
    / "landing"
    / "sp3"
    / "hourly_grid_summary"
)


# ---------------------------------------------------------
# SP4
# ---------------------------------------------------------

SP4_CHECKPOINT = (
    BASE_DIR
    / "data"
    / "landing"
    / "sp4"
    / "grid_activity_geo"
)


# ---------------------------------------------------------
# SP5 output
# ---------------------------------------------------------

OUTPUT_DIR = (
    BASE_DIR
    / "data"
    / "landing"
    / "sp5"
)


# ---------------------------------------------------------
# Logs
# ---------------------------------------------------------

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
        "SP5-Performance-Analysis"
    )
    .config(
        "spark.sql.shuffle.partitions",
        "32"
    )
    .config(
        "spark.default.parallelism",
        "32"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel(
    "WARN"
)


# =========================================================
# Main
# =========================================================

try:

    # =====================================================
    # Load SP2
    # =====================================================

    print(
        "\nLoading SP2 clean checkpoint..."
    )

    print(
        SP2_CHECKPOINT
    )

    sp2_df = (
        spark.read
        .parquet(
            str(
                SP2_CHECKPOINT
            )
        )
    )

    # =====================================================
    # Load SP3
    # =====================================================

    print(
        "\nLoading SP3 hourly checkpoint..."
    )

    print(
        SP3_CHECKPOINT
    )

    sp3_df = (
        spark.read
        .parquet(
            str(
                SP3_CHECKPOINT
            )
        )
    )

    # =====================================================
    # Load SP4
    # =====================================================

    print(
        "\nLoading SP4 geospatial checkpoint..."
    )

    print(
        SP4_CHECKPOINT
    )

    sp4_df = (
        spark.read
        .parquet(
            str(
                SP4_CHECKPOINT
            )
        )
    )

    # =====================================================
    # Checkpoint counts
    # =====================================================

    print(
        "\n" + "=" * 70
    )

    print(
        "CHECKPOINT ROW COUNTS"
    )

    print(
        "=" * 70
    )

    sp2_count = (
        sp2_df.count()
    )

    sp3_count = (
        sp3_df.count()
    )

    sp4_count = (
        sp4_df.count()
    )

    print(
        f"SP2 clean rows : "
        f"{sp2_count:,}"
    )

    print(
        f"SP3 hourly rows: "
        f"{sp3_count:,}"
    )

    print(
        f"SP4 geo rows   : "
        f"{sp4_count:,}"
    )

    # =====================================================
    # Run SP5
    # =====================================================

    analyzer = PerformanceAnalyzer(

        spark=spark,

        sp2_df=sp2_df,

        sp3_df=sp3_df,

        sp4_df=sp4_df,

        output_dir=str(
            OUTPUT_DIR
        ),

        log_dir=str(
            LOG_DIR
        ),

        # -------------------------------------------------
        # IMPORTANT:
        #
        # Do not cache the complete 15M-row SP2 dataset.
        #
        # 10% sample + column pruning is sufficient to
        # demonstrate the cache behaviour while avoiding
        # local JVM heap exhaustion.
        # -------------------------------------------------

        cache_sample_fraction=0.10,

        cache_partitions=32
    )

    result = (
        analyzer.process()
    )

    # =====================================================
    # Final results
    # =====================================================

    print(
        "\n" + "=" * 70
    )

    print(
        "SP5 COMPLETED"
    )

    print(
        "=" * 70
    )

    # -----------------------------------------------------
    # Cache
    # -----------------------------------------------------

    print(
        "\nCACHE TIMING"
    )

    cache = (
        result[
            "cache_timings"
        ]
    )

    print(
        f"Baseline uncached : "
        f"{cache['baseline_uncached_seconds']:.3f} sec"
    )

    print(
        f"First persisted   : "
        f"{cache['first_persisted_action_seconds']:.3f} sec"
    )

    print(
        f"Second persisted  : "
        f"{cache['second_persisted_action_seconds']:.3f} sec"
    )

    print(
        f"Timing change     : "
        f"{cache['improvement_percentage']:.2f}%"
    )

    print(
        f"Rows in experiment: "
        f"{cache['first_action_rows']:,}"
    )

    print(
        f"Storage level     : "
        f"{cache['storage_level']}"
    )

    # -----------------------------------------------------
    # Repartition
    # -----------------------------------------------------

    print(
        "\nREPARTITION"
    )

    repartition = (
        result[
            "repartition"
        ]
    )

    print(
        f"Original partitions : "
        f"{repartition['original_partitions']}"
    )

    print(
        f"Requested partitions: "
        f"{repartition['requested_partitions']}"
    )

    print(
        f"Observed partitions : "
        f"{repartition['observed_partitions']}"
    )

    # -----------------------------------------------------
    # Column pruning
    # -----------------------------------------------------

    print(
        "\nCOLUMN PRUNING"
    )

    pruning = (
        result[
            "column_pruning"
        ]
    )

    print(
        f"Original columns : "
        f"{pruning['original_column_count']}"
    )

    print(
        f"Required columns : "
        f"{pruning['required_column_count']}"
    )

    print(
        f"Columns removed  : "
        f"{pruning['columns_removed']}"
    )

    # -----------------------------------------------------
    # Observations
    # -----------------------------------------------------

    print(
        "\nPERFORMANCE OBSERVATIONS"
    )

    for index, observation in enumerate(
        result[
            "observations"
        ],
        start=1
    ):

        print(
            f"\nObservation {index}"
        )

        print(
            f"  Observation: "
            f"{observation['observation']}"
        )

        print(
            f"  Evidence: "
            f"{observation['evidence']}"
        )

        print(
            f"  Decision: "
            f"{observation['decision']}"
        )

        if "replacement" in observation:

            print(
                f"  Replacement: "
                f"{observation['replacement']}"
            )

    # -----------------------------------------------------
    # Acceptance
    # -----------------------------------------------------

    print(
        "\nACCEPTANCE"
    )

    acceptance = (
        result[
            "acceptance"
        ]
    )

    for name, passed in (
        acceptance[
            "criteria"
        ].items()
    ):

        print(
            f"  {name}: "
            f"{'PASS' if passed else 'FAIL'}"
        )

    # -----------------------------------------------------
    # Output files
    # -----------------------------------------------------

    print(
        "\nOUTPUTS"
    )

    for name, path in (
        result[
            "output_paths"
        ].items()
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
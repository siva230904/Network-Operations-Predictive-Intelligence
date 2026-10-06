# =========================================================
# SP7 — Telecom Pipeline Runner
# File: spark/run_telecom_pipeline.py
# =========================================================

import subprocess
import sys
from pathlib import Path


# =========================================================
# Project root
# =========================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)


# =========================================================
# Configurable paths
# =========================================================

# ---------------------------------------------------------
# Training input
#
# Change this path if your supplied daily files are stored
# somewhere else.
# ---------------------------------------------------------

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
)


# ---------------------------------------------------------
# SP7 output
# ---------------------------------------------------------

OUTPUT_PATH = (
    PROJECT_ROOT
    / "data"
)


# ---------------------------------------------------------
# Static Milan grid reference
# ---------------------------------------------------------

REFERENCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "reference"
    / "milano-grid.geojson"
)


# ---------------------------------------------------------
# SP7 logs
# ---------------------------------------------------------

LOG_DIR = (
    PROJECT_ROOT
    / "data"
    / "logs"
)


# =========================================================
# Main
# =========================================================

def main():

    pipeline_file = (
        Path(__file__)
        .resolve()
        .parent
        /
        "telecom_pipeline.py"
    )

    # -----------------------------------------------------
    # Check configuration before starting Spark.
    # -----------------------------------------------------

    if not INPUT_PATH.exists():

        print(
            "ERROR: Input folder does not exist:"
        )

        print(
            f"  {INPUT_PATH}"
        )

        return 1

    if not REFERENCE_PATH.exists():

        print(
            "ERROR: Milan grid reference does not exist:"
        )

        print(
            f"  {REFERENCE_PATH}"
        )

        return 1

    # -----------------------------------------------------
    # Show configuration.
    # -----------------------------------------------------

    print("\n" + "=" * 70)
    print("SP7 TELECOM PIPELINE")
    print("=" * 70)

    print(
        f"\nInput:"
        f"\n  {INPUT_PATH}"
    )

    print(
        f"\nOutput:"
        f"\n  {OUTPUT_PATH}"
    )

    print(
        f"\nReference:"
        f"\n  {REFERENCE_PATH}"
    )

    print(
        f"\nLogs:"
        f"\n  {LOG_DIR}"
    )

    print(
        "\n" + "=" * 70
    )

    # -----------------------------------------------------
    # Execute telecom_pipeline.py.
    #
    # The job itself owns the ETL logic.
    # -----------------------------------------------------

    command = [
        sys.executable,
        str(pipeline_file),

        "--input",
        str(INPUT_PATH),

        "--output",
        str(OUTPUT_PATH),

        "--reference",
        str(REFERENCE_PATH),

        "--log-dir",
        str(LOG_DIR),
    ]

    result = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT)
    )

    # -----------------------------------------------------
    # Preserve the Spark job exit code.
    # -----------------------------------------------------

    return result.returncode


# =========================================================
# Script entry point
# =========================================================

if __name__ == "__main__":

    sys.exit(
        main()
    )


from __future__ import annotations

import os
from pathlib import Path


# =========================================================
# Project root
# =========================================================

PROJECT_ROOT = Path(
    os.getenv(
        "PROJECT_ROOT",
        Path(__file__).resolve().parents[2],
    )
).resolve()


# =========================================================
# DE2 storage zones
# =========================================================

DATA_ROOT = PROJECT_ROOT / "data"

LANDING_DIR = DATA_ROOT / "landing"
RAW_DIR = DATA_ROOT / "raw"
REJECTED_DIR = DATA_ROOT / "rejected"
REFERENCE_DIR = DATA_ROOT / "reference"
LOG_DIR = DATA_ROOT / "logs"


# =========================================================
# Input file pattern
# =========================================================

INPUT_FILE_PATTERN = "sms-call-internet-mi-*.csv"
SOURCE_FILENAME_REGEX = (r"^sms-call-internet-mi-\d{4}-\d{2}-\d{2}\.csv$")

# =========================================================
# Expected source schema
# =========================================================

REQUIRED_COLUMNS = [
    "datetime",
    "CellID",
    "countrycode",
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
]


# =========================================================
# Column validation rules
# =========================================================

ACTIVITY_COLUMNS = [
    "smsin",
    "smsout",
    "callin",
    "callout",
    "internet",
]


# Missing activity measurements are allowed.
# Populated activity values must be numeric and non-negative.

ALLOW_MISSING_ACTIVITY = True
ALLOW_NEGATIVE_ACTIVITY = False


# =========================================================
# Idempotency behavior
# =========================================================

# If the same source filename has already been accepted into
# the raw zone, the ingestion attempt is skipped and recorded
# in the audit log.

SKIP_ALREADY_PROCESSED = True


# =========================================================
# Audit log
# =========================================================

AUDIT_LOG_FILE = (
    LOG_DIR / "de2_ingestion_audit.csv"
)


# =========================================================
# Ensure DE2 directories exist
# =========================================================

def ensure_directories() -> None:
    """
    Create the DE2 storage directories if they do not exist.

    The reference directory is intentionally NOT created or
    modified by DE2 because the static GeoJSON is managed
    independently.
    """

    LANDING_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    RAW_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    REJECTED_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    LOG_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )
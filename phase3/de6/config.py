# =========================================================
# DE6 — Configuration
# File: phase3/de6/config.py
# =========================================================

from pathlib import Path
import os

from dotenv import load_dotenv


load_dotenv()


# =========================================================
# Project paths
# =========================================================

PROJECT_ROOT = Path(
    os.environ.get(
        "PROJECT_ROOT",
        Path(__file__).resolve().parents[2]
    )
)


# =========================================================
# Spark output
# =========================================================

HOURLY_GRID_SUMMARY_PATH = (
    PROJECT_ROOT
    / "data"
    / "landing"
    / "sp3"
    / "hourly_grid_summary"
)

HOURLY_GRID_SUMMARY_PATH = Path(
    os.environ.get(
        "HOURLY_GRID_SUMMARY_PATH",
        str(HOURLY_GRID_SUMMARY_PATH)
    )
)


# =========================================================
# Static reference
# =========================================================

GEOJSON_PATH = Path(
    os.environ.get(
        "GEOJSON_PATH",
        str(
            PROJECT_ROOT
            / "data"
            / "reference"
            / "milano-grid.geojson"
        )
    )
)


# =========================================================
# DE6 logs
# =========================================================

LOG_DIR = Path(
    os.environ.get(
        "DE6_LOG_DIR",
        str(
            PROJECT_ROOT
            / "data"
            / "logs"
        )
    )
)


# =========================================================
# MySQL configuration
# =========================================================

MYSQL_HOST = os.getenv(
    "MYSQL_HOST",
    "localhost"
)

MYSQL_PORT = int(
    os.getenv(
        "MYSQL_PORT",
        "3306"
    )
)

MYSQL_USER = os.getenv(
    "MYSQL_USER",
    "root"
)

MYSQL_PASSWORD = os.getenv(
    "MYSQL_PASSWORD",
    ""
)

MYSQL_DATABASE = os.getenv(
    "MYSQL_DATABASE",
    "telecom_analytics"
)

if not MYSQL_PASSWORD:
    raise RuntimeError(
        "MYSQL_PASSWORD is not configured. "
        "Set it in D:\\Project1\\.env"
    )


# =========================================================
# MySQL connection settings
# =========================================================

MYSQL_CONNECT_TIMEOUT = int(
    os.getenv(
        "MYSQL_CONNECT_TIMEOUT",
        "30"
    )
)


# =========================================================
# DE6 expectations
# =========================================================

EXPECTED_GRID_COUNT = 10000
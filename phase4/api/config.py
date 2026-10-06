# =========================================================
# API1 — Configuration
# File: phase4/api/config.py
# =========================================================

import os
from pathlib import Path

from dotenv import load_dotenv


# =========================================================
# Project root
# =========================================================

PROJECT_ROOT = Path(
    __file__
).resolve().parents[2]


# =========================================================
# Load .env
# =========================================================

ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(
    ENV_FILE
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

NP3_ALERT_PATH = (
    PROJECT_ROOT
    / "data"
    / "landing"
    / "network_alerts.csv"
)
# =========================================================
# SQLAlchemy
# =========================================================

DATABASE_URL = (
    "mysql+mysqlconnector://"
    f"{MYSQL_USER}:"
    f"{MYSQL_PASSWORD}@"
    f"{MYSQL_HOST}:"
    f"{MYSQL_PORT}/"
    f"{MYSQL_DATABASE}"
)


# =========================================================
# API configuration
# =========================================================

API_TITLE = (
    "Telecom Network Intelligence API"
)

API_VERSION = "1.0.0"
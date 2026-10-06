# =========================================================
# API1 — SQLAlchemy Database
# File: phase4/api/db/database.py
# =========================================================

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from ..config import DATABASE_URL


# =========================================================
# SQLAlchemy Engine
# =========================================================

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    future=True,
)


# =========================================================
# Session Factory
# =========================================================

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)


# =========================================================
# FastAPI Dependency
# =========================================================

def get_db():
    """
    Provide one SQLAlchemy session per request.
    """

    db = SessionLocal()

    try:

        yield db

    finally:

        db.close()
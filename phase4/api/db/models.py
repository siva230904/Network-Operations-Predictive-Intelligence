# =========================================================
# API1 — SQLAlchemy Warehouse Models
# File: phase4/api/db/models.py
# =========================================================

from sqlalchemy import (
    BigInteger,
    Date,
    DateTime,
    Float,
    Integer,
    SmallInteger,
    Text,
    Column,
    String,
)

from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
)


# =========================================================
# Base
# =========================================================

class Base(DeclarativeBase):
    pass


# =========================================================
# DIM_TIME
# =========================================================

class DimTime(Base):

    __tablename__ = "dim_time"

    time_key: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True
    )

    timestamp: Mapped[object] = mapped_column(
        DateTime,
        nullable=False
    )

    date: Mapped[object] = mapped_column(
        Date,
        nullable=False
    )

    hour: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False
    )

    day_of_week: Mapped[int] = mapped_column(
        SmallInteger,
        nullable=False
    )


# =========================================================
# DIM_GRID
# =========================================================

class DimGrid(Base):

    __tablename__ = "dim_grid"

    grid_key: Mapped[int] = mapped_column(
        Integer,
        primary_key=True
    )

    grid_id: Mapped[int] = mapped_column(
        Integer,
        nullable=False
    )

    latitude: Mapped[float | None] = mapped_column(
        Float,
        nullable=True
    )

    longitude: Mapped[float | None] = mapped_column(
        Float,
        nullable=True
    )

    geometry_reference: Mapped[str | None] = mapped_column(
        Text,
        nullable=True
    )


# =========================================================
# FACT_NETWORK_ACTIVITY
# =========================================================

class FactNetworkActivity(Base):

    __tablename__ = "fact_network_activity"

    time_key: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True
    )

    grid_key: Mapped[int] = mapped_column(
        Integer,
        primary_key=True
    )

    sms_in: Mapped[float] = mapped_column(
        Float,
        nullable=False
    )

    sms_out: Mapped[float] = mapped_column(
        Float,
        nullable=False
    )

    call_in: Mapped[float] = mapped_column(
        Float,
        nullable=False
    )

    call_out: Mapped[float] = mapped_column(
        Float,
        nullable=False
    )

    internet_activity: Mapped[float] = mapped_column(
        Float,
        nullable=False
    )

    total_sms: Mapped[float] = mapped_column(
        Float,
        nullable=False
    )

    total_calls: Mapped[float] = mapped_column(
        Float,
        nullable=False
    )

    total_activity: Mapped[float] = mapped_column(
        Float,
        nullable=False
    )

# =========================================================
# API4 — ML2 Feature Table Model
# Add to:
# phase4/api/db/models.py
# =========================================================



# Use the same Base that your existing models use.
# For example, if your file already has:
#
# from .database import Base
#
# keep that existing import.

# =========================================================
# ML GRID FEATURES
# =========================================================

class MLGridFeatures(Base):
    """
    Stored ML2 feature table.

    API4 reads these values exactly as persisted by ML2.

    IMPORTANT:
        API4 does not calculate any feature values.
    """

    __tablename__ = "ml_grid_features"

    grid_id = Column(
        Integer,
        primary_key=True,
        nullable=False,
    )

    feature_timestamp = Column(
        DateTime,
        primary_key=True,
        nullable=False,
    )

    avg_activity = Column(
        Float,
        nullable=False,
    )

    activity_growth = Column(
        Float,
        nullable=False,
    )

    active_hours = Column(
        Integer,
        nullable=False,
    )

    peak_ratio = Column(
        Float,
        nullable=False,
    )

    variability = Column(
        Float,
        nullable=False,
    )

    internet_share = Column(
        Float,
        nullable=False,
    )

# =========================================================
# ML6 — NETWORK RISK SCORES
# =========================================================

class NetworkRiskScore(Base):
    """
    Persisted ML6 batch scoring result.

    One row represents one grid at one scoring timestamp.

    ML3 risk fields:
        risk_score
        risk_level
        model_version

    ML4 anomaly fields:
        anomaly_score
        anomaly_direction
        is_anomaly
        anomaly_reason

    ML6 does not retrain the model.
    It operationalizes the frozen ML3 model and
    existing ML4 anomaly methodology.
    """

    __tablename__ = "network_risk_scores"

    grid_id = Column(
        Integer,
        primary_key=True,
        nullable=False,
    )

    timestamp = Column(
        DateTime,
        primary_key=True,
        nullable=False,
    )

    risk_score = Column(
        Float,
        nullable=False,
    )

    risk_level = Column(
        String(20),
        nullable=False,
    )

    model_version = Column(
        String(100),
        nullable=False,
    )

    anomaly_score = Column(
        Float,
        nullable=True,
    )

    anomaly_direction = Column(
        String(10),
        nullable=True,
    )

    is_anomaly = Column(
        Integer,
        nullable=False,
        default=0,
    )

    anomaly_reason = Column(
        Text,
        nullable=True,
    )
from __future__ import annotations

import sys
import os
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy import delete, select

# Make the project root importable when this file is run directly.
PROJECT_ROOT = Path(
    os.getenv(
        "PROJECT_ROOT",
        Path(__file__).resolve().parents[2],
    )
).resolve()

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from phase4.api.db.database import SessionLocal
from phase4.api.db.models import (
    DimGrid,
    DimTime,
    FactNetworkActivity,
    MLGridFeatures,
)

from phase6.ml.features import (
    ActivityRecord,
    calculate_features,
)


# =========================================================
# Configuration
# =========================================================

MIN_HISTORY_HOURS = 48


# =========================================================
# Data loading
# =========================================================

def load_activity_data(session):
    """
    Load historical network activity joined with grid/time dimensions.

    Returns rows ordered by grid and timestamp.
    """

    statement = (
        select(
            DimGrid.grid_id,
            DimTime.timestamp,
            FactNetworkActivity.total_activity,
            FactNetworkActivity.internet_activity,
        )
        .join(
            FactNetworkActivity,
            FactNetworkActivity.grid_key == DimGrid.grid_key,
        )
        .join(
            DimTime,
            DimTime.time_key == FactNetworkActivity.time_key,
        )
        .order_by(
            DimGrid.grid_id,
            DimTime.timestamp,
        )
    )

    return session.execute(statement).all()


# =========================================================
# Feature generation
# =========================================================

def build_features(rows):
    """
    Build ML2 features for every grid/time combination
    with at least 48 consecutive hourly observations.

    Recent window:
        t-23 ... t

    Prior window:
        t-47 ... t-24
    """

    by_grid = defaultdict(list)

    for row in rows:
        by_grid[row.grid_id].append(
            ActivityRecord(
                grid_id=row.grid_id,
                timestamp=row.timestamp,
                total_activity=float(row.total_activity),
                internet_activity=float(row.internet_activity),
            )
        )

    generated = []

    for grid_id, records in by_grid.items():

        records.sort(key=lambda record: record.timestamp)

        if len(records) < MIN_HISTORY_HOURS:
            continue

        for end_index in range(
            MIN_HISTORY_HOURS - 1,
            len(records),
        ):

            window = records[
                end_index - MIN_HISTORY_HOURS + 1:
                end_index + 1
            ]

            prior_24h = window[:24]
            recent_24h = window[24:]

            # -------------------------------------------------
            # Verify that the observations really represent
            # 48 consecutive hourly timestamps.
            # -------------------------------------------------

            expected_start = window[0].timestamp

            is_continuous = all(
                window[index].timestamp
                - window[index - 1].timestamp
                == timedelta(hours=1)
                for index in range(1, len(window))
            )

            if not is_continuous:
                continue

            assert recent_24h[-1].timestamp == records[end_index].timestamp
            assert prior_24h[-1].timestamp < recent_24h[0].timestamp

            features = calculate_features(
                recent_24h=recent_24h,
                prior_24h=prior_24h,
            )

            generated.append(features)

    return generated


# =========================================================
# Database persistence
# =========================================================

def save_features(session, features):
    """
    Persist ML2 features in manageable batches.

    The feature table is rebuilt from the current source data.
    Batching prevents a single 1.2M-row transaction from overwhelming
    the MySQL connection.
    """

    BATCH_SIZE = 5000

    # Clear existing generated features.
    session.execute(delete(MLGridFeatures))
    session.commit()

    total = len(features)

    for start in range(0, total, BATCH_SIZE):
        batch = features[start:start + BATCH_SIZE]

        rows = [
            {
                "grid_id": feature.grid_id,
                "feature_timestamp": feature.feature_timestamp,
                "avg_activity": feature.avg_activity,
                "activity_growth": feature.activity_growth,
                "active_hours": feature.active_hours,
                "peak_ratio": feature.peak_ratio,
                "variability": feature.variability,
                "internet_share": feature.internet_share,
            }
            for feature in batch
        ]

        session.bulk_insert_mappings(
            MLGridFeatures,
            rows,
        )

        session.commit()

        inserted = min(start + BATCH_SIZE, total)

        print(
            f"Saved {inserted:,} / {total:,} feature rows..."
        )

# =========================================================
# Main generation workflow
# =========================================================

def generate_features():
    """
    Full ML2 feature-generation workflow.
    """

    session = SessionLocal()

    try:

        print("Loading network activity data...")

        rows = load_activity_data(session)

        print(
            f"Loaded {len(rows):,} activity rows."
        )

        if not rows:
            raise RuntimeError(
                "No network activity data was found."
            )

        print("Generating ML2 features...")

        features = build_features(rows)

        print(
            f"Generated {len(features):,} feature rows."
        )

        if not features:
            raise RuntimeError(
                "No ML2 features were generated. "
                "Check that at least 48 consecutive hourly "
                "observations exist for the grids."
            )

        print("Saving features to ml_grid_features...")

        save_features(
            session,
            features,
        )

        print("ML2 feature generation completed successfully.")

    except Exception:
        session.rollback()
        raise

    finally:
        session.close()


if __name__ == "__main__":
    generate_features()
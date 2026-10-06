# =========================================================
# API4 — Grid Feature Service
# File:
# phase4/api/services/feature_service.py
# =========================================================

from datetime import datetime
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from ..db.models import MLGridFeatures


# =========================================================
# Service
# =========================================================

class FeatureService:
    """
    Service layer for API4.

    Responsibilities:

        - Read stored ML2 feature values.
        - Validate grid existence through the feature table.
        - Apply optional timestamp filtering.
        - Return data-quality and freshness metadata.

    This service DOES NOT calculate ML features.
    """

    # =====================================================
    # Constructor
    # =====================================================

    def __init__(
        self,
        db: Session,
    ):
        self.db = db

    # =====================================================
    # 1. Resolve feature timestamp
    # =====================================================

    def get_latest_feature_timestamp(
        self,
        grid_id: int,
    ) -> Optional[datetime]:
        """
        Return the latest stored feature timestamp
        for the requested grid.
        """

        return (
            self.db
            .query(
                func.max(
                    MLGridFeatures.feature_timestamp
                )
            )
            .filter(
                MLGridFeatures.grid_id == grid_id
            )
            .scalar()
        )

    # =====================================================
    # 2. Determine freshness
    # =====================================================

    @staticmethod
    def determine_freshness(
        feature_timestamp: datetime,
        latest_timestamp: datetime,
    ) -> str:
        """
        Determine freshness relative to the latest stored
        feature timestamp.

        This is metadata only.

        No feature calculation occurs here.
        """

        if feature_timestamp == latest_timestamp:
            return "CURRENT"

        if feature_timestamp < latest_timestamp:
            return "HISTORICAL"

        return "UNKNOWN"

    # =====================================================
    # 3. Get grid features
    # =====================================================

    def get_grid_features(
        self,
        grid_id: int,
        feature_timestamp: Optional[datetime] = None,
    ):
        """
        Return the stored ML2 feature vector.

        If feature_timestamp is supplied:
            return the stored feature record for that
            grid/timestamp.

        If feature_timestamp is omitted:
            return the latest stored feature record
            for that grid.

        IMPORTANT:
            No feature arithmetic is performed.
        """

        # -------------------------------------------------
        # Validate grid range
        # -------------------------------------------------

        if grid_id < 1 or grid_id > 10000:

            return None

        # -------------------------------------------------
        # Explicit timestamp
        # -------------------------------------------------

        if feature_timestamp is not None:

            row = (
                self.db
                .query(
                    MLGridFeatures
                )
                .filter(
                    MLGridFeatures.grid_id
                    == grid_id
                )
                .filter(
                    MLGridFeatures.feature_timestamp
                    == feature_timestamp
                )
                .first()
            )

            if row is None:
                return None

            latest_timestamp = (
                self.get_latest_feature_timestamp(
                    grid_id
                )
            )

        # -------------------------------------------------
        # Latest stored feature
        # -------------------------------------------------

        else:

            latest_timestamp = (
                self.get_latest_feature_timestamp(
                    grid_id
                )
            )

            if latest_timestamp is None:
                return None

            row = (
                self.db
                .query(
                    MLGridFeatures
                )
                .filter(
                    MLGridFeatures.grid_id
                    == grid_id
                )
                .filter(
                    MLGridFeatures.feature_timestamp
                    == latest_timestamp
                )
                .first()
            )

            if row is None:
                return None

        # -------------------------------------------------
        # Data-quality status
        # -------------------------------------------------

        values = [
            row.avg_activity,
            row.activity_growth,
            row.active_hours,
            row.peak_ratio,
            row.variability,
            row.internet_share,
        ]

        if any(
            value is None
            for value in values
        ):
            data_quality = "INVALID"

        else:
            data_quality = "VALID"

        # -------------------------------------------------
        # Freshness
        # -------------------------------------------------

        if latest_timestamp is None:

            freshness = "UNKNOWN"

        else:

            freshness = (
                self.determine_freshness(
                    row.feature_timestamp,
                    latest_timestamp,
                )
            )

        # -------------------------------------------------
        # Return stored values exactly.
        # -------------------------------------------------

        return {
            "grid_id": int(
                row.grid_id
            ),

            "avg_activity": float(
                row.avg_activity
            ),

            "activity_growth": float(
                row.activity_growth
            ),

            "active_hours": int(
                row.active_hours
            ),

            "peak_ratio": float(
                row.peak_ratio
            ),

            "variability": float(
                row.variability
            ),

            "internet_share": float(
                row.internet_share
            ),

            "feature_timestamp":
                row.feature_timestamp,

            "data_quality":
                data_quality,

            "freshness":
                freshness,
        }
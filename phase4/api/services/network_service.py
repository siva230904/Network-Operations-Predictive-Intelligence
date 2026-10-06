# =========================================================
# API1 + API2 — Network Service
# File: phase4/api/services/network_service.py
# =========================================================

from datetime import datetime, timedelta

from sqlalchemy import (
    func,
    select,
)
from sqlalchemy.orm import Session

from ..db.models import (
    DimGrid,
    DimTime,
    FactNetworkActivity,
)


# =========================================================
# Network Service
# =========================================================

class NetworkService:

    def __init__(
        self,
        db: Session
    ):

        self.db = db

    # =====================================================
    # API1
    # Determine effective AS_OF
    # =====================================================

    def get_effective_as_of(
        self,
        requested_as_of: datetime | None
    ) -> datetime:

        if requested_as_of is not None:

            return requested_as_of

        statement = (
            select(
                func.max(
                    DimTime.timestamp
                )
            )
        )

        result = self.db.execute(
            statement
        )

        maximum_timestamp = (
            result.scalar_one_or_none()
        )

        if maximum_timestamp is None:

            raise RuntimeError(
                "Analytics warehouse contains "
                "no timestamps."
            )

        return maximum_timestamp

    # =====================================================
    # API1
    # Total activity
    # =====================================================

    def get_total_activity(
        self,
        as_of: datetime
    ) -> float:

        statement = (
            select(
                func.coalesce(
                    func.sum(
                        FactNetworkActivity.total_activity
                    ),
                    0
                )
            )
            .join(
                DimTime,
                FactNetworkActivity.time_key
                ==
                DimTime.time_key
            )
            .where(
                DimTime.timestamp <= as_of
            )
        )

        result = self.db.execute(
            statement
        )

        return float(
            result.scalar_one()
        )

    # =====================================================
    # API1
    # Active grids
    # =====================================================

    def get_active_grids(
        self,
        as_of: datetime
    ) -> int:

        statement = (
            select(
                func.count(
                    func.distinct(
                        FactNetworkActivity.grid_key
                    )
                )
            )
            .join(
                DimTime,
                FactNetworkActivity.time_key
                ==
                DimTime.time_key
            )
            .where(
                DimTime.timestamp <= as_of
            )
            .where(
                FactNetworkActivity.total_activity > 0
            )
        )

        result = self.db.execute(
            statement
        )

        return int(
            result.scalar_one()
        )

    # =====================================================
    # API1
    # Peak hour
    # =====================================================

    def get_peak_hour(
        self,
        as_of: datetime
    ) -> datetime:

        statement = (
            select(
                DimTime.timestamp,
                func.sum(
                    FactNetworkActivity.total_activity
                ).label(
                    "total_activity"
                )
            )
            .join(
                DimTime,
                FactNetworkActivity.time_key
                ==
                DimTime.time_key
            )
            .where(
                DimTime.timestamp <= as_of
            )
            .group_by(
                DimTime.timestamp
            )
            .order_by(
                func.sum(
                    FactNetworkActivity.total_activity
                ).desc(),
                DimTime.timestamp.asc()
            )
            .limit(1)
        )

        result = self.db.execute(
            statement
        )

        row = result.first()

        if row is None:

            raise RuntimeError(
                "Unable to determine peak "
                "activity hour."
            )

        return row.timestamp

    # =====================================================
    # API1
    # Top grid
    # =====================================================

    def get_top_grid(
        self,
        as_of: datetime
    ) -> int:

        statement = (
            select(
                DimGrid.grid_id,
                func.sum(
                    FactNetworkActivity.total_activity
                ).label(
                    "total_activity"
                )
            )
            .join(
                DimGrid,
                FactNetworkActivity.grid_key
                ==
                DimGrid.grid_key
            )
            .join(
                DimTime,
                FactNetworkActivity.time_key
                ==
                DimTime.time_key
            )
            .where(
                DimTime.timestamp <= as_of
            )
            .group_by(
                DimGrid.grid_id
            )
            .order_by(
                func.sum(
                    FactNetworkActivity.total_activity
                ).desc(),
                DimGrid.grid_id.asc()
            )
            .limit(1)
        )

        result = self.db.execute(
            statement
        )

        row = result.first()

        if row is None:

            raise RuntimeError(
                "Unable to determine top grid."
            )

        return int(
            row.grid_id
        )

    # =====================================================
    # API1
    # Complete summary
    # =====================================================

    def get_summary(
        self,
        requested_as_of: datetime | None
    ) -> dict:

        effective_as_of = (
            self.get_effective_as_of(
                requested_as_of
            )
        )

        return {
            "total_activity":
                self.get_total_activity(
                    effective_as_of
                ),

            "active_grids":
                self.get_active_grids(
                    effective_as_of
                ),

            "peak_hour":
                self.get_peak_hour(
                    effective_as_of
                ),

            "top_grid":
                self.get_top_grid(
                    effective_as_of
                ),

            "as_of":
                effective_as_of,
        }

    # =====================================================
    # API2
    # Validate grid
    # =====================================================

    def grid_exists(
        self,
        grid_id: int
    ) -> bool:

        # -------------------------------------------------
        # API2 requirement:
        # Any grid outside 1-10000 is unknown.
        # -------------------------------------------------

        if not 1 <= grid_id <= 10000:

            return False

        statement = (
            select(
                DimGrid.grid_key
            )
            .where(
                DimGrid.grid_id == grid_id
            )
            .limit(1)
        )

        result = self.db.execute(
            statement
        )

        return (
            result.scalar_one_or_none()
            is not None
        )

    # =====================================================
    # API2
    # Determine grid activity window
    # =====================================================

    def get_grid_activity(
        self,
        grid_id: int,
        requested_date=None,
        requested_hour: int | None = None,
        requested_as_of: datetime | None = None
    ) -> dict:

        # -------------------------------------------------
        # 1. Validate grid
        # -------------------------------------------------

        if not self.grid_exists(
            grid_id
        ):

            raise LookupError(
                f"Grid {grid_id} was not found."
            )

        # -------------------------------------------------
        # 2. Determine AS_OF
        # -------------------------------------------------

        effective_as_of = (
            self.get_effective_as_of(
                requested_as_of
            )
        )

        # -------------------------------------------------
        # 3. Build base query
        # -------------------------------------------------

        statement = (
            select(
                DimTime.timestamp,
                FactNetworkActivity.sms_in,
                FactNetworkActivity.sms_out,
                FactNetworkActivity.call_in,
                FactNetworkActivity.call_out,
                FactNetworkActivity.internet_activity,
                FactNetworkActivity.total_sms,
                FactNetworkActivity.total_calls,
                FactNetworkActivity.total_activity,
            )
            .join(
                DimTime,
                FactNetworkActivity.time_key
                ==
                DimTime.time_key
            )
            .join(
                DimGrid,
                FactNetworkActivity.grid_key
                ==
                DimGrid.grid_key
            )
            .where(
                DimGrid.grid_id == grid_id
            )
        )

        # -------------------------------------------------
        # 4. Apply AS_OF
        # -------------------------------------------------

        statement = statement.where(
            DimTime.timestamp <= effective_as_of
        )

        # -------------------------------------------------
        # 5. Optional date filter
        # -------------------------------------------------

        if requested_date is not None:

            statement = statement.where(
                DimTime.date == requested_date
            )

        # -------------------------------------------------
        # 6. Optional hour filter
        # -------------------------------------------------

        if requested_hour is not None:

            statement = statement.where(
                DimTime.hour == requested_hour
            )

        # -------------------------------------------------
        # 7. Default trailing 24-hour window
        #
        # Only apply the default when date and hour
        # are not explicitly supplied.
        # -------------------------------------------------

        if (
            requested_date is None
            and
            requested_hour is None
        ):

            window_start = (
                effective_as_of
                -
                timedelta(hours=23)
            )

            statement = statement.where(
                DimTime.timestamp >= window_start
            )

        # -------------------------------------------------
        # 8. Order chronologically
        # -------------------------------------------------

        statement = statement.order_by(
            DimTime.timestamp.asc()
        )

        result = self.db.execute(
            statement
        )

        rows = result.fetchall()

        # -------------------------------------------------
        # 9. Convert rows
        # -------------------------------------------------

        activity = []

        seen_timestamps = set()

        for row in rows:

            timestamp = row.timestamp

            # -------------------------------------------------
            # Safety check against duplicate timestamps.
            # -------------------------------------------------

            if timestamp in seen_timestamps:

                raise RuntimeError(
                    "Duplicate timestamp detected for "
                    f"grid {grid_id}: {timestamp}"
                )

            seen_timestamps.add(
                timestamp
            )

            activity.append(
                {
                    "timestamp":
                        timestamp,

                    "sms_in":
                        float(row.sms_in),

                    "sms_out":
                        float(row.sms_out),

                    "call_in":
                        float(row.call_in),

                    "call_out":
                        float(row.call_out),

                    "internet_activity":
                        float(
                            row.internet_activity
                        ),

                    "total_sms":
                        float(row.total_sms),

                    "total_calls":
                        float(row.total_calls),

                    "total_activity":
                        float(
                            row.total_activity
                        ),
                }
            )

        return {
            "grid_id":
                grid_id,

            "as_of":
                effective_as_of,

            "activity":
                activity,
        }
# =========================================================
# API3 — Hotspot & Alert Service
# File:
# phase4/api/services/hotspot_alert_service.py
# =========================================================

from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from ..config import NP3_ALERT_PATH

from ..db.models import (
    DimGrid,
    DimTime,
    FactNetworkActivity,
)


# =========================================================
# Severity mapping
# =========================================================

ALERT_SEVERITY = {
    "HIGH_ACTIVITY": "HIGH",
    "ACTIVITY_SPIKE": "MEDIUM",
    "ACTIVITY_DROP": "MEDIUM",
}


# =========================================================
# Hotspot Service
# =========================================================

class HotspotAlertService:
    """
    Service layer for API3.

    Responsibilities:

        /network/hotspots
            -> MySQL analytics warehouse

        /network/alerts
            -> NP3 rule-based alert output

    No raw activity CSV is read here.
    """

    def __init__(
        self,
        db: Session
    ):
        self.db = db

    # =====================================================
    # 1. Determine AS_OF
    # =====================================================

    def get_as_of(
        self,
        requested_as_of: Optional[datetime] = None
    ) -> datetime:
        """
        Resolve the effective reporting timestamp.

        If as_of is explicitly supplied, use it.

        Otherwise use MAX(timestamp) from the analytics
        warehouse.
        """

        if requested_as_of is not None:
            return requested_as_of

        max_timestamp = (
            self.db
            .query(
                func.max(
                    DimTime.timestamp
                )
            )
            .scalar()
        )

        if max_timestamp is None:

            raise RuntimeError(
                "Analytics warehouse contains no timestamps."
            )

        return max_timestamp

    # =====================================================
    # 2. Get hotspots
    # =====================================================

    def get_hotspots(
        self,
        limit: int,
        as_of: Optional[datetime] = None
    ):
        """
        Return the highest-activity grid/hour records.

        Source:
            fact_network_activity
            dim_grid
            dim_time

        The query is restricted to the effective AS_OF
        timestamp so the response represents the current
        reporting window.
        """

        effective_as_of = self.get_as_of(
            as_of
        )

        query = (
            self.db
            .query(
                DimGrid.grid_id,
                DimTime.timestamp,

                FactNetworkActivity.total_activity,

                FactNetworkActivity.total_sms,

                FactNetworkActivity.total_calls,

                FactNetworkActivity.internet_activity,
            )
            .join(
                DimTime,
                FactNetworkActivity.time_key
                == DimTime.time_key
            )
            .join(
                DimGrid,
                FactNetworkActivity.grid_key
                == DimGrid.grid_key
            )
            .filter(
                DimTime.timestamp
                <= effective_as_of
            )
            .order_by(
                FactNetworkActivity.total_activity.desc(),
                DimGrid.grid_id.asc(),
                DimTime.timestamp.asc()
            )
            .limit(limit)
        )

        rows = query.all()

        results = []

        for row in rows:

            results.append(
                {
                    "grid_id":
                        int(row.grid_id),

                    "timestamp":
                        row.timestamp,

                    "total_activity":
                        float(row.total_activity),

                    "sms_activity":
                        float(row.total_sms),

                    "call_activity":
                        float(row.total_calls),

                    "internet_activity":
                        float(
                            row.internet_activity
                        ),

                    "as_of":
                        effective_as_of,
                }
            )

        return results

    # =====================================================
    # 3. Validate NP3 file
    # =====================================================

    def validate_alert_file(self) -> Path:
        """
        Validate that the NP3 alert output exists and
        contains the expected columns.
        """

        path = Path(
            NP3_ALERT_PATH
        )

        if not path.exists():

            raise FileNotFoundError(
                "NP3 alert output does not exist: "
                f"{path}"
            )

        required_columns = {
            "grid_id",
            "timestamp",
            "alert_type",
            "current_activity",
            "baseline_activity",
            "reason",
        }

        try:

            header = pd.read_csv(
                path,
                nrows=0
            )

        except Exception as exc:

            raise RuntimeError(
                "Unable to read NP3 alert output: "
                f"{exc}"
            ) from exc

        missing = (
            required_columns
            - set(header.columns)
        )

        if missing:

            raise ValueError(
                "NP3 alert output is missing "
                f"required columns: {sorted(missing)}"
            )

        return path

    # =====================================================
    # 4. Load NP3 alerts
    # =====================================================

    def load_alerts(self) -> pd.DataFrame:
        """
        Load the existing NP3 rule-based alert output.
        """

        path = self.validate_alert_file()

        try:

            df = pd.read_csv(
                path
            )

        except Exception as exc:

            raise RuntimeError(
                "Unable to load NP3 alert output: "
                f"{exc}"
            ) from exc

        if df.empty:
            return df

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce"
        )

        if df["timestamp"].isna().any():

            raise ValueError(
                "NP3 alert output contains invalid timestamps."
            )

        # -------------------------------------------------
        # Validate grid IDs
        # -------------------------------------------------

        if (
            df["grid_id"].isna()
            .any()
        ):

            raise ValueError(
                "NP3 alert output contains "
                "missing grid IDs."
            )

        df["grid_id"] = (
            pd.to_numeric(
                df["grid_id"],
                errors="coerce"
            )
        )

        if df["grid_id"].isna().any():

            raise ValueError(
                "NP3 alert output contains "
                "invalid grid IDs."
            )

        df["grid_id"] = (
            df["grid_id"]
            .astype(int)
        )

        invalid_grid_ids = df[
            (df["grid_id"] < 1)
            |
            (df["grid_id"] > 10000)
        ]

        if not invalid_grid_ids.empty:

            raise ValueError(
                "NP3 alert output contains grid IDs "
                "outside 1-10000."
            )

        # -------------------------------------------------
        # Validate alert types
        # -------------------------------------------------

        unknown_types = (
            set(df["alert_type"].dropna())
            - set(ALERT_SEVERITY.keys())
        )

        if unknown_types:

            raise ValueError(
                "NP3 alert output contains unknown "
                f"alert types: {sorted(unknown_types)}"
            )

        return df

    # =====================================================
    # 5. Check grid IDs against MySQL
    # =====================================================

        # =====================================================
    # 5. Check grid IDs against MySQL
    # =====================================================

    def validate_grid_ids(
        self,
        df: pd.DataFrame
    ):
        """
        Confirm every alert grid exists in dim_grid.

        Uses a temporary in-memory DataFrame-derived query
        only for validation. The alert data itself remains
        sourced from the NP3 output.
        """

        if df.empty:
            return

        # -------------------------------------------------
        # Get unique grid IDs as normal Python integers.
        # -------------------------------------------------

        alert_grid_ids = {
            int(grid_id)
            for grid_id in
            df["grid_id"].dropna().unique()
        }

        if not alert_grid_ids:
            return

        # -------------------------------------------------
        # Instead of building a huge SQL IN (...) query,
        # validate the allowed range first.
        #
        # load_alerts() already validates 1-10000.
        # dim_grid is expected to contain all 10000 grids.
        # -------------------------------------------------

        if any(
            grid_id < 1 or grid_id > 10000
            for grid_id in alert_grid_ids
        ):
            raise ValueError(
                "NP3 alert output contains grid IDs "
                "outside 1-10000."
            )

        # -------------------------------------------------
        # Check the dimension row count.
        #
        # DE6 acceptance requires dim_grid to contain
        # exactly 10000 unique grid IDs.
        # -------------------------------------------------

        grid_count = (
            self.db
            .query(
                func.count(
                    DimGrid.grid_id
                )
            )
            .scalar()
        )

        distinct_grid_count = (
            self.db
            .query(
                func.count(
                    func.distinct(
                        DimGrid.grid_id
                    )
                )
            )
            .scalar()
        )

        if (
            grid_count != 10000
            or distinct_grid_count != 10000
        ):
            raise ValueError(
                "dim_grid does not contain the expected "
                "10,000 unique Milan grid IDs. "
                f"Rows={grid_count}, "
                f"Distinct={distinct_grid_count}."
            )

    # =====================================================
    # 6. Get alerts
    # =====================================================

    def get_alerts(
        self,
        limit: int,
        severity: Optional[str] = None,
        as_of: Optional[datetime] = None
    ):
        """
        Return NP3 rule-based alerts.

        Ordering is deterministic:

            timestamp DESC
            severity rank DESC
            grid_id ASC
            alert_type ASC
        """

        effective_as_of = (
            as_of
            if as_of is not None
            else self.get_np3_as_of()
        )

        df = self.load_alerts()

        if df.empty:
            return []

        # -------------------------------------------------
        # Only alerts at or before AS_OF
        # -------------------------------------------------

        df = df[
            df["timestamp"]
            <= effective_as_of
        ].copy()

        if df.empty:
            return []

        # -------------------------------------------------
        # Severity
        # -------------------------------------------------

        df["severity"] = (
            df["alert_type"]
            .map(ALERT_SEVERITY)
        )

        # -------------------------------------------------
        # Severity filter
        # -------------------------------------------------

        if severity is not None:

            df = df[
                df["severity"]
                == severity
            ].copy()

        if df.empty:
            return []

        # -------------------------------------------------
        # Confirm grid IDs exist
        # -------------------------------------------------

        self.validate_grid_ids(
            df
        )

        # -------------------------------------------------
        # Deterministic severity rank
        # -------------------------------------------------

        severity_rank = {
            "HIGH": 3,
            "MEDIUM": 2,
            "LOW": 1,
        }

        df["_severity_rank"] = (
            df["severity"]
            .map(severity_rank)
            .fillna(0)
        )

        # -------------------------------------------------
        # Deterministic ordering
        # -------------------------------------------------

        df = df.sort_values(
            by=[
                "timestamp",
                "_severity_rank",
                "grid_id",
                "alert_type",
            ],
            ascending=[
                False,
                False,
                True,
                True,
            ],
        )

        # -------------------------------------------------
        # Apply limit
        # -------------------------------------------------

        df = df.head(
            limit
        )

        results = []

        for _, row in df.iterrows():

            results.append(
                {
                    "grid_id":
                        int(row["grid_id"]),

                    "timestamp":
                        row["timestamp"].to_pydatetime(),

                    "alert_type":
                        str(row["alert_type"]),

                    "severity":
                        str(row["severity"]),

                    "current_activity":
                        float(
                            row["current_activity"]
                        ),

                    "baseline_activity":
                        float(
                            row["baseline_activity"]
                        ),

                    "reason":
                        str(row["reason"]),

                    "as_of":
                        effective_as_of,

                    # -------------------------------------------------
                    # Future ML fields
                    # -------------------------------------------------

                    "risk_score":
                        None,

                    "risk_level":
                        None,

                    "model_version":
                        None,
                }
            )

        return results

    # =====================================================
    # 7. Determine NP3 AS_OF
    # =====================================================

    def get_np3_as_of(self) -> datetime:
        """
        Determine AS_OF from the maximum timestamp present
        in the NP3 alert output.

        This avoids hardcoding a historical date.
        """

        df = self.load_alerts()

        if df.empty:

            raise RuntimeError(
                "NP3 alert output contains no alerts."
            )

        maximum_timestamp = (
            df["timestamp"].max()
        )

        if pd.isna(maximum_timestamp):

            raise RuntimeError(
                "NP3 alert output contains no valid timestamps."
            )

        return maximum_timestamp.to_pydatetime()
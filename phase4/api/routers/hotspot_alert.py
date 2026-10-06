# =========================================================
# API3 — Hotspot & Alert Routes
# File:
# phase4/api/routers/hotspot_alert.py
# =========================================================

from datetime import datetime
from typing import Optional

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
)
from sqlalchemy.orm import Session

from ..db.database import get_db

from ..models.network import (
    AlertResponse,
    HotspotResponse,
)

from ..services.hotspot_alert_service import (
    HotspotAlertService,
)


# =========================================================
# Router
# =========================================================

router = APIRouter(
    prefix="/network",
    tags=["Network Hotspots & Alerts"],
)


# =========================================================
# GET /network/hotspots
# =========================================================

@router.get(
    "/hotspots",
    response_model=list[HotspotResponse],
    summary="Get high-activity grid hotspots",
)
def get_hotspots(
    limit: int = Query(
        default=10,
        ge=1,
        le=100,
        description="Maximum number of hotspot records."
    ),

    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Optional effective reporting timestamp. "
            "When omitted, the latest warehouse timestamp "
            "is used."
        )
    ),

    db: Session = Depends(get_db),
):
    """
    Return the highest-activity grid/hour records.

    Data source:
        MySQL analytics warehouse.

    The endpoint does not read raw activity CSV files.
    """

    service = HotspotAlertService(
        db
    )

    try:

        return service.get_hotspots(
            limit=limit,
            as_of=as_of,
        )
    

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Unable to retrieve hotspot data: "
                f"{exc}"
            ),
        ) from exc


# =========================================================
# GET /network/alerts
# =========================================================

@router.get(
    "/alerts",
    response_model=list[AlertResponse],
    summary="Get operational network alerts",
)
def get_alerts(
    limit: int = Query(
        default=10,
        ge=1,
        le=100,
        description="Maximum number of alert records."
    ),

    severity: Optional[str] = Query(
        default=None,
        description=(
            "Optional severity filter: HIGH or MEDIUM."
        )
    ),

    as_of: Optional[datetime] = Query(
        default=None,
        description=(
            "Optional effective reporting timestamp. "
            "When omitted, the latest NP3 alert timestamp "
            "is used."
        )
    ),

    db: Session = Depends(get_db),
):
    """
    Return rule-based operational alerts produced by NP3.

    ML fields are reserved as nullable fields so future
    model outputs can be added without breaking clients.
    """

    # -----------------------------------------------------
    # Validate severity
    # -----------------------------------------------------

    if severity is not None:

        severity = severity.upper()

        allowed_severities = {
            "HIGH",
            "MEDIUM",
            "LOW",
        }

        if severity not in allowed_severities:

            raise HTTPException(
                status_code=422,
                detail=(
                    "severity must be one of: "
                    "HIGH, MEDIUM, LOW."
                ),
            )

    service = HotspotAlertService(
        db
    )

    try:

        return service.get_alerts(
            limit=limit,
            severity=severity,
            as_of=as_of,
        )

    except FileNotFoundError as exc:

        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc

    except (
        ValueError,
        RuntimeError,
    ) as exc:

        raise HTTPException(
            status_code=500,
            detail=str(exc),
        ) from exc

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Unable to retrieve alert data: "
                f"{exc}"
            ),
        ) from exc
    
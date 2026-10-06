# =========================================================
# API1 + API2 — Network Routes
# File: phase4/api/routers/network.py
# =========================================================

from datetime import date, datetime

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
)
from sqlalchemy.orm import Session

from ..db.database import get_db
from ..models.network import (
    GridActivityResponse,
    NetworkSummaryResponse,
)
from ..services.network_service import NetworkService


# =========================================================
# Router
# =========================================================

router = APIRouter(
    prefix="/network",
    tags=["Network"]
)


# =========================================================
# API1
# GET /network/summary
# =========================================================

@router.get(
    "/summary",
    response_model=NetworkSummaryResponse,
    summary="Get network summary",
)
def get_network_summary(
    as_of: datetime | None = Query(
        default=None,
        description=(
            "Optional reporting cutoff timestamp. "
            "When omitted, the maximum timestamp "
            "available in the analytics warehouse is used."
        )
    ),
    db: Session = Depends(get_db)
):

    try:

        service = NetworkService(db)

        return service.get_summary(
            requested_as_of=as_of
        )

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Unable to retrieve network summary: "
                f"{exc}"
            )
        ) from exc


# =========================================================
# API2
# GET /network/grid/{grid_id}
# =========================================================

@router.get(
    "/grid/{grid_id}",
    response_model=GridActivityResponse,
    summary="Get grid activity",
    description=(
        "Return hourly network activity for one grid. "
        "Without explicit date or hour filters, the "
        "endpoint returns the trailing 24 hourly "
        "intervals ending at AS_OF."
    )
)
def get_grid_activity(
    grid_id: int,
    date: date | None = Query(
        default=None,
        description=(
            "Optional calendar date filter."
        )
    ),
    hour: int | None = Query(
        default=None,
        ge=0,
        le=23,
        description=(
            "Optional hour-of-day filter, 0-23."
        )
    ),
    as_of: datetime | None = Query(
        default=None,
        description=(
            "Optional reporting cutoff timestamp. "
            "When omitted, MAX(timestamp) from the "
            "analytics warehouse is used."
        )
    ),
    db: Session = Depends(get_db)
):

    # -----------------------------------------------------
    # Explicit range validation
    # -----------------------------------------------------

    if not 1 <= grid_id <= 10000:

        raise HTTPException(
            status_code=404,
            detail=(
                f"Grid {grid_id} was not found."
            )
        )

    try:

        service = NetworkService(db)

        result = service.get_grid_activity(
            grid_id=grid_id,
            requested_date=date,
            requested_hour=hour,
            requested_as_of=as_of,
        )

        return result

    except LookupError as exc:

        raise HTTPException(
            status_code=404,
            detail=str(exc)
        ) from exc

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Unable to retrieve grid activity: "
                f"{exc}"
            )
        ) from exc
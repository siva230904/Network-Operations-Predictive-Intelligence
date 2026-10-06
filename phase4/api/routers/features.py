# =========================================================
# API4 — Grid Feature Routes
# File:
# phase4/api/routers/features.py
# =========================================================

from datetime import datetime
from typing import Optional

from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    Query,
)
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from ..db.database import get_db

from ..models.network import (
    GridFeatureResponse,
)

from ..services.feature_service import (
    FeatureService,
)


# =========================================================
# Router
# =========================================================

router = APIRouter(
    prefix="/network",
    tags=["Network ML Features"],
)


# =========================================================
# GET /network/grid/{grid_id}/features
# =========================================================

@router.get(
    "/grid/{grid_id}/features",
    response_model=GridFeatureResponse,
    summary="Get stored ML features for a grid",
)
def get_grid_features(
    grid_id: int,

    feature_timestamp: Optional[datetime] = Query(
        default=None,
        description=(
            "Optional ML feature timestamp. "
            "When omitted, the latest stored feature "
            "record for the grid is returned."
        ),
    ),

    db: Session = Depends(get_db),
):
    """
    Return the stored ML2 feature vector for a grid.

    Data source:
        Stored ML2 feature table.

    Important:
        This endpoint does not calculate features.
        It serves the values persisted by ML2.
    """

    # -----------------------------------------------------
    # Grid ID validation
    # -----------------------------------------------------

    if grid_id < 1 or grid_id > 10000:

        raise HTTPException(
            status_code=404,
            detail=(
                f"Grid {grid_id} was not found."
            ),
        )

    service = FeatureService(
        db
    )

    try:

        result = (
            service.get_grid_features(
                grid_id=grid_id,
                feature_timestamp=
                    feature_timestamp,
            )
        )

        # -------------------------------------------------
        # No stored feature record
        # -------------------------------------------------

        if result is None:

            raise HTTPException(
                status_code=404,
                detail=(
                    "No stored ML features found "
                    f"for grid {grid_id}."
                ),
            )

        # -------------------------------------------------
        # Data quality failure
        # -------------------------------------------------

        if result["data_quality"] != "VALID":

            raise HTTPException(
                status_code=500,
                detail=(
                    "Stored ML feature record failed "
                    "data-quality validation."
                ),
            )

        return result

    except HTTPException:
        raise

    except SQLAlchemyError as exc:

        raise HTTPException(
            status_code=503,
            detail=(
                "ML feature data source is unavailable."
            ),
        ) from exc

    except Exception as exc:

        raise HTTPException(
            status_code=500,
            detail=(
                "Unable to retrieve stored ML features."
            ),
        ) from exc
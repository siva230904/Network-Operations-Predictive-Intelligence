from fastapi import APIRouter, HTTPException, Request

from ..models.network import PredictionRequest, PredictionResponse


router = APIRouter(
    prefix="/network",
    tags=["Network Prediction"],
)


@router.post(
    "/predict-risk",
    response_model=PredictionResponse,
    summary="Predict network risk",
)
def predict_risk(
    payload: PredictionRequest,
    http_request: Request,
):
    """
    Generate a network risk prediction using the model loaded at startup.
    """

    service = http_request.app.state.prediction_service

    try:
        return service.predict(payload)

    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=str(exc),
        ) from exc

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail="Unable to generate network risk prediction.",
        ) from exc


# # =========================================================
# # API5 — Prediction Route
# # File:
# # phase4/api/routers/prediction.py
# # =========================================================

# from fastapi import (
#     APIRouter,
#     HTTPException,
# )

# from ..models.network import (
#     PredictionRequest,
#     PredictionResponse,
# )

# from ..services.prediction_service import (
#     PredictionService,
# )


# # =========================================================
# # Router
# # =========================================================

# router = APIRouter(
#     prefix="/network",
#     tags=["Network Prediction"],
# )


# # =========================================================
# # POST /network/predict-risk
# # =========================================================

# @router.post(
#     "/predict-risk",
#     response_model=PredictionResponse,
#     summary="Predict network risk",
# )
# def predict_risk(
#     request: PredictionRequest,
# ):
#     """
#     Return a network risk prediction.

#     Current implementation is a stub.

#     ML5 will later replace the prediction implementation
#     while preserving this API contract.
#     """

#     service = PredictionService()

#     try:

#         return service.predict(
#             request
#         )

#     except ValueError as exc:

#         raise HTTPException(
#             status_code=422,
#             detail=str(exc),
#         ) from exc

#     except Exception as exc:

#         raise HTTPException(
#             status_code=500,
#             detail=(
#                 "Unable to generate network risk prediction."
#             ),
#         ) from exc
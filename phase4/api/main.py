from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import API_TITLE, API_VERSION
from .services.prediction_service import PredictionService
from .routers.features import router as features_router
from .routers.hotspot_alert import router as hotspot_alert_router
from .routers.network import router as network_router
from .routers.prediction import router as prediction_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Load and validate ML artifacts during API startup.

    If the model or scaler is missing/corrupt, startup fails instead
    of silently falling back to a prediction stub.
    """

    app.state.prediction_service = PredictionService.from_artifacts()

    yield


app = FastAPI(
    title=API_TITLE,
    version=API_VERSION,
    description=(
        "Telecom network intelligence API providing network summaries, "
        "hotspot alerts, ML features, and predictive risk scoring."
    ),
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(network_router)
app.include_router(hotspot_alert_router)
app.include_router(features_router)
app.include_router(prediction_router)


@app.get("/health", tags=["Health"])
def health():
    return {"status": "ok"}


# # =========================================================
# # API1 — FastAPI Application
# # File: phase4/api/main.py
# # =========================================================

# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from .config import (
#     API_TITLE,
#     API_VERSION,
# )
# from phase4.api.routers.features import (
#     router as features_router,
# )
# from phase4.api.routers.prediction import (
#     router as prediction_router,
# )
# from .routers.network import router as network_router
# from .routers.hotspot_alert import router as hotspot_alert_router


# # =========================================================
# # Application
# # =========================================================

# app = FastAPI(
#     title=API_TITLE,
#     version=API_VERSION,
#     description=(
#         "REST API for the Telecom Network "
#         "Intelligence analytics warehouse."
#     )
# )

# # =========================================================
# # CORS
# # =========================================================

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=[
#         "http://localhost:5173",
#         "http://127.0.0.1:5173",
#     ],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # =========================================================
# # Routers
# # =========================================================

# app.include_router(
#     network_router
# )

# app.include_router(
#     hotspot_alert_router
# )

# app.include_router(
#     features_router
# )

# app.include_router(
#     prediction_router
# )

# # =========================================================
# # Health endpoint
# # =========================================================

# @app.get(
#     "/health",
#     tags=["Health"]
# )
# def health():

#     return {
#         "status": "ok"
#     }
from pathlib import Path

import joblib
import pandas as pd

from ..db.database import SessionLocal
from ..db.models import MLGridFeatures
from ..models.network import PredictionRequest, PredictionResponse


PROJECT_ROOT = Path(__file__).resolve().parents[3]
MODEL_DIR = PROJECT_ROOT / "phase6" / "ml" / "artifacts"

MODEL_PATH = MODEL_DIR / "logistic_regression.joblib"
SCALER_PATH = MODEL_DIR / "feature_scaler.joblib"

MODEL_VERSION = "ml3-logistic-regression-v1"

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]

RISK_PROBABILITY_THRESHOLD = 0.5


class PredictionService:
    """
    Loads the trained ML3 model and performs predictions using
    ML2 features stored in the database.
    """

    def __init__(self, model, scaler):
        self.model = model
        self.scaler = scaler

    @classmethod
    def from_artifacts(
        cls,
        model_path: Path = MODEL_PATH,
        scaler_path: Path = SCALER_PATH,
    ):
        """
        Load and validate the trained model and scaler.
        """

        if not model_path.is_file():
            raise RuntimeError(
                f"ML model artifact is missing: {model_path}"
            )

        if not scaler_path.is_file():
            raise RuntimeError(
                f"ML feature scaler artifact is missing: {scaler_path}"
            )

        try:
            model = joblib.load(model_path)
        except Exception as exc:
            raise RuntimeError(
                f"Unable to load ML model artifact: {model_path}"
            ) from exc

        try:
            scaler = joblib.load(scaler_path)
        except Exception as exc:
            raise RuntimeError(
                f"Unable to load ML feature scaler artifact: {scaler_path}"
            ) from exc

        if not hasattr(model, "predict_proba"):
            raise RuntimeError(
                "Loaded ML model does not support predict_proba()."
            )

        if not hasattr(scaler, "transform"):
            raise RuntimeError(
                "Loaded ML scaler does not support transform()."
            )

        expected_feature_count = len(FEATURE_COLUMNS)

        model_feature_count = getattr(model, "n_features_in_", None)
        if (
            model_feature_count is not None
            and model_feature_count != expected_feature_count
        ):
            raise RuntimeError(
                "Loaded ML model expects "
                f"{model_feature_count} features, but the API provides "
                f"{expected_feature_count}."
            )

        scaler_feature_count = getattr(scaler, "n_features_in_", None)
        if (
            scaler_feature_count is not None
            and scaler_feature_count != expected_feature_count
        ):
            raise RuntimeError(
                "Loaded ML scaler expects "
                f"{scaler_feature_count} features, but the API provides "
                f"{expected_feature_count}."
            )

        return cls(model=model, scaler=scaler)

    def predict(self, request: PredictionRequest) -> PredictionResponse:
        """
        Retrieve the ML2 feature vector for the requested grid/timestamp
        and generate the ML3 next-hour high-activity prediction.
        """

        db = SessionLocal()

        try:
            feature_row = (
                db.query(MLGridFeatures)
                .filter(
                    MLGridFeatures.grid_id == request.grid_id,
                    MLGridFeatures.feature_timestamp
                    == request.feature_timestamp,
                )
                .first()
            )

            if feature_row is None:
                raise ValueError(
                    "No ML2 features found for "
                    f"grid_id={request.grid_id} at "
                    f"feature_timestamp={request.feature_timestamp.isoformat()}."
                )

            feature_values = pd.DataFrame(
                [
                    [
                        feature_row.avg_activity,
                        feature_row.activity_growth,
                        feature_row.active_hours,
                        feature_row.peak_ratio,
                        feature_row.variability,
                        feature_row.internet_share,
                    ]
                ],
                columns=FEATURE_COLUMNS,
            )

            scaled_features = self.scaler.transform(feature_values)

            probabilities = self.model.predict_proba(scaled_features)

            risk_score = float(probabilities[0][1])

            if risk_score >= RISK_PROBABILITY_THRESHOLD:
                risk_level = "HIGH"
            else:
                risk_level = "LOW"

            return PredictionResponse(
                grid_id=request.grid_id,
                feature_timestamp=feature_row.feature_timestamp,
                risk_score=risk_score,
                risk_level=risk_level,
                model_version=MODEL_VERSION,
                explanation_note=(
                    "ML3 Logistic Regression predicts the probability "
                    "that next-hour total activity will be at least 2000. "
                    "The prediction uses the ML2 features stored for the "
                    "requested grid and timestamp. Risk level is HIGH "
                    "when the predicted probability is at least 0.50."
                ),
            )

        finally:
            db.close()



            
# from pathlib import Path

# import pandas as pd

# import joblib

# from ..models.network import PredictionRequest, PredictionResponse


# PROJECT_ROOT = Path(__file__).resolve().parents[3]
# MODEL_DIR = PROJECT_ROOT / "phase6" / "ml" / "artifacts"

# MODEL_PATH = MODEL_DIR / "logistic_regression.joblib"
# SCALER_PATH = MODEL_DIR / "feature_scaler.joblib"

# MODEL_VERSION = "ml3-logistic-regression-v1"

# FEATURE_COLUMNS = [
#     "avg_activity",
#     "activity_growth",
#     "active_hours",
#     "peak_ratio",
#     "variability",
#     "internet_share",
# ]

# RISK_PROBABILITY_THRESHOLD = 0.5


# class PredictionService:
#     """
#     Loads and serves the trained ML3 Logistic Regression model.

#     The model and scaler are loaded once during FastAPI startup.
#     """

#     def __init__(self, model, scaler):
#         self.model = model
#         self.scaler = scaler

#     @classmethod
#     def from_artifacts(
#         cls,
#         model_path: Path = MODEL_PATH,
#         scaler_path: Path = SCALER_PATH,
#     ):
#         """
#         Load the trained model and feature scaler.

#         Raises:
#             RuntimeError: if an artifact is missing, cannot be loaded,
#                           or does not expose the expected interface.
#         """

#         if not model_path.is_file():
#             raise RuntimeError(
#                 f"ML model artifact is missing: {model_path}"
#             )

#         if not scaler_path.is_file():
#             raise RuntimeError(
#                 f"ML feature scaler artifact is missing: {scaler_path}"
#             )

#         try:
#             model = joblib.load(model_path)
#         except Exception as exc:
#             raise RuntimeError(
#                 f"Unable to load ML model artifact: {model_path}"
#             ) from exc

#         try:
#             scaler = joblib.load(scaler_path)
#         except Exception as exc:
#             raise RuntimeError(
#                 f"Unable to load ML feature scaler artifact: {scaler_path}"
#             ) from exc

#         if not hasattr(model, "predict_proba"):
#             raise RuntimeError(
#                 "Loaded ML model does not support predict_proba()."
#             )

#         if not hasattr(scaler, "transform"):
#             raise RuntimeError(
#                 "Loaded ML scaler does not support transform()."
#             )

#         expected_feature_count = len(FEATURE_COLUMNS)

#         model_feature_count = getattr(model, "n_features_in_", None)
#         if (
#             model_feature_count is not None
#             and model_feature_count != expected_feature_count
#         ):
#             raise RuntimeError(
#                 "Loaded ML model expects "
#                 f"{model_feature_count} features, but the API provides "
#                 f"{expected_feature_count}."
#             )

#         scaler_feature_count = getattr(scaler, "n_features_in_", None)
#         if (
#             scaler_feature_count is not None
#             and scaler_feature_count != expected_feature_count
#         ):
#             raise RuntimeError(
#                 "Loaded ML scaler expects "
#                 f"{scaler_feature_count} features, but the API provides "
#                 f"{expected_feature_count}."
#             )

#         return cls(model=model, scaler=scaler)

#     def predict(self, request: PredictionRequest) -> PredictionResponse:
#         """
#         Generate an ML3 next-hour high-activity risk prediction.

#         The six incoming values must follow the exact ML3 training order.
#         """

#         feature_values = pd.DataFrame(
#             [
#                 [
#                     request.avg_activity,
#                     request.activity_growth,
#                     request.active_hours,
#                     request.peak_ratio,
#                     request.variability,
#                     request.internet_share,
#                 ]
#             ],
#             columns=FEATURE_COLUMNS,
#         )

#         scaled_features = self.scaler.transform(feature_values)

#         # feature_values = np.array(
#         #     [
#         #         [
#         #             request.avg_activity,
#         #             request.activity_growth,
#         #             request.active_hours,
#         #             request.peak_ratio,
#         #             request.variability,
#         #             request.internet_share,
#         #         ]
#         #     ],
#         #     dtype=float,
#         # )

#         # scaled_features = self.scaler.transform(feature_values)

#         probabilities = self.model.predict_proba(scaled_features)

#         risk_score = float(probabilities[0][1])

#         if risk_score >= RISK_PROBABILITY_THRESHOLD:
#             risk_level = "HIGH"
#         else:
#             risk_level = "LOW"

#         return PredictionResponse(
#             grid_id=request.grid_id,
#             risk_score=risk_score,
#             risk_level=risk_level,
#             model_version=MODEL_VERSION,
#             explanation_note=(
#                 "ML3 Logistic Regression predicts the probability that "
#                 "next-hour total activity will be at least 2000. "
#                 "Risk level is HIGH when the predicted probability is "
#                 "at least 0.50."
#             ),
#         )



























# # =========================================================
# # API5 — Prediction Service
# # File:
# # phase4/api/services/prediction_service.py
# # =========================================================

# from ..models.network import (
#     PredictionRequest,
#     PredictionResponse,
# )


# # =========================================================
# # Prediction Service
# # =========================================================

# class PredictionService:
#     """
#     API5 prediction service.

#     Current implementation:
#         STUB

#     Future implementation:
#         ML5 trained prediction model.

#     IMPORTANT:
#         The response contract must remain unchanged when
#         ML5 replaces this stub.
#     """

#     # =====================================================
#     # Predict risk
#     # =====================================================

#     def predict(
#         self,
#         request: PredictionRequest,
#     ) -> PredictionResponse:
#         """
#         Return a stub prediction.

#         No real ML model is used yet.
#         """

#         return PredictionResponse(
#             grid_id=request.grid_id,

#             risk_score=0.0,

#             risk_level="STUB",

#             model_version="stub-v1",

#             explanation_note=(
#                 "Prediction implementation is currently "
#                 "a stub. No trained ML model is being used."
#             ),
#         )
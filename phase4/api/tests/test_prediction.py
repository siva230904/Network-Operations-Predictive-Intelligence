import pytest
from fastapi.testclient import TestClient

from phase4.api.main import app
from phase4.api.services.prediction_service import (
    MODEL_VERSION,
    PredictionService,
)


@pytest.fixture
def client():
    """
    Use TestClient as a context manager so FastAPI lifespan runs.
    """
    with TestClient(app) as test_client:
        yield test_client


def test_predict_risk_real_model(client):
    payload = {
        "grid_id": 4821,
        "feature_timestamp": "2013-11-07T15:00:00",
    }

    response = client.post(
        "/network/predict-risk",
        json=payload,
    )

    assert response.status_code == 200

    data = response.json()

    assert data["grid_id"] == 4821
    assert data["feature_timestamp"].startswith("2013-11-07T15:00:00")
    assert 0.0 <= data["risk_score"] <= 1.0
    assert data["risk_level"] in {"LOW", "HIGH"}
    assert data["model_version"] == MODEL_VERSION
    assert data["model_version"] != "stub-v1"
    assert "2000" in data["explanation_note"]


def test_predict_risk_missing_field(client):
    payload = {
        "grid_id": 4821,
    }

    response = client.post(
        "/network/predict-risk",
        json=payload,
    )

    assert response.status_code == 422


def test_predict_risk_invalid_grid(client):
    payload = {
        "grid_id": 10001,
        "feature_timestamp": "2013-11-07T15:00:00",
    }

    response = client.post(
        "/network/predict-risk",
        json=payload,
    )

    assert response.status_code == 422


def test_predict_risk_rejects_unknown_field(client):
    payload = {
        "grid_id": 4821,
        "feature_timestamp": "2013-11-07T15:00:00",
        "avg_activity": 100.0,
    }

    response = client.post(
        "/network/predict-risk",
        json=payload,
    )

    assert response.status_code == 422


def test_predict_risk_missing_features(client):
    payload = {
        "grid_id": 4821,
        "feature_timestamp": "2013-11-01T00:00:00",
    }

    response = client.post(
        "/network/predict-risk",
        json=payload,
    )

    assert response.status_code == 422
    assert "No ML2 features found" in response.json()["detail"]


def test_missing_model_artifact_fails_clearly(tmp_path):
    missing_model = tmp_path / "missing_model.joblib"
    scaler_path = tmp_path / "feature_scaler.joblib"

    scaler_path.touch()

    with pytest.raises(
        RuntimeError,
        match="ML model artifact is missing",
    ):
        PredictionService.from_artifacts(
            model_path=missing_model,
            scaler_path=scaler_path,
        )







# import pytest
# from fastapi.testclient import TestClient

# from phase4.api.main import app
# from phase4.api.services.prediction_service import (
#     MODEL_VERSION,
#     PredictionService,
# )


# @pytest.fixture
# def client():
#     """
#     Use TestClient as a context manager so FastAPI lifespan runs.

#     This loads the real ML model and scaler before the tests execute.
#     """
#     with TestClient(app) as test_client:
#         yield test_client


# def test_predict_risk_real_model(client):
#     payload = {
#         "grid_id": 4821,
#         "avg_activity": 100.0,
#         "activity_growth": 0.15,
#         "active_hours": 20,
#         "peak_ratio": 2.0,
#         "variability": 25.0,
#         "internet_share": 0.65,
#     }

#     response = client.post(
#         "/network/predict-risk",
#         json=payload,
#     )

#     assert response.status_code == 200

#     data = response.json()

#     assert data["grid_id"] == 4821
#     assert 0.0 <= data["risk_score"] <= 1.0
#     assert data["risk_level"] in {"LOW", "HIGH"}
#     assert data["model_version"] == MODEL_VERSION
#     assert data["model_version"] != "stub-v1"
#     assert "2000" in data["explanation_note"]


# def test_predict_risk_missing_field(client):
#     payload = {
#         "grid_id": 4821,
#         "avg_activity": 100.0,
#         "activity_growth": 0.15,
#         "active_hours": 20,
#         "peak_ratio": 2.0,
#         "variability": 25.0,
#     }

#     response = client.post(
#         "/network/predict-risk",
#         json=payload,
#     )

#     assert response.status_code == 422


# def test_predict_risk_invalid_grid(client):
#     payload = {
#         "grid_id": 10001,
#         "avg_activity": 100.0,
#         "activity_growth": 0.15,
#         "active_hours": 20,
#         "peak_ratio": 2.0,
#         "variability": 25.0,
#         "internet_share": 0.65,
#     }

#     response = client.post(
#         "/network/predict-risk",
#         json=payload,
#     )

#     assert response.status_code == 422


# def test_predict_risk_invalid_internet_share(client):
#     payload = {
#         "grid_id": 4821,
#         "avg_activity": 100.0,
#         "activity_growth": 0.15,
#         "active_hours": 20,
#         "peak_ratio": 2.0,
#         "variability": 25.0,
#         "internet_share": 1.5,
#     }

#     response = client.post(
#         "/network/predict-risk",
#         json=payload,
#     )

#     assert response.status_code == 422


# def test_predict_risk_rejects_unknown_field(client):
#     payload = {
#         "grid_id": 4821,
#         "avg_activity": 100.0,
#         "activity_growth": 0.15,
#         "active_hours": 20,
#         "peak_ratio": 2.0,
#         "variability": 25.0,
#         "internet_share": 0.65,
#         "unknown_field": 123,
#     }

#     response = client.post(
#         "/network/predict-risk",
#         json=payload,
#     )

#     assert response.status_code == 422


# def test_missing_model_artifact_fails_clearly(tmp_path):
#     missing_model = tmp_path / "missing_model.joblib"

#     scaler_path = tmp_path / "feature_scaler.joblib"

#     # The scaler does not need to be valid for this test because
#     # the missing model should be detected first.
#     scaler_path.touch()

#     with pytest.raises(
#         RuntimeError,
#         match="ML model artifact is missing",
#     ):
#         PredictionService.from_artifacts(
#             model_path=missing_model,
#             scaler_path=scaler_path,
#         )









# # =========================================================
# # API5 — Prediction Endpoint Tests
# # File:
# # phase4/api/tests/test_prediction.py
# # =========================================================

# from fastapi.testclient import TestClient

# from phase4.api.main import app


# client = TestClient(
#     app
# )


# # =========================================================
# # Valid request
# # =========================================================

# def test_predict_risk_stub():

#     payload = {
#         "grid_id": 4821,
#         "avg_activity": 100.0,
#         "activity_growth": 0.15,
#         "active_hours": 20,
#         "peak_ratio": 2.0,
#         "variability": 25.0,
#         "internet_share": 0.65,
#     }

#     response = client.post(
#         "/network/predict-risk",
#         json=payload,
#     )

#     assert response.status_code == 200

#     data = response.json()

#     assert data["grid_id"] == 4821

#     assert data["risk_score"] == 0.0

#     assert data["risk_level"] == "STUB"

#     assert data["model_version"] == "stub-v1"

#     assert (
#         "stub"
#         in data["explanation_note"].lower()
#     )


# # =========================================================
# # Missing required field
# # =========================================================

# def test_predict_risk_missing_field():

#     payload = {
#         "grid_id": 4821,
#         "avg_activity": 100.0,
#         "activity_growth": 0.15,
#         "active_hours": 20,
#         "peak_ratio": 2.0,
#         "variability": 25.0,
#         # internet_share intentionally missing
#     }

#     response = client.post(
#         "/network/predict-risk",
#         json=payload,
#     )

#     assert response.status_code == 422

#     data = response.json()

#     assert "detail" in data


# # =========================================================
# # Invalid grid
# # =========================================================

# def test_predict_risk_invalid_grid():

#     payload = {
#         "grid_id": 10001,
#         "avg_activity": 100.0,
#         "activity_growth": 0.15,
#         "active_hours": 20,
#         "peak_ratio": 2.0,
#         "variability": 25.0,
#         "internet_share": 0.65,
#     }

#     response = client.post(
#         "/network/predict-risk",
#         json=payload,
#     )

#     assert response.status_code == 422


# # =========================================================
# # Invalid internet share
# # =========================================================

# def test_predict_risk_invalid_internet_share():

#     payload = {
#         "grid_id": 4821,
#         "avg_activity": 100.0,
#         "activity_growth": 0.15,
#         "active_hours": 20,
#         "peak_ratio": 2.0,
#         "variability": 25.0,
#         "internet_share": 1.5,
#     }

#     response = client.post(
#         "/network/predict-risk",
#         json=payload,
#     )

#     assert response.status_code == 422


# # =========================================================
# # Extra field rejection
# # =========================================================

# def test_predict_risk_rejects_unknown_field():

#     payload = {
#         "grid_id": 4821,
#         "avg_activity": 100.0,
#         "activity_growth": 0.15,
#         "active_hours": 20,
#         "peak_ratio": 2.0,
#         "variability": 25.0,
#         "internet_share": 0.65,
#         "unknown_field": 123,
#     }

#     response = client.post(
#         "/network/predict-risk",
#         json=payload,
#     )

#     assert response.status_code == 422
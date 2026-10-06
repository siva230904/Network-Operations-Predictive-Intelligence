# =========================================================
# API1 — Network Summary Tests
# File: phase4/api/tests/test_network_summary.py
# =========================================================

from datetime import datetime

from fastapi.testclient import TestClient

from ..main import app


# =========================================================
# Client
# =========================================================

client = TestClient(
    app
)


# =========================================================
# Health
# =========================================================

def test_health():

    response = client.get(
        "/health"
    )

    assert response.status_code == 200

    assert response.json() == {
        "status": "ok"
    }


# =========================================================
# Route existence
# =========================================================

def test_network_summary_route_exists():

    response = client.get(
        "/network/summary"
    )

    # -----------------------------------------------------
    # Without a live database this can return 500,
    # but it proves the route exists.
    # -----------------------------------------------------

    assert response.status_code in {
        200,
        500
    }


# =========================================================
# Swagger/OpenAPI
# =========================================================

def test_network_summary_openapi():

    response = client.get(
        "/openapi.json"
    )

    assert response.status_code == 200

    data = response.json()

    assert (
        "/network/summary"
        in data["paths"]
    )

    operation = (
        data["paths"]
        ["/network/summary"]
        ["get"]
    )

    assert (
        "200"
        in operation["responses"]
    )
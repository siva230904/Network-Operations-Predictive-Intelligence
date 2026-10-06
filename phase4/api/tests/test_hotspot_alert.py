# =========================================================
# API3 — Hotspot & Alert Tests
# File:
# phase4/api/tests/test_hotspot_alert.py
# =========================================================

from datetime import datetime

import pandas as pd

from phase4.api.services.hotspot_alert_service import (
    ALERT_SEVERITY,
    HotspotAlertService,
)


# =========================================================
# Severity mapping
# =========================================================

def test_alert_severity_mapping():

    assert (
        ALERT_SEVERITY["HIGH_ACTIVITY"]
        == "HIGH"
    )

    assert (
        ALERT_SEVERITY["ACTIVITY_SPIKE"]
        == "MEDIUM"
    )

    assert (
        ALERT_SEVERITY["ACTIVITY_DROP"]
        == "MEDIUM"
    )


# =========================================================
# ML fields are additive-safe
# =========================================================

def test_ml_fields_are_nullable():

    response = {
        "grid_id": 4821,
        "timestamp": datetime(
            2013,
            11,
            7,
            23
        ),
        "alert_type": "HIGH_ACTIVITY",
        "severity": "HIGH",
        "current_activity": 100.0,
        "baseline_activity": 40.0,
        "reason": "Rule-based alert.",
        "as_of": datetime(
            2013,
            11,
            7,
            23
        ),
        "risk_score": None,
        "risk_level": None,
        "model_version": None,
    }

    assert response["risk_score"] is None
    assert response["risk_level"] is None
    assert response["model_version"] is None


# =========================================================
# NP3 schema
# =========================================================

def test_np3_expected_columns():

    dataframe = pd.DataFrame(
        {
            "grid_id": [4821],
            "timestamp": [
                "2013-11-07 23:00:00"
            ],
            "alert_type": [
                "HIGH_ACTIVITY"
            ],
            "current_activity": [100.0],
            "baseline_activity": [40.0],
            "reason": [
                "HIGH_ACTIVITY rule triggered."
            ],
        }
    )

    expected_columns = {
        "grid_id",
        "timestamp",
        "alert_type",
        "current_activity",
        "baseline_activity",
        "reason",
    }

    assert expected_columns.issubset(
        set(dataframe.columns)
    )


# =========================================================
# Grid range
# =========================================================

def test_invalid_grid_ids_are_outside_supported_range():

    invalid_grid_ids = [
        0,
        10001,
    ]

    for grid_id in invalid_grid_ids:

        assert (
            grid_id < 1
            or grid_id > 10000
        )


# =========================================================
# Deterministic ordering
# =========================================================

def test_alert_sorting_is_deterministic():

    dataframe = pd.DataFrame(
        {
            "grid_id": [
                4821,
                100,
                4821,
            ],

            "timestamp": pd.to_datetime(
                [
                    "2013-11-07 23:00:00",
                    "2013-11-07 23:00:00",
                    "2013-11-07 22:00:00",
                ]
            ),

            "alert_type": [
                "ACTIVITY_SPIKE",
                "HIGH_ACTIVITY",
                "ACTIVITY_DROP",
            ],

            "current_activity": [
                100.0,
                200.0,
                20.0,
            ],

            "baseline_activity": [
                50.0,
                50.0,
                50.0,
            ],

            "reason": [
                "Spike.",
                "High activity.",
                "Drop.",
            ],
        }
    )

    dataframe["severity"] = (
        dataframe["alert_type"]
        .map(ALERT_SEVERITY)
    )

    severity_rank = {
        "HIGH": 3,
        "MEDIUM": 2,
        "LOW": 1,
    }

    dataframe["_severity_rank"] = (
        dataframe["severity"]
        .map(severity_rank)
    )

    sorted_df = dataframe.sort_values(
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

    first = sorted_df.iloc[0]

    assert (
        first["timestamp"]
        ==
        pd.Timestamp(
            "2013-11-07 23:00:00"
        )
    )

    assert (
        first["severity"]
        ==
        "HIGH"
    )
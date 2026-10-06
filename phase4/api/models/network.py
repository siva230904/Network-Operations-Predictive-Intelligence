# =========================================================
# API1 + API2 — Network API Models
# File: phase4/api/models/network.py
# =========================================================

from pydantic import BaseModel, ConfigDict,Field
from datetime import datetime
from typing import Optional


# =========================================================
# API1 — Network Summary Response
# =========================================================

class NetworkSummaryResponse(BaseModel):

    model_config = ConfigDict(
        from_attributes=True
    )

    total_activity: float

    active_grids: int

    peak_hour: datetime

    top_grid: int

    as_of: datetime


# =========================================================
# API2 — Grid Activity Point
# =========================================================

class GridActivityPoint(BaseModel):

    timestamp: datetime

    sms_in: float

    sms_out: float

    call_in: float

    call_out: float

    internet_activity: float

    total_sms: float

    total_calls: float

    total_activity: float


# =========================================================
# API2 — Grid Activity Response
# =========================================================

class GridActivityResponse(BaseModel):

    grid_id: int

    as_of: datetime

    activity: list[GridActivityPoint]

# =========================================================
# API3 — Hotspot and Alert Response Models
# =========================================================




# =========================================================
# Hotspot
# =========================================================

class HotspotResponse(BaseModel):
    """
    One high-activity grid returned by the hotspot endpoint.
    """

    grid_id: int = Field(
        ...,
        description="Milan grid identifier."
    )

    timestamp: datetime = Field(
        ...,
        description="Hourly activity timestamp."
    )

    total_activity: float = Field(
        ...,
        description="Total activity for the grid/hour."
    )

    sms_activity: float = Field(
        ...,
        description="Combined inbound and outbound SMS activity."
    )

    call_activity: float = Field(
        ...,
        description="Combined inbound and outbound call activity."
    )

    internet_activity: float = Field(
        ...,
        description="Internet activity for the grid/hour."
    )

    as_of: datetime = Field(
        ...,
        description="Effective reporting timestamp."
    )


# =========================================================
# Alert
# =========================================================

class AlertResponse(BaseModel):
    """
    Rule-based operational alert.

    ML fields are intentionally nullable so they can be
    added later without changing the existing contract.
    """

    grid_id: int = Field(
        ...,
        description="Milan grid identifier."
    )

    timestamp: datetime = Field(
        ...,
        description="Hourly alert timestamp."
    )

    alert_type: str = Field(
        ...,
        description="Rule that generated the alert."
    )

    severity: str = Field(
        ...,
        description="Operational alert severity."
    )

    current_activity: float = Field(
        ...,
        description="Current grid/hour activity."
    )

    baseline_activity: float = Field(
        ...,
        description="Baseline activity used by the alert rule."
    )

    reason: str = Field(
        ...,
        description="Human-readable explanation for the alert."
    )

    as_of: datetime = Field(
        ...,
        description="Effective reporting timestamp."
    )

    # -----------------------------------------------------
    # Future ML fields
    # -----------------------------------------------------

    risk_score: Optional[float] = Field(
        default=None,
        description="Future ML risk score."
    )

    risk_level: Optional[str] = Field(
        default=None,
        description="Future ML risk level."
    )

    model_version: Optional[str] = Field(
        default=None,
        description="Future ML model version."
    )

# =========================================================
# API4 — Grid Feature Response
# Add to:
# phase4/api/models/network.py
# =========================================================



class GridFeatureResponse(BaseModel):
    """
    API4 stable ML feature contract.

    The six ML feature names must match ML2 exactly.
    """

    model_config = ConfigDict(
        from_attributes=True
    )

    grid_id: int

    avg_activity: float

    activity_growth: float

    active_hours: int

    peak_ratio: float

    variability: float

    internet_share: float

    feature_timestamp: datetime

    data_quality: str

    freshness: str

# =========================================================
# API5 — Prediction Models
# File:
# phase4/api/models/network.py
# =========================================================


class PredictionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grid_id: int = Field(..., ge=1, le=10000)
    feature_timestamp: datetime


class PredictionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    grid_id: int
    feature_timestamp: datetime
    risk_score: float = Field(..., ge=0, le=1)
    risk_level: str
    model_version: str
    explanation_note: str


# # =========================================================
# # Prediction Request
# # =========================================================

# class PredictionRequest(BaseModel):
#     """
#     Stable request contract for ML5.

#     ML5 will later use the same request contract
#     when the real prediction model replaces the stub.
#     """

#     model_config = ConfigDict(
#         extra="forbid"
#     )

#     grid_id: int = Field(
#         ...,
#         ge=1,
#         le=10000,
#         description="Milan grid identifier.",
#     )

#     avg_activity: float = Field(
#         ...,
#         ge=0,
#         description="Average activity from the ML feature vector.",
#     )

#     activity_growth: float = Field(
#         ...,
#         description="Recent activity growth.",
#     )

#     active_hours: int = Field(
#         ...,
#         ge=0,
#         description="Number of active hours in the feature window.",
#     )

#     peak_ratio: float = Field(
#         ...,
#         ge=0,
#         description="Peak activity divided by average activity.",
#     )

#     variability: float = Field(
#         ...,
#         ge=0,
#         description="Activity variability.",
#     )

#     internet_share: float = Field(
#         ...,
#         ge=0,
#         le=1,
#         description="Internet activity divided by total activity.",
#     )


# # =========================================================
# # Prediction Response
# # =========================================================

# class PredictionResponse(BaseModel):
#     """
#     Stable prediction response contract.

#     ML5 must preserve these fields when the real model
#     replaces the API5 stub.
#     """

#     model_config = ConfigDict(
#         extra="forbid"
#     )

#     grid_id: int

#     risk_score: float = Field(
#         ...,
#         ge=0,
#         le=1,
#         description="Predicted risk score between 0 and 1.",
#     )

#     risk_level: str = Field(
#         ...,
#         description="Predicted risk level.",
#     )

#     model_version: str = Field(
#         ...,
#         description="Model version used for prediction.",
#     )

#     explanation_note: str = Field(
#         ...,
#         description="Human-readable explanation of the prediction.",
#     )
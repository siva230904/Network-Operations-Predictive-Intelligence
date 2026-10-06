# API5 — Prediction Contract

## Endpoint

POST `/network/predict-risk`

## Purpose

Provide a stable prediction contract for network risk.

The current implementation is a stub.

ML5 will replace the stub implementation without changing
the endpoint contract.

---

## Request

The request body contains:

| Field | Type | Required | Constraints |
|---|---|---:|---|
| grid_id | integer | Yes | 1–10000 |
| avg_activity | number | Yes | >= 0 |
| activity_growth | number | Yes | ML2 feature |
| active_hours | integer | Yes | >= 0 |
| peak_ratio | number | Yes | >= 0 |
| variability | number | Yes | >= 0 |
| internet_share | number | Yes | 0–1 |

These six feature fields correspond exactly to the ML2
feature contract:

- avg_activity
- activity_growth
- active_hours
- peak_ratio
- variability
- internet_share

---

## Response

| Field | Type | Description |
|---|---|---|
| grid_id | integer | Milan grid identifier |
| risk_score | number | Risk score between 0 and 1 |
| risk_level | string | Risk classification |
| model_version | string | Model implementation/version |
| explanation_note | string | Human-readable explanation |

---

## Current Stub Response

The current implementation returns:

- `risk_score = 0.0`
- `risk_level = "STUB"`
- `model_version = "stub-v1"`
- `explanation_note` explicitly states that the implementation
  is currently a stub.

This is not a real model prediction.

---

## ML5 Integration Rule

ML5 must preserve the API5 endpoint and response fields.

The following fields must remain available:

- risk_score
- risk_level
- model_version
- explanation_note

ML5 may replace the implementation behind the service
without requiring a React client contract change.

---

## Validation

Invalid requests return HTTP 422.

Examples:

- missing required feature
- grid_id outside 1–10000
- negative activity values where prohibited
- peak_ratio below zero
- variability below zero
- internet_share outside 0–1
- unknown request fields
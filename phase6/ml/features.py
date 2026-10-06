from __future__ import annotations

from dataclasses import dataclass
from math import isfinite
from statistics import mean, pstdev
from typing import Sequence


@dataclass(frozen=True)
class ActivityRecord:
    """One hourly activity observation for a grid."""

    grid_id: int
    timestamp: object
    total_activity: float
    internet_activity: float


@dataclass(frozen=True)
class NetworkFeatures:
    """ML2 feature vector for one grid at one feature timestamp."""

    grid_id: int
    feature_timestamp: object
    avg_activity: float
    activity_growth: float
    active_hours: int
    peak_ratio: float
    variability: float
    internet_share: float


def calculate_features(
    recent_24h: Sequence[ActivityRecord],
    prior_24h: Sequence[ActivityRecord],
) -> NetworkFeatures:
    """
    Calculate the approved ML2 feature set.

    Recent window:
        t-23 ... t

    Prior baseline:
        t-47 ... t-24

    The feature timestamp is the timestamp of the final
    observation in the recent window.
    """

    if len(recent_24h) != 24:
        raise ValueError("recent_24h must contain exactly 24 observations.")

    if len(prior_24h) != 24:
        raise ValueError("prior_24h must contain exactly 24 observations.")

    grid_ids = {record.grid_id for record in recent_24h}
    if len(grid_ids) != 1:
        raise ValueError("recent_24h must contain observations for one grid.")

    prior_grid_ids = {record.grid_id for record in prior_24h}
    if prior_grid_ids != grid_ids:
        raise ValueError("Recent and prior windows must belong to the same grid.")

    grid_id = recent_24h[0].grid_id
    feature_timestamp = recent_24h[-1].timestamp

    recent_activity = [
        float(record.total_activity)
        for record in recent_24h
    ]

    prior_activity = [
        float(record.total_activity)
        for record in prior_24h
    ]

    avg_activity = mean(recent_activity)
    prior_avg_activity = mean(prior_activity)

    if prior_avg_activity == 0:
        activity_growth = 0.0
    else:
        activity_growth = (
            (avg_activity - prior_avg_activity)
            / prior_avg_activity
        )

    active_hours = sum(
        1 for activity in recent_activity
        if activity > 0
    )

    peak_activity = max(recent_activity)

    if avg_activity == 0:
        peak_ratio = 0.0
    else:
        peak_ratio = peak_activity / avg_activity

    variability = pstdev(recent_activity)

    total_activity_sum = sum(recent_activity)
    internet_activity_sum = sum(
        float(record.internet_activity)
        for record in recent_24h
    )

    if total_activity_sum == 0:
        internet_share = 0.0
    else:
        internet_share = (
            internet_activity_sum / total_activity_sum
        )

    features = NetworkFeatures(
        grid_id=grid_id,
        feature_timestamp=feature_timestamp,
        avg_activity=avg_activity,
        activity_growth=activity_growth,
        active_hours=active_hours,
        peak_ratio=peak_ratio,
        variability=variability,
        internet_share=internet_share,
    )

    _validate_features(features)

    return features


def _validate_features(features: NetworkFeatures) -> None:
    """Reject undefined or non-finite feature values."""

    numeric_values = {
        "avg_activity": features.avg_activity,
        "activity_growth": features.activity_growth,
        "peak_ratio": features.peak_ratio,
        "variability": features.variability,
        "internet_share": features.internet_share,
    }

    for name, value in numeric_values.items():
        if not isfinite(value):
            raise ValueError(
                f"Feature '{name}' is not finite: {value}"
            )

    if features.active_hours < 0 or features.active_hours > 24:
        raise ValueError(
            f"active_hours must be between 0 and 24: "
            f"{features.active_hours}"
        )

    if not 0 <= features.internet_share <= 1:
        raise ValueError(
            f"internet_share must be between 0 and 1: "
            f"{features.internet_share}"
        )
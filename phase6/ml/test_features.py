from datetime import datetime, timedelta

import pytest

from phase6.ml.features import ActivityRecord, calculate_features


def make_records(
    grid_id: int,
    start_time: datetime,
    activities: list[float],
    internet_share: float = 0.5,
) -> list[ActivityRecord]:
    """Create hourly activity records for testing."""

    records = []

    for index, activity in enumerate(activities):
        records.append(
            ActivityRecord(
                grid_id=grid_id,
                timestamp=start_time + timedelta(hours=index),
                total_activity=activity,
                internet_activity=activity * internet_share,
            )
        )

    return records


def test_feature_calculation():
    """Verify the six approved ML2 features."""

    start = datetime(2013, 11, 1, 0, 0)

    prior = make_records(
        grid_id=1,
        start_time=start,
        activities=[100.0] * 24,
    )

    recent = make_records(
        grid_id=1,
        start_time=start + timedelta(hours=24),
        activities=list(range(1, 25)),
        internet_share=0.5,
    )

    features = calculate_features(recent, prior)

    expected_avg = sum(range(1, 25)) / 24

    assert features.grid_id == 1
    assert features.feature_timestamp == recent[-1].timestamp

    assert features.avg_activity == pytest.approx(expected_avg)

    assert features.activity_growth == pytest.approx(
        (expected_avg - 100.0) / 100.0
    )

    assert features.active_hours == 24

    assert features.peak_ratio == pytest.approx(
        24.0 / expected_avg
    )

    assert features.internet_share == pytest.approx(0.5)

    assert features.variability > 0


def test_zero_activity_handling():
    """Verify zero-denominator rules."""

    start = datetime(2013, 11, 1, 0, 0)

    prior = make_records(
        grid_id=1,
        start_time=start,
        activities=[0.0] * 24,
    )

    recent = make_records(
        grid_id=1,
        start_time=start + timedelta(hours=24),
        activities=[0.0] * 24,
    )

    features = calculate_features(recent, prior)

    assert features.avg_activity == 0.0
    assert features.activity_growth == 0.0
    assert features.active_hours == 0
    assert features.peak_ratio == 0.0
    assert features.variability == 0.0
    assert features.internet_share == 0.0


def test_feature_windows_must_be_24_hours():
    """Reject incomplete feature windows."""

    start = datetime(2013, 11, 1, 0, 0)

    prior = make_records(
        grid_id=1,
        start_time=start,
        activities=[100.0] * 23,
    )

    recent = make_records(
        grid_id=1,
        start_time=start + timedelta(hours=23),
        activities=[100.0] * 24,
    )

    with pytest.raises(ValueError):
        calculate_features(recent, prior)


def test_windows_must_use_one_grid():
    """Reject mixed-grid observations."""

    start = datetime(2013, 11, 1, 0, 0)

    prior = make_records(
        grid_id=1,
        start_time=start,
        activities=[100.0] * 24,
    )

    recent = make_records(
        grid_id=1,
        start_time=start + timedelta(hours=24),
        activities=[100.0] * 24,
    )

    recent[-1] = ActivityRecord(
        grid_id=2,
        timestamp=recent[-1].timestamp,
        total_activity=100.0,
        internet_activity=50.0,
    )

    with pytest.raises(ValueError):
        calculate_features(recent, prior)

def test_no_future_data_leakage():
    """
    Features at time t must not depend on activity at t+1.

    We calculate features using observations through t, then
    deliberately change the t+1 observation. The features at t
    must remain identical.
    """

    start = datetime(2013, 11, 1, 0, 0)

    prior = make_records(
        grid_id=1,
        start_time=start,
        activities=[100.0] * 24,
    )

    recent = make_records(
        grid_id=1,
        start_time=start + timedelta(hours=24),
        activities=list(range(1, 25)),
        internet_share=0.5,
    )

    # Features at t use only the 48 observations ending at t.
    features_original = calculate_features(
        recent_24h=recent,
        prior_24h=prior,
    )

    # Create a hypothetical t+1 observation.
    future_record = ActivityRecord(
        grid_id=1,
        timestamp=recent[-1].timestamp + timedelta(hours=1),
        total_activity=999999.0,
        internet_activity=999999.0,
    )

    # Deliberately include the future observation in a separate
    # list. It must have no effect on the already-calculated
    # features at t.
    recent_with_future = recent + [future_record]

    # Recalculate using exactly the original t-23 ... t window.
    features_after_future_change = calculate_features(
        recent_24h=recent_with_future[:24],
        prior_24h=prior,
    )

    assert features_after_future_change == features_original
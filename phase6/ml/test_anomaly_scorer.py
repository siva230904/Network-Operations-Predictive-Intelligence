from __future__ import annotations

import pandas as pd

from anomaly_scorer import NetworkAnomalyScorer


def make_test_data() -> pd.DataFrame:
    rows = []

    # Three days of history for two grids.
    for day in range(1, 4):
        for grid_id in [1, 2]:
            rows.append(
                {
                    "grid_id": grid_id,
                    "timestamp": (
                        f"2013-11-{day:02d} 03:00:00"
                    ),
                    "total_activity": (
                        100
                        if grid_id == 1
                        else 200
                    ),
                }
            )

    return pd.DataFrame(rows)


def test_hour_of_day_baseline_uses_multiple_days():
    df = make_test_data()

    scorer = NetworkAnomalyScorer(
        file_path="dummy.csv",
        anomaly_threshold=1.0,
        min_history_days=2,
    )

    scorer.data = df.copy()

    baseline = (
        scorer.build_hour_of_day_baseline()
    )

    assert "hour_of_day" in baseline.columns
    assert "baseline_activity" in baseline.columns
    assert "history_count" in baseline.columns

    # Three observations exist in each grid/hour bucket.
    assert baseline["history_count"].min() == 3
    assert baseline["history_count"].max() == 3


def test_baseline_excludes_current_observation():
    df = pd.DataFrame(
        [
            {
                "grid_id": 1,
                "timestamp": "2013-11-01 03:00:00",
                "total_activity": 100,
            },
            {
                "grid_id": 1,
                "timestamp": "2013-11-02 03:00:00",
                "total_activity": 100,
            },
            {
                "grid_id": 1,
                "timestamp": "2013-11-03 03:00:00",
                "total_activity": 1000,
            },
        ]
    )

    scorer = NetworkAnomalyScorer(
        file_path="dummy.csv",
        anomaly_threshold=1.0,
        min_history_days=2,
    )

    scorer.data = df.copy()

    result = (
        scorer.calculate_anomalies()
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    # The 1000 observation must be compared against
    # the historical 100/100 median, not against a
    # median containing itself.
    assert result.loc[2, "baseline_activity"] == 100


def test_high_anomaly_is_detected():
    df = pd.DataFrame(
        [
            {
                "grid_id": 1,
                "timestamp": "2013-11-01 03:00:00",
                "total_activity": 100,
            },
            {
                "grid_id": 1,
                "timestamp": "2013-11-02 03:00:00",
                "total_activity": 100,
            },
            {
                "grid_id": 1,
                "timestamp": "2013-11-03 03:00:00",
                "total_activity": 300,
            },
        ]
    )

    scorer = NetworkAnomalyScorer(
        file_path="dummy.csv",
        anomaly_threshold=1.0,
        min_history_days=2,
    )

    scorer.data = df.copy()

    result = (
        scorer.calculate_anomalies()
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    row = result.iloc[2]

    assert row["direction"] == "HIGH"
    assert row["is_anomaly"] is True


def test_low_anomaly_is_detected():
    df = pd.DataFrame(
        [
            {
                "grid_id": 1,
                "timestamp": "2013-11-01 03:00:00",
                "total_activity": 100,
            },
            {
                "grid_id": 1,
                "timestamp": "2013-11-02 03:00:00",
                "total_activity": 100,
            },
            {
                "grid_id": 1,
                "timestamp": "2013-11-03 03:00:00",
                "total_activity": 0,
            },
        ]
    )

    scorer = NetworkAnomalyScorer(
        file_path="dummy.csv",
        anomaly_threshold=1.0,
        min_history_days=2,
    )

    scorer.data = df.copy()

    result = (
        scorer.calculate_anomalies()
        .sort_values("timestamp")
        .reset_index(drop=True)
    )

    row = result.iloc[2]

    assert row["direction"] == "LOW"
    assert row["is_anomaly"] is True
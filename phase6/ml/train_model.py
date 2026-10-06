from __future__ import annotations

import json
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sqlalchemy import text
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    confusion_matrix,
    classification_report,
    roc_auc_score,
    average_precision_score,
)
from sklearn.preprocessing import StandardScaler


# =========================================================
# Project path
# =========================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from phase4.api.db.database import engine


# =========================================================
# Configuration
# =========================================================

TARGET_THRESHOLD = 2000.0

TRAIN_RATIO = 0.80

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]

MODEL_DIR = PROJECT_ROOT / "phase6" / "ml" / "artifacts"

MODEL_PATH = MODEL_DIR / "logistic_regression.joblib"
SCALER_PATH = MODEL_DIR / "feature_scaler.joblib"
REPORT_PATH = MODEL_DIR / "ml3_evaluation.json"


# =========================================================
# Load modeling dataset
# =========================================================

def load_modeling_data() -> pd.DataFrame:
    """
    Load ML2 features and construct the t+1 target.

    Features describe the trailing window ending at t.

    Target describes whether activity at t+1 reaches
    the approved high-activity threshold of 2,000.
    """

    query = text(
        """
        SELECT
            mf.grid_id,
            mf.feature_timestamp,

            mf.avg_activity,
            mf.activity_growth,
            mf.active_hours,
            mf.peak_ratio,
            mf.variability,
            mf.internet_share,

            CASE
                WHEN f.total_activity >= :threshold THEN 1
                ELSE 0
            END AS high_activity_target

        FROM ml_grid_features mf

        INNER JOIN dim_grid dg
            ON dg.grid_id = mf.grid_id

        INNER JOIN dim_time future_time
            ON future_time.timestamp =
                DATE_ADD(
                    mf.feature_timestamp,
                    INTERVAL 1 HOUR
                )

        INNER JOIN fact_network_activity f
            ON f.grid_key = dg.grid_key
            AND f.time_key = future_time.time_key

        ORDER BY mf.feature_timestamp, mf.grid_id
        """
    )

    with engine.connect() as connection:
        dataframe = pd.read_sql(
            query,
            connection,
            params={
                "threshold": TARGET_THRESHOLD,
            },
        )

    if dataframe.empty:
        raise RuntimeError(
            "No ML3 modeling rows were returned."
        )

    return dataframe


# =========================================================
# Chronological split
# =========================================================

def chronological_split(
    dataframe: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split by timestamp.

    Entire timestamps are assigned to either train or test.
    No random row-level splitting is used.
    """

    dataframe = dataframe.sort_values(
        "feature_timestamp"
    ).reset_index(drop=True)

    unique_timestamps = sorted(
        dataframe["feature_timestamp"].unique()
    )

    if len(unique_timestamps) < 2:
        raise RuntimeError(
            "At least two timestamps are required "
            "for a chronological train/test split."
        )

    split_index = int(
        len(unique_timestamps) * TRAIN_RATIO
    )

    split_index = max(
        1,
        min(
            split_index,
            len(unique_timestamps) - 1,
        ),
    )

    cutoff_timestamp = unique_timestamps[split_index]

    train = dataframe[
        dataframe["feature_timestamp"] < cutoff_timestamp
    ].copy()

    test = dataframe[
        dataframe["feature_timestamp"] >= cutoff_timestamp
    ].copy()

    if train.empty or test.empty:
        raise RuntimeError(
            "Chronological split produced an empty "
            "train or test set."
        )

    if train["feature_timestamp"].max() >= test["feature_timestamp"].min():
        raise RuntimeError(
            "Train and test timestamps overlap."
        )

    return train, test


# =========================================================
# Model training
# =========================================================

def train_model(
    train: pd.DataFrame,
):
    """
    Train interpretable Logistic Regression.

    Standardization is applied because the six features
    have very different numerical scales.
    """

    X_train = train[FEATURE_COLUMNS]
    y_train = train["high_activity_target"]

    scaler = StandardScaler()

    X_train_scaled = scaler.fit_transform(
        X_train
    )

    model = LogisticRegression(
        max_iter=1000,
        class_weight=None,
        random_state=42,
    )

    model.fit(
        X_train_scaled,
        y_train,
    )

    return model, scaler


# =========================================================
# Evaluation
# =========================================================

def evaluate_model(
    model,
    scaler,
    train: pd.DataFrame,
    test: pd.DataFrame,
) -> dict:
    """
    Evaluate the model on the chronological test set.

    Also performs the first ML3 >95% accuracy investigation:
    - trivial all-negative baseline
    - confusion matrix
    - ROC-AUC
    - PR-AUC
    - classification report
    """

    X_test = test[FEATURE_COLUMNS]
    y_test = test["high_activity_target"]

    X_test_scaled = scaler.transform(
        X_test
    )

    # -----------------------------------------------------
    # Predictions
    # -----------------------------------------------------

    # Probability of the positive class.
    y_prob = model.predict_proba(
        X_test_scaled
    )[:, 1]

    # Default Logistic Regression decision threshold = 0.50.
    y_pred = (
        y_prob >= 0.5
    ).astype(int)

    # -----------------------------------------------------
    # Standard ML3 metrics
    # -----------------------------------------------------

    accuracy = accuracy_score(
        y_test,
        y_pred,
    )

    precision = precision_score(
        y_test,
        y_pred,
        zero_division=0,
    )

    recall = recall_score(
        y_test,
        y_pred,
        zero_division=0,
    )

    # -----------------------------------------------------
    # ML3 investigation: trivial baseline
    # -----------------------------------------------------

    # Predict "not high activity" for every test row.
    baseline_pred = np.zeros(
        len(y_test),
        dtype=int,
    )

    baseline_accuracy = accuracy_score(
        y_test,
        baseline_pred,
    )

    accuracy_improvement = (
        accuracy - baseline_accuracy
    )

    # -----------------------------------------------------
    # Confusion matrix
    # -----------------------------------------------------

    tn, fp, fn, tp = confusion_matrix(
        y_test,
        y_pred,
    ).ravel()

    # -----------------------------------------------------
    # Additional diagnostic metrics
    # -----------------------------------------------------

    roc_auc = roc_auc_score(
        y_test,
        y_prob,
    )

    pr_auc = average_precision_score(
        y_test,
        y_prob,
    )

    # -----------------------------------------------------
    # Print investigation results
    # -----------------------------------------------------

    print("\n--- ML3 Investigation ---")

    print(
        f"Baseline accuracy (always 0): "
        f"{baseline_accuracy:.4%}"
    )

    print(
        f"Model accuracy:               "
        f"{accuracy:.4%}"
    )

    print(
        f"Accuracy improvement:         "
        f"{accuracy_improvement:.4%}"
    )

    print("\nConfusion matrix:")

    print(
        f"  True negatives:  "
        f"{tn:,}"
    )

    print(
        f"  False positives: "
        f"{fp:,}"
    )

    print(
        f"  False negatives: "
        f"{fn:,}"
    )

    print(
        f"  True positives:  "
        f"{tp:,}"
    )

    print("\nAdditional metrics:")

    print(
        f"ROC-AUC: {roc_auc:.4f}"
    )

    print(
        f"PR-AUC:  {pr_auc:.4f}"
    )

    print("\nClassification report:")

    print(
        classification_report(
            y_test,
            y_pred,
            target_names=[
                "Not High Activity",
                "High Activity",
            ],
            digits=4,
            zero_division=0,
        )
    )

    # -----------------------------------------------------
    # Base rates
    # -----------------------------------------------------

    train_base_rate = float(
        train["high_activity_target"].mean()
    )

    test_base_rate = float(
        test["high_activity_target"].mean()
    )

    # -----------------------------------------------------
    # Evaluation report
    # -----------------------------------------------------

    report = {
        "target_definition": (
            "1 when total_activity at t+1 >= 2000, "
            "otherwise 0"
        ),

        "target_threshold": TARGET_THRESHOLD,

        "feature_timestamp_definition": (
            "Features use data through t; "
            "target is measured at t+1."
        ),

        "algorithm": "Logistic Regression",

        "split_strategy": (
            "Chronological 80/20 split by timestamp; "
            "no random splitting."
        ),

        "train": {
            "rows": int(len(train)),
            "earliest_timestamp": str(
                train["feature_timestamp"].min()
            ),
            "latest_timestamp": str(
                train["feature_timestamp"].max()
            ),
            "positive_rows": int(
                train["high_activity_target"].sum()
            ),
            "negative_rows": int(
                (train["high_activity_target"] == 0).sum()
            ),
            "base_rate": train_base_rate,
        },

        "test": {
            "rows": int(len(test)),
            "earliest_timestamp": str(
                test["feature_timestamp"].min()
            ),
            "latest_timestamp": str(
                test["feature_timestamp"].max()
            ),
            "positive_rows": int(
                test["high_activity_target"].sum()
            ),
            "negative_rows": int(
                (test["high_activity_target"] == 0).sum()
            ),
            "base_rate": test_base_rate,
        },

        "metrics": {
            "accuracy": float(accuracy),
            "precision": float(precision),
            "recall": float(recall),
        },

        "investigation": {
            "baseline_accuracy_all_negative": float(
                baseline_accuracy
            ),

            "accuracy_improvement_over_baseline": float(
                accuracy_improvement
            ),

            "confusion_matrix": {
                "true_negatives": int(tn),
                "false_positives": int(fp),
                "false_negatives": int(fn),
                "true_positives": int(tp),
            },

            "roc_auc": float(
                roc_auc
            ),

            "pr_auc": float(
                pr_auc
            ),

            "default_probability_threshold": 0.5,
        },

        "accuracy_warning": (
            "Accuracy exceeds 95%; investigate for "
            "leakage, circular labeling, or class "
            "imbalance before sign-off."
            if accuracy > 0.95
            else "Accuracy is not above the 95% investigation threshold."
        ),
    }

    return report


# =========================================================
# Coefficient inspection
# =========================================================

def get_coefficients(model) -> list[dict]:
    """
    Return Logistic Regression coefficients.

    Because features are standardized before training,
    coefficient magnitudes can be compared directly.
    """

    coefficients = model.coef_[0]

    result = []

    for feature_name, coefficient in zip(
        FEATURE_COLUMNS,
        coefficients,
    ):
        result.append(
            {
                "feature": feature_name,
                "coefficient": float(coefficient),
                "direction": (
                    "positive"
                    if coefficient > 0
                    else "negative"
                    if coefficient < 0
                    else "neutral"
                ),
            }
        )

    return result


# =========================================================
# Save artifacts
# =========================================================

def save_artifacts(
    model,
    scaler,
    report,
):
    """
    Save the trained model, scaler and evaluation report.
    """

    MODEL_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    joblib.dump(
        model,
        MODEL_PATH,
    )

    joblib.dump(
        scaler,
        SCALER_PATH,
    )

    with REPORT_PATH.open(
        "w",
        encoding="utf-8",
    ) as file:
        json.dump(
            report,
            file,
            indent=2,
        )


# =========================================================
# Main
# =========================================================

def main():
    print("=" * 60)
    print("ML3 — Logistic Regression Baseline")
    print("=" * 60)

    print("\nLoading ML3 modeling dataset...")

    dataframe = load_modeling_data()

    print(
        f"Loaded {len(dataframe):,} usable rows."
    )

    print(
        f"Earliest feature timestamp: "
        f"{dataframe['feature_timestamp'].min()}"
    )

    print(
        f"Latest feature timestamp: "
        f"{dataframe['feature_timestamp'].max()}"
    )

    print("\nPerforming chronological split...")

    train, test = chronological_split(
        dataframe
    )

    print(
        f"Train rows: {len(train):,}"
    )

    print(
        f"Test rows: {len(test):,}"
    )

    print(
        f"Train range: "
        f"{train['feature_timestamp'].min()} "
        f"→ "
        f"{train['feature_timestamp'].max()}"
    )

    print(
        f"Test range:  "
        f"{test['feature_timestamp'].min()} "
        f"→ "
        f"{test['feature_timestamp'].max()}"
    )

    print("\nTraining Logistic Regression...")

    model, scaler = train_model(
        train
    )

    print("Model training completed.")

    print("\nEvaluating model...")

    report = evaluate_model(
        model,
        scaler,
        train,
        test,
    )

    coefficients = get_coefficients(
        model
    )

    report["coefficients"] = coefficients

    print("\n" + "=" * 60)
    print("ML3 RESULTS")
    print("=" * 60)

    print(
        f"Train base rate: "
        f"{report['train']['base_rate']:.4%}"
    )

    print(
        f"Test base rate:  "
        f"{report['test']['base_rate']:.4%}"
    )

    print(
        f"Accuracy:  "
        f"{report['metrics']['accuracy']:.4%}"
    )

    print(
        f"Precision: "
        f"{report['metrics']['precision']:.4%}"
    )

    print(
        f"Recall:    "
        f"{report['metrics']['recall']:.4%}"
    )

    print("\nCoefficients:")

    for coefficient in coefficients:
        print(
            f"  {coefficient['feature']:<20} "
            f"{coefficient['coefficient']:>10.4f} "
            f"({coefficient['direction']})"
        )

    print("\n" + report["accuracy_warning"])

    print("\nSaving model artifacts...")

    save_artifacts(
        model,
        scaler,
        report,
    )

    print(
        f"\nModel:  {MODEL_PATH}"
    )

    print(
        f"Scaler: {SCALER_PATH}"
    )

    print(
        f"Report: {REPORT_PATH}"
    )

    print("\nML3 baseline training completed.")


if __name__ == "__main__":
    main()
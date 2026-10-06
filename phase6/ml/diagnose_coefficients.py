# from __future__ import annotations

# import sys
# from pathlib import Path

# import numpy as np
# import pandas as pd
# from sqlalchemy import text
# from sklearn.linear_model import LogisticRegression
# from sklearn.preprocessing import StandardScaler


# # =========================================================
# # Project path
# # =========================================================

# PROJECT_ROOT = Path(__file__).resolve().parents[2]

# if str(PROJECT_ROOT) not in sys.path:
#     sys.path.insert(0, str(PROJECT_ROOT))

# from phase4.api.db.database import engine


# # =========================================================
# # Configuration
# # =========================================================

# TARGET_THRESHOLD = 2000.0

# FEATURE_COLUMNS = [
#     "avg_activity",
#     "activity_growth",
#     "active_hours",
#     "peak_ratio",
#     "variability",
#     "internet_share",
# ]


# # =========================================================
# # Load the exact ML3 modeling dataset
# # =========================================================

# def load_data() -> pd.DataFrame:

#     query = text(
#         """
#         SELECT
#             mf.grid_id,
#             mf.feature_timestamp,

#             mf.avg_activity,
#             mf.activity_growth,
#             mf.active_hours,
#             mf.peak_ratio,
#             mf.variability,
#             mf.internet_share,

#             CASE
#                 WHEN f.total_activity >= :threshold
#                 THEN 1
#                 ELSE 0
#             END AS high_activity_target

#         FROM ml_grid_features mf

#         INNER JOIN dim_grid dg
#             ON dg.grid_id = mf.grid_id

#         INNER JOIN dim_time future_time
#             ON future_time.timestamp =
#                 DATE_ADD(
#                     mf.feature_timestamp,
#                     INTERVAL 1 HOUR
#                 )

#         INNER JOIN fact_network_activity f
#             ON f.grid_key = dg.grid_key
#             AND f.time_key = future_time.time_key

#         ORDER BY
#             mf.feature_timestamp,
#             mf.grid_id
#         """
#     )

#     print("Loading ML3 dataset...")

#     with engine.connect() as connection:
#         dataframe = pd.read_sql(
#             query,
#             connection,
#             params={
#                 "threshold": TARGET_THRESHOLD,
#             },
#         )

#     if dataframe.empty:
#         raise RuntimeError(
#             "No ML3 modeling rows were returned."
#         )

#     return dataframe


# # =========================================================
# # 1. Feature-to-feature correlations
# # =========================================================

# def inspect_feature_correlations(
#     dataframe: pd.DataFrame,
# ):
#     print("\n" + "=" * 60)
#     print("1. FEATURE-TO-FEATURE CORRELATIONS")
#     print("=" * 60)

#     correlation_matrix = (
#         dataframe[FEATURE_COLUMNS]
#         .corr()
#     )

#     print(
#         correlation_matrix.to_string(
#             float_format=lambda value: f"{value: .4f}"
#         )
#     )

#     print("\nStrong feature relationships:")

#     found_relationship = False

#     for i, feature_a in enumerate(FEATURE_COLUMNS):

#         for feature_b in FEATURE_COLUMNS[i + 1:]:

#             correlation = correlation_matrix.loc[
#                 feature_a,
#                 feature_b,
#             ]

#             if abs(correlation) >= 0.70:

#                 print(
#                     f"  {feature_a:<20} ↔ "
#                     f"{feature_b:<20} "
#                     f"{correlation:+.4f}"
#                 )

#                 found_relationship = True

#     if not found_relationship:
#         print(
#             "  No feature pair has "
#             "|correlation| >= 0.70."
#         )


# # =========================================================
# # 2. Reproduce standardized Logistic Regression
# # =========================================================

# def fit_logistic_regression(
#     dataframe: pd.DataFrame,
#     features: list[str],
# ):
#     X = dataframe[features]
#     y = dataframe["high_activity_target"]

#     scaler = StandardScaler()

#     X_scaled = scaler.fit_transform(X)

#     model = LogisticRegression(
#         max_iter=1000,
#         class_weight=None,
#         random_state=42,
#     )

#     model.fit(
#         X_scaled,
#         y,
#     )

#     coefficients = pd.Series(
#         model.coef_[0],
#         index=features,
#     )

#     return coefficients


# # =========================================================
# # 3. Full-model coefficients
# # =========================================================

# def inspect_full_model(
#     dataframe: pd.DataFrame,
# ):
#     print("\n" + "=" * 60)
#     print("2. FULL MODEL COEFFICIENTS")
#     print("=" * 60)

#     coefficients = fit_logistic_regression(
#         dataframe,
#         FEATURE_COLUMNS,
#     )

#     coefficients = coefficients.sort_values(
#         key=lambda series: series.abs(),
#         ascending=False,
#     )

#     for feature, coefficient in coefficients.items():

#         direction = (
#             "positive"
#             if coefficient > 0
#             else "negative"
#             if coefficient < 0
#             else "neutral"
#         )

#         print(
#             f"  {feature:<20} "
#             f"{coefficient:>10.4f} "
#             f"({direction})"
#         )

#     return coefficients


# # =========================================================
# # 4. Remove avg_activity
# # =========================================================

# def inspect_without_avg_activity(
#     dataframe: pd.DataFrame,
# ):
#     reduced_features = [
#         feature
#         for feature in FEATURE_COLUMNS
#         if feature != "avg_activity"
#     ]

#     print("\n" + "=" * 60)
#     print("3. MODEL WITHOUT avg_activity")
#     print("=" * 60)

#     print(
#         "Training diagnostic model without "
#         "avg_activity..."
#     )

#     coefficients = fit_logistic_regression(
#         dataframe,
#         reduced_features,
#     )

#     coefficients = coefficients.sort_values(
#         key=lambda series: series.abs(),
#         ascending=False,
#     )

#     for feature, coefficient in coefficients.items():

#         direction = (
#             "positive"
#             if coefficient > 0
#             else "negative"
#             if coefficient < 0
#             else "neutral"
#         )

#         print(
#             f"  {feature:<20} "
#             f"{coefficient:>10.4f} "
#             f"({direction})"
#         )

#     variability_coefficient = coefficients[
#         "variability"
#     ]

#     print(
#         "\nVariability coefficient without "
#         f"avg_activity: {variability_coefficient:.4f}"
#     )

#     if variability_coefficient > 0:
#         print(
#             "Interpretation: variability becomes "
#             "positive when avg_activity is removed."
#         )
#     elif variability_coefficient < 0:
#         print(
#             "Interpretation: variability remains "
#             "negative even without avg_activity."
#         )
#     else:
#         print(
#             "Interpretation: variability becomes "
#             "approximately neutral."
#         )

#     return coefficients


# # =========================================================
# # 5. Model with only avg_activity + variability
# # =========================================================

# def inspect_two_feature_model(
#     dataframe: pd.DataFrame,
# ):
#     reduced_features = [
#         "avg_activity",
#         "variability",
#     ]

#     print("\n" + "=" * 60)
#     print("4. TWO-FEATURE MODEL")
#     print("=" * 60)

#     print(
#         "Training diagnostic model using only:"
#     )

#     print(
#         "  avg_activity"
#     )

#     print(
#         "  variability"
#     )

#     coefficients = fit_logistic_regression(
#         dataframe,
#         reduced_features,
#     )

#     for feature in reduced_features:

#         coefficient = coefficients[feature]

#         direction = (
#             "positive"
#             if coefficient > 0
#             else "negative"
#             if coefficient < 0
#             else "neutral"
#         )

#         print(
#             f"  {feature:<20} "
#             f"{coefficient:>10.4f} "
#             f"({direction})"
#         )


# # =========================================================
# # 6. Variability relationship with avg_activity
# # =========================================================

# def inspect_variability_by_activity(
#     dataframe: pd.DataFrame,
# ):
#     print("\n" + "=" * 60)
#     print("5. VARIABILITY VS avg_activity")
#     print("=" * 60)

#     bins = [
#         0,
#         100,
#         250,
#         500,
#         1000,
#         1500,
#         2000,
#         3000,
#         5000,
#         np.inf,
#     ]

#     labels = [
#         "<100",
#         "100-249",
#         "250-499",
#         "500-999",
#         "1000-1499",
#         "1500-1999",
#         "2000-2999",
#         "3000-4999",
#         "5000+",
#     ]

#     dataframe = dataframe.copy()

#     dataframe["activity_band"] = pd.cut(
#         dataframe["avg_activity"],
#         bins=bins,
#         labels=labels,
#         right=False,
#     )

#     grouped = (
#         dataframe
#         .groupby(
#             "activity_band",
#             observed=False,
#         )
#         .agg(
#             rows=("variability", "size"),
#             mean_avg_activity=(
#                 "avg_activity",
#                 "mean",
#             ),
#             mean_variability=(
#                 "variability",
#                 "mean",
#             ),
#             positive_rate=(
#                 "high_activity_target",
#                 "mean",
#             ),
#         )
#     )

#     print(
#         grouped.to_string(
#             float_format=lambda value: f"{value:.4f}"
#         )
#     )


# # =========================================================
# # Main
# # =========================================================

# def main():

#     dataframe = load_data()

#     print(
#         f"\nLoaded {len(dataframe):,} usable rows."
#     )

#     print(
#         f"Positive target rate: "
#         f"{dataframe['high_activity_target'].mean():.4%}"
#     )

#     # 1
#     inspect_feature_correlations(
#         dataframe
#     )

#     # 2
#     full_coefficients = inspect_full_model(
#         dataframe
#     )

#     # 3
#     reduced_coefficients = inspect_without_avg_activity(
#         dataframe
#     )

#     # 4
#     inspect_two_feature_model(
#         dataframe
#     )

#     # 5
#     inspect_variability_by_activity(
#         dataframe
#     )

#     # -----------------------------------------------------
#     # Final interpretation
#     # -----------------------------------------------------

#     print("\n" + "=" * 60)
#     print("DIAGNOSTIC SUMMARY")
#     print("=" * 60)

#     full_variability = full_coefficients[
#         "variability"
#     ]

#     reduced_variability = reduced_coefficients[
#         "variability"
#     ]

#     print(
#         f"\nFull-model variability coefficient: "
#         f"{full_variability:.4f}"
#     )

#     print(
#         f"Without avg_activity:              "
#         f"{reduced_variability:.4f}"
#     )

#     if (
#         full_variability < 0
#         and reduced_variability > 0
#     ):
#         print(
#             "\nFinding:"
#         )

#         print(
#             "The negative variability coefficient "
#             "is likely caused by multivariate "
#             "correlation/suppression involving "
#             "avg_activity."
#         )

#     elif (
#         full_variability < 0
#         and reduced_variability < 0
#     ):
#         print(
#             "\nFinding:"
#         )

#         print(
#             "Variability remains negative even "
#             "without avg_activity. This deserves "
#             "additional investigation."
#         )

#     else:
#         print(
#             "\nFinding:"
#         )

#         print(
#             "The relationship between variability "
#             "and the other features requires "
#             "further inspection."
#         )

#     print(
#         "\nDiagnostic completed."
#     )


# if __name__ == "__main__":
#     main()


from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import text
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
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

ALL_FEATURES = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]


# =========================================================
# Load exact ML3 dataset
# =========================================================

def load_data() -> pd.DataFrame:

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
                WHEN f.total_activity >= :threshold
                THEN 1
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

        ORDER BY
            mf.feature_timestamp,
            mf.grid_id
        """
    )

    print("Loading ML3 dataset...")

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

    dataframe = (
        dataframe
        .sort_values("feature_timestamp")
        .reset_index(drop=True)
    )

    timestamps = sorted(
        dataframe["feature_timestamp"].unique()
    )

    split_index = int(
        len(timestamps) * TRAIN_RATIO
    )

    cutoff = timestamps[split_index]

    train = dataframe[
        dataframe["feature_timestamp"] < cutoff
    ].copy()

    test = dataframe[
        dataframe["feature_timestamp"] >= cutoff
    ].copy()

    return train, test


# =========================================================
# Evaluate feature set
# =========================================================

def evaluate_feature_set(
    train: pd.DataFrame,
    test: pd.DataFrame,
    features: list[str],
) -> dict:

    X_train = train[features]
    y_train = train["high_activity_target"]

    X_test = test[features]
    y_test = test["high_activity_target"]

    scaler = StandardScaler()

    X_train_scaled = scaler.fit_transform(
        X_train
    )

    X_test_scaled = scaler.transform(
        X_test
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

    probabilities = model.predict_proba(
        X_test_scaled
    )[:, 1]

    predictions = (
        probabilities >= 0.5
    ).astype(int)

    return {
        "accuracy": accuracy_score(
            y_test,
            predictions,
        ),
        "precision": precision_score(
            y_test,
            predictions,
            zero_division=0,
        ),
        "recall": recall_score(
            y_test,
            predictions,
            zero_division=0,
        ),
        "roc_auc": roc_auc_score(
            y_test,
            probabilities,
        ),
        "pr_auc": average_precision_score(
            y_test,
            probabilities,
        ),
    }


# =========================================================
# Main
# =========================================================

def main():

    dataframe = load_data()

    train, test = chronological_split(
        dataframe
    )

    print("\n" + "=" * 70)
    print("ML3 FEATURE ABLATION / SANITY CHECK")
    print("=" * 70)

    print(
        f"\nTrain rows: {len(train):,}"
    )

    print(
        f"Test rows:  {len(test):,}"
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

    feature_sets = {
        "All six features": ALL_FEATURES,

        "avg_activity only": [
            "avg_activity",
        ],

        "Without avg_activity": [
            feature
            for feature in ALL_FEATURES
            if feature != "avg_activity"
        ],
    }

    results = []

    for name, features in feature_sets.items():

        print(
            f"\nTraining: {name}"
        )

        print(
            f"Features: {', '.join(features)}"
        )

        metrics = evaluate_feature_set(
            train,
            test,
            features,
        )

        results.append(
            {
                "model": name,
                **metrics,
            }
        )

    # -----------------------------------------------------
    # Results
    # -----------------------------------------------------

    results_df = pd.DataFrame(
        results
    )

    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)

    print(
        results_df.to_string(
            index=False,
            formatters={
                "accuracy": "{:.4%}".format,
                "precision": "{:.4%}".format,
                "recall": "{:.4%}".format,
                "roc_auc": "{:.4f}".format,
                "pr_auc": "{:.4f}".format,
            },
        )
    )

    # -----------------------------------------------------
    # Interpretation
    # -----------------------------------------------------

    full = results_df[
        results_df["model"]
        == "All six features"
    ].iloc[0]

    avg_only = results_df[
        results_df["model"]
        == "avg_activity only"
    ].iloc[0]

    without_avg = results_df[
        results_df["model"]
        == "Without avg_activity"
    ].iloc[0]

    print("\n" + "=" * 70)
    print("INTERPRETATION")
    print("=" * 70)

    print(
        "\nAll six features:"
    )

    print(
        f"  Accuracy:  {full['accuracy']:.4%}"
    )

    print(
        f"  Precision: {full['precision']:.4%}"
    )

    print(
        f"  Recall:    {full['recall']:.4%}"
    )

    print(
        f"  ROC-AUC:   {full['roc_auc']:.4f}"
    )

    print(
        f"  PR-AUC:    {full['pr_auc']:.4f}"
    )

    print(
        "\navg_activity only:"
    )

    print(
        f"  Accuracy:  {avg_only['accuracy']:.4%}"
    )

    print(
        f"  Precision: {avg_only['precision']:.4%}"
    )

    print(
        f"  Recall:    {avg_only['recall']:.4%}"
    )

    print(
        f"  ROC-AUC:   {avg_only['roc_auc']:.4f}"
    )

    print(
        f"  PR-AUC:    {avg_only['pr_auc']:.4f}"
    )

    print(
        "\nWithout avg_activity:"
    )

    print(
        f"  Accuracy:  {without_avg['accuracy']:.4%}"
    )

    print(
        f"  Precision: {without_avg['precision']:.4%}"
    )

    print(
        f"  Recall:    {without_avg['recall']:.4%}"
    )

    print(
        f"  ROC-AUC:   {without_avg['roc_auc']:.4f}"
    )

    print(
        f"  PR-AUC:    {without_avg['pr_auc']:.4f}"
    )

    print("\n" + "=" * 70)

    print(
        "Diagnostic completed."
    )


if __name__ == "__main__":
    main()
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sqlalchemy import select
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# =========================================================
# Project path
# =========================================================

PROJECT_ROOT = Path(
    os.getenv(
        "PROJECT_ROOT",
        Path(__file__).resolve().parents[2],
    )
).resolve()

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# =========================================================
# Existing project database/models
# =========================================================

from phase4.api.db.models import (
    MLGridFeatures,
    NetworkRiskScore,
)


# =========================================================
# Exact ML4 warehouse anomaly implementation
# =========================================================

from phase6.ml.warehouse_anomaly_scorer import (
    calculate_anomalies,
    load_activity_data,
    validate_input,
)


# =========================================================
# ML6 WSL Database Configuration
# =========================================================

DATABASE_URL = os.getenv(
    "ML6_DATABASE_URL",
)

if not DATABASE_URL:
    raise RuntimeError(
        "ML6_DATABASE_URL environment variable is required."
    )

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=3600,
    future=True,
)

SessionLocal = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
)


# =========================================================
# Configuration
# =========================================================

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]

MODEL_VERSION = "ml3-logistic-regression-v1"

RISK_THRESHOLD = 0.5

MODEL_PATH = (
    PROJECT_ROOT
    / "phase6"
    / "ml"
    / "artifacts"
    / "logistic_regression.joblib"
)

SCALER_PATH = (
    PROJECT_ROOT
    / "phase6"
    / "ml"
    / "artifacts"
    / "feature_scaler.joblib"
)

BATCH_SIZE = 5000

logger = logging.getLogger(__name__)


# =========================================================
# Load frozen ML3 artifacts
# =========================================================

def load_model_artifacts():
    """
    Load the frozen ML3 Logistic Regression model
    and StandardScaler.

    ML6 never retrains or modifies these artifacts.
    """

    if not MODEL_PATH.exists():
        raise FileNotFoundError(
            f"ML3 model artifact does not exist: {MODEL_PATH}"
        )

    if not SCALER_PATH.exists():
        raise FileNotFoundError(
            f"ML3 scaler artifact does not exist: {SCALER_PATH}"
        )

    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)

    if not hasattr(model, "predict_proba"):
        raise RuntimeError(
            "Loaded ML3 model does not support predict_proba()."
        )

    if not hasattr(scaler, "transform"):
        raise RuntimeError(
            "Loaded ML3 scaler does not support transform()."
        )

    if getattr(model, "n_features_in_", None) != len(FEATURE_COLUMNS):
        raise RuntimeError(
            "ML3 model feature count does not match "
            f"the expected {len(FEATURE_COLUMNS)} features."
        )

    if getattr(scaler, "n_features_in_", None) != len(FEATURE_COLUMNS):
        raise RuntimeError(
            "ML3 scaler feature count does not match "
            f"the expected {len(FEATURE_COLUMNS)} features."
        )

    return model, scaler


# =========================================================
# Load ML2 features
# =========================================================

def load_features(session) -> pd.DataFrame:
    """
    Load persisted ML2 features from MySQL.

    ML6 does not recalculate ML2 features.
    """

    statement = (
        select(
            MLGridFeatures.grid_id,
            MLGridFeatures.feature_timestamp,
            MLGridFeatures.avg_activity,
            MLGridFeatures.activity_growth,
            MLGridFeatures.active_hours,
            MLGridFeatures.peak_ratio,
            MLGridFeatures.variability,
            MLGridFeatures.internet_share,
        )
        .order_by(
            MLGridFeatures.feature_timestamp,
            MLGridFeatures.grid_id,
        )
    )

    rows = session.execute(statement).all()

    if not rows:
        raise RuntimeError(
            "ML6 scoring cannot run because ml_grid_features "
            "contains no rows."
        )

    dataframe = pd.DataFrame(
        rows,
        columns=[
            "grid_id",
            "feature_timestamp",
            *FEATURE_COLUMNS,
        ],
    )

    return dataframe


# =========================================================
# Validate ML2 features
# =========================================================

def validate_features(
    dataframe: pd.DataFrame,
) -> None:

    required_columns = {
        "grid_id",
        "feature_timestamp",
        *FEATURE_COLUMNS,
    }

    missing = (
        required_columns
        - set(dataframe.columns)
    )

    if missing:
        raise ValueError(
            "ML2 feature data is missing required columns: "
            f"{sorted(missing)}"
        )

    if dataframe["grid_id"].isna().any():
        raise ValueError(
            "ML2 feature data contains missing grid IDs."
        )

    if dataframe["feature_timestamp"].isna().any():
        raise ValueError(
            "ML2 feature data contains missing timestamps."
        )

    dataframe["feature_timestamp"] = pd.to_datetime(
        dataframe["feature_timestamp"],
        errors="coerce",
    )

    if dataframe["feature_timestamp"].isna().any():
        raise ValueError(
            "ML2 feature data contains invalid timestamps."
        )

    for column in FEATURE_COLUMNS:

        dataframe[column] = pd.to_numeric(
            dataframe[column],
            errors="coerce",
        )

        if dataframe[column].isna().any():
            raise ValueError(
                f"ML2 feature column '{column}' contains "
                "invalid or missing values."
            )

        values = dataframe[column].to_numpy()

        if not np.isfinite(values).all():
            raise ValueError(
                f"ML2 feature column '{column}' contains "
                "non-finite values."
            )

    duplicate_count = int(
        dataframe.duplicated(
            subset=[
                "grid_id",
                "feature_timestamp",
            ]
        ).sum()
    )

    if duplicate_count:
        raise ValueError(
            "ML2 feature data contains "
            f"{duplicate_count:,} duplicate grid/timestamp rows."
        )


# =========================================================
# ML3 risk scoring
# =========================================================

def calculate_risk_scores(
    dataframe: pd.DataFrame,
    model,
    scaler,
) -> pd.DataFrame:
    """
    Calculate ML3 risk probabilities.

    The six ML2 features describe data through t.

    The frozen ML3 model predicts the probability that
    high activity occurs at t+1.
    """

    result = dataframe.copy()

    X = result[
        FEATURE_COLUMNS
    ]

    X_scaled = scaler.transform(
        X
    )

    result["risk_score"] = (
        model.predict_proba(
            X_scaled
        )[:, 1]
        .astype(float)
    )

    result["risk_level"] = np.where(
        result["risk_score"] >= RISK_THRESHOLD,
        "HIGH",
        "LOW",
    )

    result["model_version"] = (
        MODEL_VERSION
    )

    return result


# =========================================================
# ML4 anomaly scoring
# =========================================================

def calculate_ml4_scores(
    session,
) -> pd.DataFrame:
    """
    Calculate the exact ML4 anomaly result from the
    warehouse.

    ML4 is calculated independently from ML3 features.

    This preserves the exact validated ML4 methodology:
        grid_id + hour_of_day
        leave-one-out median baseline
        anomaly threshold = 0.5
        minimum history days = 2
    """

    logger.info(
        "Loading warehouse activity for ML4 anomaly scoring."
    )

    dataframe = load_activity_data(
        session
    )

    validate_input(
        dataframe
    )

    logger.info(
        "Calculating exact ML4 warehouse anomalies."
    )

    result = calculate_anomalies(
        dataframe
    )

    required_columns = [
        "grid_id",
        "timestamp",
        "anomaly_score",
        "direction",
        "is_anomaly",
        "reason",
    ]

    result = result[
        required_columns
    ].copy()

    result = result.rename(
        columns={
            "direction": "anomaly_direction",
            "reason": "anomaly_reason",
        }
    )

    result["is_anomaly"] = (
        result["is_anomaly"]
        .astype(int)
    )

    return result


# =========================================================
# Combine ML3 + ML4
# =========================================================

def combine_ml3_ml4(
    ml3_dataframe: pd.DataFrame,
    ml4_dataframe: pd.DataFrame,
) -> pd.DataFrame:
    """
    Combine ML3 prediction output with ML4 anomaly output.

    Join key:

        grid_id + timestamp

    ML3 remains the source of:
        risk_score
        risk_level
        model_version

    ML4 contributes:
        anomaly_score
        anomaly_direction
        is_anomaly
        anomaly_reason

    Only timestamps already present in ML3 are published
    to network_risk_scores.
    """

    ml3 = ml3_dataframe.copy()

    ml3 = ml3.rename(
        columns={
            "feature_timestamp": "timestamp",
        }
    )

    ml4 = ml4_dataframe.copy()

    ml3["timestamp"] = pd.to_datetime(
        ml3["timestamp"],
        errors="raise",
    )

    ml4["timestamp"] = pd.to_datetime(
        ml4["timestamp"],
        errors="raise",
    )

    duplicate_ml4 = int(
        ml4.duplicated(
            subset=[
                "grid_id",
                "timestamp",
            ]
        ).sum()
    )

    if duplicate_ml4:
        raise ValueError(
            "ML4 result contains "
            f"{duplicate_ml4:,} duplicate grid/timestamp rows."
        )

    combined = ml3.merge(
        ml4,
        on=[
            "grid_id",
            "timestamp",
        ],
        how="left",
        validate="one_to_one",
    )

    missing_ml4 = int(
        combined["anomaly_score"].isna().sum()
    )

    if missing_ml4:
        raise ValueError(
            "ML3/ML4 integration contains "
            f"{missing_ml4:,} ML3 rows without an ML4 result."
        )

    return combined


# =========================================================
# Prepare database rows
# =========================================================

def build_score_rows(
    dataframe: pd.DataFrame,
) -> list[dict]:
    """
    Prepare combined ML3 + ML4 results for
    network_risk_scores.
    """

    rows = []

    for row in dataframe.itertuples(
        index=False
    ):
        anomaly_score = (
            None
            if pd.isna(row.anomaly_score)
            else float(row.anomaly_score)
        )

        anomaly_direction = (
            None
            if pd.isna(row.anomaly_direction)
            else str(row.anomaly_direction)
        )

        anomaly_reason = (
            None
            if pd.isna(row.anomaly_reason)
            else str(row.anomaly_reason)
        )

        rows.append(
            {
                "grid_id": int(
                    row.grid_id
                ),

                "timestamp": row.timestamp,

                "risk_score": float(
                    row.risk_score
                ),

                "risk_level": str(
                    row.risk_level
                ),

                "model_version": str(
                    row.model_version
                ),

                "anomaly_score": anomaly_score,

                "anomaly_direction": anomaly_direction,

                "is_anomaly": int(
                    row.is_anomaly
                ),

                "anomaly_reason": anomaly_reason,
            }
        )

    return rows


# =========================================================
# Persist results
# =========================================================

def save_scores(
    rows: list[dict],
) -> int:
    """
    Atomically replace network_risk_scores with the
    newly calculated combined ML3 + ML4 dataset.

    If anything fails, the transaction is rolled back
    and the previous complete scoring dataset remains
    unchanged.
    """

    if not rows:
        raise RuntimeError(
            "No score rows to save."
        )

    db = SessionLocal()

    try:

        logging.info(
            "Publishing %d combined ML3 + ML4 "
            "risk scores atomically.",
            len(rows),
        )

        with db.begin():

            db.query(
                NetworkRiskScore
            ).delete(
                synchronize_session=False
            )

            for start in range(
                0,
                len(rows),
                BATCH_SIZE,
            ):

                batch = rows[
                    start:start + BATCH_SIZE
                ]

                db.bulk_insert_mappings(
                    NetworkRiskScore,
                    batch,
                )

                logging.info(
                    "Prepared %d/%d score rows.",
                    min(
                        start + BATCH_SIZE,
                        len(rows),
                    ),
                    len(rows),
                )

        logging.info(
            "Successfully published %d combined "
            "ML3 + ML4 risk scores.",
            len(rows),
        )

        return len(rows)

    except Exception:

        logging.exception(
            "Combined risk-score publication failed. "
            "Transaction rolled back; previous data "
            "remains unchanged."
        )

        db.rollback()

        raise

    finally:
        db.close()


# =========================================================
# Complete ML6 workflow
# =========================================================

def score_all_features() -> int:
    """
    Execute the complete ML6 scoring workflow.

    Workflow:

        load frozen ML3 artifacts
        -> load ML2 features
        -> validate ML2 features
        -> calculate ML3 probabilities
        -> calculate ML4 warehouse anomalies
        -> combine ML3 + ML4
        -> persist network_risk_scores
    """

    logger.info(
        "Starting ML6 combined ML3 + ML4 batch scoring."
    )

    # -----------------------------------------------------
    # Load frozen ML3 model
    # -----------------------------------------------------

    model, scaler = (
        load_model_artifacts()
    )

    # -----------------------------------------------------
    # Use one database session for source data
    # -----------------------------------------------------

    session = SessionLocal()

    try:

        # -------------------------------------------------
        # ML2
        # -------------------------------------------------

        logger.info(
            "Loading ML2 features from MySQL."
        )

        ml2_dataframe = load_features(
            session
        )

        logger.info(
            "Loaded %s ML2 feature rows.",
            f"{len(ml2_dataframe):,}",
        )

        validate_features(
            ml2_dataframe
        )

        # -------------------------------------------------
        # ML3
        # -------------------------------------------------

        logger.info(
            "Calculating ML3 risk scores."
        )

        ml3_dataframe = calculate_risk_scores(
            ml2_dataframe,
            model,
            scaler,
        )

        logger.info(
            "ML3 scoring produced %s rows.",
            f"{len(ml3_dataframe):,}",
        )

        # -------------------------------------------------
        # ML4
        # -------------------------------------------------

        ml4_dataframe = calculate_ml4_scores(
            session
        )

        logger.info(
            "ML4 scoring produced %s rows.",
            f"{len(ml4_dataframe):,}",
        )

    except Exception:

        session.rollback()

        raise

    finally:

        session.close()

    # -----------------------------------------------------
    # Combine ML3 + ML4
    # -----------------------------------------------------

    logger.info(
        "Combining ML3 predictions with ML4 anomalies."
    )

    combined = combine_ml3_ml4(
        ml3_dataframe,
        ml4_dataframe,
    )

    # -----------------------------------------------------
    # Prepare database rows
    # -----------------------------------------------------

    rows = build_score_rows(
        combined
    )

    # -----------------------------------------------------
    # Publish atomically
    # -----------------------------------------------------

    count = save_scores(
        rows
    )

    # -----------------------------------------------------
    # Summary
    # -----------------------------------------------------

    anomaly_count = int(
        combined["is_anomaly"].sum()
    )

    high_anomaly_count = int(
        (
            (combined["is_anomaly"] == 1)
            & (
                combined["anomaly_direction"]
                == "HIGH"
            )
        ).sum()
    )

    low_anomaly_count = int(
        (
            (combined["is_anomaly"] == 1)
            & (
                combined["anomaly_direction"]
                == "LOW"
            )
        ).sum()
    )

    high_risk_count = int(
        (
            combined["risk_level"]
            == "HIGH"
        ).sum()
    )

    low_risk_count = int(
        (
            combined["risk_level"]
            == "LOW"
        ).sum()
    )

    logger.info(
        "ML6 combined scoring completed."
    )

    print()
    print("=" * 60)
    print("ML6 COMBINED ML3 + ML4 SCORING COMPLETE")
    print("=" * 60)

    print(
        f"Rows published:       {count:,}"
    )

    print()
    print("ML3 prediction:")

    print(
        f"  HIGH risk:          {high_risk_count:,}"
    )

    print(
        f"  LOW risk:           {low_risk_count:,}"
    )

    print()
    print("ML4 anomaly:")

    print(
        f"  Anomalies:          {anomaly_count:,}"
    )

    print(
        f"  HIGH anomalies:     {high_anomaly_count:,}"
    )

    print(
        f"  LOW anomalies:      {low_anomaly_count:,}"
    )

    print()
    print(
        f"Model version:        {MODEL_VERSION}"
    )

    print(
        f"Risk threshold:       {RISK_THRESHOLD:.2f}"
    )

    print("=" * 60)

    return count


# =========================================================
# Logging
# =========================================================

def configure_logging() -> None:

    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(message)s"
        ),
    )


# =========================================================
# CLI
# =========================================================

if __name__ == "__main__":

    configure_logging()

    count = score_all_features()






# from __future__ import annotations

# import logging
# import os
# import sys
# from pathlib import Path

# import joblib
# import numpy as np
# import pandas as pd
# from sqlalchemy import select, delete


# # =========================================================
# # Project path
# # =========================================================

# PROJECT_ROOT = Path(
#     os.getenv(
#         "PROJECT_ROOT",
#         Path(__file__).resolve().parents[2],
#     )
# ).resolve()

# if str(PROJECT_ROOT) not in sys.path:
#     sys.path.insert(0, str(PROJECT_ROOT))


# # =========================================================
# # Existing project database/models
# # =========================================================

# # from phase4.api.db.database import SessionLocal
# # from phase4.api.db.models import (
# #     MLGridFeatures,
# #     NetworkRiskScore,
# # )
# from sqlalchemy import create_engine
# from sqlalchemy.orm import sessionmaker
# #from phase4.api.db.database import SessionLocal
# from phase4.api.db.models import (
#     MLGridFeatures,
#     NetworkRiskScore,
# )

# # =========================================================
# # ML6 WSL Database Configuration
# # =========================================================

# DATABASE_URL = os.getenv(
#     "ML6_DATABASE_URL",
# )

# if not DATABASE_URL:
#     raise RuntimeError(
#         "ML6_DATABASE_URL environment variable is required."
#     )

# engine = create_engine(
#     DATABASE_URL,
#     pool_pre_ping=True,
#     pool_recycle=3600,
#     future=True,
# )

# SessionLocal = sessionmaker(
#     bind=engine,
#     autoflush=False,
#     autocommit=False,
# )

# # =========================================================
# # Configuration
# # =========================================================

# FEATURE_COLUMNS = [
#     "avg_activity",
#     "activity_growth",
#     "active_hours",
#     "peak_ratio",
#     "variability",
#     "internet_share",
# ]

# MODEL_VERSION = "ml3-logistic-regression-v1"

# RISK_THRESHOLD = 0.5

# MODEL_PATH = (
#     PROJECT_ROOT
#     / "phase6"
#     / "ml"
#     / "artifacts"
#     / "logistic_regression.joblib"
# )

# SCALER_PATH = (
#     PROJECT_ROOT
#     / "phase6"
#     / "ml"
#     / "artifacts"
#     / "feature_scaler.joblib"
# )

# BATCH_SIZE = 5000


# logger = logging.getLogger(__name__)


# # =========================================================
# # Load frozen ML3 artifacts
# # =========================================================

# def load_model_artifacts():
#     """
#     Load the frozen ML3 Logistic Regression model
#     and StandardScaler.

#     ML6 never retrains or modifies these artifacts.
#     """

#     if not MODEL_PATH.exists():
#         raise FileNotFoundError(
#             f"ML3 model artifact does not exist: {MODEL_PATH}"
#         )

#     if not SCALER_PATH.exists():
#         raise FileNotFoundError(
#             f"ML3 scaler artifact does not exist: {SCALER_PATH}"
#         )

#     model = joblib.load(MODEL_PATH)
#     scaler = joblib.load(SCALER_PATH)

#     if not hasattr(model, "predict_proba"):
#         raise RuntimeError(
#             "Loaded ML3 model does not support predict_proba()."
#         )

#     if not hasattr(scaler, "transform"):
#         raise RuntimeError(
#             "Loaded ML3 scaler does not support transform()."
#         )

#     if getattr(model, "n_features_in_", None) != len(FEATURE_COLUMNS):
#         raise RuntimeError(
#             "ML3 model feature count does not match "
#             f"the expected {len(FEATURE_COLUMNS)} features."
#         )

#     if getattr(scaler, "n_features_in_", None) != len(FEATURE_COLUMNS):
#         raise RuntimeError(
#             "ML3 scaler feature count does not match "
#             f"the expected {len(FEATURE_COLUMNS)} features."
#         )

#     return model, scaler


# # =========================================================
# # Load ML2 features
# # =========================================================

# def load_features(session) -> pd.DataFrame:
#     """
#     Load persisted ML2 features from MySQL.

#     ML6 does not recalculate ML2 features.
#     """

#     statement = (
#         select(
#             MLGridFeatures.grid_id,
#             MLGridFeatures.feature_timestamp,
#             MLGridFeatures.avg_activity,
#             MLGridFeatures.activity_growth,
#             MLGridFeatures.active_hours,
#             MLGridFeatures.peak_ratio,
#             MLGridFeatures.variability,
#             MLGridFeatures.internet_share,
#         )
#         .order_by(
#             MLGridFeatures.feature_timestamp,
#             MLGridFeatures.grid_id,
#         )
#     )

#     rows = session.execute(statement).all()

#     if not rows:
#         raise RuntimeError(
#             "ML6 scoring cannot run because ml_grid_features "
#             "contains no rows."
#         )

#     dataframe = pd.DataFrame(
#         rows,
#         columns=[
#             "grid_id",
#             "feature_timestamp",
#             *FEATURE_COLUMNS,
#         ],
#     )

#     return dataframe


# # =========================================================
# # Validate ML2 features
# # =========================================================

# def validate_features(
#     dataframe: pd.DataFrame,
# ) -> None:

#     required_columns = {
#         "grid_id",
#         "feature_timestamp",
#         *FEATURE_COLUMNS,
#     }

#     missing = (
#         required_columns
#         - set(dataframe.columns)
#     )

#     if missing:
#         raise ValueError(
#             "ML2 feature data is missing required columns: "
#             f"{sorted(missing)}"
#         )

#     if dataframe["grid_id"].isna().any():
#         raise ValueError(
#             "ML2 feature data contains missing grid IDs."
#         )

#     if dataframe["feature_timestamp"].isna().any():
#         raise ValueError(
#             "ML2 feature data contains missing timestamps."
#         )

#     for column in FEATURE_COLUMNS:

#         dataframe[column] = pd.to_numeric(
#             dataframe[column],
#             errors="coerce",
#         )

#         if dataframe[column].isna().any():
#             raise ValueError(
#                 f"ML2 feature column '{column}' contains "
#                 "invalid or missing values."
#             )

#         values = dataframe[column].to_numpy()

#         if not np.isfinite(values).all():
#             raise ValueError(
#                 f"ML2 feature column '{column}' contains "
#                 "non-finite values."
#             )

#     duplicate_count = int(
#         dataframe.duplicated(
#             subset=[
#                 "grid_id",
#                 "feature_timestamp",
#             ]
#         ).sum()
#     )

#     if duplicate_count:
#         raise ValueError(
#             "ML2 feature data contains "
#             f"{duplicate_count:,} duplicate grid/timestamp rows."
#         )


# # =========================================================
# # ML3 risk scoring
# # =========================================================

# def calculate_risk_scores(
#     dataframe: pd.DataFrame,
#     model,
#     scaler,
# ) -> pd.DataFrame:
#     """
#     Calculate ML3 risk probabilities.

#     The six ML2 features describe data through t.

#     The frozen ML3 model predicts the probability that
#     high activity occurs at t+1.

#     This is the same model/scaler used by ML5.
#     """

#     result = dataframe.copy()

#     X = result[
#         FEATURE_COLUMNS
#     ]

#     # Use DataFrame column names exactly as the trained
#     # model expects.
#     X_scaled = scaler.transform(
#         X
#     )

#     result["risk_score"] = (
#         model.predict_proba(
#             X_scaled
#         )[:, 1]
#         .astype(float)
#     )

#     result["risk_level"] = np.where(
#         result["risk_score"] >= RISK_THRESHOLD,
#         "HIGH",
#         "LOW",
#     )

#     result["model_version"] = (
#         MODEL_VERSION
#     )

#     return result


# # =========================================================
# # Prepare database rows
# # =========================================================

# def build_score_rows(
#     dataframe: pd.DataFrame,
# ) -> list[dict]:
#     """
#     Prepare ML3-only results for network_risk_scores.

#     ML4 fields are intentionally left NULL at this stage.
#     """

#     rows = []

#     for row in dataframe.itertuples(
#         index=False
#     ):
#         rows.append(
#             {
#                 "grid_id": int(
#                     row.grid_id
#                 ),

#                 "timestamp": row.feature_timestamp,

#                 "risk_score": float(
#                     row.risk_score
#                 ),

#                 "risk_level": str(
#                     row.risk_level
#                 ),

#                 "model_version": str(
#                     row.model_version
#                 ),

#                 "anomaly_score": None,

#                 "anomaly_direction": None,

#                 "is_anomaly": 0,

#                 "anomaly_reason": None,
#             }
#         )

#     return rows


# # =========================================================
# # Persist results
# # =========================================================

# def save_scores(rows: list[dict]) -> int:
#     """
#     Atomically replace network_risk_scores with the newly calculated scores.

#     If anything fails, the transaction is rolled back and the previous
#     complete scoring dataset remains unchanged.
#     """
#     if not rows:
#         raise RuntimeError("No score rows to save.")

#     db = SessionLocal()

#     try:
#         logging.info(
#             "Publishing %d risk scores atomically to network_risk_scores.",
#             len(rows),
#         )

#         # Start an explicit transaction.
#         with db.begin():
#             # Remove the previous complete scoring dataset.
#             db.query(NetworkRiskScore).delete(
#                 synchronize_session=False
#             )

#             # Insert the new dataset in batches.
#             batch_size = 5000

#             for start in range(0, len(rows), batch_size):
#                 batch = rows[start:start + batch_size]

#                 db.bulk_insert_mappings(
#                     NetworkRiskScore,
#                     batch,
#                 )

#                 logging.info(
#                     "Prepared %d/%d score rows.",
#                     min(start + batch_size, len(rows)),
#                     len(rows),
#                 )

#         # Reaching this point means the transaction committed successfully.
#         logging.info(
#             "Successfully published %d risk scores.",
#             len(rows),
#         )

#         return len(rows)

#     except Exception:
#         logging.exception(
#             "Risk-score publication failed. "
#             "Transaction rolled back; previous data remains unchanged."
#         )

#         # `with db.begin()` normally handles the rollback automatically.
#         # This is defensive in case the exception occurred outside it.
#         db.rollback()

#         raise

#     finally:
#         db.close()

# # no atomicity guarantee, but this is a batch operation and
# # the table is rebuilt each time, so it's acceptable.
# # def save_scores(
# #     session,
# #     rows: list[dict],
# # ) -> int:
# #     """
# #     Publish the current full ML3 scoring batch.

# #     The scoring table is rebuilt from the current ML2
# #     feature table, matching the current ML2 rebuild model.
# #     """

# #     if not rows:
# #         raise RuntimeError(
# #             "No ML6 scoring rows were produced."
# #         )

# #     session.execute(
# #         delete(NetworkRiskScore)
# #     )

# #     session.commit()

# #     total = len(rows)

# #     for start in range(
# #         0,
# #         total,
# #         BATCH_SIZE,
# #     ):

# #         batch = rows[
# #             start:start + BATCH_SIZE
# #         ]

# #         session.bulk_insert_mappings(
# #             NetworkRiskScore,
# #             batch,
# #         )

# #         session.commit()

# #         inserted = min(
# #             start + BATCH_SIZE,
# #             total,
# #         )

# #         logger.info(
# #             "Saved %s / %s ML6 rows.",
# #             f"{inserted:,}",
# #             f"{total:,}",
# #         )

# #     return total


# # =========================================================
# # Complete ML3 batch workflow
# # =========================================================

# def score_all_features() -> int:
#     """
#     Execute ML6 ML3 batch scoring.

#     Workflow:

#         load frozen artifacts
#         -> load ML2 features
#         -> validate features
#         -> calculate ML3 probabilities
#         -> persist scores
#     """

#     logger.info(
#         "Starting ML6 ML3 batch scoring."
#     )

#     model, scaler = (
#         load_model_artifacts()
#     )

#     session = SessionLocal()

#     try:

#         logger.info(
#             "Loading ML2 features from MySQL."
#         )

#         dataframe = load_features(
#             session
#         )

#         logger.info(
#             "Loaded %s ML2 feature rows.",
#             f"{len(dataframe):,}",
#         )

#         validate_features(
#             dataframe
#         )

#         logger.info(
#             "Calculating ML3 risk scores."
#         )

#         dataframe = calculate_risk_scores(
#             dataframe,
#             model,
#             scaler,
#         )

#         rows = build_score_rows(
#             dataframe
#         )

#         count = save_scores(
#             rows,
#         )

#         logger.info(
#             "ML6 ML3 batch scoring completed. "
#             "Published %s rows.",
#             f"{count:,}",
#         )

#         return count

#     except Exception:
#         session.rollback()
#         raise

#     finally:
#         session.close()


# # =========================================================
# # Logging
# # =========================================================

# def configure_logging() -> None:

#     logging.basicConfig(
#         level=logging.INFO,
#         format=(
#             "%(asctime)s | "
#             "%(levelname)s | "
#             "%(message)s"
#         ),
#     )


# # =========================================================
# # CLI
# # =========================================================

# if __name__ == "__main__":

#     configure_logging()

#     count = score_all_features()

#     print()
#     print("=" * 60)
#     print("ML6 ML3 BATCH SCORING COMPLETE")
#     print("=" * 60)

#     print(
#         f"Rows published: {count:,}"
#     )

#     print(
#         f"Model version:  {MODEL_VERSION}"
#     )

#     print(
#         f"Risk threshold: {RISK_THRESHOLD:.2f}"
#     )

#     print("=" * 60)
from pathlib import Path

import joblib
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent

load_dotenv(PROJECT_ROOT / ".env")

MODEL_PATH = PROJECT_ROOT / "phase6" / "ml" / "artifacts" / "logistic_regression.joblib"
SCALER_PATH = PROJECT_ROOT / "phase6" / "ml" / "artifacts" / "feature_scaler.joblib"

# ---------------------------------------------------------
# Database
# ---------------------------------------------------------
# Use the same DATABASE_URL from your existing API config.
DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/telecom_analytics"

engine = create_engine(DATABASE_URL)

# ---------------------------------------------------------
# Load existing ML3 artifacts
# ---------------------------------------------------------
model = joblib.load(MODEL_PATH)
scaler = joblib.load(SCALER_PATH)

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]

# ---------------------------------------------------------
# Get ML2 features
# ---------------------------------------------------------
query = text("""
    SELECT
        grid_id,
        feature_timestamp,
        avg_activity,
        activity_growth,
        active_hours,
        peak_ratio,
        variability,
        internet_share
    FROM ml_grid_features
    ORDER BY feature_timestamp
""")

df = pd.read_sql(query, engine)

# ---------------------------------------------------------
# Score every row using the EXISTING ML3 model
# ---------------------------------------------------------
X = df[FEATURE_COLUMNS]

X_scaled = scaler.transform(X)

df["risk_score"] = model.predict_proba(X_scaled)[:, 1]

df["risk_level"] = df["risk_score"].apply(
    lambda x: "HIGH" if x >= 0.5 else "LOW"
)

# ---------------------------------------------------------
# Find examples near useful probability levels
# ---------------------------------------------------------
targets = [0.10, 0.20, 0.30, 0.40, 0.60, 0.70, 0.80, 0.90]

print("\nExamples of different ML3 probability values:\n")

for target in targets:
    row = df.iloc[(df["risk_score"] - target).abs().argsort()[:1]].iloc[0]

    print(
        f"Target ~{target:.2f} | "
        f"Grid {int(row.grid_id)} | "
        f"{row.feature_timestamp} | "
        f"Score {row.risk_score:.6f} | "
        f"{row.risk_level}"
    )
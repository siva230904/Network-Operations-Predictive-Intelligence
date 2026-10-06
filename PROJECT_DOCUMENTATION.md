# Network Operations Predictive Intelligence Platform
## Project Implementation & Technical Architecture Documentation

---

## Table of Contents

1. Executive Overview
2. System Architecture
3. Repository Structure
4. Dataset & Data Contract
5. Development Environment & Setup
6. Phase 1: Core Python Implementation
7. Phase 2: PySpark ETL Pipeline
8. Phase 3: Data Engineering & Airflow Orchestration
9. Phase 4: FastAPI Backend
10. Phase 5: React NOC Dashboard
11. Phase 6: Machine Learning Pipeline
12. Phase 7: Claude/AI-Assisted Operations (Status)
13. End-to-End Integration
14. API & Data Interface Reference
15. Configuration & Environment Variables
16. Testing Strategy
17. Security Considerations
18. Deployment & Operations
19. Troubleshooting Guide
20. Implementation Checklist
21. Implementation Gaps & Recommendations
22. Final Capstone: Network Operations Control Room

---

## 1. Executive Overview

### Project Purpose
The Network Operations Predictive Intelligence Platform is an AI-powered system designed to provide telecom network operators with real-time visibility, historical analytics, and machine learning-based predictive insights for network activity monitoring and anomaly detection across a geographical grid (Milan grid reference).

### Problem Being Solved
- **Real-time network visibility**: Operators lack comprehensive, granular visibility into network activity patterns
- **Anomaly detection**: Manual identification of unusual network behavior is time-consuming and error-prone
- **Predictive capability**: No forward-looking intelligence to anticipate high-activity periods and potential issues
- **Data accessibility**: Raw network data requires significant processing before operational usefulness

### Major Capabilities

| Capability | Status | Implementation |
|-----------|--------|-----------------|
| Dataset ingestion & profiling | Implemented | Phase 1 (NP1/NP2) |
| Rule-based alert generation | Implemented | Phase 1 (NP3) |
| Distributed ETL processing | Implemented | Phase 2 (SP1-SP7) |
| Data warehousing | Implemented | Phase 3 (DE2/DE6) |
| Airflow orchestration | Implemented | Phase 3 (Airflow DAGs) |
| REST API endpoints | Implemented | Phase 4 |
| React dashboard | Implemented | Phase 5 |
| ML feature engineering | Implemented | Phase 6 (ML2/ML4) |
| ML model training & inference | Implemented | Phase 6 (ML3) |
| Batch risk scoring | Implemented | Phase 6 (ML6) |
| Claude AI integration | Planned | Phase 7 |

### Technology Stack

**Backend**
- Python 3.x, PySpark 3.x
- FastAPI, SQLAlchemy, Uvicorn
- Apache Airflow for orchestration
- MySQL/Database backend

**Frontend**
- React 19.2.8, Vite
- Leaflet for mapping, Recharts for visualization
- React Router for navigation

**ML/Data**
- scikit-learn (Logistic Regression)
- Pandas, NumPy for data processing
- Joblib for model serialization

**Deployment**
- Docker (configured in project)
- Local/WSL execution

### Current Implementation Status
- **Phases 1-6**: Core functionality complete and tested
- **Phase 7**: Not yet implemented (Claude/LLM integration planned)
- **Data**: 4.1GB of processed telecom network activity data available
- **Models**: Trained ML3 logistic regression model with scaler artifacts
- **API**: Fully functional with 4 router groups (network, hotspot, features, prediction)

---

## 2. System Architecture

### High-Level Data Flow

```
Telecom Network
       ↓
   [Raw Data]
       ↓
   NP1/NP2 (Dataset Profiling)
       ↓
   [Landing Zone]
       ↓
   SP1-SP7 (Spark ETL)
   ├─ Cleaning (SP2)
   ├─ Aggregation (SP3)
   ├─ Enrichment (SP4)
   ├─ Performance (SP5)
   └─ Storage (SP6)
       ↓
   [Processed Zone]
       ↓
   DE2 Ingestion → Airflow Orchestration (DE7)
       ↓
   [Analytics Warehouse]
       ├─ Fact Tables
       ├─ Dimension Tables
       └─ ML Feature Tables
       ↓
   ┌───────────────────┬──────────────────┐
   ↓                   ↓                  ↓
 FastAPI          ML Pipeline       Dashboard
  (API4)          (ML2-ML6)         (Phase 5)
   ↓                   ↓                  ↓
REST Endpoints    ML Features &    React UI
                  Predictions      (Maps, Charts)
   ↓                   ↓                  ↓
Network Summary   Risk Scores      NOC Operator
Grid Activity     Anomalies        Insights
Hotspot Alerts    Classifications
```

### Component Integration Map

| Layer | Components | Purpose |
|-------|-----------|---------|
| **Data Source** | Raw telecom CSV files | SMS, calls, internet activity per grid/hour |
| **Ingestion** | NP1, NP2, SP1 | Profile, validate, and bulk-load raw data |
| **Processing** | SP2-SP6 | Clean, aggregate, enrich, and standardize |
| **Orchestration** | Airflow (DE7) | Schedule and coordinate pipeline execution |
| **Warehouse** | MySQL with dimensions/facts | Structured storage for analytics |
| **ML** | Feature engineering, training, scoring | Predictive risk assessment |
| **API** | FastAPI routers | Expose data and predictions |
| **Frontend** | React components | Visualize network state and alerts |

---

## 3. Repository Structure

```
D:\Project1/
├── phase1/                      # Core Python (NP1-NP3)
│   ├── np1/                    # Dataset profiling
│   ├── np2/                    # Usage processing
│   │   ├── usage_processor.py  # Main processor class
│   │   └── np2_run_test.py    # Integration test
│   └── np3/                    # Alert generation
│       ├── alert_detector.py   # NetworkAlertGenerator class
│       └── np3_run_test.py    # Integration test
│
├── phase2/                      # PySpark ETL (SP1-SP7)
│   ├── sp1/                    # Distributed ingestion
│   │   ├── ingestion.py
│   │   └── sp1_run_test.py
│   ├── sp2/                    # Data cleaning
│   │   ├── cleaning.py, cleaning2.py
│   │   └── sp2_run_test.py
│   ├── sp3/                    # Grid/hour aggregation
│   │   ├── aggregation.py, aggregation2.py, aggregation3.py
│   │   └── sp3_run_test.py, sp3_run_test2.py, sp3_run_test3.py
│   ├── sp4/                    # Milan grid enrichment
│   │   ├── enrichment.py, enrichment2.py, enrichment3.py
│   │   └── sp4_run_test.py, sp4_run_test2.py, sp4_run_test3.py
│   ├── sp5/                    # Performance behavior
│   │   ├── performance.py
│   │   └── sp5_run_test.py
│   ├── sp6/                    # Warehouse output
│   │   ├── storage.py
│   │   └── sp6_run_test.py
│   └── spark/                  # Reusable production pipeline
│       ├── telecom_pipeline.py # SP7 main ETL job
│       └── run_telecom_pipeline.py # Entry point
│
├── phase3/                      # Data Engineering & Airflow
│   ├── airflow/
│   │   ├── de7_end_to_end_dag.py # Main orchestration DAG
│   │   └── de3_spark_processing_dag.py
│   ├── de2/                    # Ingestion
│   │   ├── ingestion.py
│   │   ├── config.py
│   │   └── tests/test_ingestion.py
│   ├── de4/                    # Data architecture planning
│   ├── de5/                    # Landing/processed zone
│   ├── de6/                    # Warehouse & analytics
│   │   ├── warehouse_loader.py
│   │   ├── validate_warehouse.py
│   │   ├── checksparkconf.py
│   │   └── run_de6.py
│   └── de1/                    # Initial planning
│
├── phase4/                      # FastAPI Backend
│   └── api/
│       ├── main.py             # FastAPI application
│       ├── config.py           # Configuration
│       ├── db/
│       │   ├── database.py     # SQLAlchemy setup
│       │   └── models.py       # ORM models
│       ├── models/
│       │   └── network.py      # Request/response schemas
│       ├── routers/
│       │   ├── network.py      # API1, API2 endpoints
│       │   ├── hotspot_alert.py # Alert endpoint
│       │   ├── features.py     # ML2 features endpoint
│       │   └── prediction.py   # ML3 prediction endpoint
│       ├── services/
│       │   ├── network_service.py
│       │   ├── hotspot_alert_service.py
│       │   ├── feature_service.py
│       │   └── prediction_service.py
│       └── tests/
│           ├── test_network_summary.py
│           ├── test_hotspot_alert.py
│           └── test_prediction.py
│
├── phase5/                      # React Dashboard
│   ├── src/
│   │   ├── App.jsx
│   │   ├── main.jsx
│   │   ├── pages/
│   │   │   ├── Dashboard.jsx
│   │   │   ├── GridExplorer.jsx
│   │   │   ├── HotspotsAlerts.jsx
│   │   │   └── PredictiveRisk.jsx
│   │   ├── components/
│   │   │   ├── MilanGridMap.jsx
│   │   │   ├── Navbar.jsx
│   │   │   ├── LoadingState.jsx
│   │   │   └── ErrorState.jsx
│   │   └── api/
│   │       └── client.js
│   ├── package.json
│   ├── vite.config.js
│   └── index.html
│
├── phase6/                      # Machine Learning
│   ├── ml/
│   │   ├── features.py         # ML2 feature definitions
│   │   ├── feature_generator.py # ML2 feature calculation
│   │   ├── train_model.py      # ML3 model training
│   │   ├── batch_scorer.py     # ML6 batch prediction
│   │   ├── warehouse_anomaly_scorer.py # ML4 anomaly detection
│   │   ├── test_features.py
│   │   ├── test_anomaly_scorer.py
│   │   └── artifacts/
│   │       ├── logistic_regression.joblib # Trained model
│   │       ├── feature_scaler.joblib # Feature scaler
│   │       ├── ml3_evaluation.json # Training metrics
│   │       └── ml3_np3_comparison.csv # Model comparison
│   └── ML1plan.md
│
├── data/
│   ├── raw/                    # Raw daily CSV files
│   ├── landing/                # Landing zone (Spark outputs)
│   │   ├── sp2/                # Cleaned data
│   │   ├── sp3/                # Aggregated data
│   │   ├── sp4/                # Enriched data
│   │   └── sp5/                # Performance data
│   ├── processed/              # Processed zone
│   │   └── activity/
│   ├── analytics/              # Analytics zone
│   │   ├── hourly_grid_summary/
│   │   ├── daily_traffic_summary/
│   │   └── hotspot_ranking/
│   ├── sp7/                    # SP7 pipeline outputs
│   ├── reference/              # Static reference data
│   │   └── milano-grid.geojson # Milan grid geometry
│   ├── logs/
│   │   ├── pipeline_status/   # Airflow execution logs
│   │   └── de7_end_to_end_*.log
│   └── rejected/               # Data quality rejections
│
├── .github/
│   └── agents/                 # GitHub Actions agents config
│
├── requirements.txt            # Python dependencies
├── package.json               # Node.js dependencies (project-wide)
├── package-lock.json
└── Phase 1 Project - Network Operations Predictive Intelligence - Trainer Guide 1.pdf
```

---

## 4. Dataset & Data Contract

### Data Source

**Raw Input Format**
- Daily CSV files in `data/raw/`
- Filename pattern: `*.csv`
- Delivery: One file per day with hourly activity records

**Raw Dataset Structure**

| Column | Type | Description | Constraints |
|--------|------|-------------|-------------|
| grid_id | int | Grid identifier (Milan grid cell) | 1-10000 |
| timestamp | datetime | Hour-level granularity | ISO 8601 UTC |
| sms_in | float | Inbound SMS activity | >= 0 |
| sms_out | float | Outbound SMS activity | >= 0 |
| call_in | float | Inbound call activity | >= 0 |
| call_out | float | Outbound call activity | >= 0 |
| internet_activity | float | Data/internet usage | >= 0 |
| country_code | string | Geographic identifier | 2-char code |

### Data Processing Pipeline

**Transformation Sequence**
```
Raw (sms_in, sms_out, call_in, call_out, internet_activity)
    ↓
SP2 Cleaning:
    - Remove duplicates
    - Validate numeric ranges
    - Handle missing values
    - Normalize timestamp format
    ↓
SP3 Aggregation:
    - Country-code aggregation
    - Grid/hour totals
    - Calculate totals: total_sms, total_calls, total_activity
    ↓
SP4 Enrichment:
    - Join Milano grid geometry
    - Add latitude/longitude
    - Spatial validation
    ↓
SP5 Performance:
    - Verify processing metrics
    - Check output quality
    ↓
SP6 Storage:
    - Write to warehouse staging
    ↓
DE6 Warehouse Load:
    - Populate dimension tables (dim_time, dim_grid)
    - Populate fact table (fact_network_activity)
    - Create indexes
```

### Telecom-Specific Rules

**Data Quality Rules** (phase1/np3/alert_detector.py:199-322)
- Timestamps must be valid and non-null
- Grid IDs must be in range [1, 10000]
- No duplicate (timestamp, grid_id) pairs
- Total activity cannot be negative
- Total activity is: total_sms + total_calls + internet_activity

**Activity Floor Calculation** (phase1/np3/alert_detector.py:328-365)
- Default: 10th percentile of daily grid totals
- Configurable per run
- Used to filter noise in alert generation

**Geographic Reference**
- Static Milan grid: `data/reference/milano-grid.geojson`
- GeoJSON format with grid cell boundaries
- Used by SP4 enrichment for spatial coordinates
- 10,000 grid cells covering Milan metropolitan area

### Data Warehouse Schema

**Dimensional Model**

`dim_time`
- time_key (BigInteger, PK)
- timestamp (DateTime)
- date (Date)
- hour (SmallInteger, 0-23)
- day_of_week (SmallInteger, 1-7)

`dim_grid`
- grid_key (Integer, PK)
- grid_id (Integer)
- latitude (Float, nullable)
- longitude (Float, nullable)
- geometry_reference (Text, nullable)

**Fact Tables**

`fact_network_activity`
- time_key (BigInteger, PK)
- grid_key (Integer, PK)
- sms_in (Float)
- sms_out (Float)
- call_in (Float)
- call_out (Float)
- internet_activity (Float)
- total_sms (Float)
- total_calls (Float)
- total_activity (Float)

`ml_grid_features` (Phase 6 output)
- grid_id (Integer, PK)
- feature_timestamp (DateTime, PK)
- avg_activity (Float)
- activity_growth (Float)
- active_hours (Integer)
- peak_ratio (Float)
- variability (Float)
- internet_share (Float)

`network_risk_score` (Phase 6 batch output)
- grid_id (Integer, PK)
- risk_timestamp (DateTime, PK)
- risk_score (Float)
- risk_level (String: HIGH/LOW)
- model_version (String)

### Data Quality Expectations

**Volume**
- ~240 records per day (10,000 grids × 24 hours, partial coverage)
- Approximately 86,400 grid-hour records per full day
- ~4.1GB cumulative data directory

**Coverage**
- Daily files expected in `data/raw/`
- Backfill support through Spark ingestion (SP1)
- Incremental processing via Airflow

**Time Zones**
- All timestamps in UTC
- Stored as datetime without timezone info
- Interpreted as UTC by all components

---

## 5. Development Environment & Setup

### Prerequisites

**System Requirements**
- Windows 11 Enterprise (or Linux/Mac with WSL)
- 16GB RAM minimum (for local Spark)
- 100GB free disk space (data directory grows)

**Software Requirements**

| Component | Version | Purpose |
|-----------|---------|---------|
| Python | 3.8+ | Core data processing |
| Apache Spark | 3.x | Distributed ETL |
| Node.js | 18.x+ | React development |
| MySQL | 5.7+ or 8.0+ | Warehouse database |
| Apache Airflow | 2.x | Orchestration |
| Git | Latest | Version control |

### Python Environment Setup

**Create Virtual Environment**
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows
```

**Install Backend Dependencies**
```bash
pip install -r requirements.txt
```

**requirements.txt Contents**
```
fastapi
uvicorn
sqlalchemy
mysql-connector-python
python-dotenv
pydantic
pytest
httpx
joblib
pandas
numpy
scikit-learn
pyspark
apache-airflow
```

### Node.js / React Setup

**Install Dependencies**
```bash
cd phase5
npm install
```

**package.json Key Dependencies**
- react@19.2.8
- react-dom@19.2.8
- react-router-dom@7.18.3
- leaflet@1.9.4
- react-leaflet@5.0.0
- recharts@3.10.1
- vite@8.2.2

### Database Setup

**MySQL Configuration**
```bash
# Create database and user
mysql -u root -p
CREATE DATABASE telecom_intelligence;
CREATE USER 'telcom_user'@'localhost' IDENTIFIED BY 'password';
GRANT ALL PRIVILEGES ON telecom_intelligence.* TO 'telcom_user'@'localhost';
FLUSH PRIVILEGES;
```

**Connection String**
```
mysql+pymysql://telcom_user:password@localhost/telecom_intelligence
```

### Environment Variables

**Create `.env` file in phase5/ (React)**
```
VITE_API_BASE_URL=http://localhost:8000
```

**Create `.env` in phase4/api/ (FastAPI)**
```
DATABASE_URL=mysql+pymysql://user:pass@localhost/telecom_intelligence
PYTHONPATH=/path/to/Project1
```

**Create `.env` for Airflow/Spark**
```
PROJECT_ROOT=/path/to/Project1
SPARK_HOME=/path/to/spark
AIRFLOW_HOME=/path/to/airflow
ML6_DATABASE_URL=mysql+pymysql://user:pass@localhost/telecom_intelligence
```

### Spark Configuration

**Local Spark Setup**
```bash
# Download Spark 3.x
# Extract to a location, e.g., /opt/spark or C:\spark

# Set environment variable
export SPARK_HOME=/opt/spark
export PATH=$SPARK_HOME/bin:$PATH

# Verify
spark-shell --version
```

**PySpark Python Integration**
```bash
pip install pyspark==3.5.0
```

### Airflow Setup (Optional)

**Initialize Airflow**
```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# Copy DAGs
cp phase3/airflow/*.py ~/airflow/dags/
```

### Startup Sequence

**1. Start MySQL**
```bash
mysql -u root -p  # or mysql.server start (Mac) / net start MySQL (Windows)
```

**2. Initialize Warehouse (One-time)**
```bash
python phase3/de6/run_de6.py
```

**3. Run Data Pipeline (Manual or Airflow)**
```bash
# Manual execution
python phase2/spark/run_telecom_pipeline.py \
  --input data/raw \
  --output data \
  --reference data/reference/milano-grid.geojson \
  --log-dir data/logs

# Or via Airflow
airflow dags trigger de7_end_to_end_dag
```

**4. Generate ML Features**
```bash
python phase6/ml/feature_generator.py
```

**5. Train ML Model**
```bash
python phase6/ml/train_model.py
```

**6. Start FastAPI**
```bash
cd phase4
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

**7. Start React Dashboard (New Terminal)**
```bash
cd phase5
npm run dev
```

Dashboard will be available at `http://localhost:5173`

---

## 6. Phase 1: Core Python Implementation

### Objectives
- Profile raw telecom dataset
- Process usage metrics
- Generate rule-based network activity alerts
- Validate data quality

### NP1: Dataset Profiling

**Purpose**: Initial exploration and profiling of raw telecom data

**Implementation Files**
- `phase1/np1/` (directory - specific implementation not detailed in provided code)

**Outputs**
- Data summary statistics
- Column type inference
- Missing value analysis
- Distribution analysis

### NP2: Usage Processing

**File**: `phase1/np2/usage_processor.py`

**Main Class**: `UsageProcessor`

**Key Methods**
- `__init__()` - Initialize with file path or DataFrame
- `load_data()` - Read CSV into memory
- `validate_input()` - Check schema and types
- `process_usage()` - Transform raw metrics
- `aggregate_by_grid()` - Group by grid_id
- `export_results()` - Write to output location

**Processing Logic**
1. Load raw activity records (sms_in, sms_out, call_in, call_out, internet_activity)
2. Validate numeric ranges and timestamps
3. Calculate aggregates: total_sms, total_calls, total_activity
4. Group by grid/hour
5. Export grid/hour analytics

**Input**
- CSV with columns: grid_id, timestamp, sms_in, sms_out, call_in, call_out, internet_activity

**Output**
- CSV in `data/processed/` with columns: timestamp, grid_id, total_activity (and derived metrics)

**Example Usage**
```python
from phase1.np2.usage_processor import UsageProcessor

processor = UsageProcessor(
    file_path="data/raw/daily_activities.csv"
)
processor.load_data()
processor.validate_input()
processor.aggregate_by_grid()
processor.export_results(
    output_dir="data/processed",
    filename="grid_hourly_summary.csv"
)
```

**Test**: `phase1/np2/np2_run_test.py`

### NP3: Alert Generation

**File**: `phase1/np3/alert_detector.py`

**Main Class**: `NetworkAlertGenerator`

**Purpose**: Generate transparent, explainable rule-based alerts for anomalous network activity

**Alert Types**

1. **HIGH_ACTIVITY**
   - Condition: `current_activity >= baseline_activity * high_activity_ratio`
   - Default ratio: 2.0x
   - Interpretation: Current hour is 2x or more above baseline

2. **ACTIVITY_DROP**
   - Condition: `current_activity <= baseline_activity * drop_ratio`
   - Default ratio: 0.5x
   - Interpretation: Current hour is 50% or less of baseline

3. **ACTIVITY_SPIKE**
   - Condition: `current_activity >= previous_activity * spike_ratio`
   - Default ratio: 1.5x
   - Requires: `previous_activity > 0`
   - Interpretation: Hour-over-hour jump of 1.5x or more

**Baseline Calculation** (line 371-652)

Uses leave-one-out median strategy:
- For each grid, calculate within-day median of total_activity
- Current observation is **excluded** from its own baseline
- Vectorized using pandas rank() for efficiency

**Configurable Parameters**
- `high_activity_ratio` (default: 2.0)
- `spike_ratio` (default: 1.5)
- `drop_ratio` (default: 0.5)
- `activity_floor` (default: 10th percentile of daily totals)
- `log_dir` (default: "data/logs")

**Pipeline Execution** (method: `process()`)

```
load_data()
    ↓
validate_input()
    ↓
determine_activity_floor()
    ↓
build_baseline()
    ↓
generate_alerts()
    ↓
create_summary()
    ↓
export_alerts()
    ↓
print_summary()
```

**Example Usage**
```python
from phase1.np3.alert_detector import NetworkAlertGenerator

detector = NetworkAlertGenerator(
    file_path="data/processed/grid_hourly.csv",
    high_activity_ratio=2.0,
    spike_ratio=1.5,
    drop_ratio=0.5,
    activity_floor=None,  # Auto-calculated
    log_dir="data/logs"
)

result = detector.process(
    output_dir="data/processed",
    filename="network_alerts.csv"
)

# result contains:
# {
#     "alert_path": Path to CSV,
#     "log_path": Path to log file,
#     "summary": {
#         "total_grid_hours": N,
#         "total_alerts": M,
#         "alert_proportion": M/N,
#         "alerts_by_type": {"HIGH_ACTIVITY": n1, "ACTIVITY_DROP": n2, ...},
#         "top_ten_grids": {grid_id: count, ...}
#     }
# }
```

**Output CSV Columns**
- grid_id
- timestamp
- alert_type (HIGH_ACTIVITY | ACTIVITY_DROP | ACTIVITY_SPIKE)
- current_activity (total_activity value)
- baseline_activity (median baseline)
- reason (Human-readable explanation)

**Logging**
- File: `data/logs/{input_stem}_alerts_{timestamp}.log`
- Level: INFO
- Tracks: data loading, validation, calculations, alerts generated

**Test**: `phase1/np3/np3_run_test.py`

### Phase 1 Completion Status
✅ Implemented and tested
- NP1: Dataset profiling
- NP2: Usage processing
- NP3: Alert generation with configurable rules

---

## 7. Phase 2: PySpark ETL Pipeline

### Objectives
- Implement distributed, scalable data cleaning and transformation
- Aggregate network activity to grid/hour level
- Enrich with geographic reference data
- Validate and assess data quality
- Create reusable, production-grade ETL jobs

### SP1: Distributed Ingestion

**File**: `phase2/sp1/ingestion.py`

**Purpose**: Read raw CSV files at scale using Spark

**Key Functions/Classes**
- Read daily CSV files from `data/raw/`
- Schema inference or explicit schema definition
- Handle missing/corrupted files gracefully
- Partition by date if available

**Output**: Spark DataFrame with raw schema

### SP2: Data Cleaning

**Files**: 
- `phase2/sp2/cleaning.py`
- `phase2/sp2/cleaning2.py` (Latest version)

**Main Class**: `NetworkCleaner`

**Transformations**
1. Remove duplicate rows (by grid_id, timestamp, country_code)
2. Validate numeric ranges
3. Coerce timestamp to proper datetime
4. Remove rows with missing critical fields
5. Handle null/zero values consistently

**Input**: Raw Spark DataFrame

**Output**: Cleaned DataFrame ready for aggregation

### SP3: Grid/Hour Aggregation

**Files**:
- `phase2/sp3/aggregation.py`
- `phase2/sp3/aggregation2.py`
- `phase2/sp3/aggregation3.py` (Latest version)

**Main Class**: `NetworkAggregator`

**Key Transformations** (aggregation3.py)

```python
aggregated = raw_data
    .groupBy("timestamp", "grid_id")
    .agg(
        F.sum("sms_in").alias("sms_in"),
        F.sum("sms_out").alias("sms_out"),
        F.sum("call_in").alias("call_in"),
        F.sum("call_out").alias("call_out"),
        F.sum("internet_activity").alias("internet_activity")
    )
    .withColumn(
        "total_sms",
        F.col("sms_in") + F.col("sms_out")
    )
    .withColumn(
        "total_calls",
        F.col("call_in") + F.col("call_out")
    )
    .withColumn(
        "total_activity",
        F.col("total_sms") + F.col("total_calls") + F.col("internet_activity")
    )
```

**Country-Code Aggregation**
- Removes country_code dimension (SP2 retains it, SP3 aggregates)
- Final grain: timestamp + grid_id
- One record per grid per hour

**Output**: Grid/hour aggregated data

### SP4: Milan Grid Enrichment

**Files**:
- `phase2/sp4/enrichment.py`
- `phase2/sp4/enrichment2.py`
- `phase2/sp4/enrichment3.py` (Latest version)

**Main Class**: `NetworkGeoEnricher`

**Key Enrichments**
1. Load Milano grid GeoJSON from `data/reference/milano-grid.geojson`
2. Parse geometry for each grid_id
3. Extract centroid coordinates (latitude, longitude)
4. Join with aggregated activity data
5. Validate spatial relationships

**Geographic Reference**
- Source: `data/reference/milano-grid.geojson`
- Format: GeoJSON with grid_id as identifier
- Coverage: ~10,000 grid cells across Milan
- Used for: UI mapping, spatial validation

**Output**: Enriched DataFrame with coordinates

### SP5: Performance Analysis

**File**: `phase2/sp5/performance.py`

**Key Metrics**
- Data volume processed
- Processing time
- Memory usage
- Partition efficiency
- Data skew analysis

**Output**: Performance report (JSON/CSV)

### SP6: Warehouse Output

**File**: `phase2/sp6/storage.py`

**Key Functions**
- Write cleaned/aggregated/enriched data to landing zone
- Create staging tables for warehouse load
- Format for optimal warehouse ingestion
- Manage checkpoints and idempotency

**Output Locations**
- `data/landing/sp2/` - Cleaned data
- `data/landing/sp3/` - Aggregated data
- `data/landing/sp4/` - Enriched data
- `data/landing/sp5/` - Performance data

### SP7: Reusable Spark Pipeline

**Files**:
- `phase2/spark/telecom_pipeline.py` (Main ETL job)
- `phase2/spark/run_telecom_pipeline.py` (Entry point)

**Purpose**: Production-grade, reusable ETL job that orchestrates SP1-SP6

**Class**: `TelecomPipeline`

**Constructor Parameters**
```python
TelecomPipeline(
    spark,                    # SparkSession
    input_path,              # data/raw
    output_path,             # data
    reference_path,          # milano-grid.geojson
    log_dir                  # data/logs
)
```

**Key Methods**
- `read_raw()` - Call SP1 ingestion
- `clean()` - Call SP2 NetworkCleaner
- `aggregate()` - Call SP3 NetworkAggregator
- `enrich()` - Call SP4 NetworkGeoEnricher
- `write_outputs()` - Call SP6 storage
- `execute()` - Run complete pipeline

**Execution Flow**
```
raw daily CSV files
        ↓
    read_raw()
        ↓
    clean()
        ↓
    aggregate()
        ↓
    enrich()
        ↓
    write_outputs()
        ↓
Landing/processed zone files
```

**Entry Point Usage**
```bash
python phase2/spark/run_telecom_pipeline.py \
  --input data/raw \
  --output data \
  --reference data/reference/milano-grid.geojson \
  --log-dir data/logs
```

**Configuration**
- INPUT_PATH: Where daily CSV files are located
- OUTPUT_PATH: Where processed data will be written
- REFERENCE_PATH: Path to milano-grid.geojson
- LOG_DIR: Where Spark logs are written

**Tests**
- `phase2/sp1/sp1_run_test.py`
- `phase2/sp2/sp2_run_test.py`
- `phase2/sp3/sp3_run_test.py` (including sp3_run_test2.py, sp3_run_test3.py)
- `phase2/sp4/sp4_run_test.py` (including sp4_run_test2.py, sp4_run_test3.py)
- `phase2/sp5/sp5_run_test.py`
- `phase2/sp6/sp6_run_test.py`

### Phase 2 Completion Status
✅ Implemented and tested
- SP1-SP6: Individual transformation stages
- SP7: Production pipeline
- 4.1GB processed data available in data/landing/

---

## 8. Phase 3: Data Engineering & Airflow Orchestration

### Objectives
- Define data architecture (landing, raw, processed, analytics zones)
- Create MySQL warehouse schema
- Implement Airflow DAGs for scheduling and orchestration
- Ensure data quality and idempotency
- Monitor pipeline execution

### Data Architecture

**Zone Structure**

| Zone | Location | Purpose | Retention |
|------|----------|---------|-----------|
| Landing | `data/landing/` | Spark ETL outputs (SP2-SP6 results) | 30 days |
| Raw | `data/raw/` | Original CSV files | 90 days |
| Processed | `data/processed/` | Cleaned/aggregated data ready for analytics | 365 days |
| Analytics | `data/analytics/` | Business-layer summaries (hourly, daily) | 2 years |
| Reference | `data/reference/` | Static data (milano-grid.geojson) | Indefinite |

### DE2: Ingestion Pipeline

**File**: `phase3/de2/ingestion.py`

**Purpose**: Load raw CSV files into staging area

**Key Functions**
- Discover daily files in `data/raw/`
- Validate file format and schema
- Register with Airflow metadata
- Support backfill operations

**Configuration**: `phase3/de2/config.py`

**Test**: `phase3/de2/tests/test_ingestion.py`

### DE6: Warehouse & Analytics

**Files**:
- `phase3/de6/warehouse_loader.py` - Load fact/dimension tables
- `phase3/de6/validate_warehouse.py` - Data quality checks
- `phase3/de6/checksparkconf.py` - Spark configuration validation
- `phase3/de6/run_de6.py` - Entry point

**Warehouse Schema** (See Section 4: Database Models)

**Key Operations**
1. Populate `dim_time` table (one row per hour of data)
2. Populate `dim_grid` table (10,000 grid cells with coordinates)
3. Populate `fact_network_activity` table (one row per grid/hour)
4. Create indexes on time_key and grid_key for query performance

**Validation Checks**
- Referential integrity (fact rows reference valid dimensions)
- No duplicate (time_key, grid_key) pairs in fact table
- All timestamps are contiguous
- Activity values are within expected ranges

### Airflow DAGs

**DE3: Spark Processing DAG**

**File**: `phase3/airflow/de3_spark_processing_dag.py`

**Purpose**: Orchestrate SP7 Spark ETL job

**Configuration**
- DAG ID: `de3_spark_processing`
- Schedule: [TO BE CONFIRMED from DAG file]
- Tasks: Spark job submission and monitoring

**DE7: End-to-End DAG** (Primary Orchestration)

**File**: `phase3/airflow/de7_end_to_end_dag.py`

**Purpose**: Complete pipeline execution from raw data to warehouse

**DAG ID**: `de7_end_to_end_dag`

**Configuration Variables**
```python
PROJECT_ROOT = Path("/mnt/d/Project1")
PYTHON_EXECUTABLE = "/home/sivamanikandanc/airflow-project/airflow-env/bin/python"
SP7_RUNNER = PROJECT_ROOT / "phase2" / "spark" / "run_telecom_pipeline.py"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
ANALYTICS_HOURLY_DIR = PROJECT_ROOT / "data" / "analytics" / "hourly_grid_summary"
DE6_CHECKPOINT_DIR = PROJECT_ROOT / "data" / "landing" / "sp3" / "hourly_grid_summary"
REFERENCE_PATH = PROJECT_ROOT / "data" / "reference" / "milano-grid.geojson"
LOG_DIR = PROJECT_ROOT / "data" / "logs"
STATUS_DIR = LOG_DIR / "pipeline_status"
```

**Key Functions**
- `utc_now()` - Get current UTC time
- `get_run_id(context)` - Extract Airflow DAG run ID
- `create_logger(run_id)` - Create execution log
- `push(ti, key, value)` - Push XCom variables for inter-task communication

**Tasks** (Based on structure in file)
- Data availability check
- Spark ETL execution (SP7)
- Data quality validation
- Warehouse load (DE6)
- Analytics aggregation
- Notification/alerting

**Scheduling**
- Schedule interval: [TO BE CONFIRMED from DAG definition]
- Backfill support: Yes (for historical data)
- Idempotency: Enforced via state tracking

**Error Handling**
- Task retries: Configurable
- Failure notifications: Email/Slack [TO BE CONFIGURED]
- State persistence: XCom for inter-task data passing

**Logging**
- Log file: `data/logs/de7_end_to_end_{run_id}.log`
- Log level: INFO
- Rotation: Daily

### Data Quality Monitoring

**Validation Points**
1. **Ingestion** (DE2)
   - File exists and is readable
   - CSV schema matches expected
   - Timestamp format is valid

2. **Processing** (SP7)
   - No null values in required columns
   - Activity values are non-negative
   - Grid IDs are in valid range [1, 10000]
   - No duplicate grid/hour records

3. **Warehouse Load** (DE6)
   - Referential integrity
   - No missing time dimensions
   - Data volume matches input
   - Completeness for date range

**Alert Conditions**
- Processing time exceeds threshold
- Data volume outside normal range
- Missing expected data files
- Warehouse load failures

### Phase 3 Completion Status
✅ Implemented
- DE2: Ingestion pipeline
- DE6: Warehouse creation and loading
- DE3, DE7: Airflow orchestration DAGs
- Logging and monitoring

⚠️ Partial
- Error handling and alerting (framework in place, config may vary)

---

## 9. Phase 4: FastAPI Backend

### Objectives
- Expose network analytics via REST API
- Provide ML feature and prediction endpoints
- Enable real-time and historical data queries
- Support React dashboard consumption

### Application Setup

**File**: `phase4/api/main.py`

**FastAPI Configuration**
```python
app = FastAPI(
    title="Telecom Network Intelligence API",
    version="1.0.0",
    description="REST API for network analytics and ML predictions"
)
```

**CORS Configuration**
- Allowed origins: `http://localhost:5173`, `http://127.0.0.1:5173` (React dev server)
- Credentials: Enabled
- Methods: All
- Headers: All

**Lifespan Events**
- On startup: Load and validate ML artifacts (model, scaler)
- Fail fast if artifacts are missing/corrupt

**Routers**
- Network router (API1, API2)
- Hotspot alert router
- Features router (ML2)
- Prediction router (ML3)

### Startup & Execution

```bash
cd phase4
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

**Health Endpoint**
```
GET /health
Response: {"status": "ok"}
```

### API1 & API2: Network Endpoints

**File**: `phase4/api/routers/network.py`

**Router Configuration**
- Prefix: `/network`
- Tag: `Network`

**API1: Get Network Summary**

```
GET /network/summary
```

**Parameters**
- `as_of` (optional): datetime - Reporting cutoff timestamp

**Response** (`NetworkSummaryResponse`)
```json
{
  "timestamp": "2026-09-16T10:00:00Z",
  "total_grids": 10000,
  "total_activity": 5234500.0,
  "average_activity_per_grid": 523.45,
  "high_activity_grids": 234,
  "alert_count": 45,
  "anomaly_count": 12
}
```

**Implementation** (network.py:44-72)
- Service: `NetworkService(db)`
- Method: `get_summary(requested_as_of=as_of)`
- Database: Queries fact_network_activity and dims with aggregation

**API2: Get Grid Activity**

```
GET /network/grid/{grid_id}
```

**Path Parameters**
- `grid_id` (int): Grid identifier [1-10000]

**Query Parameters**
- `date` (optional): Calendar date filter
- `hour` (optional): Hour of day filter [0-23]
- `as_of` (optional): Reporting cutoff timestamp

**Response** (`GridActivityResponse`)
```json
{
  "grid_id": 5432,
  "date": "2026-09-16",
  "records": [
    {
      "timestamp": "2026-09-16T00:00:00Z",
      "hour": 0,
      "sms_in": 100.5,
      "sms_out": 95.3,
      "call_in": 150.2,
      "call_out": 145.8,
      "internet_activity": 2500.0,
      "total_activity": 3091.8,
      "latitude": 45.4642,
      "longitude": 9.1900
    }
    // ... more hourly records
  ]
}
```

**Default Behavior** (Without filters)
- Returns trailing 24 hourly intervals ending at `as_of`
- If no `as_of`, uses MAX(timestamp) from warehouse

**Error Handling**
- 404: Grid not found (grid_id outside [1, 10000])
- 404: No data available for requested filters
- 500: Internal database error

### API3: Hotspot Alerts

**File**: `phase4/api/routers/hotspot_alert.py`

**Purpose**: Identify and rank grid cells with significant activity anomalies

**Endpoint**: `GET /hotspot/alerts`

**Query Parameters**
- `as_of` (optional): Reporting cutoff
- `alert_type` (optional): Filter by HIGH_ACTIVITY | ACTIVITY_DROP | ACTIVITY_SPIKE

**Response**
```json
{
  "as_of": "2026-09-16T10:00:00Z",
  "total_alerts": 45,
  "alerts": [
    {
      "grid_id": 1234,
      "alert_type": "HIGH_ACTIVITY",
      "current_activity": 5200.0,
      "baseline_activity": 2000.0,
      "reason": "HIGH_ACTIVITY: current activity 5200.00 is at least 2.0x the baseline 2000.00.",
      "latitude": 45.4642,
      "longitude": 9.1900
    }
    // ... more alerts
  ]
}
```

**Service**: `HotspotAlertService`
**Implementation File**: `phase4/api/services/hotspot_alert_service.py`

### API4: ML2 Features

**File**: `phase4/api/routers/features.py`

**Purpose**: Retrieve calculated ML features for a grid/timestamp

**Endpoint**: `GET /features/{grid_id}/{feature_timestamp}`

**Response** (`FeatureResponse`)
```json
{
  "grid_id": 1234,
  "feature_timestamp": "2026-09-16T10:00:00Z",
  "avg_activity": 1200.5,
  "activity_growth": 0.15,
  "active_hours": 22,
  "peak_ratio": 3.2,
  "variability": 450.3,
  "internet_share": 0.45
}
```

**Service**: `FeatureService`
**Implementation File**: `phase4/api/services/feature_service.py`

### API5: ML3 Predictions

**File**: `phase4/api/routers/prediction.py`

**Purpose**: Get next-hour high-activity risk prediction for a grid

**Endpoint**: `POST /prediction/risk`

**Request Body** (`PredictionRequest`)
```json
{
  "grid_id": 1234,
  "feature_timestamp": "2026-09-16T10:00:00Z"
}
```

**Response** (`PredictionResponse`)
```json
{
  "grid_id": 1234,
  "feature_timestamp": "2026-09-16T10:00:00Z",
  "risk_score": 0.72,
  "risk_level": "HIGH",
  "model_version": "ml3-logistic-regression-v1",
  "explanation_note": "ML3 Logistic Regression predicts the probability that next-hour total activity will be at least 2000. The prediction uses the ML2 features stored for the requested grid and timestamp. Risk level is HIGH when the predicted probability is at least 0.50."
}
```

**Risk Scoring Logic**
1. Retrieve ML2 features from database (grid_id, feature_timestamp)
2. Scale features using `feature_scaler.joblib`
3. Call `model.predict_proba(scaled_features)`
4. Extract probability for class 1 (high activity)
5. Classify as HIGH if probability >= 0.50, else LOW

**Service**: `PredictionService`
**Implementation File**: `phase4/api/services/prediction_service.py`

### Database Connectivity

**File**: `phase4/api/db/database.py`

**SQLAlchemy Configuration**
```python
DATABASE_URL = "mysql+pymysql://user:password@localhost/telecom_intelligence"
engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
```

**Models**: `phase4/api/db/models.py` (See Section 4)

### Request/Response Models

**File**: `phase4/api/models/network.py`

**Pydantic Models** (Type validation and serialization)
- NetworkSummaryResponse
- GridActivityResponse
- GridHourRecord
- PredictionRequest
- PredictionResponse
- FeatureResponse
- HotspotAlertRecord
- HotspotAlertsResponse

### Testing

**Tests Directory**: `phase4/api/tests/`

**Test Files**
- `test_network_summary.py` - API1 network summary endpoint
- `test_hotspot_alert.py` - Hotspot alerts endpoint
- `test_prediction.py` - Prediction endpoint

**Test Framework**: pytest

**Running Tests**
```bash
cd phase4
pytest api/tests/ -v
```

### Phase 4 Completion Status
✅ Fully implemented
- Core endpoints: Network summary, grid activity, hotspots, features, predictions
- Database models and schema
- Request/response validation with Pydantic
- Error handling and HTTP status codes
- CORS for React frontend
- ML model integration

---

## 10. Phase 5: React NOC Dashboard

### Objectives
- Provide real-time network visualization
- Display hotspot locations and alerts
- Show predictive risk assessments
- Enable grid-level drill-down and exploration

### Technology Stack

**Dependencies** (package.json)
- react@19.2.8
- react-dom@19.2.8
- react-router-dom@7.18.3
- leaflet@1.9.4 (maps)
- react-leaflet@5.0.0 (React wrapper for Leaflet)
- recharts@3.10.1 (charts/visualization)
- vite@8.2.2 (build tool)

### Application Structure

**File**: `phase5/src/App.jsx`

**Key Components**
- `<Navbar />` - Navigation and branding
- `<Routes>` - Page routing (React Router)
- Page components based on URL path

### Pages

**Dashboard** (`phase5/src/pages/Dashboard.jsx`)
- Overview of network state
- Total activity summary
- Alert counts
- High-risk grid count
- Charts showing trends

**Grid Explorer** (`phase5/src/pages/GridExplorer.jsx`)
- Search/filter grids
- View grid-specific hourly activity
- Historical timeline view
- Drill-down into individual hours

**Hotspots & Alerts** (`phase5/src/pages/HotspotsAlerts.jsx`)
- List of active alerts
- Filter by alert type (HIGH_ACTIVITY, ACTIVITY_DROP, ACTIVITY_SPIKE)
- Click to view grid details
- Map view of alert locations

**Predictive Risk** (`phase5/src/pages/PredictiveRisk.jsx`)
- Show grids with HIGH risk scores
- ML3 prediction results
- Risk score distribution
- Risk threshold explanation

### Map Component

**File**: `phase5/src/components/MilanGridMap.jsx`

**Features**
- Interactive Leaflet map centered on Milan
- Grid cell visualization
- Color-coded by activity level or risk
- Hover tooltips with grid info
- Click to select grid and view details
- Zoom and pan controls

**Data Source**
- Milano grid geometry from `data/reference/milano-grid.geojson`
- Activity data from `/network/summary` and `/network/grid/{id}` endpoints
- Risk data from `/prediction/risk` endpoint

### API Client

**File**: `phase5/src/api/client.js`

**Base URL**: `VITE_API_BASE_URL` from `.env` (default: `http://localhost:8000`)

**API Functions**
- `getNetworkSummary(asOf)` - Call `GET /network/summary`
- `getGridActivity(gridId, filters)` - Call `GET /network/grid/{grid_id}`
- `getHotspotAlerts(asOf)` - Call `GET /hotspot/alerts`
- `getGridFeatures(gridId, timestamp)` - Call `GET /features/{grid_id}/{timestamp}`
- `getPrediction(gridId, timestamp)` - Call `POST /prediction/risk`

**Error Handling**
- Try/catch wrapping API calls
- User-facing error messages
- Network timeout handling
- 404 handling (no data available)

### Supporting Components

**LoadingState** (`phase5/src/components/LoadingState.jsx`)
- Loading indicator displayed while data fetches
- Spinner animation
- "Loading..." text

**ErrorState** (`phase5/src/components/ErrorState.jsx`)
- Error message display
- Retry button
- Error details (in development)

**Navbar** (`phase5/src/components/Navbar.jsx`)
- Project logo/title
- Navigation links to pages
- Timestamp of last update

### Build & Development

**Development Server**
```bash
cd phase5
npm run dev
```
Dashboard available at `http://localhost:5173`

**Production Build**
```bash
npm run build
npm run preview
```

**Linting**
```bash
npm run lint
```

### Environment Configuration

**File**: `phase5/.env`
```
VITE_API_BASE_URL=http://localhost:8000
```

**File**: `phase5/envexample.txt`
```
VITE_API_BASE_URL=<API_URL>
```

### Phase 5 Completion Status
✅ Implemented
- Page structure and routing
- Dashboard overview
- Grid explorer with filters
- Hotspot alerts display
- Predictive risk view
- Milan grid map component
- API client integration
- Error and loading states

---

## 11. Phase 6: Machine Learning Pipeline

### Objectives
- Calculate rolling window features from network activity
- Train predictive model for high-activity risk
- Perform batch scoring across all grids
- Generate anomaly scores for network analysis
- Integrate predictions into API and warehouse

### ML Problem Definition

**Target**: Predict whether next hour (t+1) will have high activity

**Threshold**: 2,000 total_activity units

**Data Points**
- Feature observation at time t
- Actual activity observed at time t+1
- Supervised binary classification

**Business Rule**
- Risk prediction enables proactive NOC investigation
- Does NOT diagnose congestion, capacity, or faults
- Purely based on activity level anomalies

### ML2: Feature Engineering

**File**: `phase6/ml/features.py`

**Data Structures**

`ActivityRecord` (Dataclass)
- grid_id (int)
- timestamp (datetime)
- total_activity (float)
- internet_activity (float)

`NetworkFeatures` (Dataclass)
- grid_id (int)
- feature_timestamp (datetime)
- avg_activity (float)
- activity_growth (float)
- active_hours (int)
- peak_ratio (float)
- variability (float)
- internet_share (float)

**Feature Calculation** (function: `calculate_features`)

```
Inputs:
  recent_24h - 24 hourly activity records (t-23 to t)
  prior_24h - 24 hourly activity records (t-47 to t-24)

Outputs:
  avg_activity = mean(recent_24h.total_activity)
  activity_growth = (avg_recent - avg_prior) / avg_prior
  active_hours = count(hours where activity > 0)
  peak_ratio = max(recent_activity) / avg_activity
  variability = std_dev(recent_activity)
  internet_share = sum(internet_activity) / sum(total_activity)
```

**Feature Validation** (function: `_validate_features`)
- All numeric values must be finite (not NaN, not Inf)
- active_hours must be in [0, 24]
- internet_share must be in [0, 1]

**Feature File**: `phase6/ml/feature_generator.py`

Loads activity from warehouse and calculates features for all grids.

### ML3: Model Training

**File**: `phase6/ml/train_model.py`

**Modeling Dataset**
```sql
SELECT
  mf.grid_id,
  mf.feature_timestamp,
  mf.avg_activity,
  mf.activity_growth,
  mf.active_hours,
  mf.peak_ratio,
  mf.variability,
  mf.internet_share,
  CASE WHEN f.total_activity >= 2000 THEN 1 ELSE 0 END AS high_activity_target
FROM ml_grid_features mf
JOIN dim_grid dg ON dg.grid_id = mf.grid_id
JOIN dim_time future_time ON future_time.timestamp = DATE_ADD(mf.feature_timestamp, INTERVAL 1 HOUR)
JOIN fact_network_activity f ON f.grid_key = dg.grid_key AND f.time_key = future_time.time_key
```

**Model Selection**: Logistic Regression
- Reason: Interpretability (feature coefficients are transparent)
- Configuration: max_iter=1000, class_weight=None, random_state=42

**Training Process**

1. **Chronological Split** (80/20 by timestamp)
   - No random shuffling (maintains temporal order)
   - All observations before cutoff_timestamp → train
   - All observations after cutoff_timestamp → test

2. **Feature Scaling**
   - StandardScaler fits on training set
   - Scales both train and test sets
   - Reason: Features have very different numerical scales

3. **Model Fit**
   ```python
   scaler = StandardScaler()
   X_train_scaled = scaler.fit_transform(X_train)
   model = LogisticRegression(max_iter=1000)
   model.fit(X_train_scaled, y_train)
   ```

**Evaluation Metrics** (Included in `ml3_evaluation.json`)
- Accuracy (train and test)
- Precision
- Recall
- Confusion matrix
- ROC-AUC score
- PR-AUC (Average Precision)
- Classification report

**Baseline Comparison**
- All-negative baseline (always predict 0)
- ML3 accuracy threshold: >95%
- Investigation required if below threshold

**Artifacts Generated**
- `logistic_regression.joblib` - Trained model
- `feature_scaler.joblib` - Feature scaler
- `ml3_evaluation.json` - Training metrics and evaluation results

**Run Training**
```bash
python phase6/ml/train_model.py
```

### ML4: Anomaly Scoring

**File**: `phase6/ml/warehouse_anomaly_scorer.py`

**Purpose**: Identify anomalous activity patterns using baseline comparison

**Function**: `calculate_anomalies()`

Computes anomaly scores similar to NP3 alert logic:
- Leave-one-out baseline per grid
- Activity deviation from baseline
- Anomaly severity score

**Inputs**
- `grid_id`
- Historical activity records
- Recent activity value

**Output**
- Anomaly score (0-1 or continuous)
- Anomaly classification (normal/moderate/severe)

### ML6: Batch Scoring

**File**: `phase6/ml/batch_scorer.py`

**Purpose**: Score all grids with risk predictions using frozen ML3 model

**Process**

1. **Load Frozen Artifacts**
   ```python
   model = joblib.load("logistic_regression.joblib")
   scaler = joblib.load("feature_scaler.joblib")
   ```

2. **Load ML2 Features**
   ```sql
   SELECT grid_id, feature_timestamp, avg_activity, activity_growth, 
          active_hours, peak_ratio, variability, internet_share
   FROM ml_grid_features
   ORDER BY feature_timestamp, grid_id
   ```

3. **Batch Prediction**
   ```python
   X_batch = features[FEATURE_COLUMNS]
   X_scaled = scaler.transform(X_batch)
   probabilities = model.predict_proba(X_scaled)
   risk_scores = probabilities[:, 1]  # Probability of class 1 (high activity)
   ```

4. **Risk Classification**
   - risk_score >= 0.5 → risk_level = "HIGH"
   - risk_score < 0.5 → risk_level = "LOW"

5. **Write Results**
   - Insert into `network_risk_score` table
   - Indexed by (grid_id, risk_timestamp)

**Configuration**
- BATCH_SIZE = 5000 (process 5000 rows at a time)
- RISK_THRESHOLD = 0.5 (probability threshold for HIGH classification)
- MODEL_VERSION = "ml3-logistic-regression-v1"

**Run Batch Scoring**
```bash
export ML6_DATABASE_URL="mysql+pymysql://user:pass@localhost/telecom_intelligence"
python phase6/ml/batch_scorer.py
```

### Tests

**Test Features** (`phase6/ml/test_features.py`)
- Feature calculation correctness
- Boundary conditions (0 active hours, 100% internet share)
- Validation enforcement

**Test Anomaly Scorer** (`phase6/ml/test_anomaly_scorer.py`)
- Anomaly score calculation
- Edge cases (single observation, all identical values)

**Run Tests**
```bash
cd phase6
pytest ml/test_*.py -v
```

### Model Artifacts

**Location**: `phase6/ml/artifacts/`

**Files**
- `logistic_regression.joblib` (911 bytes) - Trained sklearn model
- `feature_scaler.joblib` (1,063 bytes) - StandardScaler fitted on training data
- `ml3_evaluation.json` (2,234 bytes) - Training/evaluation metrics
- `ml3_np3_comparison.csv` (43.6MB) - Comparison of ML3 vs NP3 alerts
- `ml3_np3_comparison_summary.json` (1,402 bytes) - Comparison summary

### Phase 6 Completion Status
✅ Fully implemented
- ML2: Feature engineering (6 features per grid/hour)
- ML3: Logistic Regression model trained and evaluated
- ML4: Anomaly scoring
- ML6: Batch scoring pipeline
- Model artifacts stored and versioned
- Integration with API (Phase 4) and warehouse (Phase 3)

---

## 12. Phase 7: Claude/AI-Assisted Operations (Status)

### Objectives
- Integrate Claude LLM for automated incident investigation
- Provide intelligent insights and recommendations
- Assist NOC operators with decision-making
- Generate natural language explanations for alerts

### Current Status
**Status**: Planned / Not yet implemented

**Research Findings**
- No Claude API calls found in codebase
- No LLM-related imports or configurations
- No MCP server definitions
- No Claude-based tools or agents

### Planned Capabilities (From Source Document)

1. **Network Incident Investigation**
   - Input: Alert details (grid_id, alert_type, activity values)
   - Claude processes: Generate contextual insights
   - Output: Investigation report with recommendations

2. **Intelligent Alert Summarization**
   - Input: Multiple alerts and risk scores
   - Claude analyzes: Patterns and root causes
   - Output: Prioritized incident summary

3. **Context-Aware Recommendations**
   - Input: Historical patterns, current state, alerts
   - Claude suggests: Operational actions
   - Output: Decision support information

4. **Natural Language Explanations**
   - Input: ML prediction or alert
   - Claude generates: Human-readable explanation
   - Output: Narrative for NOC operator

### Required Components (To Implement)

**1. Claude SDK Integration**
```python
import anthropic

client = anthropic.Anthropic(api_key="sk-...")
message = client.messages.create(
    model="claude-3-5-sonnet-20241022",
    max_tokens=1024,
    messages=[
        {"role": "user", "content": "..."}
    ]
)
```

**2. Network Intelligence Tools** (MCP Tools)
```python
# Tools Claude can call to access network data
{
    "name": "get_grid_activity",
    "description": "Retrieve recent activity for a grid",
    "parameters": {
        "grid_id": {"type": "integer"},
        "hours": {"type": "integer", "default": 24}
    }
}

{
    "name": "get_alerts",
    "description": "Retrieve active alerts",
    "parameters": {
        "alert_type": {"type": "string", "enum": ["HIGH_ACTIVITY", "ACTIVITY_DROP", "ACTIVITY_SPIKE"]},
        "limit": {"type": "integer", "default": 10}
    }
}

{
    "name": "get_grid_risk",
    "description": "Get ML risk prediction for a grid",
    "parameters": {
        "grid_id": {"type": "integer"}
    }
}
```

**3. Incident Investigation Workflow**
```
Alert Triggered
    ↓
Fetch context (grid activity, neighbors, history)
    ↓
Call Claude with network context
    ↓
Claude calls tools to analyze patterns
    ↓
Claude generates investigation report
    ↓
Present to NOC operator
```

**4. Integration Points**

**Phase 4 (API)**
- New endpoint: `POST /claude/investigate`
  - Input: Alert details
  - Output: Claude-generated investigation report

**Phase 5 (Dashboard)**
- "AI Insights" button on alert cards
- Investigation report panel
- Recommendations section

**Phase 3 (Airflow)**
- Optional Claude investigation task in DAG
- Post-alert processing with LLM

### Implementation Roadmap

1. **Step 1**: Set up Claude SDK and authentication
2. **Step 2**: Define MCP tools for network data access
3. **Step 3**: Create investigation prompt templates
4. **Step 4**: Implement FastAPI endpoint for investigations
5. **Step 5**: Add React UI components for Claude insights
6. **Step 6**: Test with real alert scenarios
7. **Step 7**: Monitor token usage and optimize prompts

### Placeholder: Future Integration
```python
# phase4/api/routers/claude.py (To be implemented)

from fastapi import APIRouter, Depends
from anthropic import Anthropic

router = APIRouter(prefix="/claude", tags=["Claude"])

@router.post("/investigate")
def investigate_alert(alert_details: AlertDetails):
    """
    Use Claude to investigate a network alert and provide insights.
    """
    client = Anthropic()
    
    context = fetch_network_context(alert_details.grid_id)
    
    message = client.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=2048,
        tools=[...],
        messages=[
            {
                "role": "user",
                "content": f"Investigate this network alert: {alert_details}"
            }
        ]
    )
    
    return {
        "investigation": message.content,
        "recommendations": extract_recommendations(message),
        "tokens_used": message.usage.input_tokens
    }
```

---

## 13. End-to-End Integration

### Complete Data Lifecycle

```
Day 0: Data Arrives
  ↓
  Raw CSV files in data/raw/
  (One file per day: grid_id, timestamp, sms_in, sms_out, call_in, call_out, internet_activity)
  ↓

Day 1: Ingestion & Processing
  ↓
  Airflow DAG de7_end_to_end_dag triggers
  ├─ Task 1: Check data availability (data/raw/)
  ├─ Task 2: Execute SP7 pipeline (telecom_pipeline.py)
  │   ├─ SP1: Distributed ingestion (read raw CSVs)
  │   ├─ SP2: Cleaning (NetworkCleaner - remove duplicates, validate)
  │   ├─ SP3: Aggregation (NetworkAggregator - grid/hour totals, sums)
  │   ├─ SP4: Enrichment (NetworkGeoEnricher - add lat/lon from GeoJSON)
  │   ├─ SP5: Performance (validate metrics)
  │   └─ SP6: Storage (write to landing zone)
  │
  ├─ Task 3: DE6 warehouse load
  │   ├─ Populate dim_time (timestamp details)
  │   ├─ Populate dim_grid (grid coordinates)
  │   └─ Load fact_network_activity (detailed records)
  │
  ├─ Task 4: Generate NP3 alerts
  │   ├─ Load grid/hour analytics from fact table
  │   ├─ Calculate leave-one-out baseline (NetworkAlertGenerator.build_baseline)
  │   ├─ Apply rules: HIGH_ACTIVITY, ACTIVITY_DROP, ACTIVITY_SPIKE
  │   └─ Store results in alerting table / export CSV
  │
  └─ Task 5: ML feature generation
      ├─ Load last 48 hours of activity per grid
      ├─ Calculate 6 ML features (calculate_features in features.py)
      ├─ Persist to ml_grid_features table
      └─ Ready for ML inference

Day 2+: Real-Time Operations

Query APIs (Phase 4 FastAPI)
  ↓
  API1: GET /network/summary
    → Network overview dashboard
    → Fact table aggregation
    → Dashboard updates
  
  API2: GET /network/grid/{grid_id}
    → Grid-specific hourly activity
    → 24-hour trailing view
    → Grid Explorer page
  
  API3: GET /hotspot/alerts
    → Active anomalies
    → NP3 alert results
    → Hotspots & Alerts page
  
  API4: GET /features/{grid_id}/{timestamp}
    → ML2 feature vector
    → Used by ML3 model
    → Feature inspection
  
  API5: POST /prediction/risk
    → ML3 risk prediction
    → Load features → Scale → Predict → Classify
    → Predictive Risk page
  ↓

React Dashboard (Phase 5)
  ├─ Dashboard page
  │   ├─ Summary cards (total activity, alerts, risk)
  │   ├─ Activity trend chart (Recharts)
  │   └─ Top alert grids
  │
  ├─ MilanGridMap
  │   ├─ Render 10,000 grid cells
  │   ├─ Color by activity/risk
  │   ├─ Click to drill into grid
  │   └─ Tooltip with current values
  │
  ├─ Grid Explorer
  │   ├─ Search by grid_id
  │   ├─ View 24-hour hourly data
  │   ├─ Historical comparison
  │   └─ Alert overlay
  │
  ├─ Hotspots & Alerts
  │   ├─ List of active alerts
  │   ├─ Filter by type
  │   ├─ Click to view details
  │   └─ Map view of hotspots
  │
  └─ Predictive Risk
      ├─ Grids with HIGH risk
      ├─ ML3 scores and explanations
      ├─ Risk threshold visualization
      └─ Recommendations
  ↓

Continuous Monitoring
  ├─ Airflow scheduler checks for new data (every N minutes/hours)
  ├─ New data triggers pipeline execution
  ├─ Warehouse updates with latest facts
  ├─ API endpoints serve fresh data
  ├─ Dashboard refreshes (polling or WebSocket)
  └─ NOC operator monitors for alerts and risk

Phase 7 (Future): Claude Integration
  ├─ Alert triggered → Claude investigation
  ├─ Context retrieval (neighbors, history, patterns)
  ├─ Natural language analysis
  ├─ Recommendations generated
  └─ Operator informed with structured insight
```

### Component Handoff Points

| From | To | Data | Method |
|------|----|----|--------|
| Raw CSV | SP1 Ingestion | Activity records | Spark read_csv |
| SP1 | SP2 Cleaning | DataFrame | In-memory Spark |
| SP2 | SP3 Aggregation | Cleaned rows | DataFrame |
| SP3 | SP4 Enrichment | Aggregated records | DataFrame |
| SP4 | SP6 Storage | Enriched DataFrame | Write to landing zone |
| Landing zone | DE6 Warehouse | Parquet/CSV | Spark to MySQL |
| Warehouse | NP3 Alerts | Fact table rows | SQL query |
| Warehouse | ML2 Features | Fact table rows | SQL query |
| ML2 | ML3 Training | Feature vectors | In-memory Pandas |
| ML3 | FastAPI | Artifacts (.joblib) | File load on startup |
| FastAPI | React | JSON responses | HTTP REST |
| React | User | Visualizations | Browser render |

### Data Consistency & Timing

**Temporal Consistency**
- All timestamps in UTC
- No timezone conversions
- Hourly granularity maintained throughout

**Data Freshness**
- API returns latest data in warehouse (MAX(timestamp))
- User can override with `as_of` parameter
- Dashboard refreshes every N seconds (configurable)

**Idempotency**
- Spark jobs are idempotent (overwrite landing zone)
- Warehouse loads upsert by (time_key, grid_key)
- No duplicate alerts if job reruns

---

## 14. API & Data Interface Reference

### REST API Endpoints

**Base URL**: `http://localhost:8000`

**Health Check**
```
GET /health
Response: 200 OK
{"status": "ok"}
```

**Network Endpoints** (`/network`)
```
GET /network/summary?as_of=2026-09-16T10:00:00Z
→ NetworkSummaryResponse

GET /network/grid/1234?date=2026-09-16&hour=10&as_of=2026-09-16T10:00:00Z
→ GridActivityResponse
```

**Hotspot Endpoints** (`/hotspot`)
```
GET /hotspot/alerts?as_of=2026-09-16T10:00:00Z&alert_type=HIGH_ACTIVITY
→ HotspotAlertsResponse
```

**Feature Endpoints** (`/features`)
```
GET /features/1234/2026-09-16T10:00:00Z
→ FeatureResponse
```

**Prediction Endpoints** (`/prediction`)
```
POST /prediction/risk
{
  "grid_id": 1234,
  "feature_timestamp": "2026-09-16T10:00:00Z"
}
→ PredictionResponse
```

### Database Tables

**Dimensions**
- `dim_time` - Time reference (timestamp, date, hour, day_of_week)
- `dim_grid` - Grid reference (grid_id, lat, lon, geometry)

**Facts**
- `fact_network_activity` - Activity observations (10M+ rows)

**Analytics**
- `ml_grid_features` - ML2 feature vectors (hourly per grid)
- `network_risk_score` - ML3 predictions (hourly per grid)

### Data Contracts

**Input CSV Schema** (Raw data files)
```
grid_id: int
timestamp: datetime (ISO 8601)
sms_in: float
sms_out: float
call_in: float
call_out: float
internet_activity: float
country_code: string (optional)
```

**Processed Grid/Hour Schema**
```
timestamp: datetime
grid_id: int
sms_in: float
sms_out: float
call_in: float
call_out: float
internet_activity: float
total_sms: float (= sms_in + sms_out)
total_calls: float (= call_in + call_out)
total_activity: float (= total_sms + total_calls + internet_activity)
latitude: float
longitude: float
```

**Alert Record Schema**
```
grid_id: int
timestamp: datetime
alert_type: string (HIGH_ACTIVITY | ACTIVITY_DROP | ACTIVITY_SPIKE)
current_activity: float
baseline_activity: float
reason: string (human-readable)
```

**Feature Vector Schema**
```
grid_id: int
feature_timestamp: datetime
avg_activity: float
activity_growth: float
active_hours: int
peak_ratio: float
variability: float
internet_share: float
```

**Prediction Record Schema**
```
grid_id: int
risk_timestamp: datetime
risk_score: float (0-1 probability)
risk_level: string (HIGH | LOW)
model_version: string
```

---

## 15. Configuration & Environment Variables

### Python Environment Variables

**Phase 4 API** (`phase4/api/`)
```bash
DATABASE_URL=mysql+pymysql://telcom_user:password@localhost/telecom_intelligence
PYTHONPATH=/path/to/Project1
```

**Phase 6 ML** (`phase6/ml/`)
```bash
PROJECT_ROOT=/path/to/Project1
ML6_DATABASE_URL=mysql+pymysql://telcom_user:password@localhost/telecom_intelligence
```

**Phase 3 Airflow**
```bash
AIRFLOW_HOME=/path/to/airflow
PROJECT_ROOT=/path/to/Project1
PYTHON_EXECUTABLE=/path/to/python (in virtual environment)
```

**Phase 2 Spark**
```bash
SPARK_HOME=/path/to/spark
HADOOP_HOME=/path/to/hadoop
PROJECT_ROOT=/path/to/Project1
```

### React Environment Variables

**Phase 5** (`phase5/.env`)
```bash
VITE_API_BASE_URL=http://localhost:8000
```

### Configuration Files

**Phase 3 Airflow DAG Config**
- `phase3/airflow/de7_end_to_end_dag.py` contains hardcoded paths:
  - `PROJECT_ROOT = Path("/mnt/d/Project1")`
  - `REFERENCE_PATH = PROJECT_ROOT / "data" / "reference" / "milano-grid.geojson"`
  - `LOG_DIR = PROJECT_ROOT / "data" / "logs"`

**Phase 6 ML Configuration**
- `MODEL_PATH = "phase6/ml/artifacts/logistic_regression.joblib"`
- `SCALER_PATH = "phase6/ml/artifacts/feature_scaler.joblib"`
- `RISK_THRESHOLD = 0.5`
- `BATCH_SIZE = 5000`

**Phase 4 API Configuration**
- `phase4/api/config.py` - API title, version
- CORS allowed origins hardcoded in `main.py`

### Secrets Management

⚠️ **Current State**: Database credentials in environment variables (plain text)

**Recommendation**: Migrate to secure secrets manager
- AWS Secrets Manager
- HashiCorp Vault
- Kubernetes Secrets (if containerized)
- Environment files encrypted with git-crypt

**Sensitive Variables** (Never commit to git)
- DATABASE_URL (password)
- CLAUDE_API_KEY (if Phase 7 implemented)
- Any AWS/cloud credentials

---

## 16. Testing Strategy

### Test Coverage by Phase

**Phase 1: Core Python** - Manual/Integration Tests
- `phase1/np2/np2_run_test.py` - NP2 processing
- `phase1/np3/np3_run_test.py` - NP3 alert generation
- Tests: Data loading, validation, alert rules

**Phase 2: Spark** - Manual/Integration Tests
- `phase2/sp1/sp1_run_test.py` - Ingestion
- `phase2/sp2/sp2_run_test.py` - Cleaning
- `phase2/sp3/sp3_run_test.py`, `sp3_run_test2.py`, `sp3_run_test3.py` - Aggregation (3 variants)
- `phase2/sp4/sp4_run_test.py`, `sp4_run_test2.py`, `sp4_run_test3.py` - Enrichment (3 variants)
- `phase2/sp5/sp5_run_test.py` - Performance
- `phase2/sp6/sp6_run_test.py` - Storage
- Tests: Schema validation, data integrity, transformation accuracy

**Phase 3: Airflow** - Integration Tests
- `phase3/de2/tests/test_ingestion.py` - Ingestion pipeline

**Phase 4: FastAPI** - Unit/Integration Tests
- `phase4/api/tests/test_network_summary.py` - API1 endpoint
- `phase4/api/tests/test_hotspot_alert.py` - Hotspot endpoint
- `phase4/api/tests/test_prediction.py` - Prediction endpoint
- Tests: Response schemas, error handling, database queries

**Phase 6: ML** - Unit Tests
- `phase6/ml/test_features.py` - Feature calculation correctness
- `phase6/ml/test_anomaly_scorer.py` - Anomaly scoring logic
- Tests: Feature validation, edge cases, model loading

### Running Tests

**Phase 4 (FastAPI)**
```bash
cd phase4
pytest api/tests/ -v

# Individual test
pytest api/tests/test_prediction.py::test_prediction_success -v
```

**Phase 6 (ML)**
```bash
cd phase6
pytest ml/test_features.py -v
pytest ml/test_anomaly_scorer.py -v
```

### Test Categories

**Unit Tests**
- Feature calculation functions
- Validation logic
- Data transformation functions

**Integration Tests**
- Full pipeline execution (raw → warehouse)
- API endpoint + database roundtrip
- ML model loading + prediction

**Manual Tests** (Run test scripts)
- Each phase has *_run_test.py scripts
- Execute: `python phase/sp1/sp1_run_test.py`
- Verify data output in landing zone

### Data Quality Tests

**In DE6 Warehouse Validation**
- Referential integrity (foreign keys)
- Row counts match expected volumes
- No NULL values in required columns
- Timestamp contiguity
- Activity values non-negative

**In NP3 Alert Validation**
- Baseline calculation correctness
- Alert criteria applied properly
- No duplicate alerts
- Alert proportion reasonable (~5-15% of grid-hours)

### Test Data

**Location**: Test data embedded in test files or located in `data/raw/` (small sample files)

**Volume**: Varies by phase
- Phase 1: Small sample CSVs (1-100 records)
- Phase 2: Full daily files (~240K records)
- Phase 3: Warehouse integration (full dataset)

### CI/CD Integration

**GitHub Actions** (If configured in `.github/workflows/`)
- [TO BE CONFIRMED from .github directory]

**Missing**: 
- Automated test execution on commit
- Automated test reporting
- Coverage metrics

---

## 17. Security Considerations

### Current Security Posture

**Strengths**
- MySQL user with limited privileges (ideally)
- API running on localhost (development)
- No API key exposure in code
- FastAPI uses dependency injection for database connections

**Gaps**
- No authentication/authorization on API endpoints
- Database credentials in environment variables
- No HTTPS/TLS in local development
- No rate limiting
- Limited input validation

### Secrets & Credentials

**Database Credentials**
- Currently: Plain text in `.env` or environment variable
- Should be: Encrypted or vault-managed
- Never commit: `DATABASE_URL` with passwords

**Claude API Key** (When Phase 7 implemented)
- Must use: `ANTHROPIC_API_KEY` environment variable
- Never hardcode: API key in source files
- Rotate: Regularly in production

### Data Protection

**At Rest**
- MySQL database (configured with encryption optional)
- Data files in `data/` directory (no encryption)
- Recommendation: Encrypt `data/` partition if sensitive

**In Transit**
- API currently HTTP (local dev)
- Production: Must use HTTPS/TLS
- Airflow: Configure SSL for inter-node communication

### Access Control

**Current**: None (all endpoints open)

**Needed**
- FastAPI middleware for API key validation
- Optionally: Role-based access control (admin/operator/viewer)
- Example implementation:
  ```python
  from fastapi.security import HTTPBearer
  security = HTTPBearer()
  
  @app.get("/network/summary")
  def get_summary(credentials: HTTPAuthCredential = Depends(security)):
      validate_token(credentials.credentials)
      ...
  ```

### Input Validation

**API Validation** (Via Pydantic)
- ✅ Request body validation
- ✅ Path parameter type checking
- ✅ Query parameter constraints (e.g., hour 0-23)
- ⚠️ SQL injection: SQLAlchemy ORM prevents most cases

**Data Validation**
- ✅ NP3 validates grid_id [1, 10000]
- ✅ Timestamp format validation
- ⚠️ Activity values: Only non-negative check (no upper bound)

### Logging & Monitoring

**Logs Locations**
- `data/logs/de7_end_to_end_*.log` - Airflow DAG execution
- `data/logs/{input}_alerts_*.log` - NP3 execution
- `data/logs/sp7_*.log` - Spark pipeline execution

**Logged Information**
- ✅ Processing steps, data volumes
- ✅ Errors and exceptions
- ⚠️ Passwords/credentials (should NEVER be logged)

**Missing**
- Centralized log aggregation (ELK, CloudWatch)
- Security event tracking
- Audit logging (who accessed what, when)

### Recommendations

1. **Implement API authentication** before production
2. **Use secrets manager** (Vault, AWS Secrets Manager)
3. **Enable HTTPS** in production
4. **Add rate limiting** to prevent abuse
5. **Implement audit logging** for all data access
6. **Encrypt sensitive data** in storage and transit
7. **Regular security scanning** of dependencies
8. **Least privilege** for database user account

---

## 18. Deployment & Operations

### Local Development Deployment

**Prerequisites**
1. Python 3.8+, Node.js 18+, MySQL 8.0+, Spark 3.x

**Deployment Steps**

```bash
# 1. Clone and setup
cd D:\Project1
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 2. Database initialization
mysql -u root -p < setup_database.sql  # [Create if needed]

# 3. Warehouse setup (one-time)
python phase3/de6/run_de6.py

# 4. Process initial data
python phase2/spark/run_telecom_pipeline.py \
  --input data/raw \
  --output data \
  --reference data/reference/milano-grid.geojson \
  --log-dir data/logs

# 5. Generate ML features
python phase6/ml/feature_generator.py

# 6. Train model
python phase6/ml/train_model.py

# 7. Start API (Terminal 1)
cd phase4
uvicorn api.main:app --reload --port 8000

# 8. Start Dashboard (Terminal 2)
cd phase5
npm install
npm run dev
```

**Services Running**
- MySQL: `localhost:3306`
- FastAPI: `localhost:8000`
- React: `localhost:5173`
- Airflow: `localhost:8080` (if configured)

### Docker Deployment

**Status**: [TO BE CONFIRMED] - Docker configuration may exist but not detailed in repository

**Recommended Dockerfile Structure**
```dockerfile
# Backend
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["uvicorn", "phase4.api.main:app", "--host", "0.0.0.0"]

# Frontend
FROM node:18-alpine
WORKDIR /app
COPY phase5/package*.json .
RUN npm install
COPY phase5/src src
RUN npm run build
CMD ["npm", "run", "preview"]
```

### Production Considerations

**Database**
- Use managed MySQL service (RDS, Cloud SQL)
- Enable automated backups
- Configure read replicas for HA
- Connection pooling for API

**API**
- Deploy behind load balancer
- Use WSGI server (Gunicorn) instead of Uvicorn
- Enable monitoring (Prometheus, CloudWatch)
- API versioning strategy

**Frontend**
- Serve from CDN (CloudFront, CloudFlare)
- Enable caching headers
- Minified builds

**Data Pipeline**
- Run Airflow on dedicated scheduler/workers
- Configure task parallelism
- Enable monitoring and alerting
- Backup landing zone data

**Monitoring & Alerting**
- Pipeline success/failure notifications
- API response time monitoring
- Database query performance
- Storage usage tracking

### Backup Strategy

**What to Backup**
1. MySQL database (regular scheduled backups)
2. Landing zone data (`data/landing/`)
3. Configuration files (.env, DAG definitions)
4. ML artifacts (`phase6/ml/artifacts/`)

**What Not to Backup**
- `data/raw/` (source of truth, keep indefinitely but archive)
- `node_modules/`, `__pycache__/` (reconstructable)
- Logs older than retention period

**Frequency**
- Database: Daily
- Data files: Daily
- Configuration: On change

### Monitoring Checklist

- [ ] Pipeline execution time within SLA
- [ ] No missing data for date range
- [ ] Alert generation functioning (daily verification)
- [ ] API response times < 500ms
- [ ] Dashboard refresh working
- [ ] ML model batch scoring completed
- [ ] Database disk usage < 80%
- [ ] No stuck Airflow tasks
- [ ] Log files rotating properly

---

## 19. Troubleshooting Guide

### Common Issues & Solutions

#### Issue: Spark Job Fails with "HADOOP_HOME not set"

**Symptom**: Error when running `python phase2/spark/run_telecom_pipeline.py`

**Solution**:
```bash
export HADOOP_HOME=C:\Path\To\hadoop-win-utils  # or /path/to/hadoop
export PATH=$HADOOP_HOME/bin:$PATH
python phase2/spark/run_telecom_pipeline.py ...
```

#### Issue: Database Connection Error - "Access denied for user"

**Symptom**: `pymysql.Error: (1045, "Access denied for user 'telcom_user'@'localhost'..."`

**Solution**:
1. Verify MySQL is running: `mysql -u root -p`
2. Check DATABASE_URL in .env
3. Verify user exists: `SELECT User FROM mysql.user WHERE User='telcom_user';`
4. Reset password if needed: `ALTER USER 'telcom_user'@'localhost' IDENTIFIED BY 'newpassword';`

#### Issue: API Returns 500 - "ML model artifact is missing"

**Symptom**: FastAPI startup fails because `logistic_regression.joblib` not found

**Solution**:
1. Train model: `python phase6/ml/train_model.py`
2. Verify artifacts exist: `ls phase6/ml/artifacts/`
3. Check MODEL_PATH in `phase4/api/services/prediction_service.py`

#### Issue: React Dashboard Shows "Network Error"

**Symptom**: Dashboard can't fetch data from API

**Solution**:
1. Verify API is running: `curl http://localhost:8000/health`
2. Check `phase5/.env` - `VITE_API_BASE_URL` correct
3. CORS issue - verify FastAPI CORS config
4. Browser console for specific error message

#### Issue: Airflow DAG Doesn't Trigger

**Symptom**: `de7_end_to_end_dag` doesn't start on schedule

**Solution**:
1. Verify Airflow scheduler is running
2. Check DAG definition syntax: `airflow dags list`
3. Check task logs: `airflow tasks logs de7_end_to_end_dag task_id`
4. Verify paths in DAG (PROJECT_ROOT, PYTHON_EXECUTABLE) are correct

#### Issue: NP3 Generates No Alerts

**Symptom**: Alert CSV is empty despite activity data

**Solution**:
1. Check activity_floor: `print(detector.activity_floor)`
2. Verify baseline calculation: Check analytics_data has `baseline_activity` column
3. Check thresholds: Default ratios (2.0, 1.5, 0.5) might be too strict
4. Verify data: Are there 24+ hours per grid? NP3 requires for leave-one-out

#### Issue: ML Model Accuracy Below 95%

**Symptom**: `ml3_evaluation.json` shows accuracy < 95%

**Possible Causes**:
- Insufficient training data
- Feature engineering issue (missing/NaN values)
- Class imbalance (not enough high-activity examples)
- Feature threshold (2000) not discriminative

**Solutions**:
1. Check data volume: `SELECT COUNT(*) FROM ml_grid_features;`
2. Inspect features: `python phase6/ml/investigate_ml3.py` (if exists)
3. Adjust target threshold
4. Collect more historical data

### Log File Locations

| Component | Log File | Location |
|-----------|----------|----------|
| Airflow DAG | de7_end_to_end_*.log | `data/logs/` |
| Spark Pipeline | sp7_telecom_pipeline_*.log | `data/logs/` |
| NP3 Alerts | {input}_alerts_*.log | `data/logs/` |
| FastAPI | stdout | Terminal where `uvicorn` runs |
| React | Browser console | Press F12 in browser |

### Debug Mode

**Enable verbose logging**:

**Python (Spark/Airflow)**:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

**React**:
```javascript
// phase5/src/api/client.js
// Add console.log statements to track API calls
```

**FastAPI**:
```bash
uvicorn api.main:app --log-level debug
```

### Performance Tuning

**Slow Spark Jobs**
- Check partition count: Should match number of CPU cores
- Enable caching for reused DataFrames
- Monitor memory usage (--driver-memory, --executor-memory)

**Slow API Queries**
- Add database indexes (already defined for fact table)
- Check query plan with EXPLAIN
- Pagination for large result sets

**Dashboard Slow to Load**
- Reduce data refresh frequency
- Use map clustering for many markers
- Lazy load pages

---

## 20. Implementation Checklist

### Phase 1: Core Python ✅

- [x] NP1 - Dataset profiling
- [x] NP2 - Usage processing
  - [x] UsageProcessor class implemented
  - [x] Data loading and validation
  - [x] Aggregation by grid/hour
  - [x] Export functionality
- [x] NP3 - Alert generation
  - [x] NetworkAlertGenerator class
  - [x] Leave-one-out median baseline
  - [x] Alert rule engines (HIGH_ACTIVITY, ACTIVITY_DROP, ACTIVITY_SPIKE)
  - [x] Configurable thresholds
  - [x] Test coverage

### Phase 2: PySpark ETL ✅

- [x] SP1 - Distributed ingestion
  - [x] Read raw CSV files at scale
  - [x] Schema handling
- [x] SP2 - Data cleaning
  - [x] NetworkCleaner class
  - [x] Deduplication, validation
  - [x] Type coercion
- [x] SP3 - Aggregation
  - [x] NetworkAggregator class
  - [x] Grid/hour grouping
  - [x] Total calculations
  - [x] Multiple implementations (aggregation, aggregation2, aggregation3)
- [x] SP4 - Enrichment
  - [x] NetworkGeoEnricher class
  - [x] GeoJSON loading
  - [x] Spatial joins
  - [x] Coordinate extraction
- [x] SP5 - Performance
  - [x] Metrics calculation
  - [x] Data quality checks
- [x] SP6 - Storage
  - [x] Landing zone writing
  - [x] Idempotent writes
- [x] SP7 - Production pipeline
  - [x] TelecomPipeline orchestration
  - [x] Reusable job structure
  - [x] Logging and error handling

### Phase 3: Data Engineering & Airflow ⚠️ Partial

- [x] Data architecture design
  - [x] Landing, raw, processed, analytics zones
  - [x] Retention policies
- [x] Warehouse schema
  - [x] dim_time table
  - [x] dim_grid table
  - [x] fact_network_activity table
  - [x] ml_grid_features table
  - [x] network_risk_score table
- [x] DE2 - Ingestion
  - [x] File discovery
  - [x] Schema validation
- [x] DE6 - Warehouse loading
  - [x] Dimension population
  - [x] Fact table loading
  - [x] Index creation
  - [x] Validation checks
- [x] Airflow DAGs
  - [x] DE3 - Spark processing DAG
  - [x] DE7 - End-to-end orchestration DAG
  - [x] Logging and XCom
- ⚠️ Error handling & notifications (framework, config may vary)
- ⚠️ Backfill mechanisms (partial)

### Phase 4: FastAPI Backend ✅

- [x] FastAPI application setup
  - [x] CORS configuration
  - [x] Lifespan events (ML artifact loading)
  - [x] Health endpoint
- [x] API1 - Network summary
  - [x] GET /network/summary endpoint
  - [x] Optional as_of parameter
  - [x] Response schema
  - [x] Database queries
- [x] API2 - Grid activity
  - [x] GET /network/grid/{grid_id} endpoint
  - [x] Date, hour, as_of filters
  - [x] Default 24-hour trailing window
  - [x] Error handling (404 for missing data)
- [x] API3 - Hotspot alerts
  - [x] GET /hotspot/alerts endpoint
  - [x] Alert filtering
  - [x] Geographic coordinates
- [x] API4 - ML features
  - [x] GET /features/{grid_id}/{timestamp} endpoint
  - [x] Feature retrieval from warehouse
- [x] API5 - Predictions
  - [x] POST /prediction/risk endpoint
  - [x] Model and scaler loading
  - [x] Feature scaling and prediction
  - [x] Risk classification
- [x] Database models (SQLAlchemy)
- [x] Request/response validation (Pydantic)
- [x] Test coverage (pytest)

### Phase 5: React Dashboard ✅

- [x] Application structure
  - [x] React Router setup
  - [x] Page components
  - [x] Navigation
- [x] Pages
  - [x] Dashboard (overview)
  - [x] Grid Explorer (drill-down)
  - [x] Hotspots & Alerts
  - [x] Predictive Risk
- [x] Map component
  - [x] Milan grid visualization
  - [x] Activity/risk coloring
  - [x] Interactive features
- [x] Charts and visualizations
  - [x] Activity trends (Recharts)
  - [x] Alert summaries
- [x] API client
  - [x] Endpoint integration
  - [x] Error handling
  - [x] Loading states
- [x] Build and deployment (Vite)
- [x] Environment configuration

### Phase 6: Machine Learning ✅

- [x] ML2 - Feature engineering
  - [x] NetworkFeatures dataclass
  - [x] calculate_features() function
  - [x] Feature validation
  - [x] Feature generation pipeline
- [x] ML3 - Model training
  - [x] Data loading from warehouse
  - [x] Chronological train/test split
  - [x] Feature scaling (StandardScaler)
  - [x] Logistic Regression training
  - [x] Model evaluation (accuracy, precision, recall, ROC-AUC)
  - [x] Artifact serialization (.joblib files)
- [x] ML4 - Anomaly scoring
  - [x] Anomaly calculation logic
  - [x] Baseline comparison
- [x] ML6 - Batch scoring
  - [x] Model loading and validation
  - [x] Feature loading from warehouse
  - [x] Batch prediction with scaling
  - [x] Risk classification
  - [x] Result persistence
- [x] Tests
  - [x] Feature calculation tests
  - [x] Anomaly scorer tests
- [x] Model artifacts
  - [x] logistic_regression.joblib
  - [x] feature_scaler.joblib
  - [x] Evaluation metrics (ml3_evaluation.json)

### Phase 7: Claude/AI Integration ❌ Not Started

- [ ] Claude SDK setup
- [ ] API authentication
- [ ] Network intelligence tools (MCP)
  - [ ] get_grid_activity
  - [ ] get_alerts
  - [ ] get_grid_risk
- [ ] Investigation workflow
- [ ] FastAPI endpoint for investigations
- [ ] React UI for Claude insights
- [ ] Prompt engineering and testing
- [ ] Token usage monitoring

### Infrastructure & DevOps ⚠️ Partial

- [x] Local development setup
- [x] MySQL database configuration
- [x] Python virtual environment
- [x] Environment variables (.env)
- ⚠️ Docker configuration (may exist)
- ⚠️ CI/CD pipeline (GitHub Actions - not detailed)
- [ ] Production deployment guide
- [ ] High availability setup
- [ ] Disaster recovery plan
- [ ] Monitoring and alerting

### Documentation ⚠️ Partial

- [x] Code comments (minimal, as intended)
- [x] README files (phase5/README.md exists)
- [x] Configuration documentation
- ⚠️ API documentation (auto-generated by Swagger)
- ⚠️ Deployment runbook
- [x] This implementation documentation

---

## 21. Implementation Gaps & Recommendations

### Fully Implemented (Production Ready)

| Capability | Status | Evidence |
|-----------|--------|----------|
| Raw data ingestion | ✅ | SP1 reads CSV files |
| Data cleaning | ✅ | SP2/cleaning2.py implemented |
| Data aggregation | ✅ | SP3/aggregation3.py grid/hour totals |
| Geographic enrichment | ✅ | SP4/enrichment3.py + milano-grid.geojson |
| Warehouse schema | ✅ | dim_time, dim_grid, fact_network_activity in DE6 |
| Spark ETL pipeline | ✅ | SP7/telecom_pipeline.py orchestrates SP1-SP6 |
| Airflow orchestration | ✅ | de7_end_to_end_dag.py schedules execution |
| Rule-based alerts | ✅ | NP3/alert_detector.py (HIGH_ACTIVITY, etc.) |
| FastAPI REST API | ✅ | 5 endpoint groups (network, hotspot, features, prediction, health) |
| React NOC dashboard | ✅ | 4 pages + map + charts |
| ML feature generation | ✅ | ML2/features.py (6 features per grid/hour) |
| ML model training | ✅ | ML3 logistic regression trained, artifacts saved |
| ML batch scoring | ✅ | ML6/batch_scorer.py generates risk scores |
| Database integration | ✅ | SQLAlchemy ORM, Pydantic validation |
| Testing framework | ✅ | pytest tests, manual test scripts |

### Partially Implemented

| Capability | Status | Gap | Recommendation |
|-----------|--------|-----|-----------------|
| Error handling | ⚠️ | Basic try/catch, no comprehensive strategy | Add circuit breaker, retry logic, graceful degradation |
| Monitoring | ⚠️ | Logs only, no metrics/alerts | Integrate Prometheus, Grafana, set SLA thresholds |
| Backfill | ⚠️ | Framework exists, not fully tested | Document backfill procedure, test with historical dates |
| API authentication | ⚠️ | None currently | Add API key or JWT validation before production |
| Data encryption | ⚠️ | Not implemented | Encrypt database + data directory at rest |
| HTTPS | ⚠️ | No TLS in dev | Configure SSL certificates for production |
| Load balancing | ⚠️ | Single instance | Deploy API behind load balancer (nginx, ALB) |

### Not Implemented (From Source Document)

| Requirement | Status | Priority | Implementation Effort |
|------------|--------|----------|----------------------|
| Claude LLM integration | ❌ | High | 2-4 weeks |
| MCP tools for network data | ❌ | High | 1-2 weeks |
| Incident investigation workflow | ❌ | Medium | 1-2 weeks |
| WebSocket real-time dashboard updates | ❌ | Medium | 1 week |
| Advanced anomaly detection (isolation forest, etc.) | ❌ | Low | 2-3 weeks |
| Capacity planning module | ❌ | Low | 3-4 weeks |
| Network topology visualization | ❌ | Low | 2-3 weeks |
| Mobile app | ❌ | Low | 4-6 weeks |

### Gaps in Current Implementation

**1. API Authentication**
- **Current**: No auth required
- **Risk**: Any client can call endpoints
- **Fix**: Implement API key or OAuth2
  ```python
  from fastapi.security import HTTPBearer
  security = HTTPBearer()
  ```

**2. Secrets Management**
- **Current**: Database password in .env
- **Risk**: Credentials in version control (if not .gitignore'd)
- **Fix**: Use AWS Secrets Manager or HashiCorp Vault

**3. Data Validation Boundaries**
- **Current**: Activity ranges not enforced
- **Risk**: Garbage data accepted if numeric
- **Fix**: Add upper bounds, statistical outlier detection

**4. Rate Limiting**
- **Current**: No limits on API calls
- **Risk**: Denial of service possible
- **Fix**: Add slowapi or similar
  ```python
  from slowapi import Limiter
  limiter = Limiter(key_func=get_remote_address)
  ```

**5. Comprehensive Logging**
- **Current**: File logs only
- **Risk**: Hard to troubleshoot production issues
- **Fix**: Centralize logs (ELK, CloudWatch)

**6. Automated Testing in CI/CD**
- **Current**: Manual test execution
- **Risk**: Regressions slip through
- **Fix**: GitHub Actions workflow for pytest, lint, etc.

**7. Data Retention & Archival**
- **Current**: Retention policies defined but not enforced
- **Risk**: Disk space fills up
- **Fix**: Implement automated archival to S3/GCS

**8. Disaster Recovery**
- **Current**: Manual backups advised
- **Risk**: Data loss if not backed up
- **Fix**: Automated backup with point-in-time recovery

---

## 22. Final Capstone: Network Operations Control Room

### Integrated User Journey

A telecom Network Operations Center (NOC) operator begins their shift:

**1. Dashboard Login** (Phase 5)
- Opens `http://localhost:5173`
- Dashboard loads with latest network state
- Summary cards show:
  - Total activity: 5.2M units
  - Grids with activity: 9,847 / 10,000
  - Active alerts: 47
  - High-risk grids: 23

**2. Morning Briefing** (Network Summary API)
```
GET /network/summary
Response: {
  "timestamp": "2026-09-16T06:00:00Z",
  "total_grids": 10000,
  "total_activity": 5200000.0,
  "average_activity_per_grid": 520.0,
  "high_activity_grids": 234,
  "alert_count": 47,
  "anomaly_count": 12
}
```

**3. Check Hotspots** (Hotspot Alerts Page)
- Map displays Milan with color-coded hotspots
- Red zones = HIGH_ACTIVITY alerts
- Yellow zones = ACTIVITY_SPIKE detected
- Blue zones = ACTIVITY_DROP (potential issues)
- Clicking a hotspot shows:
  - Grid ID
  - Current activity: 4,500 units (2.5x baseline)
  - Recent trend
  - Associated risk score

**4. Grid-Level Investigation** (Grid Explorer)
- Operator searches for specific high-risk grid: "5432"
- Retrieves 24-hour activity chart
  - Peak at 14:00 (expected)
  - Unusual drop at 20:00
  - Spike at 23:00
- Compares to prior day (normal pattern)
- Identifies potential issue

**5. Predictive Context** (Predictive Risk Page)
- ML model predicts next-hour risk for grid 5432
- POST /prediction/risk:
  ```json
  {
    "grid_id": 5432,
    "feature_timestamp": "2026-09-16T06:00:00Z"
  }
  ```
- Response:
  ```json
  {
    "risk_score": 0.72,
    "risk_level": "HIGH",
    "model_version": "ml3-logistic-regression-v1",
    "explanation": "ML3 predicts 72% probability of next-hour activity >= 2000"
  }
  ```

**6. Claude Investigation** (Phase 7 - Future)
- Operator clicks "Get AI Insights" button
- Claude processes:
  - Current activity pattern
  - Historical baseline for this grid
  - Neighboring grid activity
  - Time-of-day seasonality
  - Recent alerts
- Claude returns analysis:
  ```
  Grid 5432 shows elevated activity compared to baseline.
  
  Observations:
  - 23:00 spike coincides with peak internet usage
  - Neighboring grids (5431, 5433) show similar patterns
  - This is typical for Friday evenings
  
  Recommendations:
  - Monitor for sustained elevated activity
  - Not indicative of fault or congestion
  - Expected to return to normal by 02:00
  ```

**7. Operational Decision**
- Operator decides: No immediate action needed
- Documents in NOC log: "Expected Friday evening peak in grid 5432"
- Sets reminder to check grid 5432 again at 02:00
- Continues monitoring other grids

**8. Automated Response** (Airflow + ML Pipeline)
- Background: Airflow DAG runs every hour
- Latest 24 hours of data ingested
- Spark ETL cleans, aggregates, enriches
- Warehouse updated with latest facts
- ML2 features recalculated for all grids
- ML3 predictions generated for all grids
- API returns fresh data for next refresh
- Dashboard auto-updates every 5 minutes

### Data Flow Through the System

```
Friday 23:00 Activity Event
        ↓
Raw CSV ingestion (data/raw/)
        ↓
Airflow DAG de7_end_to_end_dag.trigger()
        ├─ Check for new raw files
        ├─ Execute Spark ETL (SP1-SP7)
        │   ├─ Read raw: grid 5432, timestamp 23:00, activity 4500
        │   ├─ Clean: validate, deduplicate
        │   ├─ Aggregate: grid/hour total = 4500
        │   ├─ Enrich: add lat/lon from GeoJSON
        │   └─ Store: landing zone
        │
        ├─ Load to warehouse (DE6)
        │   ├─ Populate dim_time (2026-09-16 23:00)
        │   ├─ Populate dim_grid (5432 with coordinates)
        │   └─ Insert fact_network_activity row
        │
        ├─ Generate alerts (NP3)
        │   ├─ Calculate baseline for grid 5432 (1800, median of day)
        │   ├─ Check rules:
        │   │   - HIGH_ACTIVITY: 4500 >= 1800*2.0? YES → Alert
        │   │   - ACTIVITY_SPIKE: 4500 >= prev_hour*1.5? [depends on prev]
        │   └─ Export alerts.csv
        │
        ├─ Generate ML features (ML2)
        │   ├─ Load trailing 48 hours for grid 5432
        │   ├─ Calculate 6 features
        │   └─ Persist to ml_grid_features
        │
        └─ Batch score (ML6)
            ├─ Load features for all grids
            ├─ Scale with feature_scaler.joblib
            ├─ Predict with logistic_regression.joblib
            ├─ Grid 5432 score = 0.72 → HIGH risk
            └─ Insert into network_risk_score

Friday 23:05 - API Queries
        ↓
        GET /network/summary
            ← Aggregates fact table (updated at 23:00)
            → Returns total_activity: 5.2M, alerts: 47
        ↓
        GET /hotspot/alerts
            ← Queries alert table
            → Returns grid 5432 with HIGH_ACTIVITY reason
        ↓
        GET /network/grid/5432
            ← Queries facts for grid 5432, last 24 hours
            → Returns hourly records with coordinates
        ↓
        POST /prediction/risk (grid_id=5432, timestamp=23:00)
            ← Loads features from ml_grid_features
            ← Scales and predicts with ML3
            → Returns risk_score: 0.72, risk_level: HIGH

Friday 23:06 - Dashboard Display
        ↓
React components fetch API data
        ├─ Dashboard page
        │   ├─ Summary cards updated
        │   ├─ Activity trend chart refreshed
        │   └─ Alert count: 47
        │
        ├─ Hotspots & Alerts page
        │   ├─ Map renders with color-coded grids
        │   ├─ Grid 5432 highlighted in red (HIGH_ACTIVITY)
        │   ├─ Marker shows: "Grid 5432, Activity: 4500, Baseline: 1800"
        │   └─ Click to see full details
        │
        └─ Predictive Risk page
            ├─ Lists grids with HIGH risk scores
            ├─ Grid 5432: 0.72 probability
            └─ Explanation: "ML3 predicts 72% chance of next-hour activity >= 2000"

Friday 23:07+ - Operator Decisions
        ↓
Operator receives alerts and predictions
        ├─ Option A: Investigate immediately (click grid)
        ├─ Option B: Monitor and set reminder
        ├─ Option C (Future): Get Claude insights
        └─ Option D: Escalate to engineering team

Throughout the Night
        ↓
Airflow scheduler continues hourly execution
        ├─ 00:00 - Process data from Friday 00:00
        ├─ 01:00 - Process data from Friday 01:00
        ├─ ...
        └─ Each cycle updates warehouse, predictions, alerts
        ↓

Next Morning (Saturday)
        ↓
Operator logs in and sees:
        ├─ Grid 5432 activity returned to normal
        ├─ No recent alerts
        ├─ Risk score dropped to 0.15 (LOW)
        ├─ Confirms Friday event was expected spike
        └─ Concludes: Normal operation, no action needed
```

### System Resilience

**Designed Redundancies**
- Landing zone files persist if warehouse load fails
- Backfill capability allows reprocessing
- Idempotent operations prevent duplicate records
- ML artifacts frozen (old predictions still valid if model unavailable)

**Graceful Degradation**
- If Airflow down: Manual Spark job execution possible
- If database down: API returns last-known data from cache
- If React app down: API still serves data for other clients
- If Claude unavailable: Operator proceeds without AI insights

### Scalability Path

**Current Scale**
- 10,000 grids
- ~240K grid-hour records per day
- 4.1GB data directory
- Single-node Spark execution

**Scaling to 100,000 Grids**
- Multi-node Spark cluster
- Partition raw data by date + region
- Incremental warehouse updates
- Read replicas for API queries
- Caching layer (Redis) for API results

**Scaling to Real-Time**
- Kafka instead of batch files
- Streaming Spark pipeline
- WebSocket dashboard updates
- Event-driven alerting

### Operational Success Metrics

| Metric | Target | Current |
|--------|--------|---------|
| Pipeline SLA | 99.5% | [To Be Measured] |
| Data freshness | <1 hour | Hourly |
| API latency (p99) | <500ms | [To Be Measured] |
| Alert accuracy | >95% | [Comparing NP3 vs ML3] |
| Model retraining | Monthly | [As needed] |
| Disaster recovery | <4 hours | [To Be Tested] |

---

## Appendix: Quick Reference

### File Locations

| Component | Primary File | Related Files |
|-----------|--------------|---------------|
| Core Python | phase1/{np1,np2,np3}/ | Test scripts per phase |
| Spark Pipeline | phase2/spark/telecom_pipeline.py | SP1-SP6 components |
| Airflow DAGs | phase3/airflow/de7_end_to_end_dag.py | de3_spark_processing_dag.py |
| FastAPI | phase4/api/main.py | routers/, services/, db/, models/ |
| React | phase5/src/App.jsx | pages/, components/, api/client.js |
| ML | phase6/ml/features.py | train_model.py, batch_scorer.py |

### Key Commands

```bash
# Data pipeline
python phase2/spark/run_telecom_pipeline.py --input data/raw --output data --reference data/reference/milano-grid.geojson --log-dir data/logs

# Model training
python phase6/ml/train_model.py

# Start API
cd phase4 && uvicorn api.main:app --reload --port 8000

# Start dashboard
cd phase5 && npm run dev

# Run tests
cd phase4 && pytest api/tests/ -v
cd phase6 && pytest ml/test_*.py -v

# Airflow
airflow dags list
airflow dags trigger de7_end_to_end_dag
airflow tasks logs de7_end_to_end_dag task_id
```

### Important Ports

- MySQL: 3306
- FastAPI: 8000
- React: 5173
- Airflow Web: 8080

### Important Paths

- Raw data: `data/raw/`
- Processed data: `data/landing/`, `data/processed/`
- Warehouse: MySQL database
- ML artifacts: `phase6/ml/artifacts/`
- Logs: `data/logs/`
- Reference: `data/reference/milano-grid.geojson`

---

**Document Version**: 1.0
**Last Updated**: 2026-09-16
**Status**: Complete (Phase 1-6), Phase 7 Planned


# DE1 — Telecom Data Architecture Design

## 1. Purpose

This document defines the end-to-end data architecture for the telecom network intelligence platform.

The architecture supports daily telecom activity files, static geographic reference data, batch processing, analytical storage, machine-learning scoring, API access, and React-based presentation.

The design separates incoming data, immutable raw data, reference data, processed analytical data, warehouse data, and operational pipeline information so that each layer has a clear responsibility.

---

## 2. Architecture Overview

```text
                         TELECOM DATA ARCHITECTURE

 Daily Telecom CSV
        |
        v
 +--------------+
 |   LANDING    |
 | Incoming     |
 | files        |
 +--------------+
        |
        v
 +--------------+
 |     RAW      |
 | Immutable    |
 | accepted CSV |
 +--------------+
        |
        v
 +--------------+
 |  PROCESSED   |
 | Spark output |
 | Parquet      |
 +--------------+
        |
        v
 +----------------------+
 | ANALYTICS / WAREHOUSE|
 | fact_network_activity|
 | dim_grid             |
 | dim_time             |
 +----------------------+
        |
        +----------------------+
        |                      |
        v                      v
   ML2 Features          Analytics Queries
        |
        v
   ML3 Prediction
        |
        +------------------+
        |                  |
        |                  v
        |             ML4 Anomaly
        |             Detection
        |                  |
        +--------+---------+
                 |
                 v
       network_risk_scores
                 |
                 v
             FastAPI
                 |
                 v
              React


 Static GeoJSON Reference
        |
        v
 +--------------+
 |  REFERENCE   |
 | Grid / zone  |
 | definitions  |
 +--------------+
        |
        +---------------------> Warehouse / processing
```

The static GeoJSON is maintained separately from the daily telecom event-file flow. It is reference data describing the geographic grid rather than an incoming telecom activity dataset.

---

## 3. Architecture Layers

| Layer                 | Responsibility                                                     | Primary Data                         |
| --------------------- | ------------------------------------------------------------------ | ------------------------------------ |
| Landing               | Receive incoming telecom files without transforming their contents | Daily CSV                            |
| Raw                   | Preserve accepted source files immutably                           | Accepted CSV                         |
| Rejected              | Isolate files that fail validation                                 | Invalid CSV/files + rejection reason |
| Reference             | Store static geographic definitions                                | GeoJSON                              |
| Processed             | Store Spark-processed analytical data                              | Parquet                              |
| Analytics / Warehouse | Provide structured analytical and ML source data                   | Fact and dimension tables            |
| ML                    | Generate features, predictions, and anomaly results                | ML feature and risk tables           |
| API                   | Expose curated results to application consumers                    | FastAPI responses                    |
| Presentation          | Display network intelligence to users                              | React application                    |
| Logs / Audit          | Record pipeline execution and data-quality events                  | Run/audit records                    |

---

## 4. Component Responsibilities

Each component has one primary responsibility.

### Landing

**Responsibility:** receive incoming daily telecom CSV files.

Landing is the entry point for source files. Files are not treated as trusted until validation has completed.

### Raw

**Responsibility:** preserve accepted source files as immutable source records.

The raw layer provides reproducibility and allows downstream processing to be traced back to the accepted source file.

### Rejected

**Responsibility:** isolate files that fail ingestion or quality validation.

Rejected files remain available for troubleshooting without entering the trusted processing flow.

### Reference

**Responsibility:** provide static geographic grid definitions.

The GeoJSON reference is maintained independently from daily telecom event ingestion.

### Spark Processing

**Responsibility:** transform validated raw telecom data into the processed analytical representation.

Spark is responsible for processing work; orchestration should not contain the data-cleaning or aggregation logic itself.

### Analytics / Warehouse

**Responsibility:** provide structured, queryable telecom activity data for analytics and downstream ML.

The current warehouse uses:

* `fact_network_activity`
* `dim_grid`
* `dim_time`

The fact table stores network activity while geographic and temporal attributes are represented through dimensions.

### ML Feature Generation

**Responsibility:** generate persisted ML2 features from warehouse activity.

The existing ML2 process generates the six features used by the ML3 model.

### ML3 Prediction

**Responsibility:** predict next-hour high-activity risk using the frozen trained model.

The current ML3 model uses:

* `avg_activity`
* `activity_growth`
* `active_hours`
* `peak_ratio`
* `variability`
* `internet_share`

The model predicts the probability of high activity at `t+1`.

### ML4 Anomaly Detection

**Responsibility:** identify unusual current-hour activity relative to the historical grid/hour-of-day baseline.

ML4 uses the warehouse activity data and the validated leave-one-out median baseline methodology.

### ML6 Risk Output

**Responsibility:** combine ML3 prediction results and ML4 anomaly results into the curated `network_risk_scores` output.

The current output contains:

* `risk_score`
* `risk_level`
* `model_version`
* `anomaly_score`
* `anomaly_direction`
* `is_anomaly`
* `anomaly_reason`

### FastAPI

**Responsibility:** expose curated network intelligence to application consumers.

FastAPI provides the application-facing interface to the analytical and ML outputs.

### React

**Responsibility:** present network intelligence to end users.

React consumes the API rather than directly querying the warehouse.

### Logs / Pipeline Status

**Responsibility:** record pipeline execution and data-quality status.

The final orchestration layer will maintain machine-readable pipeline status that can later be exposed through the API.

---

## 5. Data Flow

The normal trusted data path is:

```text
Source CSV
   |
   v
Landing
   |
   | schema + quality validation
   |
   +---- invalid ----> Rejected
   |
   v
Raw
   |
   v
Spark Processing
   |
   v
Processed Parquet
   |
   v
Analytics / Warehouse
   |
   +----> ML2 Feature Generation
   |             |
   |             v
   |          ML3 Prediction
   |             |
   |             +------+
   |                    |
   +----> ML4 ----------+
                        |
                        v
              network_risk_scores
                        |
                        v
                     FastAPI
                        |
                        v
                      React
```

The static geographic reference follows a separate path:

```text
Static GeoJSON
      |
      v
REFERENCE
      |
      v
Grid / zone definition
      |
      v
Warehouse / processing
```

The reference dataset is not repeatedly ingested as if it were a daily telecom activity file.

---

## 6. Quality Gates

Quality gates prevent invalid or unreliable data from progressing through the pipeline.

### Gate 1 — File Detection

Confirm that an expected telecom input file has been detected.

Expected naming pattern:

```text
sms-call-internet-mi-*.csv
```

### Gate 2 — Schema Validation

Confirm that required columns exist and that the incoming structure is compatible with the processing pipeline.

### Gate 3 — Minimum Data Quality

Validate basic data quality before accepting the file into the trusted raw zone.

Examples include:

* valid timestamps
* valid grid identifiers
* valid activity values
* absence of prohibited negative activity
* required fields present

### Gate 4 — Processing Success

Spark processing must complete successfully before downstream warehouse loading occurs.

### Gate 5 — Warehouse Quality

Validate warehouse integrity after loading.

Important checks include:

* expected row counts
* duplicate `(grid_id, timestamp)` records
* referential integrity
* expected grid coverage
* timestamp coverage

### Gate 6 — ML Output Quality

Validate that ML outputs contain the expected number of records and valid prediction/anomaly fields.

### Gate 7 — Pipeline Health

Record machine-readable execution status including task outcomes and an analytics `AS_OF` timestamp.

---

## 7. Static Geographic Reference

The geographic GeoJSON is treated as a static reference dataset.

It defines the geographic grid/zone information required to interpret telecom activity spatially.

It is deliberately separated from the daily event-file ingestion flow because:

1. it does not represent daily telecom activity;
2. it does not need to be re-ingested with every daily file;
3. it provides relatively stable reference information;
4. warehouse grid definitions can be built from the reference independently.

The reference layer therefore remains a separate architectural zone.

---

## 8. Analytics and ML Outputs

The warehouse provides the structured source for downstream intelligence.

Current ML flow:

```text
Warehouse
    |
    v
ML2 Features
    |
    v
ML3 Prediction
    |
    +----------------+
                     |
Warehouse Activity -> ML4 Anomaly
                     |
                     v
             ML6 Combination
                     |
                     v
          network_risk_scores
```

The current ML6 output is designed so that predictive risk and anomaly evidence remain distinguishable.

`risk_score` represents the ML3 prediction probability.

`risk_level` represents the ML3 classification based on the configured threshold.

`anomaly_score`, `anomaly_direction`, `is_anomaly`, and `anomaly_reason` provide the ML4 anomaly context.

---

## 9. Application Consumption

The application architecture follows:

```text
MySQL / Analytics
       |
       v
     FastAPI
       |
       v
      React
```

The React application does not directly access the warehouse.

FastAPI provides the controlled application interface and allows analytical and ML results to be exposed consistently.

---

## 10. Pipeline Health and Operational Status

Pipeline execution information will be stored separately from the analytical fact data.

The operational status record will support later monitoring and API exposure.

The planned status information includes:

* `run_id`
* run timestamp
* per-task status
* rows in
* rows rejected
* nulls handled
* rows published
* maximum analytics timestamp (`AS_OF`)

This operational information will support troubleshooting, reruns, and downstream API visibility.

---

## 11. Non-Goals

The architecture does **not** claim to measure or provide:

* network capacity
* network throughput
* network utilization

The available dataset represents telecom activity rather than infrastructure capacity or utilization measurements.

Therefore, risk and anomaly results must be interpreted as activity-based intelligence, not as direct measurements of network capacity or utilization.

---

## 12. Architecture Design Principles

The architecture follows these principles:

1. **Separate source receipt from trusted data.**
2. **Preserve accepted source data immutably.**
3. **Keep static reference data separate from daily event ingestion.**
4. **Keep processing logic inside Spark rather than the orchestration layer.**
5. **Use structured warehouse data for analytics and ML.**
6. **Keep ML3 prediction and ML4 anomaly evidence distinguishable.**
7. **Expose curated outputs through FastAPI.**
8. **Keep presentation concerns inside React.**
9. **Use quality gates between major pipeline stages.**
10. **Record machine-readable pipeline health for operational visibility.**

---

## 13. DE1 Deliverables

The DE1 architecture establishes the foundation for the remaining data-engineering work:

```text
DE1  Architecture Design
 |
 +--> DE2  Landing → Raw ingestion
 |
 +--> DE3  Spark processing
 |
 +--> DE4  Batch vs Streaming decision
 |
 +--> DE5  Storage strategy
 |
 +--> DE7  End-to-End Airflow orchestration
 |
 +--> DE8  Reliability controls
```

The completed DE6 warehouse and ML2–ML6 components fit into this architecture as downstream analytical and machine-learning consumers.

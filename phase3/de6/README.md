# DE6 — MySQL Analytics Warehouse

## Purpose

DE6 converts the existing Spark `hourly_grid_summary` output into a
MySQL analytics warehouse using a minimal star schema.

The warehouse contains:

- `fact_network_activity`
- `dim_time`
- `dim_grid`

MySQL is the only supported warehouse database for this implementation.

---

## Source Data

### Activity source

The activity source is the existing SP3:

`hourly_grid_summary` Parquet checkpoint.

Expected columns:

- timestamp
- grid_id
- sms_in
- sms_out
- call_in
- call_out
- internet_activity
- total_sms
- total_calls
- total_activity
- date
- hour
- day_of_week

The source grain is:

`timestamp + grid_id`

---

## Reference Data

The Milan grid reference is:

`data/reference/milano-grid.geojson`

The authoritative grid identifier is:

`properties.cellId`

The top-level GeoJSON `id` is not used as the grid identifier.

The reference contains 10,000 Milan grid features.

---

# Warehouse Model

## dim_time

Stores one row per distinct timestamp.

Columns:

- time_key
- timestamp
- date
- hour
- day_of_week

`time_key` is generated from:

`YYYYMMDDHH`

Example:

`2013-11-01 05:00:00`

becomes:

`2013110105`

---

## dim_grid

Stores grid-level reference information.

Columns:

- grid_key
- grid_id
- latitude
- longitude
- geometry_reference

The dimension is populated once from the static Milan GeoJSON.

Expected full dimension:

10,000 rows.

`grid_id` is unique.

---

## fact_network_activity

Stores network activity measures.

Columns:

- time_key
- grid_key
- sms_in
- sms_out
- call_in
- call_out
- internet_activity
- total_sms
- total_calls
- total_activity

The fact table does not contain geometry.

Primary key:

`time_key + grid_key`

---

# Grain

The fact table has exactly one row per:

`timestamp + grid_id`

The fact row count must equal the SP3
`hourly_grid_summary` row count.

A dimension join must not create fan-out.

---

# Indexing

Indexes exist on:

- dim_time.date
- dim_time.hour
- dim_grid.grid_id
- fact_network_activity.grid_key + time_key
- fact_network_activity.time_key

These support common filtering and joining operations.

---

# Validation

DE6 validates:

1. Required tables exist.
2. `dim_grid` contains 10,000 rows.
3. `dim_grid.grid_id` contains no duplicates.
4. The fact table contains no geometry.
5. The fact table contains no duplicate time/grid keys.
6. Foreign keys do not contain orphan records.
7. Fact row count matches the Spark source.
8. Top-grid SQL query executes.
9. Hourly trend SQL query executes.
10. Internet-heavy-window query executes.
11. Required indexes exist.

---

# Failure Conditions

The DE6 job fails if:

- the SP3 Parquet checkpoint is missing;
- the reference GeoJSON is missing;
- required source columns are missing;
- the source contains zero rows;
- the GeoJSON is not a FeatureCollection;
- the GeoJSON does not contain exactly 10,000 features;
- a feature is missing `properties.cellId`;
- a grid ID is outside 1–10,000;
- duplicate grid IDs exist;
- dimension joins create fact fan-out;
- the fact row count differs from the Spark source;
- duplicate fact keys are detected;
- MySQL cannot be reached;
- MySQL table creation fails;
- MySQL loading fails;
- warehouse validation fails.

---

# Running

From the project root:

```powershell
cd D:\Project1

python phase3\de6\run_de6.py
# 🚕 Uber Trip Analytics Platform

> A production-grade, end-to-end ELT platform that ingests 100,000 NYC taxi trip records, models them dimensionally, enforces data quality, orchestrates the pipeline with Airflow, and surfaces operational insights through an interactive Power BI dashboard.

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8?logo=snowflake)
![dbt](https://img.shields.io/badge/dbt-1.12-FF694B?logo=dbt)
![Airflow](https://img.shields.io/badge/Apache_Airflow-2.9.1-017CEE?logo=apache-airflow)
![Power BI](https://img.shields.io/badge/Power_BI-Dashboard-F2C811?logo=powerbi)

---

##  Table of Contents

- [Business Problem](#business-problem)
- [Architecture](#architecture)
- [Key Engineering Decisions](#key-engineering-decisions)
- [Tech Stack](#tech-stack)
- [Dataset](#dataset)
- [Snowflake Schema Design](#snowflake-schema-design)
- [Ingestion Pipeline](#ingestion-pipeline)
- [dbt Transformation Layer](#dbt-transformation-layer)
- [Data Quality Framework](#data-quality-framework)
- [Airflow Orchestration](#airflow-orchestration)
- [Dashboard](#dashboard)
- [Project Structure](#project-structure)
- [How to Run Locally](#how-to-run-locally)
- [Decision Records](#decision-records)

---

## Business Problem

> Ride-hailing operations and business stakeholders need a centralized platform to monitor trip demand patterns, analyze revenue drivers, and evaluate trip efficiency — enabling data-driven decisions around fleet deployment, pricing strategy, and operational performance.

**Three analytical pillars this drives:**

1. **Demand** — When and where do people ride? Which hours, days, and locations see peak activity?
2. **Revenue** — What drives fare amounts, tips, and total revenue? How do payment type and rate code affect earnings?
3. **Efficiency** — How efficient are trips? What is the relationship between distance, duration, and fare?

This statement was written **before** inspecting the dataset or designing any tables. Every architectural decision flows from these three questions — not from the shape of the source data.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    NYC TLC CSV Dataset                              │
│              100,000 yellow taxi trip records                       │
│                    (March 1-10, 2016)                               │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Python Ingestion Layer
                            │  • Reads CSV with pandas
                            │  • Adds metadata columns (raw_id, batch_id)
                            │  • Bulk loads via write_pandas()
                            │  • Watermark tracking (resumable loads)
                            │  • Audit logging per run
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   Snowflake — RAW Schema                            │
│              TRIPS_STAGED (flat typed columns)                      │
│              100,000 rows, all strings preserved                    │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  dbt Staging Layer (6 models → views)
                            │  • Cast strings to proper types
                            │  • Derive trip_duration, fare_per_mile
                            │  • Flag bad data (zero distance, negative fares)
                            │  • Extract distinct dimension entities
                            │  • Build static lookup tables from data dictionary
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                   Snowflake — STAGING Schema                        │
│   stg_trips · stg_datetime · stg_locations                         │
│   stg_vendors · stg_rate_codes · stg_payment_types                 │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  dbt Marts Layer (6 models → tables)
                            │  • Add md5 surrogate keys
                            │  • Borough classification from coordinates
                            │  • Filter 977 invalid trips to quarantine
                            │  • Join dimensions to fact
                            │  • 19 automated data quality tests
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Snowflake — MARTS Schema                         │
│   fact_trip · dim_datetime · dim_location                          │
│   dim_vendor · dim_rate_code · dim_payment_type                    │
│                   99,023 valid trips                                │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    Power BI Dashboard                               │
│   Revenue KPIs · Demand by time/location · Efficiency metrics      │
└─────────────────────────────────────────────────────────────────────┘

         ↑ Entire pipeline orchestrated by Apache Airflow ↑
    DAG: extract_and_load → dbt_staging → dbt_marts → dbt_test → notify
```

---

## Tech Stack

| Component | Technology | Version |
|---|---|---|
| Ingestion | Python | 3.12 |
| Data loading | snowflake-connector-python[pandas] | 4.6+ |
| Data Warehouse | Snowflake | Trial (X-Small warehouse) |
| Transformation | dbt-snowflake | 1.12.0 |
| Orchestration | Apache Airflow | 2.9.1 |
| Metadata DB | PostgreSQL | 16.10 |
| Visualization | Power BI | Desktop + Service |
| Source Dataset | NYC TLC Yellow Taxi | March 2016 |
| Version Control | Git + GitHub | — |

---

## Dataset

**Source:** NYC Taxi & Limousine Commission (TLC) Yellow Taxi Trip Records
**Reference:** [Darshil Parmar's Uber ETL Project](https://github.com/darshilparmar/uber-etl-pipeline-data-engineering-project)

| Property | Value |
|---|---|
| Rows | 100,000 trips |
| Size | 16MB CSV |
| Date range | March 1–10, 2016 |
| Columns | 18 (VendorID, timestamps, coordinates, fares, etc.) |

**Encoded columns decoded:**

| Column | Code | Meaning |
|---|---|---|
| VendorID | 1 | Creative Mobile Technologies (CMT) |
| VendorID | 2 | VeriFone Inc. |
| RatecodeID | 1 | Standard Rate |
| RatecodeID | 2 | JFK Airport |
| RatecodeID | 3 | Newark Airport |
| payment_type | 1 | Credit Card |
| payment_type | 2 | Cash |
| payment_type | 3 | No Charge |
| payment_type | 4 | Dispute |

**Data quality issues found in source:**

| Issue | Count | Action |
|---|---|---|
| Zero distance trips | 584 | Filtered to quarantine |
| Zero passenger trips | 3 | Filtered to quarantine |
| Negative fare amounts | 70 | Filtered to quarantine |
| Invalid timestamps | 0 | None found |
| Total rejected | 977 (0.98%) | 99,023 valid trips remain |

---

## Snowflake Schema Design

Six schemas with deliberate separation of concerns:

| Schema | Purpose | Who writes | Who reads |
|---|---|---|---|
| `RAW` | Raw CSV rows, all strings | Python ingestion | dbt staging |
| `STAGING` | Cleaned, typed, flattened records | dbt | dbt marts |
| `MARTS` | Dimensional model for BI | dbt | Power BI |
| `AUDIT_LOGS` | Pipeline run history | Python audit.py | Engineers |
| `QUARANTINE` | Invalid records (design ready) | dbt/pipeline | Engineers |
| `CONTROL` | Watermarks and pipeline state | Python watermark.py | Python watermark.py |

---

## Ingestion Pipeline

### Files (`ingestion/`)

```
ingestion/
├── config.py          # Loads credentials from environment variables
├── logger.py          # Structured logging — INFO/WARNING/ERROR to console + file
├── loader.py          # Reads CSV, builds DataFrame, bulk loads via write_pandas()
├── watermark.py       # Tracks last processed row in CONTROL.WATERMARKS
├── audit.py           # Writes run outcomes to AUDIT_LOGS.PIPELINE_RUNS
└── run_pipeline.py    # Orchestrates all components end-to-end
```

### Idempotency
The table is `TRUNCATE`d before every load. Running the pipeline twice produces the same result — exactly 100,000 rows in `RAW.TRIPS_STAGED`, never 200,000.

---

## dbt Transformation Layer

### Models (12 total)

**Staging layer (views) — reads from RAW:**

| Model | Grain | Purpose |
|---|---|---|
| `stg_trips` | 1 row per raw CSV record | Cast types, derive metrics, flag bad data |
| `stg_datetime` | 1 row per distinct pickup timestamp | Extract calendar/time attributes |
| `stg_locations` | 1 row per distinct coordinate pair | Combine pickup + dropoff locations |
| `stg_vendors` | 2 rows (hardcoded) | VendorID lookup from data dictionary |
| `stg_rate_codes` | 6 rows (hardcoded) | RatecodeID lookup from data dictionary |
| `stg_payment_types` | 6 rows (hardcoded) | payment_type lookup from data dictionary |

**Marts layer (tables) — reads from staging:**

| Model | Rows | Purpose |
|---|---|---|
| `dim_datetime` | 38,332 | Calendar dimension with time-of-day buckets |
| `dim_location` | 194,957 | Coordinate dimension with borough classification |
| `dim_vendor` | 2 | Vendor dimension |
| `dim_rate_code` | 6 | Rate code dimension |
| `dim_payment_type` | 6 | Payment type dimension |
| `fact_trip` | 99,023 | Central fact table — one row per valid trip |

### Dimensional Model

```
                    dim_datetime
                    (38,332 rows)
                         │ datetime_key
                         │
dim_vendor ──────────────┤
(2 rows)                 │
                     fact_trip
                    (99,023 rows)
                         │
dim_payment_type ────────┤
(6 rows)                 │
                         │ pickup_location_key  (active)
dim_location ────────────┤ dropoff_location_key (inactive)
(194,957 rows)
```



## Airflow Orchestration

**DAG:** `uber_trip_analytics`
**Schedule:** `@daily`
**Executor:** LocalExecutor
**Metadata DB:** PostgreSQL 16

```
extract_and_load      BashOperator
        │             Activates ingestion venv, runs run_pipeline.py
        │             Credentials injected via Airflow Variables
        ▼
dbt_run_staging       BashOperator
        │             dbt run --select staging
        ▼
dbt_run_marts         BashOperator
        │             dbt run --select marts
        ▼
dbt_test              BashOperator
        │             dbt test (all 19 tests must pass)
        ▼
notify_success        PythonOperator
                      Logs completion with timestamp
```

**Why staging and marts are separate Airflow tasks:**
If marts fail, Airflow retries only from marts — not from staging. Granular task boundaries make retries cheaper and failure diagnosis faster.

---

## Dashboard

**Tool:** Power BI Desktop + Power BI Service
**Data source:** DirectQuery → Snowflake MARTS schema

**Key metrics:**
- Total Revenue: $1,618,145
- Total Trips: 99,023
- Average Fare: $16.34
- Average Trip Distance: 3.05 miles
- Average Duration: 14.4 minutes
- Average Tip: 14%

**Insights from data:**
- **Manhattan dominates** — 91,478 trips (92.4% of all trips)
- **Queens airport runs are most expensive** — avg $36.45 (JFK/LaGuardia)
- **Midday is busiest** — 44,442 trips vs 20,977 Late Night
- **Late Night has highest fares** — $17.40 avg (longer trips, surcharges)
- **Credit card is dominant payment** — enables tip tracking

---

## Project Structure

```
uber-trip-analytics-platform/
├── ingestion/
│   ├── config.py
│   ├── logger.py
│   ├── loader.py
│   ├── watermark.py
│   ├── audit.py
│   └── run_pipeline.py
├── uber_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── stg_trips.sql
│   │   │   ├── stg_datetime.sql
│   │   │   ├── stg_locations.sql
│   │   │   ├── stg_vendors.sql
│   │   │   ├── stg_rate_codes.sql
│   │   │   └── stg_payment_types.sql
│   │   └── marts/
│   │       ├── marts.yml
│   │       ├── dim_datetime.sql
│   │       ├── dim_location.sql
│   │       ├── dim_vendor.sql
│   │       ├── dim_rate_code.sql
│   │       ├── dim_payment_type.sql
│   │       └── fact_trip.sql
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── tests/
│       ├── assert_no_negative_fares.sql
│       └── assert_dropoff_after_pickup.sql
├── airflow/
│   └── dags/
│       └── uber_etl_dag.py
├── data/
│   └── raw/              # gitignored — download separately
├── docs/
│   ├── decision_records.md
│   └── source_analysis.md
├── logs/                 # gitignored
├── .env                  # gitignored — never committed
├── .env.example
├── .gitignore
├── airflow_env.sh
├── requirements.txt
└── README.md
```

---

## How to Run Locally

### Prerequisites
- Python 3.12
- Snowflake account (free trial works)
- PostgreSQL (for Airflow metadata)

### 1. Clone and set up environment

```bash
git clone https://github.com/LEAKONO/uber-trip-analytics-platform.git
cd uber-trip-analytics-platform

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Download the dataset

```bash
mkdir -p data/raw
curl -L "https://github.com/darshilparmar/uber-etl-pipeline-data-engineering-project/raw/refs/heads/main/data/uber_data.csv" \
  -o data/raw/uber_data.csv
```

### 3. Configure environment

```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### 4. Set up Snowflake

Run `docs/snowflake_setup.sql` in a Snowflake worksheet to create the database, schemas, and tables.

### 5. Run ingestion

```bash
python3 -m ingestion.run_pipeline
```

### 6. Run dbt

```bash
dbt run --project-dir uber_dbt
dbt test --project-dir uber_dbt
```

### 7. Start Airflow

```bash
python3 -m venv venv-airflow
source venv-airflow/bin/activate
pip install "apache-airflow==2.9.1" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.12.txt"
pip install psycopg2-binary

source airflow_env.sh
airflow db migrate
airflow standalone
```

## Author

**Emmanuel Leakono**

Data Engineer · Kenya
---


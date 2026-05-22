# 🏠 Airbnb Streaming Data Engineering Pipeline

> A production-grade streaming data pipeline built on AWS, Snowflake, and dbt — featuring real-time ingestion, a three-layer Medallion architecture, SCD-2 history tracking, and automated orchestration via Apache Airflow.

---

## 📌 What makes this project different

Most portfolio projects download a clean CSV and run some SQL on it. This project does the opposite — it **intentionally generates dirty data** with 10 realistic data quality problems, then builds a transformation pipeline that systematically detects, flags, and fixes every one of them.

The result is a pipeline that mirrors what real production systems actually look like: messy at the source, clean at the analytical layer, with full audit trails of what was wrong and how it was fixed.

---

## 🏗️ Architecture

```
EventBridge (every 30 min)
        │
        ▼
Lambda (Python 3.12) ── Faker + 10 dirty patterns ── 185 records/run
        │
        ├──► airbnb-events-stream      (3 shards)  → listing_events
        ├──► airbnb-transactions-stream (2 shards) → bookings + reviews
        └──► airbnb-dimensions-stream  (1 shard)   → listings + hosts + guests + calendar
                    │
                    ▼
        Kinesis Firehose (60s buffer, dynamic partitioning on entity_type)
                    │
                    ▼
        S3: s3://airbnb-de-project-nirmal/airbnb/{entity_type}/year=/month=/day=/
                    │
                    ▼  (SQS notification on PUT)
        Snowpipe (AUTO_INGEST=TRUE, 7 pipes, shared SQS queue)
                    │
                    ▼
        Snowflake AIRBNB_DE.RAW (7 tables — all VARCHAR)
                    │
                    ▼  (Airflow DAG every 12 hours)
        dbt Bronze → dbt Silver → dbt Snapshot (SCD-2) → dbt Gold
                    │
                    ▼
        Star Schema: 3 Dimensions + 1 Date + 3 Facts + 1 OBT
```

### Architecture Diagram

> 📸 **[INSERT: Full architecture diagram here]**
> *(Screenshot of the complete AWS → Snowflake → dbt → Airflow flow)*

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Scheduling | AWS EventBridge | Triggers Lambda every 30 minutes |
| Compute | AWS Lambda (Python 3.12) | Synthetic data generation and Kinesis publishing |
| Streaming | AWS Kinesis Data Streams (3) | Durable ordered record storage, 7-day retention |
| Delivery | AWS Kinesis Firehose (3) | 60s buffering + dynamic S3 partitioning by entity |
| Storage | AWS S3 | Raw JSON files partitioned by entity and date |
| Auto-ingest | Snowpipe + SQS | Event-driven loading from S3 into Snowflake RAW |
| Data Warehouse | Snowflake | RAW → BRONZE → SILVER → GOLD → SNAPSHOTS schemas |
| Transformation | dbt Core 1.11 | Medallion architecture with incremental models |
| dbt Adapter | dbt-snowflake | Snowflake-specific SQL generation |
| Orchestration | Apache Airflow 2.9 | Scheduled DAG with test gates between layers |
| Python Env | uv | Fast Python + venv management |

---

## 📊 Pipeline Metrics

| Metric | Value |
|---|---|
| RAW tables | 7 |
| Total columns across RAW | 138 |
| Records generated per Lambda run | ~185 |
| Records per hour (at full throughput) | ~13,310 |
| dbt models | 21 |
| dbt data quality tests | 62 |
| Dirty data patterns designed in | 10 |
| Snowpipe latency | < 10 seconds |
| Pipeline schedule | Every 12 hours |

---

## 🦠 The 10 Intentional Dirty Data Patterns

This is the core differentiator of the project. Every pattern below was deliberately injected into the generator and is systematically fixed in the Silver layer.

| # | Pattern | Dirty Example | Silver Fix |
|---|---|---|---|
| 1 | Boolean chaos | `t / f / Y / N / 1 / 0 / yes / no` | `CASE WHEN LOWER(TRIM(...)) IN (...)` |
| 2 | Price string formatting | `$1,200.00 / $120 / 120.5` | `REGEXP_REPLACE + TRY_TO_DECIMAL` |
| 3 | Rating scale mismatch | `4.8` (0–5 scale) vs `96` (0–100 scale) | `CASE WHEN val > 5 THEN val / 20.0` |
| 4 | European decimal comma | `4,5` instead of `4.5` | `REPLACE(',', '.') before casting` |
| 5 | Date format variety | `2019-01-15 / 01/15/2019 / January 15th, 2019` | `REGEXP_REPLACE ordinals + TRY_TO_DATE AUTO` |
| 6 | Amenities encoding chaos | `["WiFi","Pool"] / WiFi\|Pool / WiFi, Pool` | `TRY_PARSE_JSON → SPLIT → TO_ARRAY` |
| 7 | Pre-computed wrong nights | `nights_count = 5` but `checkout - checkin = 7` | Recalculate + `nights_count_corrected` flag |
| 8 | Price reconciliation failure | `total_price ≠ base + fees + taxes` (20% of records) | Recalculate from components + `is_price_mismatch` flag |
| 9 | Logical invalids | `check_out < check_in` / `num_children = -1` | NULL + `is_date_valid = FALSE` flag |
| 10 | Micro-batch duplicates | Same record in two Firehose buffer windows | `ROW_NUMBER() PARTITION BY id ORDER BY _loaded_at DESC` |

---

## 📁 Project Structure

```
airbnb-streaming-de-project/
│
├── lambda/
│   ├── generator.py          # Faker-based data generator with 10 dirty patterns
│   └── lambda_handler.py     # AWS Lambda entry point
│
├── airbnb_de_project_dbt/
│   ├── models/
│   │   ├── bronze/           # 7 incremental models — dedup + watermark
│   │   ├── silver/           # 7 incremental models — all dirty patterns fixed
│   │   └── gold/             # 3 dimensions + 3 facts + 1 OBT
│   ├── snapshots/
│   │   ├── hosts_snapshot.sql     # SCD-2 on is_superhost, response_rate
│   │   └── listings_snapshot.sql  # SCD-2 on price_per_night, cancellation_policy
│   ├── seeds/
│   │   └── dim_date.csv      # 3,288 rows, 2020–2028
│   ├── tests/
│   │   ├── assert_booking_dates_valid.sql
│   │   ├── assert_ratings_in_range.sql
│   │   └── assert_revenue_positive.sql
│   ├── macros/
│   │   └── generate_schema_name.sql  # prevents dbt_schema_bronze naming
│   ├── dbt_project.yml
│   └── packages.yml
│
├── airflow_dags/
│   └── airbnb_dbt_pipeline.py  # 7-task DAG with test gates
│
├── profiles_example.yml       # Snowflake connection template (no credentials)
└── README.md
```

---

## 🥇 Gold Layer — Star Schema Design

```
                    ┌─────────────┐
                    │  dim_date   │
                    │ 3,288 rows  │
                    └──────┬──────┘
                           │
┌─────────────┐     ┌──────▼──────┐     ┌─────────────┐
│  dim_hosts  │     │fct_bookings │     │  dim_guests │
│  (SCD-2)    ├────►│  121 rows   │◄────│  384 rows   │
└─────────────┘     └──────┬──────┘     └─────────────┘
                           │
┌─────────────┐            │
│ dim_listings│◄───────────┘
│  (SCD-2)    │
└─────────────┘

Also: fct_reviews (347 rows) · fct_listing_events (1,200 rows) · obt_bookings (121 rows)
```

### Key design decisions

- **SCD-2 on hosts and listings only** — Superhost status and listing price changes affect revenue attribution. Guest profile changes are data corrections, not analytical state changes.
- **Surrogate keys via MD5** — `MD5(host_id || '|' || dbt_valid_from)` creates a version-specific key, preventing the duplicate row problem on SCD-2 joins.
- **Metadata-driven OBT** — 48-column `gold_obt_bookings` is generated via a Jinja `for` loop over `dbt_project.yml` vars. Adding a column = one line of YAML, zero SQL changes.
- **3 separate fact tables** — bookings, reviews, and listing events are distinct business processes with different grains. Mixing them would produce an 80% NULL table with an undefined grain.

---

## 🔄 Airflow DAG

7 tasks in a sequential chain with test gates — if any test fails, downstream layers are skipped. Bad data never propagates forward.

```
bronze_run → bronze_test → silver_run → silver_test → snapshot → gold_run → gold_test
```

- Schedule: `0 */12 * * *` (midnight and noon every day)
- Retry: 1 automatic retry with 5-minute delay
- Each task is a BashOperator calling dbt with full path resolution

### Airflow DAG Screenshot

> 📸 **[INSERT: Airflow graph view showing all 7 tasks green]**
> *(Trigger DAG manually, wait for all tasks to succeed, screenshot the Graph tab)*

---

## 📈 dbt Lineage Graph

The full lineage from 7 RAW sources through Bronze, Silver, Snapshots, and Gold — 21 models, all dependencies tracked automatically.

> 📸 **[INSERT: dbt docs lineage graph]**
> *(Run `dbt docs generate && dbt docs serve`, click the graph icon bottom-right, screenshot with "All selected")*

---

## ✅ Proof the Pipeline Works

### 1 — Snowpipe auto-ingestion confirmed

> 📸 **[INSERT: Snowflake worksheet showing RAW table counts > 0 across all 7 tables]**
> *(Run the pipeline health check query and screenshot the results)*

```sql
SELECT 'RAW - hosts'    AS layer, COUNT(*) AS row_count FROM AIRBNB_DE.RAW.RAW_HOSTS
UNION ALL SELECT 'RAW - listings', COUNT(*) FROM AIRBNB_DE.RAW.RAW_LISTINGS
UNION ALL SELECT 'RAW - bookings', COUNT(*) FROM AIRBNB_DE.RAW.RAW_BOOKINGS
UNION ALL SELECT 'RAW - events',   COUNT(*) FROM AIRBNB_DE.RAW.RAW_LISTING_EVENTS;
```

### 2 — Full medallion architecture populated

> 📸 **[INSERT: Pipeline health check showing data in all layers — RAW through Gold]**
> *(Screenshot showing RAW, Bronze, Silver, and Gold row counts all > 0)*

### 3 — Dirty data detection working

> 📸 **[INSERT: Snowflake query showing data quality flag counts]**

The pipeline caught and flagged:
- **71** bookings with reversed check-in/check-out dates (`is_date_valid = FALSE`)
- **31** bookings where total price didn't reconcile with components (`is_price_mismatch = TRUE`)
- **16** bookings with wrong pre-computed nights count (`nights_count_corrected = TRUE`)
- **7** listings with suspicious coordinates (lat/lon swapped)
- **4** guests with negative trip counts
- **6** events with invalid position_in_results = 0

### 4 — Conversion funnel working in Gold

> 📸 **[INSERT: Snowflake query showing funnel narrowing correctly from step 1 to step 6]**

```sql
SELECT funnel_step, event_type_clean, COUNT(*) AS events
FROM AIRBNB_DE.GOLD.GOLD_FCT_LISTING_EVENTS
GROUP BY 1, 2 ORDER BY 1;
-- search_impression (79) → view → click → save → booking_start → booking_complete (20)
```

### 5 — Revenue metrics correct in Gold

> 📸 **[INSERT: gold_fct_bookings query showing revenue_per_night, booking_lead_days, is_last_minute_booking]**

### 6 — Airflow triggered dbt and Snowflake updated

> 📸 **[INSERT: Airflow Runs tab showing scheduled run + successful manual runs]**

The Airflow Runs tab shows:
- A **scheduled** run that fired automatically at 2026-05-20 20:00:00 — no human trigger
- Subsequent manual test runs all showing Success
- Next run pre-scheduled for 2026-05-21 08:00:00

### 7 — SCD-2 snapshot structure correct

> 📸 **[INSERT: Snowflake query showing hosts_snapshot with dbt_valid_from, dbt_valid_to columns]**

```sql
SELECT host_id, is_superhost, dbt_valid_from, dbt_valid_to, is_current_record
FROM AIRBNB_DE.SNAPSHOTS.HOSTS_SNAPSHOT
ORDER BY host_id, dbt_valid_from
LIMIT 10;
```

---

## 🚀 Setup Instructions

### Prerequisites

- AWS account with permissions for Lambda, Kinesis, S3, EventBridge
- Snowflake account (free trial works)
- Python 3.12
- uv package manager

### 1 — Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/airbnb-streaming-de-project.git
cd airbnb-streaming-de-project
```

### 2 — Set up Python environment

```bash
uv python install 3.12
uv venv --python 3.12
source .venv/bin/activate
uv pip install dbt-snowflake apache-airflow faker boto3
```

### 3 — Configure Snowflake connection

```bash
cp profiles_example.yml ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your Snowflake credentials
```

### 4 — Set up AWS infrastructure

1. Create S3 bucket `airbnb-de-project-yourname`
2. Create 3 Kinesis streams: `airbnb-events-stream` (3 shards), `airbnb-transactions-stream` (2 shards), `airbnb-dimensions-stream` (1 shard)
3. Create 3 Firehose delivery streams reading from each Kinesis stream, writing to S3
4. Create Lambda function from `lambda/` folder, attach IAM role with `kinesis:PutRecords`
5. Create EventBridge rule: `rate(30 minutes)` → Lambda

### 5 — Set up Snowflake

```sql
CREATE DATABASE AIRBNB_DE;
CREATE WAREHOUSE AIRBNB_WH WITH WAREHOUSE_SIZE = 'X-SMALL' AUTO_SUSPEND = 60;
-- Run RAW table DDL and Snowpipe setup
-- See full setup in docs/
```

### 6 — Run dbt pipeline

```bash
cd airbnb_de_project_dbt
dbt deps          # install dbt-utils
dbt seed          # load dim_date (3,288 rows)
dbt run           # build all 21 models
dbt snapshot      # build SCD-2 snapshots
dbt test          # run all 62 tests
dbt docs generate && dbt docs serve  # view lineage graph
```

### 7 — Start Airflow

```bash
airflow db migrate
mkdir ~/airflow/dags
cp airflow_dags/airbnb_dbt_pipeline.py ~/airflow/dags/
airflow standalone  # opens at localhost:8080
```

---

## 🧪 Running the Tests

```bash
dbt test                           # all 62 tests
dbt test --select "bronze.*"       # bronze layer only
dbt test --select "silver.*"       # silver layer only
dbt test --select "gold_*"         # gold layer only
```

All 62 tests pass including 3 custom singular tests:
- `assert_booking_dates_valid` — no completed bookings with check_out before check_in
- `assert_ratings_in_range` — all ratings on 0–5 scale after Silver normalisation
- `assert_revenue_positive` — all completed bookings have positive revenue

---

## 📝 Key Technical Decisions

**All RAW columns are VARCHAR.** If Snowpipe encounters a type mismatch, it silently drops the record. VARCHAR prevents silent data loss — every record lands regardless of how dirty the values are. Type casting happens in Silver using `TRY_TO_DECIMAL` and `TRY_TO_DATE`, which return NULL on failure instead of crashing.

**COALESCE on all incremental watermarks.** `MAX(_loaded_at)` on an empty table returns NULL, making `WHERE _loaded_at > NULL` evaluate to NULL for every row — a silent no-op. Wrapping in `COALESCE(MAX(_loaded_at), '2000-01-01'::TIMESTAMP_TZ)` ensures the first run processes all records.

**Seeded UUID pools in the generator.** `random.Random(42)` produces identical ID pools on every Lambda invocation, guaranteeing FK overlap between bookings and listings across runs. Without this, near-zero join matches in Gold.

**Separate fact tables for separate business processes.** Bookings, reviews, and listing events have different grains, different metrics, and different analytical questions. Merging them produces an undefined grain and an 80% NULL table.

---

## 📄 Detailed Documentation

Full technical documentation for each layer — including every error encountered, every fix applied, and every design decision explained:

- [Doc 1: Foundation to Bronze](docs/doc1_foundation_to_bronze.html) — AWS setup, Snowflake, dbt environment, Bronze layer
- [Doc 2: Silver Layer](docs/doc2_silver_layer.html) — All 10 dirty patterns with complete transformation code
- [Doc 3: Gold Layer](docs/doc3_gold_layer.html) — Star schema, SCD-2, surrogate keys, OBT, tests
- [Doc 4: Airflow](docs/doc4_airflow.html) — Orchestration setup, DAG design, verification

---

## 👤 Author

**Nirmalkumar Thirupallikrishnan Kesavan**

Built as a data engineering portfolio project demonstrating end-to-end streaming pipeline design, data quality engineering, dimensional modelling, and pipeline orchestration.

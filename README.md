# NYC Taxi End-to-End Data Pipeline

**Author:** Gnaneshwar Yadla | Data Engineer & Analyst | Richmond, VA
**Contact:** gnaneshwarreddy056@gmail.com | [yadla.dev](https://yadla.dev) | [LinkedIn](https://linkedin.com/in/gnaneshwar-reddy-)

---

## What This Project Does

This project builds a complete, production-style data engineering pipeline that processes 50,000+ NYC Yellow Taxi trip records through every layer of the modern data stack — from raw streaming events all the way to an interactive business dashboard. Every tool used here mirrors what data teams run at scale in real companies.

---

## Pipeline Architecture

```
NYC Taxi Parquet File
        ↓
  Kafka Producer         (streams rows as JSON events to a Kafka topic)
        ↓
  Kafka Topic            (KRaft mode — no ZooKeeper, single broker)
        ↓
  Kafka Consumer         (reads events in batches, writes JSON files to S3)
        ↓
  AWS S3 Bucket          (raw JSON files partitioned by year/month/day)
        ↓
  Snowflake COPY INTO    (bulk loads S3 files into RAW schema)
        ↓
  dbt Models             (transforms RAW → STAGING → MARTS)
        ↓
  Snowflake MARTS        (clean, query-ready fact and dimension tables)
        ↓
  Looker Studio          (live dashboard connected to Snowflake)
        ↑
  Apache Airflow         (orchestrates and schedules the full pipeline daily)
```

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Streaming | Apache Kafka 4.2.0 (KRaft) | Real-time event ingestion |
| Cloud Storage | AWS S3 | Raw data lake storage |
| Data Warehouse | Snowflake | Scalable cloud warehousing |
| Transformation | dbt 1.7 | SQL-based data modeling |
| Orchestration | Apache Airflow 2.9.3 | Pipeline scheduling & monitoring |
| Visualization | Looker Studio | Business intelligence dashboard |
| Language | Python 3.11 | Producers, consumers, loaders |
| Version Control | Git + GitHub | Source control |

---

## Project Phases

### Phase 0 — Environment Setup
The project is built on a structured local development environment. Python 3.11 is managed with pyenv, all dependencies are isolated in a virtual environment, and credentials are stored in a `.env` file that never enters version control. VS Code serves as the central development hub with extensions for Python, dbt, Snowflake, and Kafka.

---

### Phase 1 — Apache Kafka (Event Streaming)
Kafka acts as the real-time messaging backbone of the pipeline. Running in KRaft mode (which eliminates the need for a separate ZooKeeper process), a single broker hosts the `nyc_taxi_trips` topic. A Python producer reads 50,000 rows from the NYC Taxi parquet file and streams each row as a JSON event to the Kafka topic. This decouples data generation from data storage — the producer doesn't care how fast the consumer reads.

**Key concept:** Kafka persists messages to disk even after they're consumed. This means the pipeline can replay data, recover from failures, and support multiple independent consumers reading the same topic.

---

### Phase 2 — AWS S3 (Cloud Storage)
A separate Python consumer reads the JSON events from Kafka in batches of 1,000 records and uploads each batch as a `.json` file to an AWS S3 bucket. Files are organized in a date-partitioned folder structure (`raw/year=/month=/day=/`) which makes downstream loading efficient and auditable. 50 JSON batch files land in S3 per full pipeline run, covering all 50,000 trip records.

**Key concept:** S3 acts as the data lake — a cost-effective, durable store for raw data before it enters the warehouse. Partitioning by date allows Snowflake to load only the latest data on each run.

---

### Phase 3 — Snowflake (Data Warehouse)
Snowflake hosts three schemas that mirror the data transformation layers:

- **RAW** — Raw trip and zone data exactly as it arrived from S3, loaded with `COPY INTO` using the Snowflake-native S3 integration. No transformations applied.
- **STAGING** — Cleaned, typed, and renamed columns produced by dbt staging models.
- **MARTS** — Business-ready fact and dimension tables ready for dashboards and analysis.

The Snowflake warehouse `TAXI_WH` is configured to auto-suspend when idle, keeping costs minimal for a development project.

---

### Phase 4 — dbt (Data Transformation)
dbt (data build tool) transforms raw Snowflake data through a three-layer model architecture using pure SQL:

**Staging Layer** — Two models clean the raw data:
- `stg_taxi_trips` casts columns to correct data types, renames fields to snake_case, and filters out null or invalid records.
- `stg_taxi_zones` normalizes the NYC zone lookup table with consistent borough and zone naming.

**Marts Layer** — Two models produce business-ready tables:
- `fct_trips` is the core fact table containing 47,638 clean trip records with pickup/dropoff zones, fare amounts, distances, and payment types.
- `dim_zones` is the dimension table containing all 265 NYC taxi zones mapped to their boroughs and service zones.

Four data quality tests run after every transformation to validate primary keys, referential integrity, and non-null constraints. All four pass on every run.

---

### Phase 5 — Apache Airflow (Orchestration)
Airflow schedules and monitors the entire pipeline as a single Directed Acyclic Graph (DAG) named `nyc_taxi_pipeline`. The DAG runs on a daily schedule and chains five tasks in sequence:

1. **produce_to_kafka** — Runs the Python producer to stream 50K rows into Kafka
2. **reset_consumer_group** — Resets Kafka consumer offsets so the pipeline can safely re-run
3. **consume_to_s3** — Runs the Python consumer to drain Kafka and upload JSON batches to S3
4. **copy_into_snowflake** — Truncates and reloads `RAW.taxi_trips` from the latest S3 files
5. **dbt_run** — Executes all dbt models to refresh STAGING and MARTS

If any task fails, Airflow retries once before marking the run failed and stopping the chain. The web UI at `localhost:8080` provides real-time task logs, run history, and manual trigger capability.

---

### Phase 6 — Looker Studio (Dashboard)
A live dashboard connects directly to the Snowflake `MARTS` schema using the Looker Studio Snowflake connector. The dashboard surfaces six key visualizations built on top of `fct_trips` and `dim_zones`:

- **Total Trips** — Scorecard showing 47,638 clean trip records
- **Total Revenue** — Scorecard showing $1.41M in total fares
- **Average Fare** — Scorecard showing mean fare per trip
- **Revenue by Borough** — Bar chart breaking down earnings by NYC pickup borough
- **Trips by Payment Type** — Pie chart showing cash vs. card distribution
- **Top 10 Pickup Zones** — Bar chart ranking the busiest NYC pickup zones by revenue

---

## Data

| Item | Detail |
|---|---|
| Source | NYC TLC Yellow Taxi Trip Records |
| Period | January 2023 |
| Raw rows | 50,000 |
| Clean rows | 47,638 (after dbt quality filtering) |
| NYC Zones | 265 taxi zones across 6 boroughs |
| S3 files | 50 JSON batch files per run |

---

## Repository Structure

```
nyc-taxi-pipeline/
├── kafka/
│   ├── producer.py          Streams parquet rows to Kafka as JSON events
│   ├── consumer.py          Basic Kafka consumer for testing
│   └── consumer_s3.py       Consumes Kafka events and uploads to S3
├── config/
│   └── settings.py          Loads all credentials from .env file
├── dbt/taxi_pipeline/
│   ├── models/staging/      stg_taxi_trips and stg_taxi_zones
│   ├── models/marts/        fct_trips and dim_zones
│   ├── seeds/               taxi_zones.csv reference data
│   └── profiles.yml.example Snowflake connection template (safe to share)
├── airflow/
│   └── dags/
│       └── nyc_taxi_pipeline_dag.py   Full 5-task orchestration DAG
├── airflow_setup.sh         One-command Airflow install script
├── .env.example             Credential template (never commit .env)
└── README.md
```

---

## Security

All credentials (AWS keys, Snowflake password, Kafka config) are stored in a `.env` file excluded from version control via `.gitignore`. A `profiles.yml.example` and `.env.example` are provided as templates so anyone cloning this repo can configure their own credentials without exposing real secrets.

---

## Key Engineering Decisions

**Why Kafka instead of reading the file directly into S3?**
Kafka decouples producers from consumers and enables real-time streaming at scale. In a production scenario, trip events would stream from taxi hardware in real time — this pipeline mirrors that architecture using a parquet file as the data source.

**Why dbt for transformations?**
dbt brings software engineering practices (version control, testing, documentation, modularity) to SQL transformations. Every model is a `.sql` file tracked in git, tested with schema tests, and documented inline.

**Why Snowflake over a traditional database?**
Snowflake separates compute from storage, scales elastically, and natively integrates with S3 via external stages. The warehouse auto-suspends when idle, making it cost-efficient for development workloads.

**Why Airflow for orchestration?**
Airflow's DAG model makes dependencies explicit and observable. The web UI provides a visual graph of every task run, making debugging fast. For a daily batch pipeline, Airflow's cron-based scheduling and retry logic are exactly the right fit.
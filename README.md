# FinTech Data Platform

[![dbt CI](https://github.com/gabensaw/fintech-data-platform/actions/workflows/dbt-ci.yml/badge.svg)](https://github.com/gabensaw/fintech-data-platform/actions/workflows/dbt-ci.yml)

End-to-end data engineering portfolio project that simulates a fintech transaction analytics platform.

The project ingests synthetic transaction events, processes them through a layered data lake, loads business metrics into PostgreSQL, transforms them with dbt, orchestrates the analytical workflow with Airflow, and exposes the data to Metabase for reporting.

The goal is to show a realistic local data platform, not a course-style single-script demo.

---

## Table of Contents

- Overview
  - [Business Problem](#business-problem)
  - [Architecture](#architecture)
  - [Technology Stack](#technology-stack)
  - [Dashboard Preview](#dashboard-preview)
  - [Demo Walkthrough](#demo-walkthrough)
- Running the Project
  - [Quick Start](#quick-start)
  - [Local URLs](#local-urls)
  - [End-to-End Runbook](#end-to-end-runbook)
  - [Verify a Successful Pipeline Run](#verify-a-successful-pipeline-run)
- Operations
  - [Reset Pipeline Data Without Deleting Metabase Dashboards](#reset-pipeline-data-without-deleting-metabase-dashboards)
  - [Backup and Restore Metabase Dashboards](#backup-and-restore-metabase-dashboards)
  - [Useful Commands](#useful-commands)
  - [Test Rejected Records](#test-rejected-records)
- Technical Details
  - [Repository Structure](#repository-structure)
  - [Data Lake Layers](#data-lake-layers)
  - [Warehouse and dbt Models](#warehouse-and-dbt-models)
  - [Airflow DAG](#airflow-dag)
  - [Analytical Outputs](#analytical-outputs)
- Portfolio Notes
  - [Key Engineering Decisions](#key-engineering-decisions)
  - [What This Project Demonstrates](#what-this-project-demonstrates)
  - [Design Trade-offs](#design-trade-offs)
  - [Future Improvements](#future-improvements)

---

## Business Problem

Fintech companies process large volumes of payment transactions. Business teams need reliable access to metrics such as:

- daily revenue,
- transaction volume,
- average transaction value,
- merchant performance,
- fraud indicators,
- platform-level KPIs.

Raw event data is not suitable for direct reporting. This project transforms raw transaction events into clean, business-ready analytical tables.

---

## Architecture

```mermaid
flowchart LR
    A[Python transaction producer] --> B[Kafka topic: transactions]
    B --> C[Spark Structured Streaming]
    C --> D[Bronze Parquet data lake]
    D --> E[Spark Silver job]
    E --> F[Silver Parquet data lake]
    F --> G[Spark Gold job]
    G --> H[Gold Parquet data lake]
    H --> I[PostgreSQL warehouse]
    I --> J[dbt staging and marts]
    J --> K[Metabase dashboard]
    L[Airflow]
    L --> C
    L --> E
    L --> G
    L --> I
    L --> J
```

### Data flow

1. `producer/producer.py` generates synthetic fintech transactions.
2. Kafka stores events in the `transactions` topic.
3. Spark reads Kafka events and stores raw JSON in the Bronze layer.
4. Spark parses, validates, and standardizes data into the Silver layer.
5. Spark creates Gold business aggregates.
6. Gold datasets are loaded into PostgreSQL through JDBC.
7. dbt builds staging models and analytical marts.
8. Metabase connects to PostgreSQL for dashboarding.
9. Airflow orchestrates the analytical part of the pipeline.

---

## Technology Stack

| Area | Technology | Purpose |
| --- | --- | --- |
| Event generation | Python | Synthetic fintech transaction producer |
| Streaming | Apache Kafka | Event transport and decoupling |
| Processing | Apache Spark | Streaming ingestion, validation, aggregation |
| Data lake | Parquet | Bronze, Silver, Gold storage |
| Warehouse | PostgreSQL | Serving layer for analytics |
| Transformations | dbt | SQL models, tests, documentation |
| Orchestration | Apache Airflow | Pipeline execution and dependency management |
| BI | Metabase | Dashboard and data exploration |
| Runtime | Docker Compose | Local reproducible environment |

---

## Repository Structure

```text
.
+-- airflow/
|   +-- dags/
|   +-- Dockerfile
|   +-- entrypoint.sh
|   +-- wait-for-postgres.sh
+-- dbt/
|   +-- models/
|   |   +-- staging/
|   |   +-- marts/
|   +-- dbt_project.yml
|   +-- profiles.yml
+-- docs/
|   +-- images/
+-- metabase/
|   +-- backup/
+-- scripts/
|   +-- export_metabase.ps1
|   +-- import_metabase.ps1
|   +-- reset_pipeline_data.ps1
+-- producer/
|   +-- Dockerfile
|   +-- producer.py
|   +-- requirements.txt
+-- spark/
|   +-- app/
|   |   +-- bronze_available_now_job.py
|   |   +-- bronze_stream.py
|   |   +-- silver_job.py
|   |   +-- gold_job.py
|   |   +-- load_gold_to_postgres.py
|   |   +-- schemas.py
|   +-- conf/
|   +-- Dockerfile
+-- postgres/
|   +-- initdb/
+-- docker-compose.yml
+-- README.md
+-- ARCHITECTURE.md
```

---

## Documentation

- [Technical architecture](ARCHITECTURE.md)

---

## Dashboard Preview

The final analytics layer is exposed in Metabase through a dashboard built on top of dbt marts and PostgreSQL warehouse tables.

![Metabase dashboard preview](docs/images/metabase-dashboard.png)

---

## Demo Walkthrough

For a quick project review:

1. Start the stack with Docker Compose.
2. Generate transaction events for 1-2 minutes.
3. Stop the producer to keep the demo dataset stable.
4. Trigger the Airflow DAG `daily_fintech_pipeline`.
5. Check that dbt tests pass.
6. Open the Metabase dashboard and review platform KPIs, revenue trends, fraud rate, and merchant performance.

This walkthrough demonstrates the complete path from generated Kafka events to business-facing analytics.

---

## Quick Start

### 1. Requirements

Install:

- Docker Desktop,
- Docker Compose,
- Git.

### 2. Create local environment file

Copy the example environment file:

```bash
cp .env.example .env
```

On Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

The file contains:

```env
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
KAFKA_TOPIC=transactions
```

### 3. Start the platform

```bash
docker compose up --build -d
```

Note: after the platform starts, the `producer` container continuously sends transaction events to Kafka. For demos or controlled tests, you can stop only this container after enough data has been generated:

```bash
docker compose stop producer
```

### 4. Check running services

```bash
docker compose ps
```

Expected core services:

- `postgres`
- `kafka`
- `kafka-ui`
- `producer`
- `spark`
- `dbt`
- `airflow`
- `metabase`

---

## Local URLs

| Service | URL |
| --- | --- |
| Kafka UI | http://localhost:8080 |
| dbt docs server | http://localhost:8081 after running `dbt docs serve` |
| Airflow UI | http://localhost:8088 |
| Metabase | http://localhost:3000 |
| PostgreSQL | localhost:5432 |

Airflow local credentials:

```text
user: admin
password: admin
```

These credentials are intended only for local development and portfolio demos.

PostgreSQL local credentials:

```text
user: fintech
password: fintech
```

Local PostgreSQL databases:

```text
fintech  -> warehouse tables and dbt models
airflow  -> Airflow metadata
metabase -> Metabase metadata, dashboards, questions, users, and settings
```

---

## End-to-End Runbook

Use this section when you want to run the whole platform from scratch and verify that data moved through every important layer.

### 1. Start Docker Desktop

Make sure Docker Desktop is running before using Docker Compose.

On Windows PowerShell, you can verify Docker with:

```powershell
docker version
```

### 2. Create the local `.env` file

If this is your first run, create `.env` from the provided example:

```powershell
Copy-Item .env.example .env
```

The default values are enough for local development.

### 3. Start all containers

From the project root directory, run:

```powershell
docker compose up --build -d
```

This starts Kafka, Kafka UI, the transaction producer, Spark, PostgreSQL, dbt, Airflow, and Metabase.

The first run can take a few minutes because Docker needs to build images and initialize services.

### 4. Check that containers are running

Run:

```powershell
docker compose ps
```

Expected result: the main services should be `running` or `healthy`.

Important containers:

- `kafka`
- `kafka-ui`
- `producer`
- `spark`
- `postgres`
- `dbt`
- `airflow`
- `metabase`

### 5. Let the producer generate data

The `producer` container continuously sends transaction events to Kafka.

For a normal demo, let it run for 1-2 minutes so Kafka has data available for the pipeline.

### 6. Check Kafka topic

Open Kafka UI:

```text
http://localhost:8080
```

Check that the `transactions` topic exists and contains messages.

This confirms:

```text
Python Producer -> Kafka
```

After Kafka contains messages, stop only the producer so it does not keep generating data forever:

```powershell
docker compose stop producer
```

This keeps the rest of the platform running.

### 7. Open Airflow

Open:

```text
http://localhost:8088
```

Use the local development credentials:

```text
user: admin
password: admin
```

### 8. Trigger the pipeline DAG

In Airflow:

1. Open the `Dags` page.
2. Find `daily_fintech_pipeline`.
3. Enable the DAG if it is paused.
4. Click `Trigger`.
5. Wait until all tasks finish with `success`.

Expected task order:

```text
ingest_bronze_layer_for_demo -> build_silver_layer -> build_gold_layer -> load_gold_to_postgres -> dbt_run -> dbt_test
```

This confirms:

```text
Kafka -> Bronze Parquet -> Silver Parquet -> Gold Parquet -> PostgreSQL -> dbt marts
```

The first task, `ingest_bronze_layer_for_demo`, uses Spark Structured Streaming with `availableNow=True`. It reads the currently available Kafka messages, writes them to Bronze Parquet, and then finishes. This makes it suitable for a local Airflow demo.

### 9. Verify Bronze Parquet files

Check that Bronze files were created:

```powershell
docker exec spark ls -R /opt/spark-data/bronze/transactions
```

This confirms:

```text
Kafka -> Bronze
```

### 10. Verify warehouse load in PostgreSQL

Check when the warehouse tables were loaded:

```powershell
docker exec postgres psql -U fintech -d fintech -c "select max(warehouse_loaded_at) from daily_transaction_metrics;"
```

```powershell
docker exec postgres psql -U fintech -d fintech -c "select max(warehouse_loaded_at) from merchant_metrics;"
```

Check row counts:

```powershell
docker exec postgres psql -U fintech -d fintech -c "select count(*) from daily_transaction_metrics;"
```

```powershell
docker exec postgres psql -U fintech -d fintech -c "select count(*) from merchant_metrics;"
```

If rows exist and `warehouse_loaded_at` is filled, the Gold-to-PostgreSQL load worked.

### 11. Verify dbt marts

Check one of the final dbt models:

```powershell
docker exec postgres psql -U fintech -d fintech -c "select * from mart_platform_summary;"
```

Run dbt tests manually if needed:

```powershell
docker exec dbt dbt test
```

If dbt tests pass, the warehouse models meet the declared data quality rules.

### 12. Inspect Parquet files in Spark

Open PySpark inside the Spark container:

```powershell
docker exec -it spark /opt/spark/bin/pyspark
```

Check Silver records:

```python
silver_df = spark.read.parquet("/opt/spark-data/silver/transactions")
silver_df.show(10, truncate=False)
silver_df.printSchema()
```

Check Gold records:

```python
gold_df = spark.read.parquet("/opt/spark-data/gold/daily_transaction_metrics")
gold_df.show(10, truncate=False)
```

Exit PySpark:

```python
exit()
```

This confirms that the data lake layers were created correctly.

### 13. Open Metabase

Open:

```text
http://localhost:3000
```

Use Metabase to inspect the PostgreSQL warehouse tables and dashboard.

Main tables to check:

- `daily_transaction_metrics`
- `merchant_metrics`
- `mart_daily_kpis`
- `mart_merchant_performance`
- `mart_platform_summary`
- `mart_top_merchants`

### 14. Stop the platform

When you are done:

```powershell
docker compose down
```

If you only want to stop data generation but keep the platform running:

```powershell
docker compose stop producer
```

---

## Reset Pipeline Data Without Deleting Metabase Dashboards

Use this section when you want to generate a fresh dataset and rerun the pipeline without deleting Metabase dashboards.

This is the recommended reset option for local demos.

It deletes:

- Bronze, Silver, and Gold Parquet files from `spark/data`,
- Spark streaming checkpoints from `spark/data/checkpoints`,
- warehouse tables and dbt-created models stored in PostgreSQL,
- Kafka topic data from the `transactions` topic.

It does not delete:

- Metabase dashboards,
- Metabase questions,
- Metabase database connection setup,
- source code,
- Docker images.

### 1. Make sure the platform is running

```powershell
docker compose up -d
```

### 2. Run the reset script

From the project root:

```powershell
.\scripts\reset_pipeline_data.ps1
```

The script:

- stops the producer,
- removes Spark lake files and checkpoints,
- drops warehouse and dbt tables from the `fintech` database,
- deletes and recreates the Kafka topic `transactions`,
- keeps Metabase metadata untouched.

If you want to keep existing Kafka messages, run:

```powershell
.\scripts\reset_pipeline_data.ps1 -SkipKafkaReset
```

### 3. Generate fresh Kafka data

Start the producer:

```powershell
docker compose start producer
```

Let the `producer` run for 1-2 minutes.

Then stop only the producer:

```powershell
docker compose stop producer
```

### 4. Run the Airflow pipeline

Open Airflow:

```text
http://localhost:8088
```

Use:

```text
user: admin
password: admin
```

Trigger:

```text
daily_fintech_pipeline
```

Expected task order:

```text
ingest_bronze_layer_for_demo -> build_silver_layer -> build_gold_layer -> load_gold_to_postgres -> dbt_run -> dbt_test
```

### 5. Verify fresh data

Check warehouse load timestamps:

```powershell
docker exec postgres psql -U fintech -d fintech -c "select max(warehouse_loaded_at) from daily_transaction_metrics;"
```

Check row counts:

```powershell
docker exec postgres psql -U fintech -d fintech -c "select count(*) from daily_transaction_metrics;"
```

Check Bronze files:

```powershell
docker exec spark ls -R /opt/spark-data/bronze/transactions
```

If these checks return data, the pipeline was rebuilt from a clean local state.

### Full environment reset

Use this only if you also want to delete Metabase dashboards and all local PostgreSQL data.

Warning: this removes the PostgreSQL Docker volume `postgres_data`. In this project, that volume stores both:

```text
database fintech  -> warehouse and dbt tables
database airflow  -> Airflow metadata
database metabase -> Metabase dashboards, questions, users, and settings
```

Full reset command:

```powershell
docker compose down -v
```

After this, Metabase setup and dashboards need to be recreated.

---

## Backup and Restore Metabase Dashboards

Metabase stores dashboards, questions, collections, users, and connection metadata in its application database. In this project, that application database is the local PostgreSQL database named `metabase`.

To avoid losing dashboard work during local resets, back up the Metabase metadata database after creating or updating dashboards.

### Back up Metabase metadata

From the project root:

```powershell
.\scripts\export_metabase.ps1
```

The backup is written to:

```text
metabase/backup/metabase_metadata.dump
```

This uses PostgreSQL `pg_dump` against the local `metabase` database. It works with the open-source Metabase Docker image and stores dashboards, questions, collections, users, and Metabase database connections.

Recommended workflow:

1. Create or update dashboards in Metabase.
2. Run `.\scripts\export_metabase.ps1`.
3. Commit the backup together with the project.

### Restore Metabase metadata

Restoring replaces the local Metabase metadata database, so the script requires the explicit `-Force` flag.

Run:

```powershell
.\scripts\import_metabase.ps1 -Force
```

Then open Metabase:

```text
http://localhost:3000
```

If the warehouse connection is missing or invalid, reconnect Metabase to the local PostgreSQL warehouse manually:

```text
host: postgres
port: 5432
database: fintech
user: fintech
password: fintech
```

---

## Useful Commands

### Start containers

```bash
docker compose up --build -d
```

### Stop containers

```bash
docker compose down
```

### View logs

```bash
docker compose logs -f producer
docker compose logs -f spark
docker compose logs -f airflow
```

### Stop or start only the producer

The `producer` service runs continuously and keeps generating Kafka events. For demos and local testing, stop it when you have enough data. This prevents the Bronze layer from growing too quickly and keeps Spark jobs faster.

Stop only the producer:

```bash
docker compose stop producer
```

Start it again:

```bash
docker compose start producer
```

### Run Kafka-to-Bronze ingestion for Airflow demo

```powershell
docker exec spark /opt/spark/bin/spark-submit /opt/spark-apps/app/bronze_available_now_job.py
```

Use this version for local demos and Airflow runs.

It processes currently available Kafka messages, writes them to Bronze Parquet, and then finishes automatically. This behavior is useful for a portfolio demo because the Airflow task can complete successfully and the DAG can continue to Silver, Gold, PostgreSQL, and dbt.

### Run production-style Kafka-to-Bronze stream manually

```powershell
docker exec spark /opt/spark/bin/spark-submit /opt/spark-apps/app/bronze_stream.py
```

This is the production-style version of Bronze ingestion.

It keeps running until stopped with `Ctrl + C`, continuously reading new Kafka messages and appending them to the Bronze layer. In a real production setup, this type of job would usually be managed as a separate long-running streaming process.

For normal local demos, use the Airflow DAG. Do not run `bronze_stream.py` while the Airflow DAG is running, because both jobs write to the same Bronze folder:

```text
/opt/spark-data/bronze/transactions
```

### Run dbt models

```bash
docker exec dbt dbt run
```

### Run dbt tests

```bash
docker exec dbt dbt test
```

### Generate dbt docs

```bash
docker exec dbt dbt docs generate
```

### Serve dbt docs

```bash
docker exec dbt dbt docs serve --host 0.0.0.0 --port 8081
```

---

## Verify a Successful Pipeline Run

Before triggering the Airflow DAG, make sure Kafka contains messages. The DAG will run the demo Bronze ingestion task automatically.

After triggering `daily_fintech_pipeline` in Airflow, all tasks should finish with `success`:

```text
ingest_bronze_layer_for_demo -> build_silver_layer -> build_gold_layer -> load_gold_to_postgres -> dbt_run -> dbt_test
```

Check when warehouse tables were last loaded:

```powershell
docker exec postgres psql -U fintech -d fintech -c "select max(warehouse_loaded_at) from daily_transaction_metrics;"
```

```powershell
docker exec postgres psql -U fintech -d fintech -c "select max(warehouse_loaded_at) from merchant_metrics;"
```

Check row counts:

```powershell
docker exec postgres psql -U fintech -d fintech -c "select count(*) from daily_transaction_metrics;"
```

```powershell
docker exec postgres psql -U fintech -d fintech -c "select count(*) from merchant_metrics;"
```

Check a dbt mart:

```powershell
docker exec postgres psql -U fintech -d fintech -c "select * from mart_platform_summary;"
```

Note: PostgreSQL timestamps may be shown in UTC. For example, `19:24 UTC` equals `21:24` in Poland during summer time.

---

## Test Rejected Records

The normal producer generates valid transactions. To test Silver-layer rejection logic, send five intentionally invalid events:

For a controlled test, stop the normal producer first so it does not keep generating valid events in the background:

```powershell
docker compose stop producer
```

If the stack was already running before this script was added, rebuild the producer image first:

```powershell
docker compose build producer
```

```powershell
docker compose run --rm producer python send_invalid_transactions.py
```

Trigger `daily_fintech_pipeline` in Airflow. The first DAG task will ingest the invalid Kafka events into Bronze, and the Silver task will write invalid rows to the rejected-records path.

After the test, start the normal producer again only if you want to generate more live data:

```powershell
docker compose start producer
```

After the DAG finishes, inspect rejected records:

```powershell
docker exec -it spark /opt/spark/bin/pyspark
```

Inside PySpark:

```python
df = spark.read.parquet("/opt/spark-data/silver/rejected_transactions")
df.groupBy("rejection_reason").count().show(truncate=False)
df.select("rejection_reason", "raw_event_json").show(20, truncate=False)
```

Expected rejection reasons include:

- `missing_transaction_id`
- `non_positive_amount`
- `missing_merchant`
- `invalid_event_timestamp`
- `missing_customer_id`
- `missing_amount`
- `missing_currency`
- `missing_payment_method`
- `missing_fraud_flag`

Expected grouped result:

```text
+-------------------------------------------------------------------------------------------------+-----+
|rejection_reason                                                                                 |count|
+-------------------------------------------------------------------------------------------------+-----+
|missing_transaction_id                                                                           |1    |
|non_positive_amount                                                                              |1    |
|missing_merchant                                                                                 |1    |
|invalid_event_timestamp                                                                          |1    |
|missing_customer_id, missing_amount, missing_currency, missing_payment_method, missing_fraud_flag|1    |
+-------------------------------------------------------------------------------------------------+-----+
```

---

## Data Lake Layers

### Bronze

Stores raw Kafka events as JSON strings with Kafka metadata.

Purpose:

- preserve raw input,
- support auditability,
- allow reprocessing.

### Silver

Stores parsed and validated transaction records.

Main transformations:

- JSON parsing,
- type casting,
- timestamp normalization,
- required field validation,
- positive amount validation,
- rejected-record capture with rejection reasons,
- ingestion metadata.

Rejected records are written separately to:

```text
/opt/spark-data/silver/rejected_transactions
```

This keeps invalid records auditable without allowing them into analytical datasets.

### Gold

Stores business-ready aggregates.

Current Gold datasets:

- `daily_transaction_metrics`
- `merchant_metrics`

---

## Warehouse and dbt Models

Spark loads Gold datasets into PostgreSQL.

Base warehouse tables:

- `daily_transaction_metrics`
- `merchant_metrics`

Both warehouse tables include `warehouse_loaded_at`, a technical timestamp showing when the current Gold dataset was loaded into PostgreSQL.

The current loading strategy is full refresh: each DAG run truncates and reloads the warehouse tables from the current Gold Parquet datasets. This keeps the portfolio pipeline simple, reproducible, and easy to debug while preserving dbt dependencies.

dbt then builds analytical models:

- `mart_daily_kpis`
- `mart_merchant_performance`
- `mart_merchant_tier`
- `mart_platform_summary`
- `mart_top_merchants`

dbt tests validate model assumptions such as not-null fields, accepted values, uniqueness, and relationships.

---

## Airflow DAG

The project contains an Airflow DAG:

```text
daily_fintech_pipeline
```

Current DAG sequence:

```text
ingest Bronze layer for demo -> build Silver layer -> build Gold layer -> load Gold data to PostgreSQL -> dbt run -> dbt test
```

The first DAG task uses `spark/app/bronze_available_now_job.py`. This is a finite Spark Structured Streaming job using `availableNow=True`: it processes Kafka messages that are currently available, writes them to Bronze Parquet, and then exits.

The project also keeps `spark/app/bronze_stream.py` as the production-style long-running streaming variant:

```powershell
docker exec spark /opt/spark/bin/spark-submit /opt/spark-apps/app/bronze_stream.py
```

In a real production setup, the long-running Bronze stream would usually run as a separate continuously managed process. For local portfolio demos, the finite Airflow task is easier to run and verify end-to-end.

Summary:

```text
bronze_available_now_job.py -> local demo / Airflow-friendly
bronze_stream.py            -> production-style long-running stream
```

---

## Analytical Outputs

The platform supports reporting for:

- daily transaction KPIs,
- total revenue,
- average transaction amount,
- fraud rate,
- merchant revenue,
- merchant transaction volume,
- top merchants,
- merchant tier classification,
- platform-level summary metrics.

These outputs are consumed by Metabase dashboards.

---

## Key Engineering Decisions

### Why Kafka?

Kafka simulates event-driven ingestion. It decouples the transaction producer from downstream processing and reflects how real transactional systems often publish events.

### Why Spark?

Spark demonstrates distributed processing concepts and supports both streaming ingestion and batch-style transformations over data lake files.

### Why Parquet?

Parquet is a columnar storage format commonly used in data lakes. It is efficient for analytical workloads and works well with Spark.

### Why PostgreSQL?

PostgreSQL acts as a lightweight local serving warehouse. It keeps the project easy to run while still supporting realistic SQL analytics and BI integration.

### Why dbt?

dbt separates analytical SQL transformations from processing code. It adds tests, documentation, and a clear modeling layer.

### Why Airflow?

Airflow shows workflow orchestration, dependency management, and repeatable execution of the analytical pipeline.

### Why no Kubernetes or microservices?

The project is designed for a portfolio and local reproducibility. Docker Compose is enough to demonstrate the data platform architecture without unnecessary operational complexity.

---

## What This Project Demonstrates

This project demonstrates practical Data Engineering skills across the full data platform lifecycle:

- Event-driven ingestion using Python, Kafka, and Spark Structured Streaming.
- Medallion architecture with Bronze, Silver, and Gold Parquet layers.
- Data validation and rejected-record handling in the Silver layer.
- Business aggregation design for transaction, merchant, fraud, and platform KPIs.
- Warehouse loading from Spark to PostgreSQL through JDBC.
- Analytics engineering with dbt staging models, marts, tests, and documentation.
- Workflow orchestration with Airflow DAG dependencies.
- BI integration with Metabase on top of warehouse marts.
- Local reproducibility with Docker Compose and documented operational scripts.
- Clear separation between analytical data, Airflow metadata, and Metabase metadata.

---

## Design Trade-offs

This project intentionally uses a simple but realistic local architecture:

- Docker Compose is used instead of Kubernetes to keep the project easy to run locally.
- PostgreSQL is used as a lightweight warehouse instead of a cloud warehouse.
- Parquet is used as a local data lake format for Bronze, Silver, and Gold layers.
- Full refresh loading is used for portfolio simplicity instead of incremental warehouse loading.
- The Airflow demo DAG uses an available-now Bronze ingestion job, while `bronze_stream.py` shows the production-style long-running streaming variant.
- Metabase metadata is backed up separately from pipeline data so dashboards can survive local data resets.

---

## Future Improvements

High-value improvements for portfolio quality:

- add a small demo script for recruiters,
- add CI checks for dbt tests and SQL linting.

Lower-priority improvements:

- cloud deployment,
- Snowflake or BigQuery warehouse,
- Great Expectations,
- monitoring stack,
- Kubernetes.

These are intentionally not part of the current version to avoid overengineering.

# FinTech Data Platform - Interview Guide

This guide helps explain the project during Data Engineer interviews.

It focuses on what the project does, why the architecture was chosen, what trade-offs were made, and how to discuss future improvements professionally.

---

## 1. 60-Second Project Pitch

I built an end-to-end fintech data platform that simulates how transaction events can be processed for analytics.

A Python producer generates realistic payment transactions and sends them to Kafka. Spark consumes the events and stores them in a Bronze/Silver/Gold data lake using Parquet. Gold-level business metrics are loaded into PostgreSQL, where dbt builds analytical models, tests, and documentation. Airflow orchestrates the analytical workflow, and Metabase exposes the final marts through dashboards.

The project demonstrates event-driven ingestion, Spark processing, layered data architecture, warehouse modeling, dbt tests, orchestration, and BI consumption in a Docker Compose environment.

---

## 2. What Problem Does the Project Solve?

The project solves a common analytics problem in fintech:

Raw transaction events are not directly useful for business reporting. They need to be ingested, cleaned, validated, aggregated, modeled, and exposed to analysts or business users.

This project turns raw payment events into business metrics such as:

- daily revenue,
- transaction count,
- average transaction amount,
- fraud rate,
- merchant performance,
- merchant tiers,
- platform-level KPIs.

---

## 3. Architecture Explanation

The main flow is:

```text
Python Producer
-> Kafka
-> Spark
-> Bronze Parquet
-> Silver Parquet
-> Gold Parquet
-> PostgreSQL
-> dbt
-> Metabase
```

Airflow orchestrates the analytical workflow.

The most important design idea is separation of responsibilities:

| Layer | Responsibility |
| --- | --- |
| Producer | Generate synthetic transaction events |
| Kafka | Decouple event generation from processing |
| Bronze | Store raw events for audit and replay |
| Silver | Store cleaned and validated transaction records |
| Gold | Store business aggregates |
| PostgreSQL | Serve analytics data |
| dbt | Build SQL marts and run tests |
| Airflow | Coordinate execution |
| Metabase | Present business metrics |

---

## 4. Why This Stack?

### Why Kafka?

Kafka represents a realistic event streaming layer. In a real company, payment systems often publish transaction events asynchronously. Kafka decouples producers from downstream processing.

### Why Spark?

Spark is used because it is common in enterprise data platforms and supports both streaming and batch-style processing. In this project it reads Kafka events, processes data lake layers, and creates aggregations.

### Why Parquet?

Parquet is a columnar format commonly used in data lakes. It works well with Spark and is efficient for analytical reads.

### Why PostgreSQL?

PostgreSQL is used as a local warehouse simulation. It is easy to run in Docker Compose and works well with dbt and Metabase. In production, this could be replaced by Snowflake, BigQuery, Redshift, or Synapse.

### Why dbt?

dbt separates analytical SQL logic from processing code. It gives structure to staging and mart models, provides tests, and generates documentation.

### Why Airflow?

Airflow manages task dependencies and provides operational visibility. It is used for orchestration, not transformation logic.

### Why Metabase?

Metabase makes the project business-facing. It shows that the pipeline does not only produce files and tables, but also useful analytics.

---

## 5. Strong Interview Talking Points

Use these points when explaining the project:

- I designed the platform in layers instead of writing one large script.
- Kafka decouples event generation from processing.
- Bronze keeps raw data for auditability and reprocessing.
- Silver contains cleaned, typed, validated records.
- Gold contains business-level aggregates.
- dbt handles warehouse modeling and data quality tests.
- Airflow is responsible only for orchestration.
- Metabase shows the final business value of the pipeline.
- Docker Compose makes the project reproducible locally.
- I intentionally avoided Kubernetes and microservices to keep the project focused and maintainable.

---

## 6. Questions and Good Answers

### Q: Why did you use Bronze, Silver, and Gold layers?

Because each layer has a different responsibility.

Bronze stores raw events and allows replay. Silver contains cleaned and standardized transaction records. Gold contains business-ready aggregates. This separation makes debugging easier and keeps business logic away from raw data.

### Q: Why not load Kafka events directly into PostgreSQL?

That would be simpler, but it would skip the data lake and processing layers. The goal of this project is to demonstrate realistic data engineering patterns: raw storage, cleaning, validation, aggregation, and warehouse loading.

### Q: What does Spark do in this project?

Spark reads Kafka events, writes raw data to Bronze, parses and validates data into Silver, creates Gold aggregates, and loads Gold data into PostgreSQL using JDBC.

### Q: What does dbt add if Spark already creates Gold tables?

Spark prepares engineering-level datasets and aggregates. dbt handles warehouse analytics modeling: staging views, marts, SQL transformations, tests, and documentation. This mirrors the split between data engineering and analytics engineering.

### Q: Why is PostgreSQL used instead of a cloud warehouse?

Because the project is designed to run locally and be easy to reproduce. PostgreSQL is enough to demonstrate warehouse-style modeling with dbt and BI integration. In production, the warehouse layer could be replaced by Snowflake, BigQuery, Redshift, or Synapse.

### Q: What does Airflow orchestrate?

The current DAG orchestrates the analytical workflow:

```text
build Silver layer -> build Gold layer -> load Gold data to PostgreSQL -> dbt run -> dbt test
```

Airflow controls execution order, while Spark and dbt contain the actual transformation logic.

### Q: Does the warehouse load use incremental loading?

No. The current project uses full refresh loading from Gold Parquet to PostgreSQL.

This means each DAG run truncates and reloads the warehouse tables from the current Gold datasets. This is intentional for the portfolio version because it is simpler, deterministic, and easier to debug locally.

To keep the load auditable, the warehouse tables include `warehouse_loaded_at`, which shows when the current data was loaded into PostgreSQL.

### Q: How is data quality handled?

There are two levels:

1. Spark Silver validation separates valid records from rejected records, such as missing IDs, missing timestamps, or non-positive amounts.
2. dbt tests validate assumptions in warehouse and mart models, such as not-null fields, uniqueness, accepted values, and relationships.

Rejected records are stored separately with a rejection reason, which makes data quality issues auditable without polluting the clean Silver dataset.

### Q: Is this a real-time platform?

It includes a streaming ingestion layer through Kafka and Spark Structured Streaming for Bronze. The downstream Silver, Gold, warehouse, and dbt steps are batch-style analytical processing. So it is a hybrid streaming and batch analytics platform.

### Q: Why did you not use Kubernetes?

Kubernetes would increase operational complexity without improving the main portfolio goal. Docker Compose is enough to demonstrate the architecture locally. For this project, clarity and reproducibility are more important than infrastructure complexity.

### Q: What would you improve next?

The highest-value next improvements are:

1. Add screenshots of Airflow, dbt docs, Kafka UI, and Metabase.
2. Add CI checks for dbt tests.

---

## 7. Known Limitations and How to Explain Them

### Limitation: Synthetic data

The data is generated locally, not taken from a real production system.

Good explanation:

> I used synthetic data because the goal is to demonstrate data platform design without using sensitive financial data. The generator includes realistic merchant distributions, amount ranges, payment methods, and fraud probabilities.

### Limitation: Local Docker Compose runtime

The platform runs locally, not in the cloud.

Good explanation:

> I used Docker Compose to make the project easy to run and review. The architecture could be migrated to cloud services, but local reproducibility is more valuable for a portfolio project.

### Limitation: Simplified security

Credentials are local and simplified.

Good explanation:

> For a production system, I would use a secret manager, TLS, IAM/RBAC, network restrictions, and proper environment separation. For this portfolio version, I kept credentials simple to make the project runnable.

## 8. How to Demo the Project

Recommended demo flow:

1. Open `README.md` and explain the architecture diagram.
2. Show `docker-compose.yml` to demonstrate the local platform stack.
3. Open `producer/producer.py` and explain synthetic transaction generation.
4. Open Spark jobs:
   - `bronze_stream.py`
   - `silver_job.py`
   - `gold_job.py`
   - `load_gold_to_postgres.py`
5. Show dbt models in:
   - `dbt/models/staging/`
   - `dbt/models/marts/`
6. Show `daily_fintech_pipeline.py` in Airflow.
7. Show dbt tests.
8. Show Metabase dashboard.
9. Finish with known limitations and next improvements.

---

## 9. Short Technical Summary

Use this version when the interviewer asks for a concise technical explanation:

> The platform uses Kafka for event ingestion, Spark for processing and Parquet data lake layers, PostgreSQL as a serving warehouse, dbt for analytical modeling and tests, Airflow for orchestration, and Metabase for reporting. The architecture follows a Bronze/Silver/Gold pattern and is fully containerized with Docker Compose.

---

## 10. What This Project Proves

This project demonstrates that I can:

- design an end-to-end data pipeline,
- work with Kafka-based ingestion,
- use Spark for data processing,
- structure a data lake into Bronze/Silver/Gold layers,
- load curated data into a warehouse,
- build dbt models and tests,
- orchestrate workflows with Airflow,
- connect data outputs to BI,
- document engineering decisions clearly,
- keep a multi-service project reproducible with Docker Compose.

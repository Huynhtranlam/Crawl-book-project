# Realtime Price Data Pipeline

## Overview

This project builds a real-time data pipeline for crawling product data, streaming via Kafka, processing, and storing into a data warehouse for analytics.

The system includes:

* Web crawler (data ingestion)
* Kafka (event streaming)
* Stream processing (clean + validate)
* PostgreSQL (data warehouse)
* MinIO (data lake simulation)
* Airflow (orchestration)
* dbt (transformations)
* Metabase (dashboard)
* Docker Compose (local infra)
* GitHub Actions (CI/CD)

---

## Architecture (High-level)

Crawler → Kafka → Stream Processor → Postgres / MinIO → dbt → Dashboard

---

## Tech Stack

* Python
* Apache Kafka
* PostgreSQL
* MinIO
* Apache Airflow
* dbt Core
* Docker Compose
* GitHub Actions

---

## How to Run (target)

```bash
make up
```

---

## Project Structure

```bash
apps/
airflow/
dbt/
infra/
tests/
.github/
```

---

## Roadmap

See `PLAN.md` for detailed phases.

---

## Notes

* This project is built in phases
* Each phase must be completed independently
* Do NOT implement future phases early

---

## Commit Convention

Use conventional commits with the phase in scope:

```bash
<type>(phase-X): <short description>
```

Allowed types:

* feat
* fix
* chore
* docs
* refactor
* test

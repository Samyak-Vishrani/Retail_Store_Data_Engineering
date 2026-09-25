# Retail Store Data Engineering Project

An ELT pipeline on Databricks. Airflow triggers a Databricks CDC ingestion job, then dbt builds the silver and gold layers.

## Stack

- Databricks
- dbt Core with dbt-databricks
- Apache Airflow 3

## Pipeline

The `orchestrator` DAG runs monthly (1st of the month, 07:00 UTC):

1. `ingest_cdc`: runs the Databricks ingestion job and waits for it to finish
2. `source_freshness`: `dbt source freshness`
3. `silver_technical` and tests: `dbt run/test --select silver_tech`
4. `silver_business` and tests: `dbt run/test --select silver_business`
5. `gold_ephemeral`: `dbt run --select gold/ephemeral`
6. `gold_dimensions`: `dbt snapshot` (SCD2 dimensions)
7. `gold_fact`: `dbt run --select gold/fact`

## Project structure

```
airflow/
  dags/orchestrator.py          Airflow DAG
  data_engineering_project/     dbt project (models, snapshots, tests, macros)
  config/airflow.cfg            Airflow config
  docker-compose.yaml
  Dockerfile
```

## Setup

Create `airflow/.env` (and `.env` in the root for running dbt locally):

```
AIRFLOW_UID=50000
DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
DATABRICKS_TOKEN=<personal access token>
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/<warehouse id>
DATABRICKS_CATALOG=walmart
DATABRICKS_SCHEMA=dbt_schema
DATABRICKS_JOBID=<ingestion job id>

FERNET_KEY=<fernet key>
AIRFLOW__API__SECRET_KEY=<random string>
AIRFLOW__API_AUTH__JWT_SECRET=<random string>
```

Generate a Fernet key with:

```
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

`.env` files are git ignored. Never commit credentials.

## Run

```
cd airflow
docker compose up -d --build
```

Open http://localhost:8080 (default login `airflow` / `airflow`), then unpause and trigger the `orchestrator` DAG.

To run dbt locally instead, export the variables from `.env` into your shell (dbt does not load `.env` on its own), then:

```
cd airflow/data_engineering_project
dbt debug
dbt build
```

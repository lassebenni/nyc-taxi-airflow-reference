# dags/taxi_pipeline.py
import io
import os
from datetime import datetime

import pandas as pd
import requests
from sqlalchemy import text
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator

# Per-student schema isolation. Set AIRFLOW_STUDENT in airflow_settings.yaml
# or your shell so every student writes into their own airflow_<name>
# schema and cannot accidentally drop a classmate's raw_trips table.
STUDENT = os.environ.get("AIRFLOW_STUDENT", "default")
SCHEMA = f"airflow_{STUDENT}"
DBT_DIR = "/usr/local/airflow/include/dbt_project"
TLC_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "green_tripdata_2024-01.parquet"
)
ZONES_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

DBT_ENV = {
    "PG_HOST": "{{ conn.azure_pg.host }}",
    "PG_USER": "{{ conn.azure_pg.login }}",
    "PG_PASSWORD": "{{ conn.azure_pg.password }}",
    "PG_DBNAME": "{{ conn.azure_pg.schema }}",   # Airflow stores DB name in the "schema" field
    "PG_SCHEMA": SCHEMA,                         # where dbt writes stg_/fct_ models
}


@dag(
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["week12", "taxi"],
)
def taxi_pipeline():
    @task()
    def ingest_taxi_month() -> int:
        # Download the TLC parquet and load it into airflow_<student>.raw_trips
        # in one task. We keep download + load together because passing a
        # filesystem path between tasks via XCom is fragile under Airflow 3's
        # TaskSDK: the downstream task can end up receiving the path wrapped
        # in a way that pandas treats as a binary buffer, and the load fails
        # with a cryptic "Parquet magic bytes not found" error. Staying in
        # one process keeps the parquet bytes in memory and sidesteps the
        # whole class of issues.
        resp = requests.get(TLC_URL, timeout=60)
        resp.raise_for_status()   # turn 403/404 into a real exception
        df = pd.read_parquet(io.BytesIO(resp.content))

        hook = PostgresHook(postgres_conn_id="azure_pg")
        engine = hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'))
        df.to_sql(
            "raw_trips",
            engine,
            schema=SCHEMA,          # per-student isolation
            if_exists="replace",    # safe: only affects this student's schema
            index=False,
            method="multi",         # batch 1000 rows per INSERT, not 1-by-1
            chunksize=1000,         # ~10x faster over TLS to Azure Postgres
        )
        return len(df)

    @task()
    def ingest_zones_lookup() -> int:
        # Independent source: the 265-row TLC zone lookup. Runs in parallel
        # with ingest_taxi_month because neither depends on the other.
        resp = requests.get(ZONES_URL, timeout=60)
        resp.raise_for_status()
        df = pd.read_csv(io.BytesIO(resp.content))

        hook = PostgresHook(postgres_conn_id="azure_pg")
        engine = hook.get_sqlalchemy_engine()
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"'))
        df.to_sql(
            "raw_zones",
            engine,
            schema=SCHEMA,
            if_exists="replace",
            index=False,
        )
        return len(df)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"uvx --python 3.11 --from 'dbt-core==1.10.*' --with 'dbt-postgres==1.10.*' dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env=DBT_ENV,
        append_env=True,
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"uvx --python 3.11 --from 'dbt-core==1.10.*' --with 'dbt-postgres==1.10.*' dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env=DBT_ENV,
        append_env=True,
    )

    # Both ingests run in parallel; gate waits for both before dbt starts.
    gate = EmptyOperator(task_id="gate")
    [ingest_taxi_month(), ingest_zones_lookup()] >> gate >> dbt_run >> dbt_test


taxi_pipeline()

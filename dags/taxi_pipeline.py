# dags/taxi_pipeline.py
import io
import os
from datetime import datetime

import pandas as pd
import requests
from airflow.sdk import dag, get_current_context, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator

STUDENT = os.environ.get("AIRFLOW_STUDENT", "default")
SCHEMA = f"airflow_{STUDENT}"
DBT_DIR = "/usr/local/airflow/include/dbt_project"
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"

DBT_ENV = {
    "PG_HOST": "{{ conn.azure_pg.host }}",
    "PG_USER": "{{ conn.azure_pg.login }}",
    "PG_PASSWORD": "{{ conn.azure_pg.password }}",
    "PG_DBNAME": "{{ conn.azure_pg.schema }}",
    "PG_SCHEMA": SCHEMA,
}


def parquet_url_for(ds: str) -> str:
    """Return the TLC green-taxi parquet URL for a logical date.

    Pure function extracted from `ingest_taxi_month` so it can be
    unit-tested without an Airflow runtime (the Testing DAGs chapter
    uses it as its unit-test target).
    """
    year_month = ds[:7]  # "2024-01-01" -> "2024-01"
    return f"{TLC_BASE}/green_tripdata_{year_month}.parquet"


def _ds_from_context() -> str:
    """Return the logical-date string for the current task run.

    `ds: str` auto-injection into TaskFlow tasks works for scheduled
    runs (Airflow sets `logical_date` to the interval boundary) but
    breaks for *manual* triggers in Airflow 3, where `logical_date`
    defaults to `None`. Reading through `get_current_context()` with
    a `run_after` fallback works in both modes.
    """
    ctx = get_current_context()
    dr = ctx["dag_run"]
    dt = dr.logical_date or dr.run_after
    return dt.strftime("%Y-%m-%d")


@dag(
    schedule="@monthly",                 # one run per month (was @daily)
    start_date=datetime(2024, 1, 1),     # earliest month the backfill will claim
    catchup=False,                       # stays False: load history via
                                         # `airflow backfill create`, not
                                         # auto-firing on unpause
    max_active_runs=1,                   # serialize: concurrent dbt runs on the
                                         # same schema clash on __dbt_backup
    default_args={"retries": 2},         # retry transient failures twice
    tags=["week12", "taxi"],
)
def taxi_pipeline():
    @task()
    def ingest_taxi_month() -> int:
        ds = _ds_from_context()
        year_month = ds[:7]

        # raise_for_status converts a 403 (future month, typo'd path)
        # into a real exception instead of silently saving the HTML
        # error body as "parquet".
        resp = requests.get(parquet_url_for(ds), timeout=60)
        resp.raise_for_status()
        df = pd.read_parquet(io.BytesIO(resp.content))

        hook = PostgresHook(postgres_conn_id="azure_pg")
        engine = hook.get_sqlalchemy_engine()
        # On the first run raw_trips does not exist yet; materialize
        # an empty copy with the right schema first so the DELETE
        # below has something to delete from.
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        df.head(0).to_sql(
            "raw_trips", engine, schema=SCHEMA, if_exists="append", index=False,
            method="multi", chunksize=1000,
        )
        # DELETE runs through psycopg's raw cursor; to_sql below uses
        # SQLAlchemy's engine, because pandas.to_sql wants an engine.
        # Two APIs, same connection pool under the hood.
        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                f'DELETE FROM "{SCHEMA}".raw_trips '
                "WHERE to_char(lpep_pickup_datetime, 'YYYY-MM') = %s",
                (year_month,),
            )
        df.to_sql(
            "raw_trips",
            engine,
            schema=SCHEMA,
            if_exists="append",   # was "replace": append adds this month's
            index=False,           # rows to the previous months, DELETE first
            method="multi",        # batch 1000 rows per INSERT (10x faster
            chunksize=1000,        # over TLS than pandas' default per-row)
        )                          # removes any prior copy of this month.
        return len(df)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"uvx --python 3.11 --from 'dbt-core==1.10.*' --with 'dbt-postgres==1.10.*' dbt deps --project-dir {DBT_DIR} --profiles-dir {DBT_DIR} && uvx --python 3.11 --from 'dbt-core==1.10.*' --with 'dbt-postgres==1.10.*' dbt run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env=DBT_ENV,
        append_env=True,
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"uvx --python 3.11 --from 'dbt-core==1.10.*' --with 'dbt-postgres==1.10.*' dbt test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}",
        env=DBT_ENV,
        append_env=True,
    )

    ingest_taxi_month() >> dbt_run >> dbt_test


taxi_pipeline()

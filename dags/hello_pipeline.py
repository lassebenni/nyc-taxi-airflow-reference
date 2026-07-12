# dags/hello_pipeline.py
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    schedule="0 6 * * 1-5",       # was "@daily": now every weekday at 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["week12", "intro"],
)
def hello_pipeline():
    @task()
    def ingest() -> int:
        return 42

    @task()
    def transform(count: int) -> None:
        print(f"Processed {count} rows")

    transform(ingest())


hello_pipeline()

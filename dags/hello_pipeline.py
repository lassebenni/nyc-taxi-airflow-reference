# dags/hello_pipeline.py
from datetime import datetime

from airflow.sdk import dag, task


@dag(
    schedule="@daily",
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

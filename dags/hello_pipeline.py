# dags/hello_pipeline.py
from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor


@dag(
    schedule="0 6 * * 1-5",       # was "@daily": now every weekday at 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["week12", "intro"],
)
def hello_pipeline():
    # Blocks until the blob lands in hyfstoragedev/raw. mode="reschedule" frees
    # the worker slot between checks instead of holding it.
    wait_for_blob = WasbBlobSensor(
        task_id="wait_for_blob",
        wasb_conn_id="wasb_default",
        container_name="raw",
        blob_name="week12-sensor-test/ready.flag",
        poke_interval=30,
        mode="reschedule",
        timeout=600,
    )

    @task()
    def ingest() -> int:
        return 42

    @task()
    def transform(count: int) -> None:
        print(f"Processed {count} rows")

    wait_for_blob >> transform(ingest())


hello_pipeline()

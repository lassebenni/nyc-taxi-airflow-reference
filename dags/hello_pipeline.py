# dags/hello_pipeline.py
from datetime import datetime

from airflow.sdk import dag, task

# TODO (see EXERCISE.md): import WasbBlobSensor from the Microsoft Azure provider.
# from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor


@dag(
    schedule="0 6 * * 1-5",       # was "@daily": now every weekday at 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["week12", "intro"],
)
def hello_pipeline():
    # TODO (see EXERCISE.md): add a `wait_for_blob` WasbBlobSensor on container
    # `raw`, blob `week12-sensor-test/ready.flag` (wasb_conn_id="wasb_default",
    # mode="reschedule", poke_interval=30, timeout=600), then wire it so
    # `transform` runs only after the blob lands.

    @task()
    def ingest() -> int:
        return 42

    @task()
    def transform(count: int) -> None:
        print(f"Processed {count} rows")

    # TODO: replace this so the blob gates `transform`.
    transform(ingest())


hello_pipeline()

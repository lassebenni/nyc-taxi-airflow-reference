# dags/hello_pipeline.py
from datetime import datetime

from airflow.sdk import dag, task

# TODO (see EXERCISE.md): import the FileSensor from the standard provider.
# from airflow.providers.standard.sensors.filesystem import FileSensor


@dag(
    schedule="0 6 * * 1-5",       # was "@daily": now every weekday at 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["week12", "intro"],
)
def hello_pipeline():
    # TODO (see EXERCISE.md): add a `wait_for_flag` FileSensor that blocks
    # until /tmp/ready.flag exists (fs_conn_id="fs_default",
    # mode="reschedule", poke_interval=10, timeout=600), then wire it so
    # `transform` runs only after the flag appears.

    @task()
    def ingest() -> int:
        return 42

    @task()
    def transform(count: int) -> None:
        print(f"Processed {count} rows")

    # TODO: replace this so the flag gates `transform`.
    transform(ingest())


hello_pipeline()

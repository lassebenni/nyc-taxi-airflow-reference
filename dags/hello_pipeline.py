# dags/hello_pipeline.py
from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.standard.sensors.filesystem import FileSensor


@dag(
    schedule="0 6 * * 1-5",       # was "@daily": now every weekday at 06:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["week12", "intro"],
)
def hello_pipeline():
    # Blocks until /tmp/ready.flag exists. mode="reschedule" frees the
    # worker slot between checks instead of holding it, which is the right
    # default for a sensor that may wait minutes or hours.
    wait_for_flag = FileSensor(
        task_id="wait_for_flag",
        fs_conn_id="fs_default",
        filepath="/tmp/ready.flag",
        poke_interval=10,
        mode="reschedule",
        timeout=600,
    )

    @task()
    def ingest() -> int:
        return 42

    @task()
    def transform(count: int) -> None:
        print(f"Processed {count} rows")

    # transform now waits for both ingest (for the row count) and the flag.
    wait_for_flag >> transform(ingest())


hello_pipeline()

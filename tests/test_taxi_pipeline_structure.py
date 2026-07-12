# tests/test_taxi_pipeline_structure.py
from airflow.models import DagBag


def test_ingest_runs_before_dbt_run():
    # Use dag_bag.dags[...] (the in-memory parse result), not
    # dag_bag.get_dag(...): under Airflow 3, get_dag() reads the
    # serialized DAG from the metadata DB, which the ephemeral
    # `astro dev pytest` container has not populated, so it raises a
    # SQL error. Indexing .dags stays in-process and needs no DB.
    dag = DagBag(dag_folder="dags", include_examples=False).dags["taxi_pipeline"]
    ingest = dag.get_task("ingest_taxi_month")
    dbt_run = dag.get_task("dbt_run")

    assert dbt_run in ingest.downstream_list, (
        "dbt_run should run after ingest_taxi_month"
    )

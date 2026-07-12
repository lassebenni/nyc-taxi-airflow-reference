# tests/test_dag_integrity.py
from airflow.models import DagBag


def test_no_import_errors():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    assert dag_bag.import_errors == {}, (
        f"DAG import errors: {dag_bag.import_errors}"
    )


def test_every_dag_has_tags():
    dag_bag = DagBag(dag_folder="dags", include_examples=False)
    for dag_id, dag in dag_bag.dags.items():
        assert dag.tags, f"DAG {dag_id} is missing tags"

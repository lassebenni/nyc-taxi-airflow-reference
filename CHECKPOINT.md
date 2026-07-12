# Checkpoint: end of Chapter 6 (Testing DAGs)

This branch is the state your Week 12 Astro project should be in after
[Chapter 6](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__6_testing_dags.md).

**What is here:** the Chapter 5 `dags/taxi_pipeline.py` plus a `tests/`
suite:

- `test_dag_integrity.py`: every DAG imports without error and has tags.
- `test_taxi_pipeline.py`: three unit tests for the pure `parquet_url_for`
  helper (no Airflow runtime needed).
- `test_taxi_pipeline_structure.py`: asserts `dbt_run` runs after
  `ingest_taxi_month`.

**Verified:** all three test files pass via
`astro dev pytest tests/ --args "-v"` on Airflow 3.3.

> Note: the structure test indexes `DagBag(...).dags["taxi_pipeline"]`
> rather than calling `.get_dag("taxi_pipeline")`. Under Airflow 3,
> `get_dag()` reads the serialized DAG from the metadata DB, which the
> ephemeral `astro dev pytest` container has not populated, so it raises
> a SQL error. Indexing `.dags` stays in-process and needs no DB.

**Run the suite:**

```bash
git switch ch6-tests
astro dev pytest tests/ --args "-v"
```

**Next chapters** cover monitoring, debugging, and deploying to the
shared VM. The final DAG (Chapter 7 retry tuning + Chapter 8 dual-path
dbt autodetect) lives on `main` and in the curriculum snapshot at
`Data Track/Week 12/assets/dag_snapshots/taxi_pipeline.py`.

# Exercise: skip dbt on an empty load with @task.branch

**Chapter:** Week 12, [Sequential Pipelines](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__4_sequential_pipelines.md)

## Goal

Make `taxi_pipeline` skip the expensive `dbt_run` and `dbt_test` tasks when `ingest_taxi_month` loads zero rows. There is nothing to transform, so running dbt would waste time. You will use a **branching task** (`@task.branch`) to pick the path at runtime.

## Your task

In `dags/taxi_pipeline.py` (look for the `TODO` marker):

1. Import `EmptyOperator` from `airflow.providers.standard.operators.empty`.
2. Add a `@task.branch()` task `check_rows(row_count: int)` that returns `"dbt_run"` when `row_count > 0`, and `"no_data"` when it is `0`. A branch task returns the `task_id` (or list of ids) to run; every other direct-downstream task is skipped.
3. Add a `no_data = EmptyOperator(task_id="no_data")` as the skip path.
4. Rewire the DAG:
   - `ingest_taxi_month` feeds `check_rows`.
   - `check_rows` feeds both `dbt_run` (which still feeds `dbt_test`) and `no_data`.

## Verify

1. `astro dev run dags reserialize`. The Graph view now shows `ingest_taxi_month → check_rows`, branching to `dbt_run → dbt_test` and to `no_data`.
2. Trigger a normal run: `check_rows` returns `"dbt_run"`, dbt runs, and `no_data` shows `skipped`.
3. To see the skip path, temporarily make `ingest_taxi_month` `return 0` and trigger again: `dbt_run` and `dbt_test` show `skipped`, and `no_data` goes green. Revert when done.

## Success criteria

- A normal run runs dbt and skips `no_data`.
- A zero-row run skips both dbt tasks and runs `no_data`.

Compare your answer against the `ex-branch-skip-solution` branch.

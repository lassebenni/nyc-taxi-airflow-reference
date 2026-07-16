# Exercise: parallel ingestion with a fan-in gate

**Chapter:** Week 12, [Sequential Pipelines](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__4_sequential_pipelines.md)

## Goal

Right now `taxi_pipeline` loads one source (`raw_trips`) before dbt runs. Real pipelines often load several independent sources in parallel, then wait for all of them before transforming. You will add a second ingest task and fan both into a shared gate.

## Your task

In `dags/taxi_pipeline.py` (look for the `TODO` marker):

1. Import `EmptyOperator` from `airflow.providers.standard.operators.empty`.
2. Add a task `ingest_zones_lookup()` that downloads the TLC zone-lookup CSV (`https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`, 265 rows) and loads it into a `raw_zones` table in your `airflow_<student>` schema with `if_exists="replace"`. Use the same `PostgresHook`/`to_sql` pattern as `ingest_taxi_month`, but `pd.read_csv` instead of `pd.read_parquet`. Return the row count.
3. Add a `gate = EmptyOperator(task_id="gate")`.
4. Rewire so both ingests run in parallel and both must finish before dbt:
   ```python
   [ingest_taxi_month(), ingest_zones_lookup()] >> gate >> dbt_run >> dbt_test
   ```

## Verify

1. `astro dev run dags reserialize`. The Graph view shows `ingest_taxi_month` and `ingest_zones_lookup` side by side, both feeding `gate`, then `dbt_run → dbt_test`.
2. Trigger a run. Both ingests go green (they run concurrently), then `gate`, then dbt.
3. Confirm `raw_zones` has 265 rows in your schema:
   ```sql
   SELECT count(*) FROM airflow_<student>.raw_zones;
   ```

## Success criteria

- Two ingest tasks render side by side and run in parallel.
- `gate` waits for both before `dbt_run` starts.
- `raw_zones` contains the 265 zone-lookup rows.

Compare your answer against the `ex-parallel-ingest-solution` branch.

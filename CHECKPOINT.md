# Checkpoint: end of Chapter 4 (Sequential Pipelines)

This branch is the state your Week 12 Astro project should be in after
[Chapter 4](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__4_sequential_pipelines.md).

**What is here:** `dags/taxi_pipeline.py`, the first real pipeline. A
three-task chain `ingest_taxi_month >> dbt_run >> dbt_test` on
`schedule="@daily"`, with per-student schema isolation
(`airflow_<student>`) and the dbt project run through `BashOperator`.
The ingest task downloads a single fixed month (`green_tripdata_2024-01`)
and loads it with `if_exists="replace"`. No date templating or backfills
yet: that is Chapter 5.

**Verified:** `astro dev parse` reports no errors on Airflow 3.3.

**To actually run it** you also need the `azure_pg` Airflow Connection
and the dbt project at `include/dbt_project/` (both kept on this branch).
Set `AIRFLOW_STUDENT` in your `.env` first.

**Diff your own file against it:**

```bash
git switch ch4-taxi
diff dags/taxi_pipeline.py <path-to-your-project>/dags/taxi_pipeline.py
```

**Next chapter** ([Parameterized Runs and Backfills](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__5_parameterized_runs.md))
adds `@monthly` scheduling, `{{ ds }}`-templated download URLs, and an
idempotent delete-then-append load. See branch `ch5-params`.

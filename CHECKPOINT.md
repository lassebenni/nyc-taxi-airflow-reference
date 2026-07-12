# Checkpoint: end of Chapter 5 (Parameterized Runs and Backfills)

This branch is the state your Week 12 Astro project should be in after
[Chapter 5](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__5_parameterized_runs.md).

**What is here:** `dags/taxi_pipeline.py` upgraded from the Chapter 4
version with:

- `schedule="@monthly"` and `max_active_runs=1` (serialize dbt runs).
- `parquet_url_for(ds)` and `_ds_from_context()` pulled out as module-level
  functions, so the download URL is templated per logical date.
- an **idempotent** load: create-if-missing, delete this month's rows,
  then append (`if_exists="append"`), so a re-run or a backfill of the
  same month does not duplicate rows.
- `default_args={"retries": 2}`.

This is very close to the final DAG; Chapter 7 adds retry tuning and
Chapter 8 adds the dual Astro/VM dbt-path autodetect (`find_dbt_dir()`),
which is what lands in the canonical snapshot at
`Data Track/Week 12/assets/dag_snapshots/taxi_pipeline.py`.

**Verified:** `astro dev parse` reports no errors on Airflow 3.3.

**Diff your own file against it:**

```bash
git switch ch5-params
diff dags/taxi_pipeline.py <path-to-your-project>/dags/taxi_pipeline.py
```

**Next chapter** ([Testing DAGs](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__6_testing_dags.md))
adds a `tests/` suite (integrity, unit, structure). See branch
`ch6-tests`.

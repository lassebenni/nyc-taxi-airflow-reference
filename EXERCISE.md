# Exercise: ds helper vs data_interval_start

**Chapter:** Week 12, [Parameterized Runs and Backfills](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__5_parameterized_runs.md)

## Goal

The pipeline reads its logical date through the `_ds_from_context()` helper. Airflow also exposes the raw context variable `data_interval_start`. This exercise makes you compare the two and understand when they give different answers, which is the root of most partition-key bugs.

## Your task

In `dags/taxi_pipeline.py`, inside `ingest_taxi_month` (look for the `TODO` marker):

1. After `ds = _ds_from_context()`, read `data_interval_start` from `get_current_context()` (already imported).
2. Print both. Read `data_interval_start` **defensively**, because it is *absent* on a manual run in Airflow 3 (a bare `ctx["data_interval_start"]` would raise `KeyError`):
   ```python
   ctx = get_current_context()
   dis_raw = ctx.get("data_interval_start")  # None on a manual run
   dis = dis_raw.strftime("%Y-%m-%d") if dis_raw else "None (manual run: no data interval)"
   print(f"ds={ds} data_interval_start={dis}")
   ```
3. Trigger the DAG two ways and read the printed line in the `ingest_taxi_month` log each time:
   - a **scheduled / backfilled** run (via `astro dev run backfill create ...`), and
   - a **manual** trigger from the UI (plain *Trigger* button, no config).

## The question to answer

For a `@monthly` schedule, when do `ds` and `data_interval_start` resolve to the **same** value, and when do they **diverge**? Write one sentence in your own notes.

<details>
<summary>Answer (open after you have observed both runs)</summary>

For a **scheduled or backfilled** run they match: `data_interval_start` is the first of the month, and `logical_date` (what the helper reads) is that same interval boundary, so both print `2024-01-01`. On a **manual trigger** they diverge sharply: Airflow 3 gives a manual run *no data interval at all*, so `data_interval_start` is **absent from the context** (reading `ctx["data_interval_start"]` raises `KeyError`; `ctx.get(...)` returns `None`), while `_ds_from_context()` still returns a usable date via its `run_after` fallback. That is exactly why a partition key must come from the logical-date helper, never from `data_interval_start` (which can be missing) or wall-clock `now()`.

</details>

## Success criteria

- The `ingest_taxi_month` log prints both `ds` and `data_interval_start`.
- You can state in one sentence when the two match and when they diverge.

Compare your answer against the `ex-context-vars-solution` branch.

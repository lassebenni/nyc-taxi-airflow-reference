# Checkpoint: end of Chapter 3 (Scheduling and Triggers)

This branch is the state your Week 12 Astro project should be in after
[Chapter 3](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__3_scheduling_triggers.md).

**What is here:** the same `dags/hello_pipeline.py` from Chapter 2, with
one change: `schedule="0 6 * * 1-5"` (every weekday at 06:00 UTC) instead
of `"@daily"`. This is the only edit Chapter 3 makes to the DAG file; the
rest of the chapter is about `catchup`, logical dates, and sensors you
experiment with in the UI.

**Verified:** `astro dev parse` reports no errors on Airflow 3.3.

**Diff your own file against it:**

```bash
git switch ch3-schedule
diff dags/hello_pipeline.py <path-to-your-project>/dags/hello_pipeline.py
```

**Next chapter** ([Sequential Pipelines](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__4_sequential_pipelines.md))
retires the toy DAG and builds the real `taxi_pipeline`. See branch
`ch4-taxi`.

# Checkpoint: end of Chapter 2 (Airflow Fundamentals)

This branch is the state your Week 12 Astro project should be in after
[Chapter 2](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__2_airflow_fundamentals.md).

**What is here:** `dags/hello_pipeline.py`, a two-task toy DAG
(`ingest` -> `transform`) on `schedule="@daily"`. This is the "hello
world" you build to learn the Astro CLI, the UI, and the TaskFlow
`@task` pattern. No real data, no Postgres yet.

**Verified:** `astro dev parse` reports no errors on Airflow 3.3.

**Diff your own file against it:**

```bash
git switch ch2-hello
diff dags/hello_pipeline.py <path-to-your-project>/dags/hello_pipeline.py
```

**Next chapter** ([Scheduling and Triggers](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__3_scheduling_triggers.md))
changes only the `schedule` argument. See branch `ch3-schedule`.

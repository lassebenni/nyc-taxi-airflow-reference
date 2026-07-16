# Exercise: add a FileSensor

**Chapter:** Week 12, [Scheduling and Triggers](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__3_scheduling_triggers.md)

## Goal

Make `transform` in `hello_pipeline` wait for an external file before it runs. This is the sensor pattern: a task that blocks the pipeline until a condition (here, a file on disk) is met.

## Setup

The FileSensor needs a filesystem connection. Create it once:

```bash
astro dev run connections add fs_default \
  --conn-type fs --conn-extra '{"path": "/"}'
```

## Your task

In `dags/hello_pipeline.py` (look for the `TODO` markers):

1. Import `FileSensor` from `airflow.providers.standard.sensors.filesystem`.
2. Add a `wait_for_flag` FileSensor that watches `/tmp/ready.flag`, with `fs_conn_id="fs_default"`, `mode="reschedule"`, `poke_interval=10`, and `timeout=600`.
3. Wire it so `transform` runs only after the flag appears.

## Verify

1. `astro dev run dags reserialize`, unpause `hello_pipeline`, and trigger a run.
2. `wait_for_flag` should sit in `up_for_reschedule` (not failed): it is polling, releasing its worker slot between checks.
3. Create the flag inside the scheduler container (Linux inside Docker; works from WSL, Git Bash, or PowerShell on Windows):

   ```bash
   astro dev bash --scheduler
   touch /tmp/ready.flag
   exit
   ```

   `touch` runs in the container shell, not in Windows CMD. Use `--scheduler`, not a positional argument.
4. Within about a minute the sensor turns green and `transform` runs. Check its log for `Processed 42 rows`.

## Success criteria

- `wait_for_flag` blocks until the file exists, then goes green.
- `transform` runs only after the sensor succeeds.

Compare your answer against the `ex-file-sensor-solution` branch.

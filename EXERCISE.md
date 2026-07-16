# Exercise: add a WasbBlobSensor

**Chapter:** Week 12, [Scheduling and Triggers](https://github.com/lassebenni/datatrack/blob/main/Data%20Track/Week%2012/week_12__3_scheduling_triggers.md)

## Goal

Make `transform` in `hello_pipeline` wait for a blob in Azure Blob Storage before it runs. This is the cloud version of the `FileSensor` pattern: block the pipeline until an upstream file lands in object storage.

Your `taxi_pipeline` loads into Postgres, so this exercise is deliberately separate from dbt. It teaches event-driven triggers against the `hyfstoragedev` account you already used in Week 6.

## Setup

1. **Install the Azure provider.** This branch already lists `apache-airflow-providers-microsoft-azure` in `requirements.txt`. Rebuild the stack:

   ```bash
   astro dev restart
   ```

2. **Sign in to Azure** (same `az login` you use for Postgres and blob uploads):

   ```bash
   az login
   ```

3. **Create the `wasb_default` connection** from the storage connection string in Key Vault. Run this from your laptop, not inside a container:

   ```bash
   export AZURE_STORAGE_CONNECTION_STRING="$(az keyvault secret show \
     --vault-name kv-hyf-data \
     --name storage-connection-string \
     --query value -o tsv)"

   astro dev run connections add wasb_default \
     --conn-type wasb \
     --conn-extra "{\"connection_string\": \"$AZURE_STORAGE_CONNECTION_STRING\"}"
   ```

   > ⚠️ Never commit the connection string or paste it into the DAG. The Airflow Connection stores it in the metadata database only.

## Your task

In `dags/hello_pipeline.py` (look for the `TODO` markers):

1. Import `WasbBlobSensor` from `airflow.providers.microsoft.azure.sensors.wasb`.
2. Add a `wait_for_blob` sensor on container `raw`, blob `week12-sensor-test/ready.flag`, with `wasb_conn_id="wasb_default"`, `mode="reschedule"`, `poke_interval=30`, and `timeout=600`.
3. Wire it so `transform` runs only after the blob exists.

## Verify

1. `astro dev run dags reserialize`, unpause `hello_pipeline`, and trigger a run.
2. `wait_for_blob` should sit in `up_for_reschedule` (not failed): it is polling Azure for the blob.
3. Upload the blob from your laptop:

   ```bash
   echo ready > /tmp/ready.flag
   az storage blob upload \
     --account-name hyfstoragedev \
     --container-name raw \
     --name week12-sensor-test/ready.flag \
     --file /tmp/ready.flag \
     --auth-mode login \
     --overwrite
   ```

4. Within about a minute the sensor turns green and `transform` runs. Check its log for `Processed 42 rows`.

## Success criteria

- `wait_for_blob` blocks until the blob exists in `raw`, then goes green.
- `transform` runs only after the sensor succeeds.

Compare your answer against the `ex-wasb-sensor-solution` branch.

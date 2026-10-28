"""MySQL -> BigQuery sync DAGs — one per client, separate from the MySQL ETL DAGs.

    naturo_bigqry_pipeline     naturodb         -> BigQuery naturoanalyticsdb          (19 tables)  04:00
    modernkt_bigqry_pipeline   modernkitchendb  -> BigQuery modernkitchenanalyticsdb   (19 tables)  04:30
    ulsnirt_bigqry_pipeline    ullasnirantaradb -> BigQuery ullasnirantaraanalyticsdb  (10 tables)  05:00
    hangyo_bigqry_pipeline     hangyodb         -> BigQuery hangyoanalyticsdb          (5 tables)   11:00

Tasks (one per table, strictly one after another):
    start -> sync_<table_1> -> ... -> sync_<table_n> -> publish -> finish
                                   (any failure) ------------------> on_failure

  start      create missing BigQuery tables, open the run in etl_task (pipeline_type=BIGQUERY),
             cutoff = start time of the last COMPLETED run - 1 hour (date + time;
             first run = full load)
  sync_<t>   rows changed since the cutoff -> hidden <t>__next (live untouched)
  publish    swap every __next into place together; restores on failure
  finish     COMPLETED, drop the hidden tables
  on_failure only runs if a task failed: marks the run FAILED, drops the hidden
             tables — the live BigQuery tables stay exactly as before the run

Schedule (India time): naturo 04:00, modernkt 04:30, ulsnirt 05:00 (staggered, 4 GB VM),
hangyo daily 11:00 (30 min after the
10:30 hangyo_incremental_pipeline, which takes ~10 min). Only where ETL_ENABLE_SCHEDULES=true (the Azure VM);
anywhere else manual trigger only. max_active_runs=1, no catch-up.
"""
from __future__ import annotations

import os

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.trigger_rule import TriggerRule

from config.bigquery_sync_config import BIGQUERY_PIPELINES

_SCHEDULES_ENABLED = os.getenv("ETL_ENABLE_SCHEDULES", "false").strip().lower() in ("1", "true", "yes")
# CronTriggerTimetable: a run is labelled with the time it really starts (the UI
# "Next Run" shows 2026-10-01 04:00, not the day before). start_date = 30 Sep 12:00
# (after that day's 04:00) so no "missed" run fires when Airflow loads the DAG.
_START = pendulum.datetime(2026, 9, 30, 12, tz="Asia/Kolkata")
# per client: (cron in India time, start_date). Hangyo moved to 11:00 on 1 Oct -
# its start_date is after that day's 11:00 so no "missed" 11:00 run fires on deploy.
_SCHEDULES = {
    "NATURO": ("0 4 * * *", _START),                                            # 04:00
    "HANGYO": ("0 11 * * *", pendulum.datetime(2026, 10, 1, 12, tz="Asia/Kolkata")),  # 11:00
    # added 8 Oct; staggered after naturo (4 GB VM). start_date after that day's slot
    # so no "missed" run fires on deploy - first runs 9 Oct (FULL: tables are empty)
    "MODERNKT": ("30 4 * * *", pendulum.datetime(2026, 10, 8, 12, tz="Asia/Kolkata")),  # 04:30
    "ULSNIRT": ("0 5 * * *", pendulum.datetime(2026, 10, 8, 12, tz="Asia/Kolkata")),    # 05:00
}


def _build_dag(client: str) -> DAG:
    tables = list(BIGQUERY_PIPELINES[client]["tables"])
    dag_id = f"{client.lower()}_bigqry_pipeline"

    def _start(**_):
        from engine.services.bigquery_sync_service import start_run
        return start_run(client)

    def _sync(table: str, **context):
        from engine.services.bigquery_sync_service import sync_table
        run = context["ti"].xcom_pull(task_ids="start")
        return sync_table(client, run["task_id"], table, run["cutoff"])

    def _publish(**context):
        from engine.services.bigquery_sync_service import publish
        publish(client, context["ti"].xcom_pull(task_ids="start")["task_id"], tables)

    def _finish(**context):
        from engine.services.bigquery_sync_service import finish_run
        finish_run(client, context["ti"].xcom_pull(task_ids="start")["task_id"], tables)

    def _on_failure(**context):
        from engine.services.bigquery_sync_service import fail_run
        run = context["ti"].xcom_pull(task_ids="start") or {}
        failed = [ti.task_id for ti in context["dag_run"].get_task_instances() if ti.state == "failed"]
        fail_run(client, run.get("task_id"), tables, error=f"failed task(s): {', '.join(failed) or 'unknown'}")

    with DAG(
        dag_id=dag_id,
        description=f"{client}: MySQL -> BigQuery {BIGQUERY_PIPELINES[client]['ddl'].split('/')[-1]} "
                    f"({len(tables)} tables, one task each, atomic publish)",
        start_date=_SCHEDULES[client][1],
        # India time, VM only (see _SCHEDULES)
        schedule=CronTriggerTimetable(_SCHEDULES[client][0], timezone="Asia/Kolkata") if _SCHEDULES_ENABLED else None,
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=False,
        tags=["bigquery", "sync", client.lower()],
    ) as dag:
        start = PythonOperator(task_id="start", python_callable=_start)
        prev, steps = start, [start]
        for t in tables:
            step = PythonOperator(task_id=f"sync_{t}", python_callable=_sync, op_kwargs={"table": t})
            prev >> step
            prev = step
            steps.append(step)
        publish = PythonOperator(task_id="publish", python_callable=_publish)
        finish = PythonOperator(task_id="finish", python_callable=_finish)
        prev >> publish >> finish
        steps += [publish, finish]
        # runs only when something failed (skipped on success)
        on_failure = PythonOperator(task_id="on_failure", python_callable=_on_failure,
                                    trigger_rule=TriggerRule.ONE_FAILED)
        steps >> on_failure
    return dag


# one daily incremental DAG per client that has a schedule in _SCHEDULES
for _client in _SCHEDULES:
    _dag = _build_dag(_client)
    globals()[_dag.dag_id] = _dag

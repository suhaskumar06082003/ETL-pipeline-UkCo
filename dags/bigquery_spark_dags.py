"""MySQL -> BigQuery FULL load with PySpark — manual trigger only.

    naturo_bigqry_fullload_spark     naturodb        -> BigQuery naturoanalyticsdb        (19 tables, FULL)
    modernkt_bigqry_fullload_spark   modernkitchendb  -> BigQuery modernkitchenanalyticsdb  (19 tables, FULL)
    ulsnirt_bigqry_fullload_spark    ullasnirantaradb -> BigQuery ullasnirantaraanalyticsdb (10 tables, FULL)

The daily incremental naturo_bigqry_pipeline (dags/bigquery_sync_dags.py) is not
changed (Modern Kitchen and Ullas have their own daily incremental DAGs too). Each
DAG reloads every table completely, using Spark for the extract. The first run
creates the BigQuery tables from the client's DDL file (sql/bigqry/):

    start -> spark_<table_1> -> ... -> spark_<table_n> -> publish -> finish
                                     (any failure) ------------------> on_failure

  start        open the run in etl_task (pipeline_type=BIGQUERY, stored_format=
               BIGQUERY_SPARK, no cutoff). Refuses to start while another BigQuery
               run of the client is still going (they share the hidden tables).
  spark_<t>    Spark JDBC (parallel) -> Parquet -> BigQuery <t>__delta (load job)
               -> <t>__next with the live column types (live table untouched)
  publish      swap every __next into place together; restores on failure
  finish       COMPLETED, drop the hidden tables. The next incremental run
               continues from this run's start time.
  on_failure   only if a task failed: FAILED, drop the hidden tables — the live
               BigQuery tables stay exactly as before the run

Never scheduled (schedule=None) on any Airflow. max_active_runs=1.
"""
from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

from config.bigquery_sync_config import BIGQUERY_PIPELINES

SPARK_FULL_CLIENTS = ["NATURO", "MODERNKT", "ULSNIRT"]
_START = pendulum.datetime(2026, 10, 8, tz="Asia/Kolkata")


def _build_dag(client: str) -> DAG:
    tables = list(BIGQUERY_PIPELINES[client]["tables"])

    def _start(**_):
        from engine.services.bigquery_sync_service import start_run
        return start_run(client, full=True)

    def _spark_table(table: str, **context):
        from engine.services.bigquery_spark_service import spark_full_table
        return spark_full_table(client, context["ti"].xcom_pull(task_ids="start")["task_id"], table)

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
        dag_id=f"{client.lower()}_bigqry_fullload_spark",
        description=f"{client}: MySQL -> BigQuery FULL load with PySpark ({len(tables)} tables, "
                    f"one task each, atomic publish) - manual only",
        start_date=_START,
        schedule=None,
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=False,
        tags=["bigquery", "spark", "fullload", client.lower()],
    ) as dag:
        start = PythonOperator(task_id="start", python_callable=_start)
        prev, steps = start, [start]
        for t in tables:
            step = PythonOperator(task_id=f"spark_{t}", python_callable=_spark_table, op_kwargs={"table": t})
            prev >> step
            prev = step
            steps.append(step)
        publish = PythonOperator(task_id="publish", python_callable=_publish)
        finish = PythonOperator(task_id="finish", python_callable=_finish)
        prev >> publish >> finish
        steps += [publish, finish]
        on_failure = PythonOperator(task_id="on_failure", python_callable=_on_failure,
                                    trigger_rule=TriggerRule.ONE_FAILED)
        steps >> on_failure
    return dag


for _client in SPARK_FULL_CLIENTS:
    _dag = _build_dag(_client)
    globals()[_dag.dag_id] = _dag

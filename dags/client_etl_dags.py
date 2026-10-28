"""Client-scoped ETL DAGs — one factory, clients x their pipelines.

Generates, per client, the DAGs listed for it in CLIENT_PIPELINES; every DAG
shares the SAME load logic:
    <client>_outlet_master     (OUTLETDUMP)   - incremental
    <client>_product_master    (PRODUCT)      - incremental
    <client>_secondary_sales   (SECONDARYDUMP) - every upload loaded (no date filter)

Each DAG is one-task-per-table: prepare -> load_<table>... -> finalize.
No schedule — the upload API triggers the matching DAG when a file lands
(outlet, product AND secondary), passing that upload's task_id in the run conf.
max_active_runs=1: several uploads on one day queue up and load one after the
other (each with its own task_id), never two loads into the same tables at once.
Blobs and metadata are already client-scoped (blob path is <client>/<type>/,
etl_task stores client_code), so the only per-DAG knob is the client code,
which scopes the fallback pick and the incremental watermark.

Add a client (and which pipelines it gets) to CLIENT_PIPELINES and its DAGs
appear automatically.
"""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from config.flow_config import get_flow

PIPELINES = [
    {"suffix": "outlet_master",   "file_type": "OUTLETDUMP",    "tags": ["etl", "outlet"]},
    {"suffix": "product_master",  "file_type": "PRODUCT",       "tags": ["etl", "product"]},
    {"suffix": "secondary_sales", "file_type": "SECONDARYDUMP", "tags": ["etl", "secondary"]},
]

# client -> which pipelines it gets (by suffix)
CLIENT_PIPELINES = {
    "ULSNIRT":  ["outlet_master", "product_master", "secondary_sales"],
    "NATURO":   ["outlet_master", "product_master", "secondary_sales"],
    "MODERNKT": ["outlet_master", "product_master", "secondary_sales"],
}


def _build_dag(client: str, file_type: str, suffix: str, tags: list[str]) -> DAG:
    dag_id = f"{client.lower()}_{suffix}"
    # One task per TARGET TABLE, in the exact order of the flow config —
    # generated automatically (add/reorder a table in flow_config and the task
    # graph follows). Each table loads in its own transaction; on a failure the
    # DAG stops there (re-running is idempotent via upsert / anti-join).
    tables = [t["handler"] for t in get_flow(file_type)]

    def _prepare(**context):
        from engine.services.data_loader_service import prepare_load
        conf = (context["dag_run"].conf or {}) if context.get("dag_run") else {}
        info = prepare_load(file_type, task_id=conf.get("task_id"), client_code=client)
        if not info.get("task_id"):
            raise AirflowSkipException(f"No INGESTED {file_type} task for {client}.")
        return info

    def _load_table(handler: str, **context):
        from engine.services.data_loader_service import load_one_table
        info = context["ti"].xcom_pull(task_ids="prepare")
        n = load_one_table(file_type, handler, info["task_id"], info["path"], client_code=client)
        print(f"[{client}] loaded {handler}: {n} rows")
        return n

    def _finalize(**context):
        from engine.services.data_loader_service import finalize_load
        info = context["ti"].xcom_pull(task_ids="prepare")
        finalize_load(info["task_id"], info["path"], client_code=client)
        print(f"[{client}] ETL finished for task", info["task_id"])

    with DAG(
        dag_id=dag_id,
        description=f"{client} {suffix}: blob parquet -> {client} MySQL (one task per target table, from flow config)",
        start_date=datetime(2026, 1, 1),
        schedule=None,
        catchup=False,
        max_active_runs=1,              # queue uploads: one load at a time per DAG
        is_paused_upon_creation=False,  # ready to run on a fresh setup
        tags=tags + [client.lower()],
    ) as dag:
        prepare = PythonOperator(task_id="prepare", python_callable=_prepare)
        prev = prepare
        for handler in tables:
            step = PythonOperator(
                task_id=f"load_{handler}",
                python_callable=_load_table,
                op_kwargs={"handler": handler},
            )
            prev >> step
            prev = step
        finalize = PythonOperator(task_id="finalize", python_callable=_finalize)
        prev >> finalize
    return dag


# Register each client's pipeline DAGs in this module's globals (Airflow scans these).
for _client, _suffixes in CLIENT_PIPELINES.items():
    for _p in PIPELINES:
        if _p["suffix"] in _suffixes:
            _dag = _build_dag(_client, _p["file_type"], _p["suffix"], _p["tags"])
            globals()[_dag.dag_id] = _dag

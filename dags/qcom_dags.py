"""Hangyo q-commerce ETL DAGs (pbisalesanalysisdb source tables -> hangyodb).

DAG names (filter in the Airflow UI by prefix):
    hangyo_incremental_pipeline   daily 10:30 IST, ONE DAG, every dataset in turn
    hangyo_fullload_pipeline      manual, ONE DAG, same order, whole source,
                                  replaces the platform rows
    hangyo_bigqry_pipeline        MySQL -> BigQuery, own schedule daily 11:00 IST
                                  (dags/bigquery_sync_dags.py; not triggered by these)

INCREMENTAL - hangyo_incremental_pipeline, strictly one step at a time (only one
task ever writes the shared category / product tables -> no lock conflicts):

    start
      zepto_sales      prepare -> load_category -> load_product -> load_qcom_sales -> finalize
      zepto_soh        prepare -> load_category -> load_product -> load_qcom_stock_on_hand -> finalize
      zepto_po         prepare -> load_category -> load_product -> load_qcom_po -> finalize
      instamart_sales / instamart_soh / instamart_po   (same 5 steps)
      blinkit_sales   / blinkit_soh   / blinkit_po     (same 5 steps)
    end           (no BigQuery trigger - hangyo_bigqry_pipeline runs at 11:00)

  Each dataset keeps its own etl_task row and its own watermark (rows created /
  updated since that dataset's last COMPLETED run). If one dataset fails, the
  next one still runs (its `prepare` waits for the previous dataset to be DONE,
  not to succeed); the failed one is picked up again on the next run because its
  watermark did not move. `end` turns the whole run red if any dataset failed.

  source table per dataset:
    zepto      sales hangyo_Zepto_sales             soh hangyo_zepto_vendor_inventory    po hangyo_zepto_po
    instamart  sales Hangyo_Instamart_Overall_Sales soh hangyo_instamart_stock_on_hand   po hangyo_instamart_po
    blinkit    sales Hangyo_Blinkit_Overall_Sales   soh hangyo_blinkit_stock_on_hand     po hangyo_blinkit_po

FULL RELOAD - hangyo_fullload_pipeline (manual only): the same 9 task groups in the
same order, but every `prepare` reads the WHOLE source table and the fact load
REPLACES that platform's rows (delete + load = one transaction; category /
product are upserted, never deleted). To reload only some datasets, open a run
and Clear just those task groups.

Schedules run ONLY where ETL_ENABLE_SCHEDULES=true (the Azure VM); elsewhere the
DAGs load with schedule=None (manual only).
max_active_runs=1, catchup=False.
"""
from __future__ import annotations

import os

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from config.qcom_flow_config import get_qcom_flow

CLIENT = "HANGYO"

# Only ONE Airflow may run the schedules: the Azure VM sets ETL_ENABLE_SCHEDULES=true
# (docker-compose.prod.yml). Everywhere else (a dev PC) the DAGs load but have no
# schedule, so they never start on their own and never trigger BigQuery.
_SCHEDULES_ENABLED = os.getenv("ETL_ENABLE_SCHEDULES", "false").strip().lower() in ("1", "true", "yes")

# CronTriggerTimetable: a run is labelled with the time it really starts (UI "Next
# Run" = 2026-10-08 10:30). Moved 10:00 -> 10:30 on 7 Oct: start_date = 7 Oct 10:31
# India time (just after that day's 10:30) so Airflow does not fire a "missed"
# 10:30 run the moment it loads the change.
_INCREMENTAL_START = pendulum.datetime(2026, 10, 7, 10, 31, tz="Asia/Kolkata")
_DAILY_1030AM = CronTriggerTimetable("30 10 * * *", timezone="Asia/Kolkata")
_FULLLOAD_START = pendulum.datetime(2026, 9, 30, 12, tz="Asia/Kolkata")

# run order: platform by platform, and inside a platform sales -> soh -> po
PLATFORMS = ["zepto", "instamart", "blinkit"]
DATASETS = [  # (task group name, full-load dag suffix, file_type pattern)
    ("sales", "qcom", "QCOM_{P}"),
    ("soh", "soh", "QCOM_SOH_{P}"),
    ("po", "po", "QCOM_PO_{P}"),
]


# -- task callables (shared by the incremental pipeline and the full-load DAGs) --
def _prepare(file_type: str, full: bool, **_):
    from engine.services.qcom_loader_service import qcom_prepare_load
    info = qcom_prepare_load(file_type, client_code=CLIENT, full_load=full)
    print(f"[{CLIENT}] {file_type} prepared task={info['task_id']} -> {info['path']}")
    return info


def _load_table(file_type: str, handler: str, full: bool, prepare_task: str, **context):
    from engine.services.qcom_loader_service import qcom_load_one_table
    info = context["ti"].xcom_pull(task_ids=prepare_task)
    # full reload: the fact table's platform rows are replaced (category /
    # product are still upserted - never deleted)
    n = qcom_load_one_table(file_type, handler, info["task_id"], info["path"], client_code=CLIENT,
                            replace=full and handler not in ("category", "product"))
    print(f"[{CLIENT}] {file_type} loaded {handler}: {n} rows")
    return n


def _finalize(file_type: str, prepare_task: str, **context):
    from engine.services.data_loader_service import finalize_load
    info = context["ti"].xcom_pull(task_ids=prepare_task)
    finalize_load(info["task_id"], info["path"], client_code=CLIENT)
    print(f"[{CLIENT}] {file_type} finished, task", info["task_id"])


def _dataset_tasks(file_type: str, full: bool, prefix: str = "",
                   prepare_rule: str = TriggerRule.ALL_SUCCESS):
    """prepare -> load_<table> (one per target table, flow-config order) -> finalize.
    Returns (prepare, finalize). `prefix` = the TaskGroup id when inside a group."""
    prepare = PythonOperator(task_id="prepare", python_callable=_prepare, trigger_rule=prepare_rule,
                             op_kwargs={"file_type": file_type, "full": full})
    ptask = f"{prefix}.prepare" if prefix else "prepare"
    prev = prepare
    for handler in (t["handler"] for t in get_qcom_flow(file_type)):
        step = PythonOperator(task_id=f"load_{handler}", python_callable=_load_table,
                              op_kwargs={"file_type": file_type, "handler": handler,
                                         "full": full, "prepare_task": ptask})
        prev >> step
        prev = step
    finalize = PythonOperator(task_id="finalize", python_callable=_finalize,
                              op_kwargs={"file_type": file_type, "prepare_task": ptask})
    prev >> finalize
    return prepare, finalize


def _pipeline_dag(full: bool) -> DAG:
    """ONE DAG, every dataset one after another:
    zepto sales -> soh -> po -> instamart sales -> soh -> po -> blinkit sales -> soh -> po."""
    kind = "fullload" if full else "incremental"
    with DAG(
        dag_id=f"{CLIENT.lower()}_{kind}_pipeline",
        description=(f"HANGYO {'FULL RELOAD (manual): whole source tables, replaces the platform rows' if full else 'daily incremental'}"
                     ": zepto -> instamart -> blinkit, each sales -> soh -> po "
                     "(prepare -> category -> product -> fact -> finalize)"),
        start_date=_FULLLOAD_START if full else _INCREMENTAL_START,
        # incremental: 10:30 India time, VM only; full reload: always manual
        schedule=_DAILY_1030AM if (_SCHEDULES_ENABLED and not full) else None,
        catchup=False,
        max_active_runs=1,
        is_paused_upon_creation=False,
        tags=["qcom", kind, CLIENT.lower()],
    ) as dag:
        prev = EmptyOperator(task_id="start")
        finals = []
        for platform in PLATFORMS:
            for group, _suffix, ft_pattern in DATASETS:
                gid = f"{platform}_{group}"
                with TaskGroup(group_id=gid) as tg:
                    # ALL_DONE: start this dataset even if the previous one failed
                    _first, last = _dataset_tasks(ft_pattern.format(P=platform.upper()), full=full,
                                                  prefix=gid, prepare_rule=TriggerRule.ALL_DONE)
                prev >> tg
                prev = last
                finals.append(last)
        # red run if any dataset failed (all 9 finalize must succeed)
        finals >> EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_SUCCESS)
    return dag


hangyo_incremental_pipeline = _pipeline_dag(full=False)   # daily 10:30 IST
hangyo_fullload_pipeline = _pipeline_dag(full=True)       # manual only

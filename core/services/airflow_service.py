"""Trigger an Airflow DAG run via the Airflow REST API.

Used by the upload API to auto-start the matching ETL DAG right after a file
lands in blob. Best-effort: a failure here never fails the upload.
"""
import logging

import httpx

from core.settings import get_settings

logger = logging.getLogger("ETL_AIRFLOW_TRIGGER")

# file type -> DAG name suffix (the client code is prefixed per client)
_SUFFIX_BY_FILE_TYPE = {
    "OUTLETDUMP": "outlet_master",
    "PRODUCT": "product_master",
    "SECONDARYDUMP": "secondary_sales",
}


def dag_for(client_code: str, file_type: str) -> str | None:
    """Client-scoped DAG id, e.g. ('NATURO','OUTLETDUMP') -> 'naturo_outlet_master'."""
    suffix = _SUFFIX_BY_FILE_TYPE.get(file_type)
    if not suffix or not client_code:
        return None
    return f"{str(client_code).lower()}_{suffix}"


def trigger_dag(client_code: str, file_type: str, task_id: str) -> bool:
    """Trigger the client-scoped DAG for this file type, passing the etl_task
    id in conf. Returns True if Airflow accepted the run (never raises).
    """
    s = get_settings()
    dag_id = dag_for(client_code, file_type)
    if not dag_id or not s.airflow_base_url:
        return False
    url = f"{s.airflow_base_url.rstrip('/')}/api/v1/dags/{dag_id}/dagRuns"
    try:
        r = httpx.post(
            url,
            auth=(s.airflow_username, s.airflow_password),
            json={"conf": {"task_id": task_id}},
            timeout=15,
        )
        if r.status_code in (200, 201):
            logger.info("----> [AUTO-TRIGGER] %s started for task %s", dag_id, task_id)
            return True
        logger.warning("----> [AUTO-TRIGGER] %s failed: HTTP %s %s", dag_id, r.status_code, r.text[:200])
        return False
    except Exception as e:  # never fail the upload because of a trigger issue
        logger.warning("----> [AUTO-TRIGGER] %s error: %s", dag_id, e)
        return False

"""Load runner — orchestrates one file's blob->MySQL load with stage tracking.

run_outlet_load(task_id): read the etl_task, download its Parquet from blob,
clean column-wise, inject audit columns, run the outlet flow into the naturo
tables, and record ETL_EXTRACT / ETL_TRANSFORM / ETL_LOAD / COMPLETED stages.
"""
import io
import logging
import os
import tempfile

import pandas as pd
from sqlalchemy import text

from core.services.blob_service import AzureBlobConnection
from core.queries.db_connection import db_for_client, get_engine
from core.settings import get_settings
from core.utils.logger import configure_etl_logging
from api.repositories import (
    DEFAULT_USER_ID,
    MySQLTracker,
    PipelineStage,
    StageStatus,
    TaskStatus,
)
from config.flow_config import get_flow
from engine.services.data_distribution_service import load_flow, load_table
from engine.serializers import (
    clean_outlet_df,
    clean_product_df,
    clean_secondary_df,
)

logger = logging.getLogger("ETL_LOAD_RUNNER")

_CLEANERS = {
    "OUTLETDUMP": clean_outlet_df,
    "PRODUCT": clean_product_df,
    "SECONDARYDUMP": clean_secondary_df,
}


# File types loaded incrementally (delta by date) instead of full every time.
_INCREMENTAL = {"OUTLETDUMP", "PRODUCT"}

# Raw (pre-clean) date columns to check for "created or updated since last run".
# Outlet exports vary, so we match any of these that are present in the file.
_DATE_CANDIDATES = [
    "Outlet_Creation_Date", "Last_Modified_On",
    "Created_At", "Updated_At",
    "Outlet_Updation_Date", "Outlet_Updated_Date",
    "Updated_Date", "Update_Date", "Modified_Date",
    "Last_Modified_Date", "Last_Updated_Date", "Last_Updated", "Last_Modified",
]


def _last_success_cutoff(engine, file_type: str, client_code: str | None = None):
    """Datetime of the last COMPLETED ETL for this file type (+client), None if never."""
    sql = "SELECT MAX(updated_at) FROM etl_task WHERE file_type=:ft AND status=:st"
    params = {"ft": file_type, "st": str(TaskStatus.COMPLETED)}
    if client_code:
        sql += " AND client_code=:cc"
        params["cc"] = str(client_code)
    with engine.connect() as c:
        return c.execute(text(sql), params).scalar()


def _incremental_filter(df, cutoff):
    """Keep rows whose creation/update date >= cutoff (date). Returns (df, date_cols_used).

    No cutoff (first run) or no date columns -> return df unchanged (full load).
    """
    date_cols = [c for c in _DATE_CANDIDATES if c in df.columns]
    if cutoff is None or not date_cols:
        return df, date_cols
    cutoff_day = pd.Timestamp(cutoff).normalize()  # compare by date (midnight)
    parsed = pd.DataFrame({c: pd.to_datetime(df[c], errors="coerce") for c in date_cols})
    keep = (parsed >= cutoff_day).any(axis=1)      # any date on/after the cutoff day
    return df[keep].reset_index(drop=True), date_cols


def _latest_ingested(engine, file_type: str, client_code: str | None = None) -> str | None:
    sql = "SELECT task_id FROM etl_task WHERE file_type=:ft AND status=:st"
    params = {"ft": file_type, "st": str(TaskStatus.INGESTED)}
    if client_code:
        sql += " AND client_code=:cc"
        params["cc"] = str(client_code)
    sql += " ORDER BY created_at DESC LIMIT 1"
    with engine.connect() as conn:
        row = conn.execute(text(sql), params).first()
    return row[0] if row else None


def run_load(task_id: str | None = None, file_type: str = "OUTLETDUMP") -> dict:
    """Load one ingested task's blob Parquet into the naturo tables."""
    settings = get_settings()
    engine = get_engine()
    tracker = MySQLTracker()

    if task_id is None:
        task_id = _latest_ingested(engine, file_type)
        if not task_id:
            logger.info("No INGESTED %s task to load.", file_type)
            return {"status": "nothing_to_load"}

    task = tracker.get_task(task_id)
    if not task:
        raise ValueError(f"Task {task_id} not found")

    file_type = task["file_type"]
    blob_name = task["blob_name"]
    tracker.set_status(task_id, TaskStatus.PROCESSING)

    stage = PipelineStage.ETL_EXTRACT
    try:
        # EXTRACT — download the Parquet from blob
        data = AzureBlobConnection(settings).download_blob(blob_name)
        df = pd.read_parquet(io.BytesIO(data), engine="pyarrow")
        tracker.update_stage(task_id, PipelineStage.ETL_EXTRACT, StageStatus.SUCCESS, rows=int(len(df)))

        # TRANSFORM — column-wise clean + inject audit columns
        stage = PipelineStage.ETL_TRANSFORM
        cleaner = _CLEANERS.get(file_type)
        if cleaner:
            df = cleaner(df)
        df["client_id"] = None       # no master DB
        df["ext_erp_id"] = None      # no master DB
        df["created_by"] = DEFAULT_USER_ID
        df["updated_by"] = DEFAULT_USER_ID
        tracker.update_stage(task_id, PipelineStage.ETL_TRANSFORM, StageStatus.SUCCESS, stage_rows=int(len(df)))

        # LOAD — walk the flow table-by-table into naturo
        stage = PipelineStage.ETL_LOAD
        counts = load_flow(engine, get_flow(file_type), df)
        tracker.update_stage(task_id, PipelineStage.ETL_LOAD, StageStatus.SUCCESS)

        tracker.update_stage(task_id, "FINISHED", StageStatus.SUCCESS)
        tracker.set_status(task_id, TaskStatus.COMPLETED)
        logger.info("----> [ETL COMPLETE] task=%s counts=%s", task_id, counts)
        return {"status": "completed", "task_id": task_id, "counts": counts}

    except Exception as e:
        tracker.update_stage(task_id, stage, StageStatus.FAILED, error=str(e))
        tracker.set_status(task_id, TaskStatus.FAILED)
        logger.exception("ETL load failed for task %s", task_id)
        raise


# ─────────────────────────────────────────────────────────────
# Per-table DAG steps: prepare -> load each table in order -> finalize
# ─────────────────────────────────────────────────────────────
def prepare_load(file_type: str, task_id: str | None = None, client_code: str | None = None) -> dict:
    """EXTRACT + TRANSFORM: download blob, clean column-wise, write a local
    Parquet the per-table tasks read. Returns {task_id, path}.

    client_code scopes the fallback pick and the incremental watermark to one
    client so multiple clients' pipelines never affect each other.
    """
    configure_etl_logging()
    settings = get_settings()
    engine = get_engine(client_code)
    tracker = MySQLTracker(client_code)
    if task_id is None:
        task_id = _latest_ingested(engine, file_type, client_code)
    if not task_id:
        logger.warning("----> [PREPARE] No INGESTED %s task for client=%s. Nothing to load.", file_type, client_code)
        return {"task_id": None, "path": None}

    task = tracker.get_task(task_id)
    if not task:
        raise ValueError(f"Task {task_id} not found")
    client_code = task.get("client_code") or client_code
    tracker.set_status(task_id, TaskStatus.PROCESSING)

    logger.info("========================================================================")
    logger.info("----> [ETL PREPARE START] client=%s  file_type=%s  task=%s  db=%s",
                client_code, file_type, task_id, db_for_client(client_code))
    logger.info("----> [PREPARE] blob=%s", task["blob_name"])

    data = AzureBlobConnection(settings).download_blob(task["blob_name"])
    df = pd.read_parquet(io.BytesIO(data), engine="pyarrow")
    full_count = int(len(df))
    logger.info("----> [EXTRACT] downloaded parquet -> %s rows, %s columns", full_count, len(df.columns))

    # Incremental (delta) load: only rows created/updated since the last
    # successful ETL. First run (no history) loads the full master.
    if file_type in _INCREMENTAL:
        cutoff = _last_success_cutoff(engine, file_type, client_code)
        df, date_cols = _incremental_filter(df, cutoff)
        logger.info(
            "----> [INCREMENTAL] client=%s file_type=%s cutoff=%s date_cols=%s kept %s of %s rows",
            client_code, file_type, cutoff, date_cols, len(df), full_count,
        )

    raw_count = int(len(df))
    # row_count = rows in the file as read from Blob — recorded ONCE, here; no later stage changes it
    tracker.update_stage(task_id, PipelineStage.ETL_EXTRACT, StageStatus.SUCCESS, rows=full_count)

    logger.info("----> [TRANSFORM] cleaning %s rows column-wise ...", raw_count)
    cleaner = _CLEANERS.get(file_type)
    if cleaner:
        df = cleaner(df)
    valid_count = int(len(df))                 # rows kept after cleaning/validation
    invalid_count = raw_count - valid_count     # rows dropped
    # Resolve the real client_id + ext_erp_id from the master DB (user_db) —
    # exactly like retailverse: client_code (shortcode) -> client_id, and
    # ext_erp_rec_code (rec_code) -> ext_erp_id/name. These ids scope every row
    # and drive the parent-id lookups / dedupe in the load engine.
    from engine.services.core_data_validator_service import CoreDataValidatorService
    ext_erp_rec_code = task.get("ext_erp_rec_code")
    client_id, ext_erp_id, _ext_erp_name = CoreDataValidatorService(
        client_code, ext_erp_rec_code).get_client_and_erp_ids()
    df["client_id"] = client_id
    df["ext_erp_id"] = ext_erp_id
    df["created_by"] = DEFAULT_USER_ID
    df["updated_by"] = DEFAULT_USER_ID
    logger.info("----> [TRANSFORM] valid=%s  invalid(dropped)=%s  client_id=%s  ext_erp_id=%s",
                valid_count, invalid_count, client_id, ext_erp_id)
    tracker.update_stage(
        task_id, PipelineStage.ETL_TRANSFORM, StageStatus.SUCCESS,
        valid_record_count=valid_count, invalid_record_count=invalid_count, stage_rows=valid_count,
    )

    path = os.path.join(tempfile.gettempdir(), f"etl_{task_id}_{file_type}.parquet")
    df.to_parquet(path, engine="pyarrow", index=False)
    logger.info("----> [PREPARE DONE] wrote cleaned parquet -> %s", path)
    return {"task_id": task_id, "path": path}


def load_one_table(file_type: str, handler: str, task_id: str, path: str,
                   client_code: str | None = None) -> int:
    """LOAD one target table (its own transaction) into the client's DB.

    Per-table task for the DAG: the table's insert is atomic on its own; if it
    fails, that table rolls back and the DAG stops (re-running is idempotent via
    upsert / anti-join).
    """
    configure_etl_logging()
    engine = get_engine(client_code)
    tracker = MySQLTracker(client_code)
    table = next(t for t in get_flow(file_type) if t["handler"] == handler)
    df = pd.read_parquet(path, engine="pyarrow")
    stage = f"LOAD_{table['table_name'].upper()}"
    logger.info("----> [LOAD TABLE START] client=%s  table=%s  (source rows=%s, db=%s)",
                client_code, table["table_name"], len(df), db_for_client(client_code))
    try:
        with engine.begin() as conn:          # one transaction per table
            n = load_table(conn, table, df)
        # stage row_count = records sent to insert/upsert into this table
        # (etl_task.row_count is untouched — it is set once, at extract)
        tracker.update_stage(task_id, stage, StageStatus.SUCCESS, stage_rows=n)
        logger.info("----> [LOAD TABLE DONE] table=%s  inserted/updated=%s rows\n", table["table_name"], n)
        return n
    except Exception as e:
        tracker.update_stage(task_id, stage, StageStatus.FAILED, error=str(e))
        tracker.set_status(task_id, TaskStatus.FAILED)
        logger.error("----> [LOAD TABLE FAILED] table=%s : %s", table["table_name"], str(e).splitlines()[0][:200])
        raise


def load_all(file_type: str, task_id: str, path: str, client_code: str | None = None) -> dict:
    """LOAD every target table from the prepared Parquet in ONE transaction.

    ATOMIC — all tables commit together; if ANY table fails the WHOLE load rolls
    back, leaving no partial/inconsistent data. Per-table LOAD_<TABLE> stages are
    recorded only after the single transaction commits successfully.
    """
    configure_etl_logging()
    engine = get_engine(client_code)
    tracker = MySQLTracker(client_code)
    df = pd.read_parquet(path, engine="pyarrow")
    logger.info("========================================================================")
    logger.info("----> [LOAD ALL START] client=%s file_type=%s db=%s (ATOMIC — one transaction, all tables)",
                client_code, file_type, db_for_client(client_code))
    try:
        # single transaction across every table; rolls back entirely on any failure
        counts = load_flow(engine, get_flow(file_type), df)
    except Exception as e:
        tracker.update_stage(task_id, PipelineStage.ETL_LOAD, StageStatus.FAILED, error=str(e))
        tracker.set_status(task_id, TaskStatus.FAILED)
        logger.error("----> [LOAD ALL FAILED — ROLLED BACK ALL TABLES] %s", str(e).splitlines()[0][:200])
        raise
    # committed — record per-table stages
    for table_name, n in counts.items():
        tracker.update_stage(task_id, f"LOAD_{table_name.upper()}", StageStatus.SUCCESS, stage_rows=n)
    logger.info("----> [LOAD ALL DONE] committed all tables: %s", counts)
    return counts


# File types after which beat.total_outlets is recomputed (retailverse
# TriggersService.triggermethods: OUTLETLIST + SECONDARYDUMP).
_BEAT_TRIGGER_FILE_TYPES = {"OUTLETDUMP", "SECONDARYDUMP"}


def _trigger_beat_totals(client_code: str | None) -> None:
    """beat.total_outlets = COUNT(DISTINCT outlet_id) per beat in beat_outlet —
    the retailverse beat trigger (TriggersQueries.trigger_beat_table), done as
    one UPDATE ... JOIN. Best-effort like the reference: a failure is logged,
    never fails the load."""
    try:
        with get_engine(client_code).begin() as conn:
            n = conn.execute(text(
                "UPDATE beat b JOIN (SELECT beat_id, COUNT(DISTINCT outlet_id) AS total_outlets "
                "FROM beat_outlet GROUP BY beat_id) x ON x.beat_id = b.beat_id "
                "SET b.total_outlets = x.total_outlets")).rowcount
        logger.info("----> [TRIGGER] beat.total_outlets recomputed (%s beats changed)", n)
    except Exception as e:  # noqa: BLE001 — reference only logs a warning
        logger.warning("----> [TRIGGER] beat.total_outlets update failed: %s", str(e).splitlines()[0][:200])


def finalize_load(task_id: str, path: str, client_code: str | None = None) -> None:
    """Mark FINISHED/COMPLETED, run the post-load trigger, clean up the temp Parquet."""
    configure_etl_logging()
    tracker = MySQLTracker(client_code)
    # final stage so current_stage reads FINISHED, not the last table loaded
    tracker.update_stage(task_id, "FINISHED", StageStatus.SUCCESS)
    tracker.set_status(task_id, TaskStatus.COMPLETED)
    task = tracker.get_task(task_id) or {}
    if task.get("file_type") in _BEAT_TRIGGER_FILE_TYPES:
        _trigger_beat_totals(client_code)
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except OSError:
        pass
    logger.info("----> [ETL FINISHED] task=%s -> COMPLETED", task_id)
    logger.info("========================================================================\n")

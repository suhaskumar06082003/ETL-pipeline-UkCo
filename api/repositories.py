"""ETL metadata tracker — backed by MySQL (naturo.etl_task / etl_task_stage).

Tracks each uploaded file as a "task" and records every pipeline stage from
upload through to ETL completion.

    UPLOAD_RECEIVED -> FILE_VALIDATION -> EXTRACT -> COLUMN_VALIDATION
    -> CONVERT_PARQUET -> STORE_BLOB        (ingestion API)
    -> ETL_EXTRACT -> ETL_TRANSFORM -> ETL_LOAD -> COMPLETED   (Airflow)

The store is MySQL. `MetadataTracker` is the base interface; `MySQLTracker` is
the production implementation and `InMemoryTracker` is a lightweight one for
tests. Records returned by get_task/list_tasks keep the same shape the API
used before (rows / columns_count / stages[]).
"""
import uuid
from datetime import datetime, timezone
from enum import StrEnum
from functools import lru_cache

from api import queries
from core.queries.query_executor import QueryExecutor


class PipelineStage(StrEnum):
    __repr__ = str.__repr__
    # ── ingestion API ──
    UPLOAD_RECEIVED = "UPLOAD_RECEIVED"
    FILE_VALIDATION = "FILE_VALIDATION"
    EXTRACT = "EXTRACT"
    COLUMN_VALIDATION = "COLUMN_VALIDATION"
    CONVERT_PARQUET = "CONVERT_PARQUET"
    STORE_BLOB = "STORE_BLOB"
    # ── downstream (Airflow) ──
    ETL_EXTRACT = "ETL_EXTRACT"
    ETL_TRANSFORM = "ETL_TRANSFORM"
    ETL_LOAD = "ETL_LOAD"


class StageStatus(StrEnum):
    __repr__ = str.__repr__
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class TaskStatus(StrEnum):
    __repr__ = str.__repr__
    PENDING = "PENDING"
    INGESTED = "INGESTED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# Hardcoded default user id (no auth yet). All tasks are attributed to this id.
DEFAULT_USER_ID = 6


# Map the tracker's kwargs to etl_task columns.
_TASK_COLUMN_MAP = {
    "rows": "row_count",
    "columns_count": "column_count",
    "valid_record_count": "valid_record_count",
    "invalid_record_count": "invalid_record_count",
    "stored_format": "stored_format",
    "blob_name": "blob_name",
    "blob_url": "blob_url",
    "error": "error",
    "created_by": "created_by",
    "updated_by": "updated_by",
}


class MetadataTracker:
    """Interface shared by the MySQL and in-memory implementations."""

    def create_task(self, client_code, file_type, filename, size_bytes,
                    ext_erp_rec_code=None,
                    created_by=DEFAULT_USER_ID, updated_by=DEFAULT_USER_ID) -> str:
        raise NotImplementedError

    def update_stage(self, task_id, stage, status, **fields) -> None:
        raise NotImplementedError

    def set_status(self, task_id, status) -> None:
        raise NotImplementedError

    def get_task(self, task_id) -> dict | None:
        raise NotImplementedError

    def list_tasks(self) -> list[dict]:
        raise NotImplementedError


# ─────────────────────────────────────────────────────────────
# MySQL implementation
# ─────────────────────────────────────────────────────────────
class MySQLTracker(MetadataTracker):
    def __init__(self, client_code: str | None = None):
        # metadata (etl_task / etl_task_stage) lives in the client's own DB
        self.client_code = client_code

    def _engine(self):
        from core.queries.db_connection import get_engine
        return get_engine(self.client_code)

    @staticmethod
    def _task_to_dict(row, stages) -> dict:
        return {
            "task_id": row["task_id"],
            "pipeline_type": row.get("pipeline_type"),
            "client_code": row["client_code"],
            "file_type": row["file_type"],
            "ext_erp_rec_code": row["ext_erp_rec_code"],
            "original_filename": row["original_filename"],
            "size_bytes": row["size_bytes"],
            "rows": row["row_count"],
            "columns_count": row["column_count"],
            "valid_record_count": row["valid_record_count"],
            "invalid_record_count": row["invalid_record_count"],
            "stored_format": row["stored_format"],
            "blob_name": row["blob_name"],
            "blob_url": row["blob_url"],
            "status": row["status"],
            "current_stage": row["current_stage"],
            "cutoff_date": row["cutoff_date"].isoformat() if row.get("cutoff_date") else None,
            "error": row["error"],
            "created_by": row["created_by"],
            "updated_by": row["updated_by"],
            "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
            "stages": [
                {
                    "stage": s["stage"],
                    "status": s["status"],
                    "row_count": s.get("row_count"),
                    "error": s["error"],
                    "timestamp": s["created_at"].isoformat() if s["created_at"] else None,
                }
                for s in stages
            ],
        }

    def create_task(self, client_code, file_type, filename, size_bytes,
                    ext_erp_rec_code=None,
                    created_by=DEFAULT_USER_ID, updated_by=DEFAULT_USER_ID) -> str:
        task_id = uuid.uuid4().hex[:12]
        with self._engine().begin() as conn:
            QueryExecutor.execute_on(
                conn,
                queries.INSERT_TASK,
                {
                    "task_id": task_id,
                    "client_code": str(client_code),
                    "file_type": str(file_type),
                    "ext_erp_rec_code": ext_erp_rec_code,
                    "original_filename": filename,
                    "size_bytes": size_bytes,
                    "status": str(TaskStatus.PENDING),
                    "current_stage": str(PipelineStage.UPLOAD_RECEIVED),
                    "created_by": created_by,
                    "updated_by": updated_by,
                },
            )
            QueryExecutor.execute_on(
                conn,
                queries.INSERT_TASK_STAGE,
                {
                    "task_id": task_id,
                    "file_type": str(file_type),
                    "stage": str(PipelineStage.UPLOAD_RECEIVED),
                    "status": str(StageStatus.SUCCESS),
                },
            )
        return task_id

    def update_stage(self, task_id, stage, status, **fields) -> None:
        # Build the etl_task update from known, mapped columns only (no injection).
        sets = {"current_stage": str(stage)}
        for key, value in fields.items():
            col = _TASK_COLUMN_MAP.get(key)
            if col:
                sets[col] = value

        set_clause = ", ".join(f"{col} = :{col}" for col in sets)
        with self._engine().begin() as conn:
            QueryExecutor.execute_on(
                conn,
                queries.update_task_sql(set_clause),
                {**sets, "task_id": task_id},
            )
            QueryExecutor.execute_on(
                conn,
                queries.INSERT_TASK_STAGE_WITH_ERROR,
                {
                    "task_id": task_id,
                    "stage": str(stage),
                    "status": str(status),
                    # rows at this stage: stage_rows (e.g. records upserted into a
                    # table) — or the task-level rows when that is what was passed
                    "row_count": fields.get("stage_rows", fields.get("rows")),
                    "error": fields.get("error"),
                },
            )

    def set_status(self, task_id, status) -> None:
        with self._engine().begin() as conn:
            QueryExecutor.execute_on(
                conn,
                queries.UPDATE_TASK_STATUS,
                {"status": str(status), "task_id": task_id},
            )

    def get_task(self, task_id) -> dict | None:
        with self._engine().connect() as conn:
            row = QueryExecutor.execute_on(
                conn, queries.SELECT_TASK, {"t": task_id}
            ).mappings().first()
            if not row:
                return None
            stages = QueryExecutor.execute_on(
                conn, queries.SELECT_TASK_STAGES, {"t": task_id}
            ).mappings().all()
        return self._task_to_dict(row, stages)

    def list_tasks(self) -> list[dict]:
        with self._engine().connect() as conn:
            rows = QueryExecutor.execute_on(
                conn, queries.SELECT_ALL_TASKS
            ).mappings().all()
            stage_rows = QueryExecutor.execute_on(
                conn, queries.SELECT_ALL_STAGES
            ).mappings().all()

        by_task: dict[str, list] = {}
        for s in stage_rows:
            by_task.setdefault(s["task_id"], []).append(s)
        return [self._task_to_dict(r, by_task.get(r["task_id"], [])) for r in rows]


# ─────────────────────────────────────────────────────────────
# In-memory implementation (tests / no-DB)
# ─────────────────────────────────────────────────────────────
class InMemoryTracker(MetadataTracker):
    def __init__(self) -> None:
        self._store: dict[str, dict] = {}

    def create_task(self, client_code, file_type, filename, size_bytes,
                    ext_erp_rec_code=None,
                    created_by=DEFAULT_USER_ID, updated_by=DEFAULT_USER_ID) -> str:
        task_id = uuid.uuid4().hex[:12]
        now = _now()
        self._store[task_id] = {
            "task_id": task_id,
            "client_code": str(client_code),
            "file_type": str(file_type),
            "ext_erp_rec_code": ext_erp_rec_code,
            "original_filename": filename,
            "size_bytes": size_bytes,
            "rows": None,
            "columns_count": None,
            "stored_format": None,
            "blob_name": None,
            "blob_url": None,
            "status": str(TaskStatus.PENDING),
            "current_stage": str(PipelineStage.UPLOAD_RECEIVED),
            "error": None,
            "created_by": created_by,
            "updated_by": updated_by,
            "created_at": now,
            "updated_at": now,
            "stages": [{"stage": str(PipelineStage.UPLOAD_RECEIVED),
                        "status": str(StageStatus.SUCCESS), "timestamp": now}],
        }
        return task_id

    def update_stage(self, task_id, stage, status, **fields) -> None:
        rec = self._store.get(task_id)
        if not rec:
            return
        now = _now()
        entry = {"stage": str(stage), "status": str(status), "timestamp": now,
                 "row_count": fields.get("stage_rows", fields.get("rows"))}
        if "error" in fields:
            entry["error"] = fields["error"]
        rec["stages"].append(entry)
        rec["current_stage"] = str(stage)
        rec["updated_at"] = now
        for key, value in fields.items():
            rec[key] = value

    def set_status(self, task_id, status) -> None:
        rec = self._store.get(task_id)
        if rec:
            rec["status"] = str(status)
            rec["updated_at"] = _now()

    def get_task(self, task_id) -> dict | None:
        return self._store.get(task_id)

    def list_tasks(self) -> list[dict]:
        return list(self._store.values())


def get_tracker(client_code: str | None = None) -> MetadataTracker:
    """FastAPI dependency: a MySQL-backed tracker scoped to the client's DB.

    `client_code` is read from the request's query string, so uploads and task
    lookups hit the right per-client database. Missing -> default DB (naturo).
    """
    return MySQLTracker(client_code)

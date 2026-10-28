"""API-side orchestration services used by the views (retailverse-style).

  * EtlHandler        the ingestion pipeline: validate -> read/clean -> validate
                      columns -> Parquet -> Azure Blob raw zone -> auto-trigger
                      the ETL DAG. Streams one SSE event per stage. (Mirrors
                      retailverse pipeline_launcher.EtlHandler.etl_handler, with
                      the new blob + streaming logic.)
  * GetFileTemplets   the template-builder (verbatim from retailverse).

The views stay thin: validate the request, then delegate to a service here.
"""
import asyncio
import json
import io
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
from fastapi.responses import JSONResponse, StreamingResponse

from api.repositories import PipelineStage, StageStatus, TaskStatus
from config.clients_config import CLIENT_REQUIREMENTS, blob_location
from config.etl_enums import ConfigKey, FileType
from core.services.blob_service import AzureBlobConnection
from core.utils.exceptions import ETLError, ValidationError
from core.utils.file_handler import FileHandler
from engine.services.config_validation_service import DataPreprocessor, validate_required_columns

logger = logging.getLogger("ETL_VALIDATION")

ALLOWED_EXTENSIONS = {"csv", "xlsx"}


# ─────────────────────────────────────────────────────────────
# Config-validation service — file + required-column validation.
# ─────────────────────────────────────────────────────────────
def validate_file(filename: str, contents: bytes, max_file_size: int) -> None:
    """Validate file attributes: name, extension, non-empty, size."""
    if not filename:
        raise ValidationError("File Validation Error: No file name provided. Please select a file to upload.")

    file_extension = os.path.splitext(filename)[1].lower().lstrip(".")
    if file_extension not in ALLOWED_EXTENSIONS:
        raise ValidationError(
            f"File Validation Error: Invalid file type '{file_extension}'. "
            "Only CSV and Excel files are accepted."
        )

    size = len(contents)
    if size == 0:
        raise ValidationError("File Validation Error: The uploaded file is empty. Please upload a valid file.")

    if size > max_file_size:
        file_size_mb = round(size / (1024 * 1024), 2)
        max_size_mb = round(max_file_size / (1024 * 1024), 2)
        raise ValidationError(
            f"File Validation Error: The uploaded file is {file_size_mb} MB, which exceeds the "
            f"maximum allowed size of {max_size_mb} MB. Please upload a smaller file."
        )


# validate_required_columns lives in engine.services.config_validation_service
# (one copy shared with the DB-sourced pipelines) — imported at the top.


# ─────────────────────────────────────────────────────────────
# Conversion service — DataFrame -> Parquet.
# ─────────────────────────────────────────────────────────────
def dataframe_to_parquet(df: pd.DataFrame) -> bytes:
    """Serialize a DataFrame to Parquet (pyarrow) and return the bytes."""
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        return buffer.getvalue()
    except Exception as e:
        raise ETLError(f"Failed to convert data to Parquet: {e}") from e


# DataPreprocessor (clean_file) lives in engine.services.config_validation_service
# (one copy shared with the DB-sourced pipelines) — imported at the top.

PARQUET_CONTENT_TYPE = "application/vnd.apache.parquet"
_IST = timezone(timedelta(hours=5, minutes=30))


def _sse(payload: dict) -> str:
    return f"data: {json.dumps(payload)}\n\n"


async def _flush():
    # let the event loop write the just-yielded SSE chunk to the socket now
    await asyncio.sleep(0)


class EtlHandler:

    def etl_handler(self, *, client_code, file_type, ext_erp_rec_code,filename, contents, settings, tracker) -> StreamingResponse:
        from engine.extract import FileReader
        t0 = time.monotonic()

        def ev(payload: dict) -> str:
            # every event carries when it was sent, so the live stream is visible
            payload["time"] = datetime.now(_IST).strftime("%H:%M:%S.%f")[:-3]
            payload["elapsed_sec"] = round(time.monotonic() - t0, 2)
            return _sse(payload)

        async def db(fn, *args, **kwargs):
            # tracker writes go to the (remote) MySQL — run them off the event loop,
            # so a slow DB round-trip never holds back the next streamed event
            return await asyncio.to_thread(fn, *args, **kwargs)

        async def event_stream():
            task_id = await db(tracker.create_task, client_code=client_code, file_type=file_type,
                               ext_erp_rec_code=ext_erp_rec_code, filename=filename, size_bytes=len(contents))
            yield ev({"stage": "UPLOAD_RECEIVED", "status": "SUCCESS","message": "File received", "task_id": task_id})
            await _flush()

            stage = PipelineStage.FILE_VALIDATION
            try:
                # 1. file validation
                validate_file(filename, contents, settings.max_upload_size_bytes)
                yield ev({"stage": "FILE_VALIDATION", "status": "SUCCESS", "message": "File validated"})
                await _flush()
                await db(tracker.update_stage, task_id, PipelineStage.FILE_VALIDATION, StageStatus.SUCCESS)

                # 2. read + normalize headers (space -> _)  [blocking -> thread so events flush live]
                stage = PipelineStage.EXTRACT
                df = await asyncio.to_thread(FileReader.read, filename, contents)
                df = await asyncio.to_thread(DataPreprocessor.clean_file, df)
                yield ev({"stage": "EXTRACT", "status": "SUCCESS", "message": "File read"})
                await _flush()
                await db(tracker.update_stage, task_id, PipelineStage.EXTRACT, StageStatus.SUCCESS)

                # 3. column validation
                stage = PipelineStage.COLUMN_VALIDATION
                validated_df = await asyncio.to_thread(validate_required_columns, df, file_type, client_code)
                yield ev({"stage": "COLUMN_VALIDATION", "status": "SUCCESS","message": "Columns validated","rows": int(len(validated_df)), "columns_count": len(validated_df.columns)})
                await _flush()
                # row_count is NOT set here: it is recorded once, when the ETL reads the file from Blob
                await db(tracker.update_stage, task_id, PipelineStage.COLUMN_VALIDATION, StageStatus.SUCCESS,
                         columns_count=len(validated_df.columns), stage_rows=int(len(validated_df)))

                # 4. convert to parquet
                stage = PipelineStage.CONVERT_PARQUET
                parquet_bytes = await asyncio.to_thread(dataframe_to_parquet, validated_df)
                yield ev({"stage": "CONVERT_PARQUET", "status": "SUCCESS", "message": "Converted to Parquet"})
                await _flush()
                await db(tracker.update_stage, task_id, PipelineStage.CONVERT_PARQUET, StageStatus.SUCCESS)

                # 5. store in blob — outlet/product: one fixed file, overwritten;
                #    secondary (transactional): <category>/<today>/ folder, never overwritten
                stage = PipelineStage.STORE_BLOB
                folder, parquet_name, overwrite = blob_location(client_code, file_type, filename, task_id)
                blob_url = await asyncio.to_thread(AzureBlobConnection(settings).upload_blob,parquet_bytes, parquet_name, PARQUET_CONTENT_TYPE, folder, overwrite)
                yield ev({"stage": "STORE_BLOB", "status": "SUCCESS", "message": "Uploaded to storage"})
                await _flush()
                await db(tracker.update_stage, task_id, PipelineStage.STORE_BLOB, StageStatus.SUCCESS,stored_format="parquet", blob_name=f"{folder}/{parquet_name}",blob_url=blob_url)
                await db(tracker.set_status, task_id, TaskStatus.INGESTED)

                # Auto-trigger the ETL — outlet / product masters AND the secondary
                # dump (each upload triggers its own run with its own task_id, so
                # every day-folder file gets loaded). The API's job ends here;
                # Airflow runs the load, watch it in the UI.
                if file_type in ("OUTLETDUMP", "PRODUCT", "SECONDARYDUMP"):
                    from core.services.airflow_service import dag_for, trigger_dag
                    triggered = await asyncio.to_thread(trigger_dag, client_code, file_type, task_id)
                    if triggered:
                        yield ev({"stage": "ETL_TRIGGERED", "status": "SUCCESS","message": "File uploaded and ETL triggered - track it in the Airflow UI.","task_id": task_id})
                    else:
                        yield ev({"stage": "DONE", "status": "SUCCESS","message": f"File uploaded. Auto-trigger unavailable - run {dag_for(client_code, file_type)} in Airflow.","task_id": task_id})
                else:
                    # other file types (primary sales, inventory, …): no ETL DAG yet — just confirm the upload
                    yield ev({"stage": "DONE", "status": "SUCCESS","message": "Your file uploaded successfully.", "task_id": task_id})

            except ETLError as e:
                yield ev({"stage": str(stage), "status": "FAILED", "message": str(e), "task_id": task_id})
                await _flush()
                await db(tracker.update_stage, task_id, stage, StageStatus.FAILED, error=str(e))
                await db(tracker.set_status, task_id, TaskStatus.FAILED)

        return StreamingResponse(
            event_stream(),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # disable proxy buffering so events flush live
            },
        )


# Allowed file types for the template endpoint (retailverse ETLFilesTemplateSerializer).
ALLOWED_FILE_TYPES = [
    FileType.PRODUCT, FileType.OUTLETLIST, FileType.SECONDARYDUMP,
    FileType.PRIMARYSALES, FileType.INVENTORYDATA, FileType.FINANCEDATA,
    FileType.RECEIVABLES, FileType.PAYABLES, FileType.ITEMMASTER,
    FileType.PRODUCTIONREGISTRATION, FileType.DEBTORSMASTER, FileType.CREDITORS,
]


class GetFileTemplets:
    """Template-builder service (retailverse pipeline_launcher.GetFileTemplets).

    Owns the full template flow: validate the file type, then (on download)
    validate the client + file type and build the empty Excel template.
    """

    def __init__(self, client_code, file_type, download=False):
        self.client_code = client_code
        self._raw_file_type = file_type
        self.file_type = (file_type or "").strip().upper()
        self.download = download

    def get_file_templets(self):
        # serializer-equivalent validation: file type must be an allowed one
        if self.file_type not in ALLOWED_FILE_TYPES:
            return JSONResponse(
                status_code=422,
                content={"data": {}, "success": False, "message": {
                    "file_type": f"Invalid file type '{self._raw_file_type}'. "
                                 f"Allowed types are: {', '.join(ALLOWED_FILE_TYPES)}"
                }},
            )

        # If download requested, return the Excel file
        if self.download:
            # Check client_code validity
            if self.client_code not in CLIENT_REQUIREMENTS:
                return JSONResponse(
                    status_code=422,
                    content={"data": {}, "success": False,
                             "message": f"Unsupported client_code {self.client_code}"},
                )

            # Check file_type validity
            if self.file_type not in CLIENT_REQUIREMENTS[self.client_code]:
                return JSONResponse(
                    status_code=422,
                    content={"data": {}, "success": False,
                             "message": f"Unsupported file_type {self.file_type} for client {self.client_code}"},
                )

            # Prepare empty DataFrame
            required_columns = CLIENT_REQUIREMENTS[self.client_code][self.file_type][ConfigKey.REQUIRED_COLUMNS]
            # >>> only change vs retailverse: underscores -> single space in headers <<<
            df = pd.DataFrame(columns=[col.replace("_", " ") for col in required_columns.keys()])
            file_name = f"{self.file_type.lower()}_template"
            return FileHandler.write_to_excelfile(df, file_name)

        return JSONResponse(
            status_code=400,
            content={"data": {}, "success": False,
                     "message": f"Template for {self.file_type} not downloaded. Set download=True to get the file."},
        )


class TaskService:
    """Task-tracking service — reads ETL run metadata via the tracker repository."""

    def __init__(self, tracker):
        self.tracker = tracker

    def list_tasks(self, client_code=None, status=None) -> list[dict]:
        tasks = self.tracker.list_tasks()
        if client_code:
            tasks = [t for t in tasks if t.get("client_code") == client_code]
        if status:
            tasks = [t for t in tasks if t.get("status") == status]
        tasks.sort(key=lambda t: t.get("created_at", ""), reverse=True)
        return tasks

    def get_task(self, task_id) -> dict | None:
        return self.tracker.get_task(task_id)

"""ETL API views + app bootstrap (retailverse-style thin controllers).

  * etl_file_upload        POST ingestion — thin view -> EtlHandler service (SSE)
  * download_file_template GET  template download
  * list_tasks / get_task  task-tracking views

Every view stays thin: validate the request, then delegate to a service in
``api.services``. Responses use the retailverse envelope
{"data", "success", "message"}.

This module is also the ASGI entry point:  ``uvicorn api.views:app``
"""
import logging
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, File, Query, Request, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from api.repositories import MetadataTracker, get_tracker
from api.services import EtlHandler, GetFileTemplets, TaskService
from config.etl_enums import ClientCode, FileType
from core.settings import Settings, get_settings
from core.utils.exceptions import (
    ConfigurationError,
    ETLError,
    ExtractError,
    LoadError,
    ValidationError,
)
from core.utils.logger import configure_logging

logger = logging.getLogger("ETL_API")


# ─────────────────────────────────────────────────────────────
# Upload (POST)  — thin controller (≈ retailverse ETLFileUploadAPIView.post).
# FastAPI validates the request, then we delegate to the EtlHandler service,
# which streams one Server-Sent Event per stage.
# ─────────────────────────────────────────────────────────────
async def etl_file_upload(
    client_code: ClientCode = Query(..., description="Client code, e.g. NATURO, MODERNKT, ULSNIRT, REPOSE"),
    file_type: FileType = Query(..., description="File type, e.g. PRODUCT, OUTLETDUMP, SECONDARYDUMP"),
    ext_erp_rec_code: str = Query(..., description="ERP rec code the file belongs to, e.g. ERP10001"),
    file: UploadFile = File(..., description="The CSV or Excel file to upload"),
    settings: Settings = Depends(get_settings),
    tracker: MetadataTracker = Depends(get_tracker),
):
    try:
        contents = await file.read()
        return EtlHandler().etl_handler(client_code=client_code.value,file_type=file_type.value,ext_erp_rec_code=ext_erp_rec_code,filename=file.filename,contents=contents,settings=settings,tracker=tracker)
    except (ValidationError, ValueError) as e:
        logger.error("ETL user/client error: %s", str(e))
        return JSONResponse(status_code=400,content={"data": {}, "success": False, "message": str(e)},)
    except Exception as e:  # noqa: BLE001
        logger.exception("Unexpected ETL error: %s", str(e))
        return JSONResponse(status_code=500,content={"data": {}, "success": False,"message": "ETL failed due to internal server error."},)


# ─────────────────────────────────────────────────────────────
# Template download (GET)  — thin controller -> GetFileTemplets service.
# ─────────────────────────────────────────────────────────────
async def download_file_template(
    client_code: str = Query(..., description="Client code, e.g. NATURO"),
    file_type: str = Query(..., description="File type, e.g. PRODUCT, OUTLETDUMP, SECONDARYDUMP"),
    download: bool = Query(False, description="Set true to download the template file"),
):
    """GET /api/v1/etl/upload/get-file-template — thin view; the GetFileTemplets
    service validates the file type and builds/returns the template."""
    return GetFileTemplets(client_code, file_type, download).get_file_templets()


# ─────────────────────────────────────────────────────────────
# Task tracking (GET)  — thin controllers -> TaskService.
# ─────────────────────────────────────────────────────────────
async def list_tasks(
    client_code: str | None = Query(None, description="Optional filter by client code"),
    status: str | None = Query(None, description="Optional filter by task status"),
    tracker: MetadataTracker = Depends(get_tracker),
) -> JSONResponse:
    """List all tracked ETL tasks (newest first), with optional filters."""
    tasks = TaskService(tracker).list_tasks(client_code, status)
    return JSONResponse(
        status_code=200,
        content={
            "data": {"count": len(tasks), "tasks": tasks},
            "success": True,
            "message": "Tasks fetched successfully.",
        },
    )


async def get_task(
    task_id: str,
    tracker: MetadataTracker = Depends(get_tracker),
) -> JSONResponse:
    """Get the full metadata + stage history for one task."""
    record = TaskService(tracker).get_task(task_id)
    if not record:
        return JSONResponse(
            status_code=404,
            content={"data": {}, "success": False,
                     "message": f"No task found with id '{task_id}'."},
        )
    return JSONResponse(
        status_code=200,
        content={"data": record, "success": True, "message": "Task fetched successfully."},
    )


# ─────────────────────────────────────────────────────────────
# Application bootstrap — this module IS the entry point:
#     uvicorn api.views:app
# `api_router` is imported here at the BOTTOM (after the view functions above),
# so urls.py — which imports those views — resolves with no circular import.
# ─────────────────────────────────────────────────────────────
from api.urls import api_router  # noqa: E402

_settings = get_settings()
configure_logging(log_file=_settings.log_file, level=_settings.log_level)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    """Explicit lifespan so shutdown (Ctrl+C / --reload) is clean."""
    logger.info("ETL Ingestion Service starting up.")
    try:
        yield
    except Exception:  # pragma: no cover - shutdown path
        pass
    finally:
        logger.info("ETL Ingestion Service shutting down.")


app = FastAPI(
    title="ETL Ingestion Service",
    description=(
        "Stateless ingestion API: validates a client's CSV/Excel file and lands "
        "it in the Azure Blob raw zone as Parquet. Airflow then extracts, "
        "transforms, and loads it into MySQL."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(api_router)


def _envelope(message, status_code: int) -> JSONResponse:
    """retailverse-style error response."""
    return JSONResponse(
        status_code=status_code,
        content={"data": {}, "success": False, "message": message},
    )


@app.exception_handler(ValidationError)
async def _validation_error(_: Request, exc: ValidationError) -> JSONResponse:
    return _envelope(str(exc), 400)


@app.exception_handler(ExtractError)
async def _extract_error(_: Request, exc: ExtractError) -> JSONResponse:
    return _envelope(str(exc), 400)


@app.exception_handler(ConfigurationError)
async def _configuration_error(_: Request, exc: ConfigurationError) -> JSONResponse:
    return _envelope(str(exc), 503)


@app.exception_handler(LoadError)
async def _load_error(_: Request, exc: LoadError) -> JSONResponse:
    return _envelope(str(exc), 502)


@app.exception_handler(ETLError)
async def _etl_error(_: Request, exc: ETLError) -> JSONResponse:
    # Fallback for any domain error without a more specific handler above.
    return _envelope(str(exc), 500)


@app.exception_handler(RequestValidationError)
async def _request_validation_error(_: Request, exc: RequestValidationError) -> JSONResponse:
    # FastAPI's own query/body validation (e.g. bad client_code / file_type).
    return _envelope(jsonable_encoder(exc.errors()), 422)


@app.get("/health", tags=["Health"])
def health() -> dict:
    """Liveness probe."""
    return {"data": {"status": "ok"}, "success": True, "message": "Service is healthy."}

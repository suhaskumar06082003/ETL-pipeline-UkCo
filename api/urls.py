"""URL configuration for the ETL ingestion service.

Merged from the former router / v1 URL modules:
  * api/router.py                          (root mount:  /api/v1/etl)
  * api/v1/etl/urls.py                     (app-level mount points)
  * api/v1/etl/endpoints/upload_urls.py    (file-upload route)
  * api/v1/etl/endpoints/template_urls.py  (template-download route)
  * api/v1/etl/endpoints/tasks_urls.py     (task-tracking routes)

Exposes ``api_router`` (an APIRouter) mounting the same routes at the same paths
as before:

    POST /api/v1/etl/upload/file-upload
    GET  /api/v1/etl/upload/get-file-template
    GET  /api/v1/etl/tasks
    GET  /api/v1/etl/tasks/{task_id} 
"""
from fastapi import APIRouter

from api.serializers import ErrorResponse
from api.views import (download_file_template,etl_file_upload,get_task,list_tasks)

# ── file-upload feature route (was upload_urls.py) ──
upload_router = APIRouter()
upload_router.add_api_route(
    "/file-upload",
    etl_file_upload,
    methods=["POST"],
    name="upload_product",
    summary="Validate a CSV/Excel file and stream progress per stage (SSE), storing it as Parquet",
)

# ── template-download feature route (was template_urls.py) ──
template_router = APIRouter()
template_router.add_api_route(
    "/get-file-template",
    download_file_template,
    methods=["GET"],
    name="file_template",
    responses={
        200: {
            "content": {
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {}
            },
            "description": "Excel template with the required column headers (download=true)",
        },
        400: {"description": "download not set to true"},
        422: {"description": "invalid client_code / file_type"},
    },
    summary="Download the column template (Excel) for a client + file type",
)

# ── task-tracking routes (was tasks_urls.py) ──
tasks_router = APIRouter()
tasks_router.add_api_route(
    "/tasks",
    list_tasks,
    methods=["GET"],
    name="list_tasks",
    summary="List tracked ETL tasks (upload → ETL) with optional filters",
)
tasks_router.add_api_route(
    "/tasks/{task_id}",
    get_task,
    methods=["GET"],
    name="get_task",
    responses={404: {"model": ErrorResponse}},
    summary="Get one task's full metadata + stage history",
)

# ── ETL app-level mount (was v1/etl/urls.py) ──
etl_router = APIRouter()
etl_router.include_router(upload_router, prefix="/upload")
etl_router.include_router(template_router, prefix="/upload")
etl_router.include_router(tasks_router)  # -> /api/v1/etl/tasks

# ── root URL configuration (was router.py) ──
api_router = APIRouter()

# Register apps here — one line per app.
api_router.include_router(etl_router, prefix="/api/v1/etl", tags=["ETL"])

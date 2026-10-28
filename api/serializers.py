"""Serializers for the ETL ingestion API.

Holds ONLY serialization / request-validation models — the response envelope
schemas. Every piece of processing logic (file validation, required-column
validation, Parquet conversion, header cleaning) lives in the service layer
(``api.services``); the request's query params are validated by the
view's typed parameters (ClientCode / FileType enums), i.e. the serializer layer.
"""
from pydantic import BaseModel


class UploadResponse(BaseModel):
    """Returned when a file is validated, converted to Parquet, and stored."""

    task_id: str
    client_code: str
    file_type: str
    filename: str            # original uploaded file name
    size_bytes: int          # original upload size
    rows: int
    columns_count: int
    columns: list[str]
    stored_format: str       # "parquet"
    blob_name: str
    blob_url: str
    container: str
    status: str              # task status, e.g. "ingested"


class ErrorResponse(BaseModel):
    detail: str

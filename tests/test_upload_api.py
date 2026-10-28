"""Tests for the ETL ingestion (streaming), template, and task endpoints."""
import io
import json
from unittest.mock import patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from api.views import app
from config.file_config import ProductModel
from core.settings import Settings, get_settings
from api.repositories import InMemoryTracker, get_tracker

UPLOAD_URL = "/api/v1/etl/upload/file-upload"      # now streams SSE
TEMPLATE_URL = "/api/v1/etl/upload/get-file-template"
TASKS_URL = "/api/v1/etl/tasks"
FAKE_BLOB_URL = "https://acct.blob.core.windows.net/etl-uploads/NATURO/product/product.parquet"


@pytest.fixture(autouse=True)
def isolated_tracker():
    """Isolate tests from real MySQL + Azure."""
    tracker = InMemoryTracker()
    test_settings = Settings(
        azure_storage_connection_string="",
        azure_storage_container_name="test-container",
    )
    app.dependency_overrides[get_settings] = lambda: test_settings
    app.dependency_overrides[get_tracker] = lambda: tracker
    yield tracker
    app.dependency_overrides.clear()


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=False)


def _valid_product_csv() -> bytes:
    cols = list(ProductModel.get_config_feild_assist().keys())
    header = ",".join(cols)
    row = ",".join("10" if c in ("PTR", "MRP") else "x" for c in cols)
    return (header + "\n" + row + "\n").encode()


def _spaced_headers_csv() -> bytes:
    cols = list(ProductModel.get_config_feild_assist().keys())
    header = ",".join(c.replace("_", " ") for c in cols)
    row = ",".join("10" if c in ("PTR", "MRP") else "x" for c in cols)
    return (header + "\n" + row + "\n").encode()


def _events(text: str) -> list[dict]:
    """Parse SSE 'data: {...}' blocks into dicts."""
    out = []
    for block in text.strip().split("\n\n"):
        block = block.strip()
        if block.startswith("data:"):
            out.append(json.loads(block[len("data:"):].strip()))
    return out


# ── upload (streaming) ───────────────────────────────────────────────────

def test_valid_upload_streams_stages_and_stores_parquet(client, isolated_tracker):
    with patch("api.services.AzureBlobConnection") as M:
        M.return_value.upload_blob.return_value = FAKE_BLOB_URL
        r = client.post(
            f"{UPLOAD_URL}?client_code=NATURO&file_type=PRODUCT&ext_erp_rec_code=ERP10001",
            files={"file": ("p.csv", _valid_product_csv(), "text/csv")},
        )
        kwargs = M.return_value.upload_blob.call_args.kwargs
        assert kwargs["blob_name"] == "product.parquet"          # master -> overwrite
        assert kwargs["folder_path"] == "NATURO/product"
        assert kwargs["file_data"][:4] == b"PAR1"                # Parquet magic bytes

    assert r.status_code == 200
    assert "text/event-stream" in r.headers["content-type"]
    events = _events(r.text)
    assert [e["stage"] for e in events] == [
        "UPLOAD_RECEIVED", "FILE_VALIDATION", "EXTRACT",
        "COLUMN_VALIDATION", "CONVERT_PARQUET", "STORE_BLOB", "DONE",
    ]
    assert events[-1]["message"] == "Your file uploaded successfully."

    rec = isolated_tracker.get_task(events[-1]["task_id"])
    assert rec["status"] == "INGESTED"
    assert rec["stored_format"] == "parquet"
    assert rec["rows"] == 1
    assert rec["columns_count"] == 10


def test_spaced_headers_are_normalized_and_pass(client, isolated_tracker):
    with patch("api.services.AzureBlobConnection") as M:
        M.return_value.upload_blob.return_value = FAKE_BLOB_URL
        r = client.post(
            f"{UPLOAD_URL}?client_code=NATURO&file_type=PRODUCT&ext_erp_rec_code=ERP10001",
            files={"file": ("p.csv", _spaced_headers_csv(), "text/csv")},
        )
    events = _events(r.text)
    assert events[-1]["stage"] == "DONE"
    assert isolated_tracker.get_task(events[-1]["task_id"])["columns_count"] == 10


def test_missing_columns_streams_failed_event(client, isolated_tracker):
    r = client.post(
        f"{UPLOAD_URL}?client_code=NATURO&file_type=PRODUCT&ext_erp_rec_code=ERP10001",
        files={"file": ("p.csv", b"Product,MRP\nfoo,10\n", "text/csv")},
    )
    assert r.status_code == 200  # streaming: failure is an event, not an HTTP code
    last = _events(r.text)[-1]
    assert last["stage"] == "COLUMN_VALIDATION"
    assert last["status"] == "FAILED"
    assert "Missing required columns" in last["message"]

    task = isolated_tracker.list_tasks()[-1]
    assert task["status"] == "FAILED"


def test_bad_extension_streams_failed_event(client):
    r = client.post(
        f"{UPLOAD_URL}?client_code=NATURO&file_type=PRODUCT&ext_erp_rec_code=ERP10001",
        files={"file": ("p.txt", b"hello", "text/plain")},
    )
    assert r.status_code == 200
    last = _events(r.text)[-1]
    assert last["status"] == "FAILED"
    assert "Invalid file type" in last["message"]


def test_missing_credentials_streams_failed_event(client):
    # No Azure connection string -> ConfigurationError inside the stream.
    r = client.post(
        f"{UPLOAD_URL}?client_code=NATURO&file_type=PRODUCT&ext_erp_rec_code=ERP10001",
        files={"file": ("p.csv", _valid_product_csv(), "text/csv")},
    )
    assert r.status_code == 200
    last = _events(r.text)[-1]
    assert last["stage"] == "STORE_BLOB"
    assert last["status"] == "FAILED"
    assert "not configured" in last["message"]


def test_unknown_client_returns_422(client):
    # Bad query-param enum is rejected up front (before streaming) as JSON 422.
    r = client.post(
        f"{UPLOAD_URL}?client_code=FOO&file_type=PRODUCT&ext_erp_rec_code=ERP10001",
        files={"file": ("p.csv", _valid_product_csv(), "text/csv")},
    )
    assert r.status_code == 422
    assert r.json()["success"] is False


# ── template ────────────────────────────────────────────────────────────

def test_template_download_replaces_underscores_with_spaces(client):
    r = client.get(f"{TEMPLATE_URL}?client_code=NATURO&file_type=OUTLETDUMP&download=true")
    assert r.status_code == 200
    assert r.headers["content-disposition"] == 'attachment; filename="outletdump_template.xlsx"'
    df = pd.read_excel(io.BytesIO(r.content), engine="openpyxl")
    cols = list(df.columns)
    assert len(df) == 0
    assert "L1 Position Name" in cols
    assert not any("_" in c for c in cols)


def test_template_without_download_flag_returns_400(client):
    r = client.get(f"{TEMPLATE_URL}?client_code=NATURO&file_type=OUTLETDUMP")
    assert r.status_code == 400
    assert r.json()["success"] is False


def test_template_unsupported_file_type_for_client_returns_422(client):
    r = client.get(f"{TEMPLATE_URL}?client_code=REPOSE&file_type=OUTLETDUMP&download=true")
    assert r.status_code == 422
    assert "Unsupported file_type" in r.json()["message"]


# ── task tracking ───────────────────────────────────────────────────────

def test_get_task_and_list(client, isolated_tracker):
    with patch("api.services.AzureBlobConnection") as M:
        M.return_value.upload_blob.return_value = FAKE_BLOB_URL
        up = client.post(
            f"{UPLOAD_URL}?client_code=NATURO&file_type=PRODUCT&ext_erp_rec_code=ERP10001",
            files={"file": ("p.csv", _valid_product_csv(), "text/csv")},
        )
    task_id = _events(up.text)[-1]["task_id"]

    one = client.get(f"{TASKS_URL}/{task_id}")
    assert one.status_code == 200
    assert one.json()["data"]["task_id"] == task_id

    listed = client.get(f"{TASKS_URL}?client_code=NATURO")
    assert listed.status_code == 200
    assert listed.json()["data"]["count"] == 1


def test_get_unknown_task_returns_404(client):
    r = client.get(f"{TASKS_URL}/does-not-exist")
    assert r.status_code == 404
    assert r.json()["success"] is False

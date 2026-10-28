# ETL / Data Pipeline

Data-engineering project for ingesting client sales files and loading them into
MySQL.

> **Deployed on Azure.** How it's deployed, how to ship a change (CI/CD), everyday
> commands, rebuilding the server and troubleshooting: see **[DEPLOYMENT.md](DEPLOYMENT.md)**.
> Airflow: https://etl-hangyo.southindia.cloudapp.azure.com:8443 ·
> API: https://etl-hangyo.southindia.cloudapp.azure.com/docs

**Flow:** client uploads a CSV/Excel file → the **ingestion API** validates it
(file + required columns) and lands the raw file in the **Azure Blob raw zone**
→ **Airflow** (next) orchestrates *extract from blob → transform/validate →
load into **MySQL***.

```
   client ──HTTP──▶  Ingestion API  ──▶  Azure Blob (raw zone)
                     (FastAPI)             <client>/<category>/<file>
                                              │
                                     Airflow DAG (extract → transform → load)
                                              │
                                              ▼
                                           MySQL (destination)
```

## Repository layout

```
etl/
├── etl_project/                  # the importable pipeline package
│   ├── api/                      # INGESTION SERVICE (FastAPI)
│   │   ├── main.py               #   app + exception handlers + /health
│   │   ├── router.py             #   root url config   (≈ config/urls.py)
│   │   └── v1/etl/
│   │       ├── urls.py           #   app-level routes  (≈ apps/ETL/urls.py)
│   │       └── endpoints/
│   │           ├── upload_urls.py   # feature routes (≈ etl_files/etl_urls.py)
│   │           └── upload_views.py  # request handler (≈ etl_files/etl_views.py)
│   ├── core/                     # cross-cutting
│   │   ├── config.py             #   settings from .env (Azure + MySQL)
│   │   ├── logging.py
│   │   └── exceptions.py         #   domain errors (Validation/Extract/Load/Config)
│   ├── config/                   # BUSINESS config (from retailverse ETL)
│   │   ├── etl_enums.py          #   ClientCode / FileType / ConfigKey
│   │   ├── file_config.py        #   per-file column schemas
│   │   └── clients_config.py     #   CLIENT_REQUIREMENTS + folder mapping
│   ├── connections/              # external systems
│   │   ├── azure_blob.py         #   raw zone (Azure Blob)
│   │   └── mysql.py              #   destination engine (SQLAlchemy, lazy)
│   ├── pipeline/                 # the ETL stages
│   │   ├── extract/file_reader.py    #   read CSV/Excel → DataFrame
│   │   ├── transform/validators.py   #   file + required-column validation
│   │   └── load/mysql_loader.py      #   load DataFrame → MySQL (scaffold)
│   └── schemas/etl.py            # API response models
├── dags/                         # Airflow DAGs
│   └── etl_blob_to_mysql_dag.py  #   scaffold: blob → transform → MySQL
├── sql/ddl/                      # MySQL table definitions
├── tests/                        # pytest suite
├── requirements.txt
├── .env / .env.example           # secrets (Azure + MySQL); .env is git-ignored
└── README.md
```

Every pipeline stage (`extract` / `transform` / `load`) is framework-agnostic —
the API calls them today, and Airflow will call the same code tomorrow.

### Routing (nested, like retailverse's `urls.py` chain)

| Layer | File | Adds prefix | Django equivalent |
|-------|------|-------------|-------------------|
| root  | `etl_project/api/router.py`                      | `/api/v1/etl`  | `config/urls.py` |
| app   | `etl_project/api/v1/etl/urls.py`                 | `/upload`      | `apps/ETL/urls.py` |
| feature | `etl_project/api/v1/etl/endpoints/upload_urls.py` | `/file-upload` | `apps/ETL/etl_files/etl_urls.py` |

Full path → **`POST /api/v1/etl/upload/file-upload`**.

## Setup

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Edit `.env` with your Azure connection string (and MySQL when ready):

```
AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
AZURE_STORAGE_CONTAINER_NAME="etl-uploads"
MAX_UPLOAD_SIZE_MB=100

MYSQL_HOST="localhost"
MYSQL_PORT=3306
MYSQL_USER="etl_user"
MYSQL_PASSWORD="..."
MYSQL_DATABASE="etl"
```

## Run the ingestion API

```bash
uvicorn etl_project.api.main:app --reload --port 8000
```

- Swagger UI: http://localhost:8000/docs
- Health: http://localhost:8000/health

## Upload a file

`POST /api/v1/etl/upload/file-upload`

**Query params:** `client_code` (`NATURO`/`MODERNKT`/`ULSNIRT`/`REPOSE`),
`file_type` (`PRODUCT`/`OUTLETDUMP`/`SECONDARYDUMP`/…),
`ext_erp_rec_code` (e.g. `ERP10001`).
**Body (multipart):** `file` — a `.csv` or `.xlsx`.

This endpoint **streams** progress as Server-Sent Events (one event per stage);
see "Streaming" below.

```bash
curl -N -X POST "http://localhost:8000/api/v1/etl/upload/file-upload?client_code=NATURO&file_type=PRODUCT&ext_erp_rec_code=ERP10001" \
  -F "file=@product_dump.csv"
```

The validated data is converted to **Parquet** and stored at
`<container>/<client_code>/<category>/<stem>_<task_id>.parquet` (Parquet so the
downstream Airflow tasks read it back fast). Every stage is recorded in the
metadata tracker. Success (`200`) returns:

```json
{
  "task_id": "d2974c28a381",
  "client_code": "NATURO", "file_type": "PRODUCT",
  "filename": "product_dump.csv", "size_bytes": 20481,
  "rows": 1523, "columns_count": 10, "columns": ["Product", "ERP_ID", "..."],
  "stored_format": "parquet",
  "blob_name": "NATURO/product/product_dump_d2974c28a381.parquet",
  "blob_url": "https://youraccount.blob.core.windows.net/etl-uploads/NATURO/product/product_dump_d2974c28a381.parquet",
  "container": "etl-uploads",
  "status": "ingested"
}
```

Errors: `400` invalid file / missing columns / type-not-allowed-for-client ·
`422` unknown client/file_type · `503` Azure not configured · `502` blob error.
Failed uploads are still recorded in the tracker (status `failed`, with the
stage that failed).

## Track a file (metadata)

Every upload is tracked in a JSON store (`data/metadata/etl_tasks.json`) as a
"task" with its full stage journey — swap for MySQL later without changing the
API surface.

| Method & Path | Purpose |
|---------------|---------|
| `GET /api/v1/etl/tasks` | List tasks (optional `?client_code=` / `?status=` filters). |
| `GET /api/v1/etl/tasks/{task_id}` | Full metadata + stage history for one task. |

Stages: `UPLOAD_RECEIVED → FILE_VALIDATION → EXTRACT → COLUMN_VALIDATION →
CONVERT_PARQUET → STORE_BLOB` (ingestion), then `ETL_EXTRACT → ETL_TRANSFORM →
ETL_LOAD → COMPLETED` (Airflow, later).

## Download a template

`GET /api/v1/etl/upload/get-file-template`

Ported from retailverse (`GETFileTemplate` + `GetFileTemplets` +
`FileHandler.write_to_excelfile`). Returns an **empty Excel file** with the
required column headers for a `client_code` + `file_type`. Requires
`download=true` (same as retailverse). Headers have underscores replaced with a
single space (`ERP_ID` → `ERP ID`, `L1_Position_Name` → `L1 Position Name`) —
the only change from retailverse; internal config/validation is unchanged.

Query params: `client_code`, `file_type`, `download` (default `false`).

```bash
curl -OJ "http://localhost:8000/api/v1/etl/upload/get-file-template?client_code=NATURO&file_type=OUTLETDUMP&download=true"
# -> outletdump_template.xlsx
```

Responses (same shape as retailverse):
- `200` → the `.xlsx` file (when `download=true`).
- `400` → `{"data":{},"success":false,"message":"Template for ... not downloaded. Set download=True to get the file."}` (download not set).
- `422` → unsupported `client_code` / `file_type` (JSON with `success:false`).

### Blob raw-zone layout

```
etl-uploads/
├── NATURO/{outlet,secondary,product}/…
├── MODERNKT/{outlet,secondary,product}/…
└── …
```

## Logs

All logs (app + uvicorn access/errors) are written to both the console and a
rotating log file:

```
logs/etl.log          # current log (rotates at 10 MB, keeps 5 backups)
logs/etl.log.1 ...     # older rotated logs
```

Configurable in `.env`:

```
LOG_FILE="logs/etl.log"
LOG_LEVEL="INFO"        # DEBUG / INFO / WARNING / ERROR
```

View live (Windows):

```powershell
Get-Content logs\etl.log -Wait -Tail 50
```

## Tests

```bash
pytest -q
```

## Next: Airflow + MySQL

- Install Airflow in its **own** environment (its pins conflict with app deps).
- Point `dags/` at that Airflow instance; the DAG imports `etl_project.pipeline.*`.
- Fill in `sql/ddl/` with the destination tables and flesh out
  `pipeline/load/mysql_loader.py`.

# How to run the ETL (step by step)

Everything runs in **Docker** — you do NOT need the Python venv anymore.

## 1. Start Docker Desktop
- Open **Docker Desktop** (Windows Start menu).
- Wait until the whale icon is steady and it says **"Engine running"**.

## 2. Open a terminal in the project
```bash
cd D:\etl
```

## 3. Start the whole stack (API + Airflow)
```bash
docker compose -f docker/docker-compose.yml up -d
```
- First time (or after code deps change) add `--build`:
  `docker compose -f docker/docker-compose.yml up -d --build`
- `-d` = run in background.

## 4. Open the apps
| What | URL | Login |
|------|-----|-------|
| Upload API (Swagger) | http://localhost:8000/docs | — |
| Airflow (DAGs)       | http://localhost:8080      | `admin` / `<your Airflow admin password>` |

MySQL Workbench: connect to `localhost:3306`, database **`naturo`** (your normal MySQL).

## 5. Load data (the actual work)
1. **Upload a file** at http://localhost:8000/docs → `POST /api/v1/etl/upload/file-upload`
   → set `client_code` (e.g. NATURO), `file_type` (OUTLETDUMP / PRODUCT / SECONDARYDUMP),
   `ext_erp_rec_code` (e.g. ERP10001), choose the file, Execute.
   (Upload **outlet** and **product** before **secondary**.)
2. **Trigger the DAG** in Airflow (http://localhost:8080):
   - `outlet_etl` for outlet, `product_etl` for product, `secondary_etl` for secondary.
   - Un-pause the DAG (toggle on the left) → click the ▶ Trigger button.
   - Watch each table task go green: `prepare → load_<table> … → finalize`.
3. Check results in MySQL Workbench (`naturo` tables) and `etl_task` for status/`current_stage`.

## 6. Useful commands
```bash
# see what's running
docker compose -f docker/docker-compose.yml ps

# view logs (Ctrl+C to stop watching)
docker compose -f docker/docker-compose.yml logs -f etl-api
docker compose -f docker/docker-compose.yml logs -f etl-airflow

# stop everything
docker compose -f docker/docker-compose.yml down

# restart everything
docker compose -f docker/docker-compose.yml restart
```

## 7. If a new DAG doesn't show in Airflow
Airflow scans the dags folder every few minutes. Force it:
```bash
docker exec etl_airflow airflow dags reserialize
```
Then refresh the UI (use the "All" tab, not just "Active").

## 8. Troubleshooting
- **`docker` command errors / "daemon not running"** → Docker Desktop isn't up. Start it and wait for "Engine running". If it's stuck: `wsl --shutdown`, then reopen Docker Desktop.
- **API `503` on upload** → Azure connection string missing in `.env`.
- **DAG "skipped"** → no `INGESTED` task; upload a file first.
- **Airflow admin password** (if it ever changes):
  `docker exec etl_airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated`

## Quick version (day-to-day)
```bash
# 1. Start Docker Desktop (wait for Engine running)
cd D:\etl
docker compose -f docker/docker-compose.yml up -d
# 2. Upload at http://localhost:8000/docs
# 3. Trigger DAG at http://localhost:8080  (admin / <your Airflow admin password>)
# 4. When done:  docker compose -f docker/docker-compose.yml down
```

"""MySQL -> BigQuery sync (analytics warehouse) — separate from the MySQL ETL.

One run = one etl_task row (pipeline_type=BIGQUERY). Every table is one DAG task, one after another;
nothing touches the LIVE BigQuery tables until the final publish step, so a
failed run never leaves half-new data behind.

    start_run     create missing BigQuery tables (DDL file), open the run,
                  cutoff = START TIME of the last COMPLETED run - 1 hour
                  (date + time; None -> full load)
    sync_table    per table, into hidden shadow tables only:
                    EXTRACT  MySQL rows changed on/after the cutoff time (all rows
                             on a full load), streamed in chunks, typed by the
                             BigQuery schema, loaded to <table>__delta (load job)
                    BUILD    <table>__next = live rows still present in MySQL and
                             not changed  UNION ALL  the changed rows
                             (a query job with a destination table — no DML, so
                             it works in the BigQuery Sandbox too; also mirrors
                             deletes: rows gone from MySQL are dropped)
    publish       all tables together: back up every live table (__prev),
                  then swap __next in; if any swap fails, the ones already
                  swapped are restored from __prev (rollback) and the run fails
    finish_run    COMPLETED + drop the shadow tables
    fail_run      FAILED   + drop the shadow tables (live tables untouched)

Tracking: the same etl_task / etl_task_stage as the MySQL ETL, flagged
pipeline_type='BIGQUERY', file_type='BIGQUERY_SYNC' (client's MySQL DB — the
BigQuery Sandbox allows no INSERT/UPDATE). Stages per table: EXTRACT_<table>,
BUILD_<table>, PUBLISH_<table> (ROLLBACK_<table> on a failed publish).
"""
import base64
import json
import logging
import os
import re
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal

from sqlalchemy import text

from config.bigquery_sync_config import get_bigquery_pipeline
from core.queries.db_connection import get_engine
from core.services.bigquery_service import get_bigquery_client
from core.settings import get_settings
from core.utils.logger import configure_etl_logging

logger = logging.getLogger("BQ_SYNC")

CHUNK_ROWS = 20_000                      # rows per MySQL fetch (4 GB VM safe); one load job per table
_SHADOWS = ("__delta", "__ids", "__next", "__prev")
PIPELINE = "BIGQUERY"                    # etl_task.pipeline_type flag (MySQL ETL rows = MYSQL)
FILE_TYPE = "BIGQUERY_SYNC"
# cutoff = start time of the last COMPLETED run minus this margin. Every
# incremental column is MySQL CURRENT_TIMESTAMP (same clock as etl_task.created_at);
# re-reading a row twice is harmless (the merge replaces it by key).
CUTOFF_MARGIN = timedelta(hours=1)
# a PROCESSING run younger than this blocks a new start (older = died, marked FAILED)
RUNNING_HOURS = 6
_CREATE = re.compile(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+`?(?:[\w-]+\.)?(?:\w+\.)?(\w+)`?", re.I)


# ─────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────
def _dataset(client_code: str) -> str:
    return get_settings().bq_dataset_for(client_code)


def _ref(bq, client_code: str, table: str) -> str:
    return f"{bq.project}.{_dataset(client_code)}.{table}"


def _stage(client_code, task_id, stage, status, table=None, mode=None, rows=None, error=None):
    with get_engine(client_code).begin() as c:
        c.execute(text(
            "INSERT INTO etl_task_stage (task_id, pipeline_type, file_type, stage, status, load_mode, row_count, error) "
            "VALUES (:t, :p, :ft, :st, :ss, :m, :r, :e)"),
            {"t": task_id, "p": PIPELINE, "ft": FILE_TYPE, "st": f"{stage}_{table}" if table else stage,
             "ss": status, "m": mode, "r": rows, "e": (str(error)[:4000] if error else None)})


def _task(client_code, task_id, **cols):
    sets = ", ".join(f"`{k}` = :{k}" for k in cols)
    with get_engine(client_code).begin() as c:
        c.execute(text(f"UPDATE etl_task SET {sets} WHERE task_id = :task_id"), {**cols, "task_id": task_id})


def _drop_shadows(bq, client_code, tables, suffixes=_SHADOWS):
    for t in tables:
        for suffix in suffixes:
            bq.delete_table(_ref(bq, client_code, t + suffix), not_found_ok=True)


def _root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _ddl_statements(path: str) -> list[tuple[str, str]]:
    src = "\n".join(l for l in open(path, encoding="utf-8").read().splitlines() if not l.strip().startswith("--"))
    out = []
    for stmt in (s.strip() for s in src.split(";")):
        m = _CREATE.search(stmt)
        if m:
            out.append((m.group(1), stmt))
    return out


# value -> the JSON form a BigQuery load job expects for that column type
def _to_bq(v, bq_type: str):
    if v is None:
        return None
    t = bq_type.upper()
    if isinstance(v, bytes) and t != "BYTES":
        v = int.from_bytes(v, "big") if t in ("BOOL", "BOOLEAN", "INT64", "INTEGER") else v.decode("utf-8", "replace")
    if t in ("BOOL", "BOOLEAN"):
        return bool(int(v)) if not isinstance(v, bool) else v
    if t in ("INT64", "INTEGER"):
        return int(v)
    if t in ("FLOAT64", "FLOAT"):
        return float(v)
    if t in ("NUMERIC", "BIGNUMERIC", "DECIMAL", "BIGDECIMAL"):
        return format(Decimal(str(v)), "f")
    if t in ("DATE", "DATETIME", "TIMESTAMP"):
        if isinstance(v, str):
            return None if v.startswith("0000") else v          # MySQL zero dates -> NULL
        if t == "DATE":
            return (v.date() if isinstance(v, datetime) else v).isoformat()
        if isinstance(v, date) and not isinstance(v, datetime):
            v = datetime.combine(v, time())
        return v.strftime("%Y-%m-%d %H:%M:%S.%f")
    if t == "TIME":
        return str(v)
    if t == "JSON":
        if isinstance(v, str):
            try:
                return json.loads(v)
            except ValueError:
                return v
        return v
    if t == "BYTES":
        return base64.b64encode(v if isinstance(v, bytes) else str(v).encode()).decode()
    return v.isoformat() if isinstance(v, (date, datetime, time)) else str(v)


def merge_sql(live_ref: str, ids_ref: str, delta_ref: str, keys: list[str]) -> str:
    """New table = live rows whose key is still in MySQL (ids) and did not change
    (not in delta)  UNION ALL  the changed rows (delta). Keys are compared as one
    JSON string (TO_JSON_STRING(STRUCT(...))): plain equality, so BigQuery can
    join on it, and NULL-safe (a NULL key part still matches itself)."""
    k = lambda a: "TO_JSON_STRING(STRUCT(" + ", ".join(f"{a}.`{c}` AS `{c}`" for c in keys) + "))"  # noqa: E731
    return (f"SELECT t.* FROM `{live_ref}` t "
            f"JOIN (SELECT DISTINCT {k('x')} AS _k FROM `{ids_ref}` x) i ON {k('t')} = i._k "
            f"LEFT JOIN (SELECT DISTINCT {k('d')} AS _k FROM `{delta_ref}` d) c ON {k('t')} = c._k "
            f"WHERE c._k IS NULL "
            f"UNION ALL SELECT * FROM `{delta_ref}`")


def _stream_to_bq(client_code, sql, params, bq, ref, schema) -> int:
    """Stream a MySQL SELECT into a BigQuery table. Returns rows.

    Rows are read CHUNK_ROWS at a time (constant memory) into ONE local gzipped
    NDJSON file, then sent with ONE load job. (One load job per chunk = ~100
    updates of the same table in a few minutes for 2M rows -> BigQuery 403
    "Exceeded rate limits: too many table update operations".)
    Empty values are left out of the row, so BigQuery stores a real NULL (a
    JSON column would otherwise get the JSON value `null`)."""
    import gzip
    import tempfile

    from google.cloud import bigquery
    types = {f.name: f.field_type for f in schema}
    total = 0
    fd, path = tempfile.mkstemp(prefix="bq_load_", suffix=".json.gz")
    os.close(fd)
    try:
        with gzip.open(path, "wt", encoding="utf-8") as out:
            with get_engine(client_code).connect().execution_options(stream_results=True) as conn:
                # MySQL drops a streaming read if the client pauses > net_write_timeout (60 s)
                conn.execute(text("SET SESSION net_write_timeout = 3600"))
                res = conn.execute(text(sql), params)
                keys = [k for k in res.keys() if k in types]   # columns BigQuery doesn't have are ignored
                while True:
                    chunk = res.fetchmany(CHUNK_ROWS)
                    if not chunk:
                        break
                    for r in chunk:
                        m = r._mapping
                        row = {k: _to_bq(m[k], types[k]) for k in keys}
                        out.write(json.dumps({k: v for k, v in row.items() if v is not None}))
                        out.write("\n")
                    total += len(chunk)
        if total:
            cfg = bigquery.LoadJobConfig(schema=schema, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                                         write_disposition="WRITE_TRUNCATE")
            with open(path, "rb") as f:
                bq.load_table_from_file(f, ref, job_config=cfg).result()
        else:                                                    # nothing read -> empty table with the schema
            bq.delete_table(ref, not_found_ok=True)
            bq.create_table(bigquery.Table(ref, schema=schema))
    finally:
        os.remove(path)
    return total


# ─────────────────────────────────────────────────────────────
# DAG steps
# ─────────────────────────────────────────────────────────────
def start_run(client_code: str, full: bool = False) -> dict:
    """Create missing BigQuery tables, open the run. Returns {task_id, cutoff, tables}.
    full=True (the manual Spark full load): no cutoff, every table is reloaded."""
    configure_etl_logging()
    pipe = get_bigquery_pipeline(client_code)
    tables = list(pipe["tables"])
    bq = get_bigquery_client()
    ds = _dataset(client_code)

    # the incremental and the full-load DAG share the hidden __delta/__next tables:
    # never start while another BigQuery run of this client is still going
    with get_engine(client_code).connect() as c:
        busy = c.execute(text("SELECT task_id, created_at FROM etl_task WHERE pipeline_type=:p AND client_code=:cc "
                              "AND status='PROCESSING' AND created_at > NOW() - INTERVAL :h HOUR"),
                         {"p": PIPELINE, "cc": client_code, "h": RUNNING_HOURS}).fetchone()
    if busy:
        raise RuntimeError(f"another BigQuery run for {client_code} is still running (task {busy[0]}, "
                           f"started {busy[1]}) — wait for it to finish, then trigger again")

    from google.cloud import bigquery
    dataset = bigquery.Dataset(f"{bq.project}.{ds}")
    dataset.location = get_settings().bq_location
    bq.create_dataset(dataset, exists_ok=True)                  # first run of a new client
    existing = {t.table_id for t in bq.list_tables(f"{bq.project}.{ds}")}
    for table, stmt in _ddl_statements(os.path.join(_root(), pipe["ddl"])):
        if table in pipe["tables"] and table not in existing:
            bq.query(_CREATE.sub(f"CREATE TABLE IF NOT EXISTS `{bq.project}.{ds}.{table}`", stmt, count=1)).result()
            logger.info("----> [BQ SYNC] created missing BigQuery table %s.%s", ds, table)
    _drop_shadows(bq, client_code, tables)                      # leftovers of an interrupted run

    with get_engine(client_code).begin() as c:
        # a run that died without finishing can never count as the last success
        flag = {"p": PIPELINE, "cc": client_code}
        c.execute(text("UPDATE etl_task SET status='FAILED', error='interrupted (superseded by a new run)' "
                       "WHERE pipeline_type=:p AND client_code=:cc AND status='PROCESSING'"), flag)
        last = c.execute(text("SELECT MAX(created_at) FROM etl_task "
                              "WHERE pipeline_type=:p AND client_code=:cc AND status='COMPLETED'"), flag).scalar()
        # date + time: rows updated since the last COMPLETED run STARTED, minus a
        # safety margin (a MySQL load still committing when that run read the data)
        cutoff = (last - CUTOFF_MARGIN).replace(microsecond=0) if (last and not full) else None
        task_id = uuid.uuid4().hex
        c.execute(text("INSERT INTO etl_task (task_id, pipeline_type, client_code, file_type, status, current_stage, "
                       "cutoff_date, row_count, stored_format, blob_name) "
                       "VALUES (:t, :p, :cc, :ft, 'PROCESSING', 'START', :cut, 0, :fmt, :ds)"),
                  {**flag, "t": task_id, "ft": FILE_TYPE, "cut": cutoff, "ds": f"{bq.project}.{ds}",
                   "fmt": "BIGQUERY_SPARK" if full else "BIGQUERY"})
    _stage(client_code, task_id, "START", "SUCCESS",
           mode="FULL_SPARK" if full else ("INCREMENTAL" if cutoff else "FULL"))
    logger.info("----> [BQ SYNC START] client=%s dataset=%s task=%s cutoff=%s (%s) tables=%s",
                client_code, ds, task_id, cutoff, "incremental" if cutoff else "FULL load", len(tables))
    return {"task_id": task_id, "cutoff": cutoff.strftime("%Y-%m-%d %H:%M:%S") if cutoff else None, "tables": tables}


def sync_table(client_code: str, task_id: str, table: str, cutoff: str | None) -> dict:
    """EXTRACT + BUILD one table into its shadow tables (live table untouched)."""
    configure_etl_logging()
    spec = get_bigquery_pipeline(client_code)["tables"][table]
    bq = get_bigquery_client()
    live_ref = _ref(bq, client_code, table)
    delta, ids, nxt = (live_ref + s for s in ("__delta", "__ids", "__next"))
    stage = "EXTRACT"
    try:
        live = bq.get_table(live_ref)
        schema = live.schema
        mode = "INCREMENTAL" if (cutoff and spec.get("incremental_column") and (live.num_rows or 0) > 0) else "FULL"
        _task(client_code, task_id, current_stage=f"SYNC_{table}"[:50])

        base_sql = f"SELECT {', '.join(spec['select'])} FROM {spec['from']}"
        params = {}
        if mode == "INCREMENTAL":
            base_sql += f" WHERE {spec['incremental_column']} >= :cut"
            params["cut"] = cutoff
        extracted = _stream_to_bq(client_code, base_sql, params, bq, delta, schema)
        _stage(client_code, task_id, "EXTRACT", "SUCCESS", table=table, mode=mode, rows=extracted)
        logger.info("----> [BQ SYNC] %s %s: %s rows extracted", table, mode, extracted)

        stage = "BUILD"
        from google.cloud import bigquery
        if mode == "FULL":
            # guard: an empty MySQL read must never wipe a filled BigQuery table
            if extracted == 0 and (live.num_rows or 0) > 0:
                raise RuntimeError(f"MySQL returned 0 rows for {table} but BigQuery has {live.num_rows} — refusing to empty it")
            bq.copy_table(delta, nxt, job_config=bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")).result()
        else:
            # current keys in MySQL (to mirror deletes)
            keys = spec["id_columns"]
            base_table = spec["from"].split()[0]
            key_schema = [f for f in schema if f.name in keys]
            n_ids = _stream_to_bq(client_code, f"SELECT {', '.join('`%s`' % k for k in keys)} FROM {base_table}",
                                  {}, bq, ids, key_schema)
            if n_ids == 0 and (live.num_rows or 0) > 0:
                raise RuntimeError(f"MySQL has 0 keys for {table} but BigQuery has {live.num_rows} — refusing to empty it")
            sql = merge_sql(live_ref, ids, delta, keys)
            bq.query(sql, job_config=bigquery.QueryJobConfig(
                destination=nxt, write_disposition="WRITE_TRUNCATE")).result()
        built = bq.get_table(nxt).num_rows
        _stage(client_code, task_id, "BUILD", "SUCCESS", table=table, mode=mode, rows=built)
        with get_engine(client_code).begin() as c:
            c.execute(text("UPDATE etl_task SET row_count = row_count + :n WHERE task_id = :t"),
                      {"n": extracted, "t": task_id})
        logger.info("----> [BQ SYNC] %s built: %s rows in the new table (live was %s)", table, built, live.num_rows)
        return {"table": table, "mode": mode, "extracted": extracted, "rows": built}
    except Exception as e:
        _stage(client_code, task_id, stage, "FAILED", table=table, error=e)
        logger.error("----> [BQ SYNC FAILED] %s at %s: %s", table, stage, str(e).splitlines()[0][:300])
        raise


def publish(client_code: str, task_id: str, tables: list[str]) -> None:
    """Swap every <table>__next into place — all or nothing (rollback on failure)."""
    configure_etl_logging()
    from google.cloud import bigquery
    bq = get_bigquery_client()
    truncate = bigquery.CopyJobConfig(write_disposition="WRITE_TRUNCATE")
    _task(client_code, task_id, current_stage="PUBLISH")
    for t in tables:                                             # every table must have been built
        bq.get_table(_ref(bq, client_code, t) + "__next")
    for t in tables:                                             # 1) back up all live tables first
        ref = _ref(bq, client_code, t)
        bq.copy_table(ref, ref + "__prev", job_config=truncate).result()
    done = []
    try:
        for t in tables:                                         # 2) swap the new versions in
            ref = _ref(bq, client_code, t)
            bq.copy_table(ref + "__next", ref, job_config=truncate).result()
            done.append(t)
        for t in tables:
            _stage(client_code, task_id, "PUBLISH", "SUCCESS", table=t,
                   rows=bq.get_table(_ref(bq, client_code, t)).num_rows)
        logger.info("----> [BQ SYNC] published %s tables", len(tables))
    except Exception as e:
        for t in done:                                           # 3) rollback what was already swapped
            ref = _ref(bq, client_code, t)
            bq.copy_table(ref + "__prev", ref, job_config=truncate).result()
            _stage(client_code, task_id, "ROLLBACK", "SUCCESS", table=t)
        _stage(client_code, task_id, "PUBLISH", "FAILED", error=e)
        logger.error("----> [BQ SYNC] publish failed, %s tables restored: %s", len(done), str(e).splitlines()[0][:300])
        raise


def finish_run(client_code: str, task_id: str, tables: list[str]) -> None:
    configure_etl_logging()
    _drop_shadows(get_bigquery_client(), client_code, tables)
    _task(client_code, task_id, status="COMPLETED", current_stage="FINISHED")
    _stage(client_code, task_id, "FINISHED", "SUCCESS")
    logger.info("----> [BQ SYNC FINISHED] client=%s task=%s -> COMPLETED", client_code, task_id)


def fail_run(client_code: str, task_id: str | None, tables: list[str], error: str = "a sync step failed") -> None:
    """Mark the run FAILED and drop the shadow tables — live tables were never touched
    (or were restored by publish), so BigQuery stays exactly as before the run."""
    configure_etl_logging()
    try:
        # keep <table>__prev (the pre-publish backups) for manual recovery; the
        # next run's start_run clears them. No task_id = this run never started
        # (e.g. refused because another run is going) -> its tables are not ours.
        if task_id:
            _drop_shadows(get_bigquery_client(), client_code, tables, suffixes=("__delta", "__ids", "__next"))
    finally:
        if task_id:
            _task(client_code, task_id, status="FAILED", error=error[:4000])
            _stage(client_code, task_id, "FAILED", "FAILED", error=error)
        logger.error("----> [BQ SYNC] client=%s task=%s -> FAILED (live BigQuery tables unchanged)", client_code, task_id)

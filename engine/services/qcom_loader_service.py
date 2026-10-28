"""Q-commerce load runner — MySQL source table -> hangyodb, per-table DAG steps.

Same flow as the retailverse outlet POST upload (perform_etl_validation +
execute_pipeline); ONLY the read differs — the source is a MySQL table, not a
file:

    read source (incremental, as str)      == read_data (file read with dtype=str)
    DataPreprocessor.clean_file            header normalization + blank -> None
    validate_required_columns              CLIENT_REQUIREMENTS[client][file_type]
    apply_schema                           select + RENAME ONCE + cast (file_config)
    QCOM_CLEANERS[file_type]               serializer rules on canonical names
    CoreDataValidatorService               client_id / ext_erp_id from masterdb
    inject client_id/ext_erp_id/created_by/updated_by
    flow config (canonical names)          category -> product -> qcom_sales

    qcom_prepare_load(file_type)        all of the above up to the flow -> local
                                        Parquet. Returns {task_id, path}.
    qcom_load_one_table(file_type, h..) LOAD one target table in its own txn:
                                          category / qcom_sales -> engine load_table;
                                          product -> _upsert_products (one row per
                                          item_code; a renamed code updates in place).
    finalize_load(...)                  reused from data_loader_service.

Incremental: only source rows created/updated since the last COMPLETED run for
this file_type (from hangyodb.etl_task). First run loads the full source.

The source lives in another database on the SAME MySQL host, so it's read with a
fully-qualified `pbisalesanalysisdb.<table>` query on the client's own engine.
"""
import logging
import os
import tempfile

import pandas as pd
from sqlalchemy import text

from core.queries.db_connection import db_for_client, get_engine
from core.queries.query_executor import QueryExecutor
from core.settings import get_settings
from core.utils.logger import configure_etl_logging
from api.repositories import (
    DEFAULT_USER_ID,
    MySQLTracker,
    PipelineStage,
    StageStatus,
    TaskStatus,
)
from config.qcom_flow_config import get_qcom_flow, QCOM_SOURCES
from engine.qcom_serializers import QCOM_CLEANERS
from engine.repositories import get_id_mapping
from engine.services.config_validation_service import DataPreprocessor, apply_schema, validate_required_columns
from engine.services.core_data_validator_service import CoreDataValidatorService
from engine.services.data_distribution_service import load_table
from engine.services.data_loader_service import _last_success_cutoff, finalize_load  # reuse

logger = logging.getLogger("QCOM_LOAD_RUNNER")

# Raw (source) date columns to check for "created or updated since last run".
_DATE_CANDIDATES = ["created_date", "modified_date", "updated_date", "created_at", "updated_at"]


# ─────────────────────────────────────────────────────────────
# PREPARE — read -> clean_file -> validate -> apply_schema -> serializer -> ids
# ─────────────────────────────────────────────────────────────
def qcom_prepare_load(file_type: str, client_code: str = "HANGYO", full_load: bool = False) -> dict:
    """Run the upload-validation flow on the source table and write the cleaned
    Parquet the per-table load tasks read. Returns {task_id, path}.

    full_load=True ignores the last-run watermark and reads the WHOLE source
    table, so every row is upserted by id (missing rows inserted, the rest
    updated) — used by the manual hangyo_fullload_* DAGs."""
    configure_etl_logging()
    settings = get_settings()
    engine = get_engine(client_code)
    tracker = MySQLTracker(client_code)

    src = QCOM_SOURCES[file_type]
    source_db = settings.qcom_source_db
    source_table = src["source_table"]
    ext_erp_rec_code = src["ext_erp_rec_code"]

    task_id = tracker.create_task(
        client_code=client_code, file_type=file_type,
        filename=f"{source_db}.{source_table}", size_bytes=0,
        ext_erp_rec_code=str(ext_erp_rec_code),
    )
    tracker.set_status(task_id, TaskStatus.PROCESSING)
    logger.info("========================================================================")
    logger.info("----> [QCOM PREPARE START] client=%s file_type=%s source=%s.%s dest_db=%s task=%s",
                client_code, file_type, source_db, source_table, db_for_client(client_code), task_id)

    stage = PipelineStage.ETL_EXTRACT
    try:
        # READ — cross-DB read on the same host, incremental by watermark, every
        # value as str (the DB equivalent of read_data reading files dtype=str)
        cutoff = None if full_load else _last_success_cutoff(engine, file_type, client_code)
        if full_load:
            logger.info("----> [QCOM EXTRACT] FULL RELOAD: ignoring last-run watermark, reading the whole source")
        base_sql = f"SELECT * FROM `{source_db}`.`{source_table}`"
        with engine.connect() as conn:
            cols = _source_columns(conn, source_db, source_table)
            where, params = _delta_where(cols, cutoff)
            df = _as_str_frame(pd.read_sql(text(base_sql + where), conn, params=params))
        full = int(len(df))
        logger.info("----> [QCOM EXTRACT] read %s rows from %s.%s (cutoff=%s, delta_cols=%s)",
                    full, source_db, source_table, cutoff, [c for c in _DATE_CANDIDATES if c in cols])
        tracker.update_stage(task_id, PipelineStage.ETL_EXTRACT, StageStatus.SUCCESS, rows=full)

        # CLEAN_FILE -> VALIDATE REQUIRED COLUMNS (CLIENT_REQUIREMENTS)
        stage = PipelineStage.COLUMN_VALIDATION
        df = DataPreprocessor.clean_file(df)
        df = validate_required_columns(df, file_type, client_code)
        tracker.update_stage(task_id, PipelineStage.COLUMN_VALIDATION, StageStatus.SUCCESS, stage_rows=full)

        # APPLY SCHEMA (rename ONCE to canonical names) -> SERIALIZER RULES
        stage = PipelineStage.ETL_TRANSFORM
        df = apply_schema(df, client_code, file_type)
        df = QCOM_CLEANERS[file_type](df)
        valid = int(len(df))

        # client_id / ext_erp_id from masterdb — exactly like the file ETL
        client_id, ext_erp_id, _ext_erp_name = CoreDataValidatorService(
            client_code, str(ext_erp_rec_code)).get_client_and_erp_ids()
        df["client_id"] = client_id
        df["ext_erp_id"] = ext_erp_id
        df["created_by"] = DEFAULT_USER_ID
        df["updated_by"] = DEFAULT_USER_ID
        logger.info("----> [TRANSFORM] valid=%s  invalid(dropped)=%s  client_id=%s  ext_erp_id=%s",
                    valid, full - valid, client_id, ext_erp_id)
        tracker.update_stage(task_id, PipelineStage.ETL_TRANSFORM, StageStatus.SUCCESS,
                             valid_record_count=valid, invalid_record_count=full - valid, stage_rows=valid)
    except Exception as e:
        tracker.update_stage(task_id, stage, StageStatus.FAILED, error=str(e))
        tracker.set_status(task_id, TaskStatus.FAILED)
        logger.error("----> [QCOM PREPARE FAILED] stage=%s : %s", stage, str(e).splitlines()[0][:200])
        raise

    path = os.path.join(tempfile.gettempdir(), f"qcom_{task_id}_{file_type}.parquet")
    df.to_parquet(path, engine="pyarrow", index=False)
    logger.info("----> [QCOM PREPARE DONE] cleaned %s rows -> %s", valid, path)
    return {"task_id": task_id, "path": path}


def _as_str_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Every value -> str (None stays None) — what reading a file with dtype=str
    gives the upload flow. Whole-number floats (int columns that pandas widened
    because of NULLs) render without '.0', so ids like 877000 stay '877000'."""
    out = pd.DataFrame(index=df.index)
    for c in df.columns:
        s = df[c]
        if pd.api.types.is_float_dtype(s):
            nn = s.dropna()
            if len(nn) and (nn % 1 == 0).all():
                s = s.astype("Int64")
        elif pd.api.types.is_datetime64_any_dtype(s):
            s = s.dt.strftime("%Y-%m-%d %H:%M:%S")
        s = s.astype(object)
        out[c] = s.where(s.notna(), None).map(lambda v: v if v is None else str(v))
    return out


def _source_columns(conn, schema: str, table: str) -> set:
    rows = conn.execute(
        text("SELECT COLUMN_NAME FROM information_schema.COLUMNS "
             "WHERE TABLE_SCHEMA=:s AND TABLE_NAME=:t"),
        {"s": schema, "t": table},
    ).fetchall()
    return {r[0] for r in rows}


def _delta_where(cols: set, cutoff):
    """Build a WHERE that keeps rows created/updated on/after the cutoff DAY.

    First run (no cutoff) or no date columns -> full load (empty WHERE).
    """
    if cutoff is None:
        return "", {}
    date_cols = [c for c in _DATE_CANDIDATES if c in cols]
    if not date_cols:
        return "", {}
    cutoff_day = pd.Timestamp(cutoff).date()
    ors = " OR ".join(f"`{c}` >= :cut" for c in date_cols)
    return f" WHERE {ors}", {"cut": cutoff_day}


# ─────────────────────────────────────────────────────────────
# LOAD — one target table per DAG task (own transaction)
# ─────────────────────────────────────────────────────────────
def qcom_load_one_table(file_type: str, handler: str, task_id: str, path: str,
                        client_code: str = "HANGYO", replace: bool = False) -> int:
    """Load one target table in its own transaction.

    replace=True (full-reload DAGs, fact tables only): DELETE this platform's
    rows first, then load the whole prepared source — the destination becomes
    an exact copy of the source (rows deleted in the source disappear). Delete
    + load are ONE transaction: if the load fails, the old rows come back.
    category / product are never replaced (other facts point at them)."""
    configure_etl_logging()
    engine = get_engine(client_code)
    tracker = MySQLTracker(client_code)
    table = next(t for t in get_qcom_flow(file_type) if t["handler"] == handler)
    df = pd.read_parquet(path, engine="pyarrow")
    stage = f"LOAD_{table['table_name'].upper()}"
    logger.info("----> [QCOM LOAD TABLE START] client=%s table=%s handler=%s rows=%s db=%s",
                client_code, table["table_name"], handler, len(df), db_for_client(client_code))
    try:
        with engine.begin() as conn:          # one transaction per table
            if handler == "product":
                # upsert keyed on (ext_erp_id, item_code): a renamed code updates
                # in place. The DB unique key includes product_name, so the generic
                # engine upsert can't express "one row per item_code".
                n = _upsert_products(conn, df, table)
            else:
                if replace:
                    _delete_platform_rows(conn, df, table)
                n = load_table(conn, table, df)
        # stage row_count = records sent to insert/upsert into this table
        # (etl_task.row_count is untouched — it is set once, at extract)
        tracker.update_stage(task_id, stage, StageStatus.SUCCESS, stage_rows=n)
        logger.info("----> [QCOM LOAD TABLE DONE] table=%s inserted/updated=%s rows\n", table["table_name"], n)
        return n
    except Exception as e:
        tracker.update_stage(task_id, stage, StageStatus.FAILED, error=str(e))
        tracker.set_status(task_id, TaskStatus.FAILED)
        logger.error("----> [QCOM LOAD TABLE FAILED] table=%s : %s",
                     table["table_name"], str(e).splitlines()[0][:200])
        raise


def _delete_platform_rows(conn, df: pd.DataFrame, table: dict) -> int:
    """Full reload: remove this platform's rows from the fact table (scoped by
    ext_erp_id + platform) before the whole source is loaded again.

    Guard: never wipe the table when the prepared source is empty (source table
    empty / unreadable / every row invalid) — fail instead, nothing is deleted."""
    table_name = table["table_name"]
    if table_name in ("category", "product"):
        raise ValueError(f"full reload never replaces the shared '{table_name}' table")
    if df is None or df.empty:
        raise RuntimeError(f"[QCOM FULL RELOAD] prepared source is EMPTY — refusing to delete "
                           f"{table_name} rows (check the source table); nothing was changed")
    ext_erp_id = int(df["ext_erp_id"].dropna().iloc[0])
    platform = str(df["platform"].dropna().iloc[0])
    params = {"eid": ext_erp_id, "p": platform}
    where = "WHERE ext_erp_id = :eid AND platform = :p"
    before = QueryExecutor.fetch_all_on(conn, f"SELECT COUNT(*) FROM `{table_name}` {where}", params)[0][0]
    QueryExecutor.execute_on(conn, f"DELETE FROM `{table_name}` {where}", params)
    logger.info("----> [QCOM FULL RELOAD] deleted %s existing %s rows (platform=%s ext_erp_id=%s); "
                "loading %s source rows in the same transaction", before, table_name, platform, ext_erp_id, len(df))
    return before


_AUDIT_COLS = {"client_id", "ext_erp_id", "created_by", "updated_by"}


def _product_columns(table: dict) -> tuple[str, str, str | None, dict]:
    """(key, name, category, extras) read from the flow config's product step:
    the column mapped to ext_erp_record_id, the one mapped to product_name, the
    category_id parent's col_name, and any other {canonical: product column}
    mapping (e.g. {"ean": "bar_code"}) to write through as-is."""
    step = table["flow"][0]
    req = step["required_columns"]
    key = next(c for c, d in req.items()
               if d == "ext_erp_record_id" or (isinstance(d, list) and "ext_erp_record_id" in d))
    name = next(c for c, d in req.items() if d == "product_name")
    cat = next((p["col_name"] for p in step["parent_columns"] if p["map_col_name"] == "category_id"), None)
    extras = {c: d for c, d in req.items()
              if isinstance(d, str) and d and c not in (key, name, cat) and c not in _AUDIT_COLS}
    return key, name, cat, extras


def _upsert_products(conn, df: pd.DataFrame, table: dict) -> int:
    """Upsert products by (ext_erp_id, ext_erp_record_id == item_code).

    One row per item_code, keeping the LATEST name (df is pre-sorted newest
    first by the serializer). New item_code -> INSERT. Existing -> UPDATE
    sku_code / extras, plus (per the flow config's product step):
        update_name=True           product_name := this feed's latest name
        update_category="always"   category_id  := this feed's leaf category
        update_category="if_missing" category_id only filled when empty
    category_id is resolved from the category table (scoped by ext_erp_id) that
    the earlier category task loaded. Column names come from the flow config;
    ids from the prepared frame (resolved from masterdb in prepare).
    """
    key_col, name_col, cat_col, extras = _product_columns(table)
    if df is None or df.empty or key_col not in df.columns:
        return 0
    extras = {c: d for c, d in extras.items() if c in df.columns}
    # e.g. extras {"ean": "bar_code"} -> ", `bar_code`" / ", :x_bar_code" / COALESCE on update
    x_cols = "".join(f", `{d}`" for d in extras.values())
    x_vals = "".join(f", :x_{d}" for d in extras.values())
    x_sets = "".join(f", `{d}` = COALESCE(:x_{d}, `{d}`)" for d in extras.values())
    ext_erp_id = int(df["ext_erp_id"].dropna().iloc[0])
    client_id = int(df["client_id"].dropna().iloc[0])
    prod = df.dropna(subset=[key_col]).drop_duplicates(subset=[key_col], keep="first")
    # walk codes in key order: sales + stock-on-hand DAGs run at the same time and
    # touch the same product rows — one lock order avoids deadlocks
    prod = prod.sort_values(key_col, kind="stable")
    update_name = table.get("update_name", True)
    cat_set = ("category_id = COALESCE(category_id, :cid)"
               if table.get("update_category", "always") == "if_missing" else "category_id = :cid")
    name_set = "product_name = :n, " if update_name else ""

    # resolve category_id (scoped to this platform's ext_erp_id)
    cat_names = ([str(v).strip() for v in prod[cat_col].dropna().unique() if str(v).strip()]
                 if cat_col and cat_col in prod.columns else [])
    cat_map = get_id_mapping(conn, cat_names, "category", "category_id", "category_name", ext_erp_id) if cat_names else {}

    n = 0
    dedup = 0
    for _, row in prod.iterrows():
        item_id = str(row[key_col]).strip()
        if not item_id or item_id.lower() in ("nan", "none", "null"):
            continue
        name = row.get(name_col)
        name = None if pd.isna(name) else str(name).strip()
        catn = row.get(cat_col) if cat_col else None
        cat_id = cat_map.get(str(catn).strip()) if pd.notna(catn) else None
        x_params = {f"x_{d}": (None if pd.isna(row.get(c)) else row.get(c)) for c, d in extras.items()}

        # Existing rows for this (ext_erp_id, ext_erp_record_id). NOTE: the unique
        # key is (product_name, ext_erp_id, ext_erp_record_id), so a source
        # item_id that carries >1 name over time can leave >1 row here — we must
        # update BY product_id (PK), never blindly by (eid, rec), or we'd try to
        # set a name a sibling row already holds and hit uk_unique_erpprd (1062).
        rows = QueryExecutor.fetch_all_on(
            conn,
            "SELECT product_id, product_name FROM product "
            "WHERE ext_erp_id = :eid AND ext_erp_record_id = :rec ORDER BY product_id",
            {"eid": ext_erp_id, "rec": item_id},
        )
        if not rows:
            QueryExecutor.execute_on(
                conn,
                "INSERT INTO product (product_name, sku_code, ext_erp_record_id, ext_erp_id, "
                f"client_id, category_id, created_by, updated_by{x_cols}) "
                f"VALUES (:n, :sku, :rec, :eid, :cl, :cid, :cb, :ub{x_vals})",
                {"n": name, "sku": item_id, "rec": item_id, "eid": ext_erp_id, "cl": client_id,
                 "cid": cat_id, "cb": DEFAULT_USER_ID, "ub": DEFAULT_USER_ID, **x_params},
            )
        else:
            # ONE product per item_code. The same code carrying a new name = a
            # RENAME -> update the existing row (only when update_name). If earlier
            # (non-atomic) runs left >1 row for this code, collapse to the first:
            # repoint any sales / stock off the duplicates, delete the extras (frees
            # their name so the rename below can't hit uk_unique_erpprd), then
            # update the survivor.
            keep = rows[0][0]
            for extra_id in (r[0] for r in rows[1:]):
                for fact in ("qcom_sales", "qcom_stock_on_hand", "qcom_po"):
                    QueryExecutor.execute_on(
                        conn, f"UPDATE {fact} SET product_id = :keep WHERE product_id = :ex",
                        {"keep": keep, "ex": extra_id})
                QueryExecutor.execute_on(
                    conn, "DELETE FROM product WHERE product_id = :ex", {"ex": extra_id})
                dedup += 1
            QueryExecutor.execute_on(
                conn,
                f"UPDATE product SET {name_set}sku_code = :sku, {cat_set}, "
                f"updated_by = :ub{x_sets} WHERE product_id = :pid",
                {"n": name, "sku": item_id, "cid": cat_id, "ub": DEFAULT_USER_ID, "pid": keep, **x_params},
            )
        n += 1
    logger.info("----> [QCOM PRODUCT UPSERT] %s unique item_ids upserted, %s duplicate rows collapsed (ext_erp_id=%s)",
                n, dedup, ext_erp_id)
    return n

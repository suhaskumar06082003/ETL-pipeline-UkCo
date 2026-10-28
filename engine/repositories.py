"""naturo DB layer for the load engine — ported from retailverse ETLRepository.

Reimplemented on SQLAlchemy (works with the version Airflow ships). Provides:
  create_record       INSERT (+ ON DUPLICATE KEY UPDATE, with optional CASE-WHEN
                      case_filter) in batches; skips rows with null/blank keys.
  get_id_mapping      resolve a parent id from a name/erp-id (TRIM IN (...)).
  filter_new_entries  fetch existing rows for an anti-join dedupe.
"""
import logging

import pandas as pd

from engine import queries
from engine.queries import CoreDataValidatorQueries
from core.queries.db_connection import get_master_engine
from core.queries.query_executor import QueryExecutor

logger = logging.getLogger("ETL_LOAD_REPO")

_NULLISH = {"", "null", "none", "nan", "nat", "<na>"}

# Lookup tables that live in the MASTER DB, not the client DB (retailverse
# ETLRepository.get_id_mapping routes these to database='default'). Without this
# the lookup runs on the client DB, finds no table, and pincode_id/district_id
# get upserted as NULL over existing outlets.
_MASTER_LOOKUP_TABLES = {"mpincode_office", "mdistrict"}


# ─────────────────────────────────────────────────────────────
# Master-DB (user_db) id resolution — ported from retailverse
# CoreDataValidatorRepository. Reads mclient + external_erp.
# ─────────────────────────────────────────────────────────────
class CoreDataValidatorRepository:

    @staticmethod
    def get_client_id(client_code: str) -> int:
        sql, params = CoreDataValidatorQueries.get_client_id(client_code)
        with get_master_engine().connect() as conn:
            row = QueryExecutor.fetch_one_on(conn, sql, params)
        if not row:
            raise ValueError(f"Client ID not found for client code '{client_code}'")
        return row[0]

    @staticmethod
    def get_erp_details(ext_erp_rec_code: str) -> tuple[int, str]:
        sql, params = CoreDataValidatorQueries.get_erp_details(ext_erp_rec_code)
        with get_master_engine().connect() as conn:
            row = QueryExecutor.fetch_one_on(conn, sql, params)
        if not row:
            raise ValueError(f"ERP details not found for rec code '{ext_erp_rec_code}'")
        # row -> (ext_erp_id, ext_erp_name)
        return row[0], row[1]


def _clean_val(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, str) and v.strip().lower() in ("", "null", "none"):
        return None
    if hasattr(v, "item"):  # numpy scalar
        v = v.item()
    if isinstance(v, pd.Timestamp):
        v = v.to_pydatetime()
    # whole-number floats -> int (avoid "12345.0" overflowing varchar columns)
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    return v


def create_record(conn, df: pd.DataFrame, table_name: str,
                  on_update_keys=None, case_filter=None, batch_size: int = 5000,
                  blank_not_null=None) -> int:
    """INSERT df rows into table (optionally upsert) on the given connection.

    Runs on the caller's transaction (no commit here) so the whole load can be
    one atomic transaction that rolls back on any failure.
    ``blank_not_null``: NOT NULL DEFAULT '' columns — a blank value is written as
    '' instead of NULL (everything else blank -> NULL as usual).
    """
    if df is None or df.empty:
        return 0
    cols = list(df.columns)
    records = [{c: _clean_val(row[c]) for c in cols} for row in df.to_dict(orient="records")]
    blank_cols = [c for c in (blank_not_null or []) if c in cols]
    if blank_cols:
        for r in records:
            for c in blank_cols:
                if r[c] is None:
                    r[c] = ""

    valid_keys = [k for k in (on_update_keys or []) if k in cols]
    if valid_keys:
        # drop rows whose upsert-key values are null/blank
        records = [
            r for r in records
            if all(r.get(k) is not None and str(r.get(k)).strip().lower() not in _NULLISH for k in valid_keys)
        ]
    if not records:
        return 0

    collist = ", ".join(f"`{c}`" for c in cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    sql = queries.insert_record_sql(table_name, collist, placeholders)

    if valid_keys:
        update_fields = [c for c in cols if c not in valid_keys]
        if update_fields:
            cf = [c for c in (case_filter or []) if c in cols]
            if cf:
                cond = " AND ".join(f"`{c}` = VALUES(`{c}`)" for c in cf)
                sets = ", ".join(
                    f"`{f}` = CASE WHEN {cond} THEN VALUES(`{f}`) ELSE `{f}` END" for f in update_fields
                )
            else:
                sets = ", ".join(f"`{f}` = VALUES(`{f}`)" for f in update_fields)
            sql += queries.on_duplicate_key_update_clause(sets)

    written = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        QueryExecutor.executemany_on(conn, sql, batch)
        written += len(batch)
    logger.info("----> [LOAD] %s rows into %s", written, table_name)
    return written


def get_id_mapping(conn, values, table_name, id_column, name_column, ext_erp_id=None) -> dict:
    """Return {name_value: id} for the given values (TRIM match) on the connection."""
    norm = [str(v).strip() for v in values if str(v).strip()]
    if not norm:
        return {}
    params = {f"v{i}": v for i, v in enumerate(norm)}
    inlist = ", ".join(f":v{i}" for i in range(len(norm)))
    sql = queries.id_mapping_sql(id_column, name_column, table_name, inlist)
    if ext_erp_id is not None:
        sql += queries.ID_MAPPING_EXT_ERP_FILTER
        params["eid"] = ext_erp_id
    try:
        if table_name in _MASTER_LOOKUP_TABLES:
            with get_master_engine().connect() as mconn:
                rows = QueryExecutor.fetch_all_on(mconn, sql, params)
        else:
            rows = QueryExecutor.fetch_all_on(conn, sql, params)
    except Exception as e:
        logger.warning("get_id_mapping skipped for %s: %s", table_name, str(e).splitlines()[0][:160])
        return {}
    return {r[1]: r[0] for r in rows if r[1] is not None}


def filter_new_entries(conn, table_name, db_col_dict, ext_erp_id=None) -> pd.DataFrame:
    """SELECT DISTINCT existing rows (aliased to df column names) for anti-join."""
    sel = ", ".join(f"`{db}` AS `{df}`" for df, db in db_col_dict.items())
    sql = queries.filter_new_entries_sql(sel, table_name)
    params = {}
    if ext_erp_id is not None:
        sql += queries.FILTER_NEW_ENTRIES_EXT_ERP_FILTER
        params["eid"] = ext_erp_id
    try:
        rows = QueryExecutor.fetch_all_on(conn, sql, params)
        return pd.DataFrame(rows, columns=list(db_col_dict.keys()))
    except Exception as e:
        logger.warning("filter_new_entries skipped for %s: %s", table_name, str(e).splitlines()[0][:160])
        return pd.DataFrame(columns=list(db_col_dict.keys()))

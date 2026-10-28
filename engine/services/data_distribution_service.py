"""Load orchestrator — ported from retailverse ETLTablesPipelineManager.

Walks the flow config table-by-table (and step-by-step within a table) in order.
The WHOLE load runs in ONE transaction (one connection): parent-id lookups see
the earlier tables' own uncommitted inserts within the same transaction, and if
ANY table fails the entire load rolls back — no inconsistent partial data.
"""
import logging

import pandas as pd
from sqlalchemy import text

from engine.services import data_preprocessing_service as H
from engine import repositories as repo

logger = logging.getLogger("ETL_LOAD_PIPELINE")

_COLS_CACHE: dict[str, tuple] = {}


def _table_columns(conn, table_name: str) -> tuple:
    if table_name not in _COLS_CACHE:
        rows = conn.execute(
            text("SELECT COLUMN_NAME FROM information_schema.COLUMNS "
                 "WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=:t"),
            {"t": table_name},
        ).fetchall()
        _COLS_CACHE[table_name] = tuple(r[0] for r in rows)
    return _COLS_CACHE[table_name]


def _add_beat_outlet_status(df: pd.DataFrame) -> pd.DataFrame:
    if "product_name" in df.columns:
        df["status"] = df["product_name"].apply(lambda x: "ORDER_PLACED" if pd.notnull(x) and str(x).strip() else "VISITED")
    else:
        df["status"] = "VISITED"
    return df


def _add_order_status(df: pd.DataFrame) -> pd.DataFrame:
    if "order_no" in df.columns and "sale_value" in df.columns:
        df = df.copy()
        totals = df.groupby("order_no")["sale_value"].transform(
            lambda s: pd.to_numeric(s, errors="coerce").sum()
        )
        df["total_amount"] = totals
        df["order_status"] = totals.apply(lambda t: "TOTAL_CALL" if pd.isna(t) or t == 0 else "CONFIRMED")
    return df


def _pre_secondary_item(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "product_name" in df.columns:
        df = df[df["product_name"].notna()]
    if "gross_qty" in df.columns:
        g = pd.to_numeric(df["gross_qty"], errors="coerce")
        df = df[g.notna() & (g != 0)]
    return df


# UOM code -> dimension (retailverse UOMDimensionPredictor.UOM_DIMENSION_MAP)
_UOM_DIM = {
    "KGS": "Weight", "KG": "Weight", "G": "Weight", "MG": "Weight", "LB": "Weight", "OZ": "Weight",
    "LTR": "Volume", "LT": "Volume", "ML": "Volume", "GAL": "Volume", "PT": "Volume",
    "NOS": "Count", "PCS": "Count", "EA": "Count", "UNIT": "Count", "BOX": "Count", "CBS": "Count", "PSC": "Count",
    "M": "Length", "MTR": "Length", "CM": "Length", "MM": "Length", "IN": "Length", "FT": "Length",
    "SQFT": "Area", "SQM": "Area", "SQCM": "Area", "SQIN": "Area", "CASE BOX": "Count",
}


def _post_uom_dimension(df: pd.DataFrame) -> pd.DataFrame:
    if "uom_code" in df.columns:
        df = df.copy()
        df["dimension"] = df["uom_code"].apply(
            lambda x: _UOM_DIM.get(str(x).strip().upper()) if pd.notnull(x) and str(x).strip() else None
        )
    return df


def _post_secondary_item(df: pd.DataFrame) -> pd.DataFrame:
    if "product_id" in df.columns:
        return df[df["product_id"].notna()]
    return df


_EXTRA_STEPS = {
    "beat_outlet_status": _add_beat_outlet_status,
    "order_status": _add_order_status,
    "secondary_item_pre": _pre_secondary_item,
}
_POST_STEPS = {
    "uom_dimension": _post_uom_dimension,
    "secondary_item_post": _post_secondary_item,
}


def _base_process(conn, dff, step, table, mode, forward_ext_erp_id, ext_erp_id):
    key_columns = step["key_columns"]
    check_cols = step["check_cols"]
    drop_cols = step["drop_cols"]
    required = step["required_columns"]
    parent_columns = step["parent_columns"]
    eid = ext_erp_id if forward_ext_erp_id else None

    # pre-steps
    if table.get("filter_nulls") == "key_columns":
        present = [c for c in key_columns if c in dff.columns]
        if present:
            dff = dff.dropna(subset=present, how="all")
    cnn = table.get("check_notnull")
    if cnn and cnn in dff.columns:
        dff = dff[dff[cnn].notna()]
    extra = table.get("extra")
    if extra and extra in _EXTRA_STEPS:
        dff = _EXTRA_STEPS[extra](dff)

    if mode == "JUNC":
        dff = H.map_parent_ids(conn, dff, parent_columns, eid)
        dff = H.get_unique_values_dataframe(conn, dff, key_columns, table["table_name"], "JUNC", required, check_cols, eid)
        if not dff.empty:
            dff = H.drop_columns_if_exist(dff, drop_cols)
            dff = H.rename_columns(dff, required)
            if check_cols:
                dff = H.filter_new_entries_from_df(conn, dff, table["table_name"], {c: c for c in check_cols}, eid)
    else:
        # NORMAL
        dff = H.map_parent_ids(conn, dff, parent_columns, eid)
        dff = H.get_unique_values_dataframe(conn, dff, key_columns, table["table_name"], "NORMAL", required, check_cols, eid)
        if not dff.empty:
            dff = H.drop_columns_if_exist(dff, drop_cols)
            dff = H.rename_columns(dff, required)

    # post-step (e.g. uom dimension, drop null product_id)
    post = table.get("post")
    if not dff.empty and post and post in _POST_STEPS:
        dff = _POST_STEPS[post](dff)
    return dff


def load_table(conn, table: dict, df: pd.DataFrame) -> int:
    """Run one table's flow steps and insert on the given connection."""
    table_name = table["table_name"]
    mode = table.get("mode", "NORMAL")
    forward = table.get("forward_ext_erp_id", False)
    # ext_erp_id resolved from the master DB is injected uniformly on every row
    # (see prepare_load). Use it so forward_ext_erp_id tables scope their
    # parent-id lookups / anti-join by ext_erp_id, exactly like retailverse.
    ext_erp_id = None
    if "ext_erp_id" in df.columns:
        vals = df["ext_erp_id"].dropna().unique()
        ext_erp_id = vals[0] if len(vals) else None
    n_steps = len(table["flow"])
    logger.info("---------------------> [TABLE: %s] mode=%s  steps=%s  on_update=%s",
                table_name, mode, n_steps, table.get("on_update"))
    total = 0
    for i, step in enumerate(table["flow"], start=1):
        dff = H.data_filter(df, list(step["required_columns"].keys()), step.get("condition_col"))
        logger.info("      [%s step %s/%s] keys=%s  candidate rows after filter=%s",
                    table_name, i, n_steps, step.get("key_columns"), len(dff))
        if dff.empty:
            continue
        step_df = _base_process(conn, dff, step, table, mode, forward, ext_erp_id)
        if step_df is None or step_df.empty:
            logger.info("      [%s step %s/%s] no NEW rows after dedupe/anti-join (already loaded)", table_name, i, n_steps)
            continue
        step_df = H.normalize_cells(step_df)
        # safety: only insert columns that exist in the target table
        valid_cols = [c for c in step_df.columns if c in _table_columns(conn, table_name)]
        step_df = step_df[valid_cols]
        if step_df.empty or not valid_cols:
            continue
        n = repo.create_record(
            conn, step_df, table_name,
            on_update_keys=table.get("on_update"),
            case_filter=table.get("on_update_filters"),
            blank_not_null=table.get("blank_not_null"),   # NOT NULL DEFAULT '' cols
        )
        logger.info("      [%s step %s/%s] new rows to write=%s", table_name, i, n_steps, len(step_df))
        total += n
    logger.info("---------------------> [TABLE: %s] TOTAL written=%s", table_name, total)
    return total


def load_flow(engine, flow: list, df: pd.DataFrame, on_table=None) -> dict:
    """Run every table/step in order inside ONE transaction (atomic).

    If any table raises, the whole transaction rolls back — no partial data.
    ``on_table(table_name, rows)`` is called after each table loads (for
    per-table stage tracking); it runs on a separate connection so it does not
    interfere with the load transaction.
    """
    counts: dict[str, int] = {}
    with engine.begin() as conn:  # one transaction; rolls back on any exception
        for t in flow:
            n = load_table(conn, t, df)
            counts[t["table_name"]] = n
            if on_table:
                on_table(t["table_name"], n)
    return counts

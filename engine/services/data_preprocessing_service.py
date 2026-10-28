"""Load-engine preprocessing helpers — ported from retailverse DataPreprocessor.

data_filter, map_parent_ids, the NORMAL hierarchical dedupe + JUNC dedupe,
anti-join against the DB, column rename/drop.
"""
import logging

import numpy as np
import pandas as pd

from engine import repositories as repo

logger = logging.getLogger("ETL_LOAD_HELPERS")

_INVALID = {"", "none", "null", "nan", "n/a", "na", "n.a", "-", "--"}


# ── 3A data_filter ──
def data_filter(df, required_columns, condition_cols=None):
    existing = [c for c in required_columns if c in df.columns]
    if not existing:
        return pd.DataFrame()
    df = df[existing]
    df = df.dropna(subset=existing, how="all")
    df = df.drop_duplicates(subset=existing)
    if condition_cols:
        valid = [c for c in condition_cols if c in df.columns]
        if len(valid) > 1:
            mask = df[valid].nunique(axis=1, dropna=False) != 1
            df = df[mask]
    return df[existing].copy()


# ── 3B map_parent_ids ──
def _normalize_value(val):
    if pd.isna(val):
        return None
    if isinstance(val, (int, float)) and not isinstance(val, bool):
        try:
            return str(int(val)) if float(val).is_integer() else str(val)
        except (ValueError, TypeError):
            return None
    s = str(val).strip()
    if not s:
        return None
    try:
        f = float(s)
        return str(int(f)) if f.is_integer() else str(f)
    except ValueError:
        return s


def map_parent_ids(conn, df, parent_columns, ext_erp_id=None):
    if not parent_columns:
        return df
    dff = df.copy()
    for pc in parent_columns:
        col_names = pc.get("col_name")
        map_col = pc.get("map_col_name")
        db_col = pc.get("DB_column")
        table = pc.get("table")
        id_col = pc.get("id_col")
        if not isinstance(col_names, list):
            col_names = [col_names]
        dff[map_col] = pd.NA
        for src_col in col_names:
            if src_col not in dff.columns:
                continue
            normalized_col = dff[src_col].apply(_normalize_value)
            parent_values = [v for v in normalized_col.dropna().unique().tolist() if v]
            if not parent_values:
                continue
            raw = repo.get_id_mapping(conn, parent_values, table, id_col, db_col, ext_erp_id)
            mapping = {_normalize_value(k): v for k, v in raw.items() if _normalize_value(k) is not None}
            mapped = normalized_col.map(mapping)
            if not mapped.dropna().empty:
                if dff[map_col].isna().all():
                    dff[map_col] = mapped
                else:
                    dff[map_col] = dff[map_col].combine_first(mapped)
        resolved = int(dff[map_col].notna().sum())
        logger.info("      [parent-id] %s -> %s (via %s): resolved %s/%s rows",
                    col_names, map_col, table, resolved, len(dff))
    return dff


# ── 3D NORMAL / JUNC dedupe ──
def _unique_not_null(df, key):
    col = df[key].astype(str).str.strip().str.lower()
    mask = (~col.isin(_INVALID)) & (df[key].notna())
    return df[mask].drop_duplicates(subset=key, keep="first")


def _rows_with_nulls(df, key_columns):
    clean = df[key_columns].apply(lambda c: c.map(lambda x: str(x).strip().lower() if pd.notnull(x) else "none"))
    mask = clean.apply(lambda row: all(v in _INVALID for v in row), axis=1)
    return df[mask]


def get_unique_values_dataframe(conn, df, key_columns, table_name, table_type,
                                required_columns_dict, check_cols, ext_erp_id=None):
    if not key_columns and not check_cols:
        return df.copy()

    filtered = df.copy()
    if key_columns:
        filtered = filtered.dropna(subset=key_columns, how="all").drop_duplicates(subset=key_columns)

    if table_type != "NORMAL":  # JUNC: dedupe deferred to anti-join on check_cols
        return filtered

    db_col_dict = {c: required_columns_dict[c] for c in check_cols if c in required_columns_dict}
    final = pd.DataFrame()
    key_so_far = []
    for key in key_columns:
        if key_so_far:
            filtered = _rows_with_nulls(filtered, key_so_far)
        if filtered.empty:
            continue
        uniq = _unique_not_null(filtered, key)
        if uniq.empty:
            key_so_far.append(key)
            continue
        new = filter_new_entries_from_df(conn, uniq, table_name, db_col_dict, ext_erp_id, key_so_far)
        key_so_far.append(key)
        if len(new) > 0:
            final = pd.concat([final, new], ignore_index=True)
    return final


# ── 3C anti-join vs DB ──
def filter_new_entries_from_df(conn, dff, table_name, db_col_dict, ext_erp_id=None, key_columns_so_far=None):
    if isinstance(db_col_dict, list):
        db_col_dict = {c: c for c in db_col_dict}
    if not db_col_dict:
        return dff
    db_df = repo.filter_new_entries(conn, table_name, db_col_dict, ext_erp_id)
    if db_df.empty:
        return dff
    match_cols = list(db_col_dict.keys())
    if key_columns_so_far:
        match_cols = [c for c in match_cols if c not in key_columns_so_far]
    if not match_cols:
        return dff.reset_index(drop=True)
    match_cols = [c for c in match_cols if c in dff.columns and c in db_df.columns]
    if not match_cols:
        return dff.reset_index(drop=True)
    left = dff.copy()
    for c in match_cols:
        left[c] = left[c].astype(str).str.strip()
        db_df[c] = db_df[c].astype(str).str.strip()
    merged = left.merge(db_df[match_cols].drop_duplicates(), on=match_cols, how="left", indicator=True)
    new = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    logger.info("      [anti-join] %s on %s: %s NEW of %s (already in DB=%s)",
                table_name, match_cols, len(new), len(dff), len(dff) - len(new))
    return new.reset_index(drop=True)


# ── 3E rename / drop ──
def drop_columns_if_exist(df, drop_cols):
    if not drop_cols:
        return df
    return df.drop(columns=[c for c in drop_cols if c in df.columns])


def rename_columns(df, required_columns: dict):
    new_df = df.copy()
    for old, new in required_columns.items():
        if isinstance(new, list):
            for target in new:
                if old in new_df.columns:
                    new_df[target] = new_df[old]
        elif new and old in new_df.columns:
            new_df = new_df.rename(columns={old: new})
    return new_df


# ── cell normalization before insert ──
def normalize_cells(df):
    def norm(v):
        if pd.isna(v):
            return None
        if isinstance(v, str):
            s = v.strip()
            return None if s.lower() in ("", "null", "none", "nan", "nat") else s
        if isinstance(v, pd.Timestamp):
            return v.to_pydatetime()
        if isinstance(v, np.generic):
            return v.item()
        return v
    return df.map(norm) if hasattr(df, "map") else df.applymap(norm)

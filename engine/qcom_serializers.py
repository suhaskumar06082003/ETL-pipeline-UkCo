"""Q-commerce serializer rules — column-wise (vectorized), like engine.serializers.

Runs AFTER config_validation_service.apply_schema, so every column already has
its CANONICAL name (renamed ONCE by config.file_config.QcomSalesModel). These
functions never rename — they only apply the per-platform serializer rules:
cast/derive measures, parse dates, set platform defaults, drop records missing
required fields (the serializer's "invalid" records), and order rows so the
product upsert keeps the LATEST name per item_code.

City: every qcom source table's `city_consolidated` (the ETL team's standard
city name, consistent across sales / SOH / PO and all platforms) is loaded as
city_name as-is — no city mapping here.
"""
import logging

import pandas as pd

from config.etl_enums import FileType
from engine.serializers import _strip_strings

logger = logging.getLogger("ETL_QCOM_CLEAN")

# qcom_sales NOT NULL columns without a default — a record missing any of these
# is invalid (the serializer's required=True, allow_null=False fields).
_REQUIRED_NONNULL = ["sales_id", "order_date", "city_name"]


def _measure(series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").round(6)


def _finish(df: pd.DataFrame, platform: str) -> pd.DataFrame:
    """Shared tail: platform default, drop invalid records, latest-name order."""
    df["platform"] = platform
    df["transact_uom_id"] = 1

    before = len(df)
    df = df.dropna(subset=[c for c in _REQUIRED_NONNULL if c in df.columns])
    if before - len(df):
        logger.info("----> [QCOM CLEAN %s] dropped %s invalid records missing %s",
                    platform, before - len(df), _REQUIRED_NONNULL)

    # newest first so the product upsert keeps the LATEST name per item_code;
    # product_name breaks same-date ties deterministically across runs.
    if "modified_date" in df.columns:
        df = df.assign(_ord=pd.to_datetime(df["modified_date"], errors="coerce"))
        df = df.sort_values(["_ord", "product_name"], ascending=[False, True], kind="stable").drop(columns=["_ord"])
    return df.reset_index(drop=True)


def clean_blinkit_df(df: pd.DataFrame) -> pd.DataFrame:
    """Blinkit serializer rules (canonical columns from get_config_blinkit)."""
    df = _strip_strings(df.copy())
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date

    # source `mrp` is the gross value (already renamed gross_value) -> derive the
    # unit MRP = gross_value / gross_qty (0 when qty is 0 or missing).
    df["gross_value"] = _measure(df["gross_value"]).fillna(0)
    df["gross_qty"] = _measure(df["gross_qty"]).fillna(0)
    unit = (df["gross_value"] / df["gross_qty"]).replace([float("inf"), float("-inf")], pd.NA)
    df["product_mrp"] = pd.to_numeric(unit, errors="coerce").round(6).fillna(0)

    # Blinkit defaults: single brand, no store / combo dimension
    df["brand_name"] = "Hangyo"
    df["store_id"] = None
    df["is_combo"] = 0
    df["combo_item_code"] = None
    return _finish(df, "BLINKIT")


def clean_instamart_df(df: pd.DataFrame) -> pd.DataFrame:
    """Instamart serializer rules (canonical columns from get_config_instamart)."""
    df = _strip_strings(df.copy())
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date

    # measures are direct: product_mrp=BASE_MRP, gross_qty=UNITS_SOLD, gross_value=GMV
    for col in ("product_mrp", "gross_qty", "gross_value"):
        df[col] = _measure(df[col]).fillna(0)
    df["combo_units_sold"] = _measure(df["combo_units_sold"])

    # COMBO 'Yes'/'No' -> 1/0
    df["is_combo"] = df["is_combo"].map(lambda x: 1 if str(x).strip().lower() == "yes" else 0)
    return _finish(df, "INSTAMART")


def clean_zepto_df(df: pd.DataFrame) -> pd.DataFrame:
    """Zepto serializer rules (canonical columns from get_config_zepto)."""
    df = _strip_strings(df.copy())
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce").dt.date

    # measures are direct: product_mrp=MRP, gross_qty=SalesQtyUnits, gross_value=GMV
    for col in ("product_mrp", "gross_qty", "gross_value"):
        df[col] = _measure(df[col]).fillna(0)

    # EAN arrives as '8906015614763.0' on some rows -> '8906015614763' (bar_code)
    df["ean"] = df["ean"].map(lambda x: x[:-2] if isinstance(x, str) and x.endswith(".0") else x)

    # Zepto has no store / combo dimension
    df["store_id"] = None
    df["is_combo"] = 0
    df["combo_item_code"] = None
    return _finish(df, "ZEPTO")


# ─────────────────────────────────────────────────────────────
# STOCK ON HAND serializers (canonical columns from QcomStockModel)
# ─────────────────────────────────────────────────────────────
# qcom_stock_on_hand NOT NULL columns without a default
_STOCK_REQUIRED_NONNULL = ["stock_id", "snapshot_date", "city_name"]


def _finish_stock(df: pd.DataFrame, platform: str) -> pd.DataFrame:
    """Shared tail for stock: platform default, drop invalid records, latest-name order."""
    df["platform"] = platform
    df["transact_uom_id"] = 1
    df["qty_on_hand"] = _measure(df["qty_on_hand"]).fillna(0)   # NOT NULL (default 0)

    before = len(df)
    df = df.dropna(subset=[c for c in _STOCK_REQUIRED_NONNULL if c in df.columns])
    if before - len(df):
        logger.info("----> [QCOM SOH CLEAN %s] dropped %s invalid records missing %s",
                    platform, before - len(df), _STOCK_REQUIRED_NONNULL)

    # newest snapshot first so the product upsert keeps the LATEST name per
    # item_code; product_name breaks ties deterministically across runs.
    df = df.assign(_snap=pd.to_datetime(df["snapshot_date"], errors="coerce"),
                   _mod=pd.to_datetime(df["modified_date"], errors="coerce"))
    df = df.sort_values(["_snap", "_mod", "product_name"], ascending=[False, False, True],
                        kind="stable").drop(columns=["_snap", "_mod"])
    return df.reset_index(drop=True)


def _snapshot(series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce").dt.date


def clean_soh_blinkit_df(df: pd.DataFrame) -> pd.DataFrame:
    """Blinkit stock-on-hand rules (canonical columns from QcomStockModel.get_config_blinkit)."""
    df = _strip_strings(df.copy())
    df["snapshot_date"] = _snapshot(df["snapshot_date"])

    # on hand = backend (warehouse) + frontend (dark store) inventory
    df["qty_backend"] = _measure(df["qty_backend"])
    df["qty_frontend"] = _measure(df["qty_frontend"])
    df["qty_on_hand"] = df["qty_backend"].fillna(0) + df["qty_frontend"].fillna(0)

    # no category column in the Blinkit SOH feed: the category task adds nothing
    # and the stock row takes the product's category (see the flow config)
    df["category"] = None
    return _finish_stock(df, "BLINKIT")


def clean_soh_instamart_df(df: pd.DataFrame) -> pd.DataFrame:
    """Instamart stock-on-hand rules (canonical columns from QcomStockModel.get_config_instamart)."""
    df = _strip_strings(df.copy())
    df["snapshot_date"] = _snapshot(df["snapshot_date"])

    # warehouse stock only: on hand == backend, no frontend figure
    df["qty_on_hand"] = _measure(df["qty_on_hand"]).fillna(0)
    df["qty_backend"] = df["qty_on_hand"]
    df["potential_gmv_loss"] = _measure(df["potential_gmv_loss"])
    df["open_po_qty"] = _measure(df["open_po_qty"])
    df["open_po_refs"] = df["open_po_refs"].fillna("[]")        # NOT NULL column

    # L1 / L2 lowercased — the spelling the Instamart sales feed uses, so they
    # match the existing category rows ('ice cream and indian sweets' > 'ice creams')
    for col in ("l1_category", "category"):
        df[col] = df[col].map(lambda c: c.lower() if isinstance(c, str) else c)
    return _finish_stock(df, "INSTAMART")


def clean_soh_zepto_df(df: pd.DataFrame) -> pd.DataFrame:
    """Zepto stock-on-hand rules (canonical columns from QcomStockModel.get_config_zepto)."""
    df = _strip_strings(df.copy())
    df["snapshot_date"] = _snapshot(df["snapshot_date"])
    df["qty_on_hand"] = _measure(df["qty_on_hand"]).fillna(0)
    # EAN may arrive as '8906015614763.0' -> '8906015614763' (bar_code), as in sales
    df["ean"] = df["ean"].map(lambda x: x[:-2] if isinstance(x, str) and x.endswith(".0") else x)
    return _finish_stock(df, "ZEPTO")


# ─────────────────────────────────────────────────────────────
# PURCHASE ORDER serializers (canonical columns from QcomPoModel)
# ─────────────────────────────────────────────────────────────
# qcom_po NOT NULL columns without a default
_PO_REQUIRED_NONNULL = ["po_id", "snapshot_date", "po_number", "item_code", "city_name", "po_status"]
_PO_MEASURES = ["ordered_qty", "received_qty", "pending_qty", "asn_qty", "unit_base_cost",
                "unit_landing_cost", "product_mrp", "margin_percentage", "line_value_without_tax",
                "line_value_with_tax", "po_header_amount", "tax_amount", "tax_pct", "cgst_pct",
                "sgst_pct", "igst_pct", "cess_pct"]


def _dt(series, fmt=None, utc=False) -> pd.Series:
    """Parse a text datetime; utc=True keeps the UTC wall clock without the offset."""
    out = pd.to_datetime(series, errors="coerce", format=fmt, utc=utc)
    return out.dt.tz_localize(None) if utc else out


def _finish_po(df: pd.DataFrame, platform: str, status_map: dict) -> pd.DataFrame:
    """Shared tail for PO: platform default, status mapping, measures, drop
    invalid records, latest-name order."""
    df["platform"] = platform
    df["transact_uom_id"] = 1
    df["snapshot_date"] = _snapshot(df["snapshot_date"])

    # platform status -> qcom_po.po_status enum (OPEN/CONFIRMED/RECEIVED/EXPIRED/CANCELLED)
    df["po_status"] = df["po_status_raw"].map(status_map)
    unknown = df.loc[df["po_status"].isna() & df["po_status_raw"].notna(), "po_status_raw"].unique()
    if len(unknown):
        logger.warning("----> [QCOM PO CLEAN %s] unmapped PO statuses %s — their rows are dropped; "
                       "add them to the status map in engine/qcom_serializers.py", platform, list(unknown))

    for col in _PO_MEASURES:
        if col in df.columns:
            df[col] = _measure(df[col])
    for col in ("ordered_qty", "line_value_with_tax"):       # NOT NULL (default 0)
        df[col] = df[col].fillna(0)

    before = len(df)
    df = df.dropna(subset=[c for c in _PO_REQUIRED_NONNULL if c in df.columns])
    if before - len(df):
        logger.info("----> [QCOM PO CLEAN %s] dropped %s invalid records missing %s",
                    platform, before - len(df), _PO_REQUIRED_NONNULL)

    # newest snapshot first so a NEW product gets the latest PO name
    df = df.assign(_snap=pd.to_datetime(df["snapshot_date"], errors="coerce"),
                   _mod=pd.to_datetime(df["modified_date"], errors="coerce"))
    df = df.sort_values(["_snap", "_mod", "product_name"], ascending=[False, False, True],
                        kind="stable").drop(columns=["_snap", "_mod"])
    return df.reset_index(drop=True)


_BLINKIT_PO_STATUS = {"Created": "OPEN", "Unscheduled": "OPEN", "Fulfilled": "RECEIVED",
                      "Expired": "EXPIRED", "Cancelled post Creation": "CANCELLED"}


def clean_po_blinkit_df(df: pd.DataFrame) -> pd.DataFrame:
    """Blinkit PO rules (canonical columns from QcomPoModel.get_config_blinkit)."""
    df = _strip_strings(df.copy())
    # '2026-03-05 05:22:51+00:00' / '2026-03-08T18:29:59Z' -> UTC wall clock
    df["po_date"] = _dt(df["po_date"], fmt="ISO8601", utc=True)
    df["po_expiry_date"] = _dt(df["po_expiry_date"], fmt="ISO8601", utc=True)
    df["entity_name"] = df["entity_name"].map(lambda x: x.strip('"').strip() if isinstance(x, str) else x)

    # remaining_quantity is the pending qty -> received = ordered - pending
    df["ordered_qty"] = _measure(df["ordered_qty"])
    df["pending_qty"] = _measure(df["pending_qty"])
    df["received_qty"] = df["ordered_qty"] - df["pending_qty"]

    df["category"] = None          # no category in the feed (category from product)
    return _finish_po(df, "BLINKIT", _BLINKIT_PO_STATUS)


_INSTAMART_PO_STATUS = {"CONFIRMED": "CONFIRMED", "COMPLETED": "RECEIVED", "EXPIRED": "EXPIRED",
                        "CANCELLED": "CANCELLED"}


def _split_category_path(path):
    """'Ice Cream and Indian Sweets, Ice Creams' -> ('ice cream and indian sweets', 'ice creams').
    Split on the LAST comma (an L1 name may itself contain one, e.g. 'Dairy, Bread and Eggs')."""
    if not isinstance(path, str) or not path.strip():
        return None, None
    if "," not in path:
        return None, path.strip().lower()
    l1, leaf = path.rsplit(",", 1)
    return l1.strip().lower() or None, leaf.strip().lower() or None


def clean_po_instamart_df(df: pd.DataFrame) -> pd.DataFrame:
    """Instamart PO rules (canonical columns from QcomPoModel.get_config_instamart)."""
    df = _strip_strings(df.copy())
    for col in ("po_date", "po_modified_at", "po_expiry_date"):
        df[col] = _dt(df[col])
    df["expected_delivery_date"] = _dt(df["expected_delivery_date"]).dt.date
    df["is_internal_po"] = df["internal_external"].map(
        lambda x: {"internal": 1, "external": 0}.get(str(x).strip().lower()) if isinstance(x, str) else None)

    parts = df["category_path"].map(_split_category_path)
    df["l1_category"] = parts.map(lambda p: p[0])
    df["category"] = parts.map(lambda p: p[1])
    return _finish_po(df, "INSTAMART", _INSTAMART_PO_STATUS)


_ZEPTO_PO_STATUS = {"PENDING_ACKNOWLEDGEMENT": "OPEN", "CONFIRMED": "CONFIRMED",
                    "ASN_CREATED": "CONFIRMED", "PENDING_GRN": "CONFIRMED",
                    "GRN_DONE": "RECEIVED", "COMPLETED": "RECEIVED",
                    "EXPIRED": "EXPIRED", "CANCELLED": "CANCELLED"}


def clean_po_zepto_df(df: pd.DataFrame) -> pd.DataFrame:
    """Zepto PO rules (canonical columns from QcomPoModel.get_config_zepto)."""
    df = _strip_strings(df.copy())
    # '05 Mar 2026 09:59 am'
    df["po_date"] = _dt(df["po_date"], fmt="%d %b %Y %I:%M %p")
    df["po_expiry_date"] = _dt(df["po_expiry_date"], fmt="%d %b %Y %I:%M %p")
    # SKU UUID is stored lowercase in product.ext_erp_record_id
    df["item_code"] = df["item_code"].map(lambda x: x.lower() if isinstance(x, str) else x)
    for col in ("product_ean", "product_hsn"):
        df[col] = df[col].map(lambda x: x[:-2] if isinstance(x, str) and x.endswith(".0") else x)

    df["ordered_qty"] = _measure(df["ordered_qty"])
    df["received_qty"] = _measure(df["received_qty"])           # GRN quantity
    df["pending_qty"] = df["ordered_qty"] - df["received_qty"]
    df["tax_pct"] = (_measure(df["cgst_pct"]).fillna(0) + _measure(df["sgst_pct"]).fillna(0)
                     + _measure(df["igst_pct"]).fillna(0))

    df["category"] = None          # no category in the feed (category from product)
    return _finish_po(df, "ZEPTO", _ZEPTO_PO_STATUS)


# file_type -> serializer
QCOM_CLEANERS = {
    FileType.QCOM_BLINKIT: clean_blinkit_df,
    FileType.QCOM_INSTAMART: clean_instamart_df,
    FileType.QCOM_ZEPTO: clean_zepto_df,
    FileType.QCOM_SOH_BLINKIT: clean_soh_blinkit_df,
    FileType.QCOM_SOH_INSTAMART: clean_soh_instamart_df,
    FileType.QCOM_SOH_ZEPTO: clean_soh_zepto_df,
    FileType.QCOM_PO_BLINKIT: clean_po_blinkit_df,
    FileType.QCOM_PO_INSTAMART: clean_po_instamart_df,
    FileType.QCOM_PO_ZEPTO: clean_po_zepto_df,
}

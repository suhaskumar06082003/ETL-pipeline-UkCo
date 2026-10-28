"""Column-wise (vectorized) cleaners — the retailverse serializer rules applied
per column in one pass, instead of row-by-row DRF serializers.

`clean_outlet_df` renames the file's headers to the serializer field names
(via the OutletModel config map) and cleans every column, returning a frame
ready for the load flow (lowercase source columns matching flow_config).
"""
import logging
import re
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal

import numpy as np
import pandas as pd

from config.file_config import OutletModel, ProductModel, SecondaryDumModel

logger = logging.getLogger("ETL_CLEAN")

# OutletDumpSerializer.STATUS_MAP
_STATUS_MAP = {
    "active": "Active", "no": "Active", "n": "Active", "false": "Active", "0": "Active",
    "inactive": "InActive", "in active": "InActive", "in-active": "InActive",
    "yes": "InActive", "y": "InActive", "true": "InActive", "1": "InActive",
}

# required (allow_null=False) outlet fields — rows missing these are dropped
_REQUIRED_NONNULL = ["outlet_erp_id", "outlets_name", "beats", "beaterp_id"]


def _to_decimal(series, places):
    num = pd.to_numeric(series, errors="coerce")
    return num.round(places)


def _fix_pincode_from_address(df: pd.DataFrame) -> pd.DataFrame:
    """The system-generated `formatted_address` holds the true pincode; the
    user-entered `pincode` may be wrong. Pull the 6-digit pincode from
    formatted_address (then address), falling back to the user's value.
    """
    extracted = None
    for col in ("formatted_address", "address"):
        if col in df.columns:
            ext = df[col].astype(str).str.extract(r"(\b\d{6}\b)")[0]
            extracted = ext if extracted is None else extracted.fillna(ext)
    if extracted is not None and "pincode" in df.columns:
        df["pincode"] = extracted.fillna(df["pincode"])  # keep user's if none found
    return df


# Offline map: local-language / variant state names -> canonical English (UPPER).
# Primary path (reliable, no network). Keys are matched as-is and lowercased.
_STATE_MAP = {
    # Devanagari (Hindi / Marathi)
    "महाराष्ट्र": "MAHARASHTRA", "कर्नाटक": "KARNATAKA", "तमिलनाडु": "TAMIL NADU",
    "तमिल नाडु": "TAMIL NADU", "तेलंगाना": "TELANGANA", "तेलंगाणा": "TELANGANA",
    "आंध्र प्रदेश": "ANDHRA PRADESH", "आन्ध्र प्रदेश": "ANDHRA PRADESH", "केरल": "KERALA",
    "गोवा": "GOA", "गुजरात": "GUJARAT", "राजस्थान": "RAJASTHAN", "उत्तर प्रदेश": "UTTAR PRADESH",
    "मध्य प्रदेश": "MADHYA PRADESH", "पश्चिम बंगाल": "WEST BENGAL", "बिहार": "BIHAR",
    "पंजाब": "PUNJAB", "हरियाणा": "HARYANA", "दिल्ली": "DELHI", "ओडिशा": "ODISHA",
    "उड़ीसा": "ODISHA", "झारखंड": "JHARKHAND", "छत्तीसगढ़": "CHHATTISGARH", "असम": "ASSAM",
    "उत्तराखंड": "UTTARAKHAND", "हिमाचल प्रदेश": "HIMACHAL PRADESH",
    "जम्मू और कश्मीर": "JAMMU AND KASHMIR",
    # Regional scripts (southern states + Bengal/Gujarat/Punjab/Odisha)
    "ಕರ್ನಾಟಕ": "KARNATAKA", "ತಮಿಳುನಾಡು": "TAMIL NADU", "தமிழ்நாடு": "TAMIL NADU",
    "కర్ణాటక": "KARNATAKA", "తెలంగాణ": "TELANGANA", "ఆంధ్రప్రదేశ్": "ANDHRA PRADESH",
    "ఆంధ్ర ప్రదేశ్": "ANDHRA PRADESH", "കേരളം": "KERALA", "കേരള": "KERALA",
    "ગુજરાત": "GUJARAT", "ਪੰਜਾਬ": "PUNJAB", "পশ্চিমবঙ্গ": "WEST BENGAL", "ওড়িশা": "ODISHA",
    "ଓଡ଼ିଶା": "ODISHA", "গোয়া": "GOA", "ಗೋವಾ": "GOA",
    # common English variants -> canonical (matched lowercased)
    "tamilnadu": "TAMIL NADU", "tamil nadu": "TAMIL NADU", "orissa": "ODISHA",
    "pondicherry": "PUDUCHERRY", "ncr": "DELHI", "new delhi": "DELHI",
}


def _normalize_states(series: pd.Series) -> pd.Series:
    """Local-language / variant state names -> canonical English, UPPERCASED.

    1) offline curated map (reliable);
    2) if a non-ASCII value isn't in the map, best-effort online translate (cached);
    3) otherwise just uppercase.
    """
    try:
        from deep_translator import GoogleTranslator
        translator = GoogleTranslator(source="auto", target="en")
    except Exception:
        translator = None

    cache: dict[str, str] = {}

    def conv(v):
        if not isinstance(v, str) or not v.strip():
            return None
        s = v.strip()
        if s in cache:
            return cache[s]
        if s in _STATE_MAP:
            out = _STATE_MAP[s]
        elif s.lower() in _STATE_MAP:
            out = _STATE_MAP[s.lower()]
        elif translator and any(ord(ch) > 127 for ch in s):  # unknown local script
            try:
                out = (translator.translate(s) or s)
            except Exception:
                out = s
        else:
            out = s
        out = out.strip().upper()
        cache[s] = out
        return out

    return series.map(conv)


def clean_outlet_df(df: pd.DataFrame) -> pd.DataFrame:
    """Rename to serializer field names + clean column-wise. Returns cleaned df."""
    rename_map = {src: rules["rename"] for src, rules in OutletModel.get_config_feild_assist().items()}
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # strip all object/string columns
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
            df[col] = df[col].replace({"": None, "nan": None, "None": None, "NULL": None, "null": None})

    # formatted_address fallback from address (needed before pincode extraction)
    if "formatted_address" in df.columns:
        if "address" in df.columns:
            df["formatted_address"] = df["formatted_address"].fillna(df["address"])
    elif "address" in df.columns:
        df["formatted_address"] = df["address"]

    # pincode -> take the real one from formatted_address / address
    df = _fix_pincode_from_address(df)

    # state -> translate to English (if local language) + UPPERCASE
    if "state" in df.columns:
        df["state"] = _normalize_states(df["state"])

    # lat / long -> 14 dp
    for col in ("latitude", "longitude"):
        if col in df.columns:
            df[col] = _to_decimal(df[col], 14)

    # integer-ish fields
    for col in ("owners_number", "pincode"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # creation / deactivation dates -> datetime (empty or unparseable -> NULL)
    for col in ("outlet_creation_date", "outlet_deactivation_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # status normalization (drop rows with unknown status)
    if "status" in df.columns:
        key = df["status"].apply(lambda x: str(x).strip().lower() if pd.notnull(x) else x)
        df["status"] = key.map(_STATUS_MAP)

    # drop rows missing required non-null fields
    present_required = [c for c in _REQUIRED_NONNULL if c in df.columns]
    if present_required:
        before = len(df)
        df = df.dropna(subset=present_required)
        dropped = before - len(df)
        if dropped:
            logger.info("----> [CLEAN outlet] dropped %s rows missing required fields", dropped)

    return df.reset_index(drop=True)


def _rename(df, model):
    rename_map = {src: rules["rename"] for src, rules in model.get_config_feild_assist().items()}
    return df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})


def _strip_strings(df):
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
            df[col] = df[col].replace({"": None, "nan": None, "None": None, "NULL": None, "null": None})
    return df


# product_status is an IsActive-style boolean in the file -> convert to the
# enum('Active','InActive'). truthy -> Active, falsy -> InActive.
_PRODUCT_STATUS_TRUE = {"true", "t", "yes", "y", "1", "active"}
_PRODUCT_STATUS_FALSE = {"false", "f", "no", "n", "0", "inactive"}


def _product_status(x):
    """Boolean-ish IsActive (ANY case/representation) -> enum('Active','InActive').

    Handles: True/TRUE/true, False/FALSE/false, Yes/No, Y/N, 1/0, 1.0/0.0,
    Active/InActive/ACTIVE/inactive, with surrounding whitespace. NOT NULL
    DEFAULT 'Active' -> anything unknown/blank/null becomes 'Active'.
    """
    if pd.isna(x):
        return "Active"
    s = str(x).strip().lower()
    if s.endswith(".0"):          # float-typed cells: "1.0"/"0.0"
        s = s[:-2]
    if s in _PRODUCT_STATUS_FALSE:
        return "InActive"
    if s in _PRODUCT_STATUS_TRUE:
        return "Active"
    return "Active"  # NOT NULL DEFAULT 'Active' — unknown/blank defaults to Active


def clean_product_df(df: pd.DataFrame) -> pd.DataFrame:
    """Column-wise clean for the PRODUCT master (vectorized ProductDumpSerializer)."""
    df = _strip_strings(_rename(df, ProductModel))
    for col in ("erp_id", "skus_code"):
        if col in df.columns:
            df[col] = df[col].apply(lambda x: x.upper() if isinstance(x, str) else x)

    # New product file has no separate SKU column -> create skus_code from
    # erp_id during serialization (mirror erp_id when skus_code is absent/blank).
    if "erp_id" in df.columns:
        if "skus_code" in df.columns:
            df["skus_code"] = df["skus_code"].where(df["skus_code"].notna(), df["erp_id"])
        else:
            df["skus_code"] = df["erp_id"]

    # ptr -> offer_price, mrp -> price. Both are NOT NULL DEFAULT 0 in the DB,
    # so a missing value becomes 0.000000 (an explicit NULL would be rejected).
    for col in ("ptr", "mrp"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").round(6).fillna(0)

    # product_status: IsActive true/false -> enum('Active','InActive'), default Active
    if "product_status" in df.columns:
        df["product_status"] = df["product_status"].apply(_product_status)
    else:
        df["product_status"] = "Active"

    if "product_name" in df.columns:
        df = df.dropna(subset=["product_name"])
    return df.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────
# SECONDARY DUMP — column-wise port of retailverse, in the same order:
#   _apply_schema (cast by file-config datatype)
#   -> DataPreprocessor.clean_record_fields (null-likes -> None, floats -> 3 dp)
#   -> SecondarySalesDumpSerializer.to_internal_value (pre-processing)
#   -> field validation + validate()  (invalid rows are REJECTED, not loaded)
# ─────────────────────────────────────────────────────────────
_SEC_NULL_LIKE = {"", "none", "nan", "nat", "<na>"}          # clean_record_fields
# serializer CharField max_length per field
_SEC_MAXLEN = {
    **dict.fromkeys(["l5position", "l4position", "l3position", "l2position", "user_position_name",
                     "l5position_user", "l4position_user", "l3position_user", "l2position_user", "user",
                     "outlet_name", "owners_name", "outlet_type", "outlet_channel_name", "state", "market",
                     "superstockist", "distributor", "product_division", "primary_category",
                     "secondary_category", "product_name", "beat", "packaging_type",
                     "no_sales_reason_category", "no_sales_reason", "zone", "region", "territory"], 255),
    **dict.fromkeys(["order_no", "address", "distributor_state"], 100),
    **dict.fromkeys(["esm_erp_id", "outlet_erp_id", "outlet_gstin", "superstockist_erp_id",
                     "distributor_erp_id", "distributor_gstin", "beaterp_id", "super_unit", "std_unit",
                     "unit"], 50),
    "city": 45, "product_erp_id": 20, "skus_code": 20, "owners_no": 10,
}
# allow_null=False (a blank already became None -> rejected)
_SEC_NOT_NULL = ["order_no", "order_date", "outlet_erp_id", "outlet_name", "state", "beaterp_id"]
# DecimalField(max_digits=18, decimal_places=6) -> at most 12 integer digits
_SEC_DEC_18_6 = ["qty_superunit", "qty_stdunit", "standard_unit_conversion_factor", "gross_qty", "qty_unit",
                 "product_mrp", "price", "actual_price", "stdunit_price", "gross_value", "sale_value",
                 "cgst", "sgst", "igst", "net_value", "net_value_tax_inclusive", "mrp"]
# DRF BooleanField accepted spellings (exact, like DRF)
_DRF_TRUE = {"t", "T", "y", "Y", "yes", "Yes", "YES", "true", "True", "TRUE", "on", "On", "ON", "1"}
_DRF_FALSE = {"f", "F", "n", "N", "no", "No", "NO", "false", "False", "FALSE", "off", "Off", "OFF", "0"}
_DRF_NULL = {"null", "Null", "NULL", ""}
# preprocess_datetime: explicit formats first, pandas fallback
_SEC_DT_FORMATS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M", "%Y-%m-%d",
                   "%d-%m-%Y %H:%M:%S", "%d/%m/%Y %H:%M:%S"]


def _q3(v):
    """clean_record_fields: float -> Decimal(v).quantize(0.001, ROUND_HALF_UP)."""
    return None if pd.isna(v) else float(Decimal(float(v)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP))


def _sec_datetime(v):
    if v is None:
        return None
    for fmt in _SEC_DT_FORMATS:
        try:
            return datetime.strptime(v, fmt)
        except (ValueError, TypeError):
            continue
    ts = pd.to_datetime(v, errors="coerce")
    return None if pd.isna(ts) else ts.to_pydatetime()


def _drf_bool(v):
    """-> (value, valid)."""
    if v is None or v in _DRF_NULL:
        return None, True
    if v in _DRF_TRUE:
        return 1, True
    if v in _DRF_FALSE:
        return 0, True
    return None, False


def clean_secondary_df(df: pd.DataFrame) -> pd.DataFrame:
    """SECONDARY dump — same rules as retailverse (schema cast -> clean_record_fields
    -> SecondarySalesDumpSerializer). Rows the reference serializer rejects are
    dropped here (prepare_load counts them as invalid)."""
    cfg = SecondaryDumModel.get_config_feild_assist()
    dtypes = {rules["rename"]: rules.get("datatype", "str") for rules in cfg.values()}
    df = _rename(df, SecondaryDumModel).copy()
    for col in dtypes:                                    # missing column -> None (like _apply_schema)
        if col not in df.columns:
            df[col] = None

    # 1) _apply_schema casts + 2) clean_record_fields
    for col, dt in dtypes.items():
        s = df[col]
        if dt == "float":
            df[col] = pd.to_numeric(s, errors="coerce").map(_q3).astype(float)   # NaN -> NULL at insert
        elif dt == "int":
            n = pd.to_numeric(s, errors="coerce")
            df[col] = n.where(n.notna() & (n % 1 == 0)).astype("Int64").astype(object).where(n.notna() & (n % 1 == 0), None)
        else:                                             # str / date / datetime / bool -> text
            # astype(str).strip() then null-likes -> None, as plain python objects
            # (same result on pandas 2 and 3)
            df[col] = pd.Series(
                [None if (v is None or (isinstance(v, float) and pd.isna(v)) or
                          str(v).strip().lower() in _SEC_NULL_LIKE) else str(v).strip()
                 for v in s.tolist()],
                index=df.index, dtype=object)

    reasons = pd.Series("", index=df.index)

    def reject(mask, why):
        nonlocal reasons
        mask = mask.fillna(False) & (reasons == "")
        reasons = reasons.mask(mask, why)

    # 3) to_internal_value pre-processing
    df["address"] = df["address"].map(lambda x: x[:100] if x else x)
    df["city"] = df["city"].map(lambda x: x[:45] if x else x)
    for col in ("call_start_time", "call_end_time"):
        df[col] = df[col].map(_sec_datetime)
    df["product_erp_id"] = df["product_erp_id"].map(lambda x: str(x).strip().replace(".0", "") if x not in (None, "") else None)
    if "skus_code" not in df.columns:
        df["skus_code"] = None                            # SKUs_Code is not in the file config
    df["skus_code"] = df["skus_code"].where(df["skus_code"].notna(), df["product_erp_id"])
    df["owners_no"] = df["owners_no"].map(
        lambda x: (m.group(1) if (m := re.search(r"(\d{10})", str(x))) else None) if x not in (None, "") else None)
    df["order_no"] = df["order_no"].map(lambda x: None if x is None else str(int(x)))    # CharField of the int
    for col in ("state", "distributor_state"):
        df[col] = df[col].map(lambda x: x.upper() if isinstance(x, str) else x)

    # 4) field validation (DRF) — reasons recorded for the log
    for col in _SEC_NOT_NULL:
        reject(df[col].isna(), f"{col} is null")
    od = pd.to_datetime(df["order_date"], errors="coerce", format="ISO8601")   # DateTimeField: ISO-8601 only
    reject(df["order_date"].notna() & od.isna(), "order_date not ISO-8601")
    reject(od.dt.date > date.today(), "order_date in the future")
    df["order_date"] = od.map(lambda t: None if pd.isna(t) else t.to_pydatetime())
    for col, n in _SEC_MAXLEN.items():
        if col in df.columns:
            reject(df[col].map(lambda x: isinstance(x, str) and len(x) > n), f"{col} longer than {n}")
    for col in _SEC_DEC_18_6:
        reject(pd.to_numeric(df[col], errors="coerce").abs() >= 1e12, f"{col} exceeds 18,6")
    for col in ("visit_latitude", "visit_longitude"):           # DecimalField(19,17): 2 integer digits
        reject(pd.to_numeric(df[col], errors="coerce").abs() >= 100, f"{col} exceeds 19,17")
    # RoundedIntegerField: round(float(x)) (Python round = half-to-even, same as numpy)
    dev = pd.to_numeric(df["deviation_from_outlet"], errors="coerce")
    df["deviation_from_outlet"] = dev.round().astype("Int64").astype(object).where(dev.notna(), None)
    tel = df["is_telephonic"].map(_drf_bool)
    reject(~tel.map(lambda t: t[1]), "is_telephonic not a boolean")
    df["is_telephonic"] = tel.map(lambda t: t[0])
    # validate(): a product name needs its ERP id
    reject(df["product_name"].notna() & df["product_erp_id"].isna(), "product_name without product_erp_id")

    bad = reasons != ""
    if bad.any():
        logger.info("----> [CLEAN secondary] rejected %s of %s rows: %s", int(bad.sum()), len(df),
                    reasons[bad].value_counts().to_dict())
    return df[~bad].reset_index(drop=True)

"""Q-commerce ETL flow config (pbisalesanalysisdb source table -> hangyodb).

Same flow-engine format as the file-upload flows (config.flow_config). Every
column name here is a CANONICAL name — the source columns were renamed ONCE by
the file config (config.file_config.QcomSalesModel / QcomStockModel via
config_validation_service.apply_schema), so this file never sees a source name.

Task order per platform (sales and stock on hand alike):
    sales:          category -> product -> qcom_sales
    stock on hand:  category -> product -> qcom_stock_on_hand
    purchase order: category -> product -> qcom_po

Product names: the STOCK-ON-HAND spelling wins. Sales and PO only insert NEW
item codes (update_name=False); stock on hand inserts new codes and renames
existing ones to its latest name (update_name=True).
"""
from config.etl_enums import ErpRecCode, FileType

# audit columns present on every qcom target table (canonical col -> dest col)
_QCOM_REQ0 = {"client_id": "client_id", "ext_erp_id": "ext_erp_id",
              "created_by": "created_by", "updated_by": "updated_by"}


# CATEGORY steps (engine load_table: anti-join on category_name per ext_erp_id,
# so only NEW categories are inserted).
def _category_flat() -> dict:
    """Single-level category (`category`)."""
    return {
        "handler": "category", "table_name": "category", "mode": "NORMAL", "forward_ext_erp_id": True,
        "on_update": ["category_name"], "on_update_filters": ["ext_erp_id"],
        "flow": [{
            "key_columns": ["category"], "check_cols": ["category"], "drop_cols": [], "condition_col": [],
            "required_columns": {**_QCOM_REQ0, "category": "category_name"},
            "parent_columns": [],
        }],
    }


def _category_2_level() -> dict:
    """2-level category tree: l1_category > category (leaf)."""
    return {
        "handler": "category", "table_name": "category", "mode": "NORMAL", "forward_ext_erp_id": True,
        "on_update": ["category_name"], "on_update_filters": ["ext_erp_id"],
        "flow": [
            {"key_columns": ["l1_category"], "check_cols": ["l1_category"], "drop_cols": [], "condition_col": [],
             "required_columns": {**_QCOM_REQ0, "l1_category": "category_name"}, "parent_columns": []},
            {"key_columns": ["category"], "check_cols": ["category"], "drop_cols": ["l1_category"],
             "condition_col": ["l1_category", "category"],
             "required_columns": {**_QCOM_REQ0, "category": "category_name", "l1_category": ""},
             "parent_columns": [{"col_name": "l1_category", "map_col_name": "parent_category_id",
                                 "DB_column": "category_name", "table": "category", "id_col": "category_id"}]},
        ],
    }


# PRODUCT — handled by the loader's upsert (engine.services.qcom_loader_service
# ._upsert_products), which reads its columns from THIS step: the column mapped
# to ext_erp_record_id is the key (item_code), the one mapped to product_name is
# the name, the category_id parent gives the leaf category column, and any other
# mapped column (e.g. ean -> bar_code) is written through as-is.
#   update_name      True  -> an existing item_code is renamed to this feed's name
#                    False -> existing names are left alone (new codes only)
#   update_category  "always"     -> existing product.category_id := this feed's leaf
#                    "if_missing" -> only filled when the product has none
def _qcom_product(extra_required: dict | None = None, *, update_name: bool,
                  update_category: str) -> dict:
    return {
        "handler": "product", "table_name": "product", "mode": "NORMAL", "forward_ext_erp_id": True,
        "on_update": ["ext_erp_record_id"], "on_update_filters": ["ext_erp_id"],
        "update_name": update_name, "update_category": update_category,
        "flow": [{
            "key_columns": ["item_code"], "check_cols": [], "drop_cols": ["category"], "condition_col": [],
            "required_columns": {**_QCOM_REQ0, "item_code": ["ext_erp_record_id", "sku_code"], "product_name": "product_name", "category": "", **(extra_required or {})},
            "parent_columns": [{"col_name": "category", "map_col_name": "category_id","DB_column": "category_name", "table": "category", "id_col": "category_id"}],
        }],
    }


# sales: new item codes only — the stock-on-hand feed owns product names; sales
# still owns the product's leaf category (Instamart L3).
def _sales_product(extra_required: dict | None = None) -> dict:
    return _qcom_product(extra_required, update_name=False, update_category="always")


# stock on hand: its latest name wins; the category is only filled when missing
# (its Instamart leaf is L2 — never overwrite the sales L3).
def _stock_product(extra_required: dict | None = None) -> dict:
    return _qcom_product(extra_required, update_name=True, update_category="if_missing")


def _qcom_sales(extra_required: dict) -> dict:
    """QCOM_SALES — fact; ALWAYS UPSERT on rec_code (= source id, UNIQUE).
    qcom_sales_id is an AUTO_INCREMENT PK — the ETL never writes it."""
    return {
        "handler": "qcom_sales", "table_name": "qcom_sales", "mode": "NORMAL", "forward_ext_erp_id": True,
        "filter_nulls": "key_columns", "on_update": ["rec_code"], "on_update_filters": [],
        "flow": [{
            "key_columns": ["sales_id"], "check_cols": [], "drop_cols": ["item_code"], "condition_col": [],
            "required_columns": {
                **_QCOM_REQ0,
                "sales_id": "rec_code", "platform": "platform", "order_date": "order_date",
                "item_code": "",                                   # product_id lookup only
                "product_name": "product_name", "category": "category_name",
                "city_name": "city_name", "transact_uom_id": "transact_uom_id",
                "store_id": "store_id", "is_combo": "is_combo", "combo_item_code": "combo_item_code",
                "gross_qty": "gross_qty", "gross_value": "gross_value", "product_mrp": "product_mrp",
                **extra_required,
            },
            "parent_columns": [
                {"col_name": "item_code", "map_col_name": "product_id",
                 "DB_column": "ext_erp_record_id", "table": "product", "id_col": "product_id"},
                {"col_name": "category", "map_col_name": "category_id",
                 "DB_column": "category_name", "table": "category", "id_col": "category_id"},
            ],
        }],
    }


# ── BLINKIT ── flat single-level category
QCOM_BLINKIT_FLOW = [
    _category_flat(),
    _sales_product(),
    _qcom_sales({"brand_name": "brand_name", "manufacturer_name": "manufacturer_name",
                 "manufacturer_erp_id": "manufacturer_ext_erp_record_id"}),
]


# ── INSTAMART ── 3-level category hierarchy L1 > L2 > L3 (leaf = `category`)
QCOM_INSTAMART_FLOW = [
    {
        "handler": "category", "table_name": "category", "mode": "NORMAL", "forward_ext_erp_id": True,
        "on_update": ["category_name"], "on_update_filters": ["ext_erp_id"],
        "flow": [
            {"key_columns": ["l1_category"], "check_cols": ["l1_category"], "drop_cols": [], "condition_col": [],
             "required_columns": {**_QCOM_REQ0, "l1_category": "category_name"}, "parent_columns": []},
            {"key_columns": ["l2_category"], "check_cols": ["l2_category"], "drop_cols": ["l1_category"],
             "condition_col": ["l1_category", "l2_category"],
             "required_columns": {**_QCOM_REQ0, "l2_category": "category_name", "l1_category": ""},
             "parent_columns": [{"col_name": "l1_category", "map_col_name": "parent_category_id",
                                 "DB_column": "category_name", "table": "category", "id_col": "category_id"}]},
            {"key_columns": ["category"], "check_cols": ["category"], "drop_cols": ["l2_category"],
             "condition_col": ["l2_category", "category"],
             "required_columns": {**_QCOM_REQ0, "category": "category_name", "l2_category": ""},
             "parent_columns": [{"col_name": "l2_category", "map_col_name": "parent_category_id",
                                 "DB_column": "category_name", "table": "category", "id_col": "category_id"}]},
        ],
    },
    _sales_product(),
    _qcom_sales({"brand_name": "brand_name", "area_name": "area_name",
                 "combo_units_sold": "combo_units_sold"}),
]


# ── ZEPTO ── 2-level category: SKUCategory (l1_category) > SKUSubCategory (leaf
# `category`, the one product/sale point at). SKUNumber is the product key; its
# EAN goes to product.bar_code.
QCOM_ZEPTO_FLOW = [
    _category_2_level(),
    _sales_product({"ean": "bar_code"}),
    _qcom_sales({"brand_name": "brand_name", "manufacturer_name": "manufacturer_name",
                 "manufacturer_erp_id": "manufacturer_ext_erp_record_id"}),
]


# ─────────────────────────────────────────────────────────────
# STOCK ON HAND — category -> product -> qcom_stock_on_hand, like sales.
# ─────────────────────────────────────────────────────────────
def _qcom_stock(extra_required: dict, extra_parents: list | None = None) -> dict:
    """QCOM_STOCK_ON_HAND — snapshot fact; ALWAYS UPSERT on rec_code (= source id,
    UNIQUE). qcom_stock_id is an AUTO_INCREMENT PK — the ETL never writes it."""
    return {
        "handler": "qcom_stock_on_hand", "table_name": "qcom_stock_on_hand", "mode": "NORMAL",
        "forward_ext_erp_id": True,
        "filter_nulls": "key_columns", "on_update": ["rec_code"], "on_update_filters": [],
        "flow": [{
            "key_columns": ["stock_id"], "check_cols": [], "drop_cols": ["item_code"], "condition_col": [],
            "required_columns": {
                **_QCOM_REQ0,
                "stock_id": "rec_code", "platform": "platform", "snapshot_date": "snapshot_date",
                "item_code": "",                                   # product_id lookup only
                "product_name": "product_name", "city_name": "city_name",
                "transact_uom_id": "transact_uom_id", "qty_on_hand": "qty_on_hand",
                **extra_required,
            },
            "parent_columns": [
                {"col_name": "item_code", "map_col_name": "product_id",
                 "DB_column": "ext_erp_record_id", "table": "product", "id_col": "product_id"},
                *(extra_parents or []),
            ],
        }],
    }


# category id of the stock row from its leaf `category` name
_STOCK_CATEGORY_PARENT = {"col_name": "category", "map_col_name": "category_id",
                          "DB_column": "category_name", "table": "category", "id_col": "category_id"}


# ── BLINKIT SOH ── no category in the source: the category task has nothing to
# add, and the stock row takes the product's category
# (item_code -> product.category_id -> category.category_name, run in order).
QCOM_SOH_BLINKIT_FLOW = [
    _category_flat(),
    _stock_product(),
    _qcom_stock(
        {"facility_erp_id": "facility_ext_erp_record_id", "facility_name": "facility_name",
         "qty_backend": "qty_backend", "qty_frontend": "qty_frontend"},
        extra_parents=[
            {"col_name": "item_code", "map_col_name": "category_id",
             "DB_column": "ext_erp_record_id", "table": "product", "id_col": "category_id"},
            {"col_name": "category_id", "map_col_name": "category_name",
             "DB_column": "category_id", "table": "category", "id_col": "category_name"},
        ],
    ),
]


# ── INSTAMART SOH ── 2-level category L1 > L2 (leaf = `category`), lowercased by
# the serializer to the spelling the Instamart sales feed / category table use.
QCOM_SOH_INSTAMART_FLOW = [
    _category_2_level(),
    _stock_product(),
    _qcom_stock(
        {"category": "category_name",
         "facility_name": "facility_name", "storage_type": "storage_type",
         "qty_backend": "qty_backend", "days_on_hand": "days_on_hand",
         "shelf_life_days": "shelf_life_days", "potential_gmv_loss": "potential_gmv_loss",
         "open_po_qty": "open_po_qty", "open_po_refs": "open_po_refs"},
        extra_parents=[_STOCK_CATEGORY_PARENT],
    ),
]


# ── ZEPTO SOH ── 2-level category SKU_Category > SKU_Sub_Category (leaf), EAN ->
# product.bar_code — same as Zepto sales.
QCOM_SOH_ZEPTO_FLOW = [
    _category_2_level(),
    _stock_product({"ean": "bar_code"}),
    _qcom_stock(
        {"category": "category_name", "brand_name": "brand_name",
         "manufacturer_name": "manufacturer_name",
         "manufacturer_erp_id": "manufacturer_ext_erp_record_id"},
        extra_parents=[_STOCK_CATEGORY_PARENT],
    ),
]


# ─────────────────────────────────────────────────────────────
# PURCHASE ORDERS — category -> product -> qcom_po, like sales / stock on hand.
# ─────────────────────────────────────────────────────────────
# PO: new item codes only (stock-on-hand names win); category only filled when
# the product has none.
def _po_product(extra_required: dict | None = None) -> dict:
    return _qcom_product(extra_required, update_name=False, update_category="if_missing")


# qcom_po columns that are NOT NULL DEFAULT '' — the engine turns '' into NULL,
# so these are written back as '' when the source has no value.
_PO_BLANK_NOT_NULL = ["po_status_raw", "otb_reference_number", "reference_po_number", "po_created_by",
                      "vendor_code", "product_hsn", "pack_size_text", "facility_ext_erp_record_id",
                      "facility_name"]


def _qcom_po(extra_required: dict, extra_parents: list | None = None,
             extra_drop: list | None = None) -> dict:
    """QCOM_PO — PO line snapshot fact; ALWAYS UPSERT on rec_code (= source id, UNIQUE);
    qcom_po_id is an AUTO_INCREMENT PK — the ETL never writes it.
    item_code is stored as product_ext_erp_record_id AND resolves product_id."""
    return {
        "handler": "qcom_po", "table_name": "qcom_po", "mode": "NORMAL", "forward_ext_erp_id": True,
        "filter_nulls": "key_columns", "on_update": ["rec_code"], "on_update_filters": [],
        "blank_not_null": _PO_BLANK_NOT_NULL,
        "flow": [{
            "key_columns": ["po_id"], "check_cols": [], "drop_cols": [*(extra_drop or [])], "condition_col": [],
            "required_columns": {
                **_QCOM_REQ0,
                "po_id": "rec_code", "platform": "platform", "snapshot_date": "snapshot_date",
                "po_number": "po_number", "po_date": "po_date", "po_expiry_date": "po_expiry_date",
                "po_status_raw": "po_status_raw", "po_status": "po_status",
                "vendor_name": "vendor_name",
                "item_code": "product_ext_erp_record_id", "product_name": "product_name",
                "city_name": "city_name", "facility_name": "facility_name",
                "transact_uom_id": "transact_uom_id",
                "ordered_qty": "ordered_qty", "received_qty": "received_qty", "pending_qty": "pending_qty",
                "unit_base_cost": "unit_base_cost", "product_mrp": "product_mrp",
                "line_value_with_tax": "line_value_with_tax",
                **extra_required,
            },
            "parent_columns": [
                {"col_name": "item_code", "map_col_name": "product_id",
                 "DB_column": "ext_erp_record_id", "table": "product", "id_col": "product_id"},
                *(extra_parents or []),
            ],
        }],
    }


# no category in the Blinkit / Zepto PO feeds -> the PO line takes the
# product's category_id (category_name stays empty, as in the stored rows)
_PO_CATEGORY_FROM_PRODUCT = {"col_name": "item_code", "map_col_name": "category_id",
                             "DB_column": "ext_erp_record_id", "table": "product", "id_col": "category_id"}


# ── BLINKIT PO ── city from the facility name; category from the product.
QCOM_PO_BLINKIT_FLOW = [
    _category_flat(),
    _po_product(),
    _qcom_po(
        {"entity_name": "entity_name", "manufacturer_name": "manufacturer_name",
         "product_ean": "product_ean", "pack_size_text": "pack_size_text",
         "unit_landing_cost": "unit_landing_cost", "margin_percentage": "margin_percentage",
         "tax_pct": "tax_pct", "cgst_pct": "cgst_pct", "sgst_pct": "sgst_pct",
         "igst_pct": "igst_pct", "cess_pct": "cess_pct"},
        extra_parents=[_PO_CATEGORY_FROM_PRODUCT],
    ),
]


# ── INSTAMART PO ── 2-level category from the path "L1, L2" (leaf = `category`,
# lowercased to the category-table spelling); category_name keeps the raw path.
QCOM_PO_INSTAMART_FLOW = [
    _category_2_level(),
    _po_product(),
    _qcom_po(
        {"po_modified_at": "po_modified_at", "expected_delivery_date": "expected_delivery_date",
         "po_ageing_days": "po_ageing_days", "is_internal_po": "is_internal_po",
         "otb_reference_number": "otb_reference_number", "reference_po_number": "reference_po_number",
         "vendor_code": "vendor_code", "entity_name": "entity_name",
         "facility_erp_id": "facility_ext_erp_record_id", "brand_name": "brand_name",
         "category": "", "category_path": "category_name",
         "line_value_without_tax": "line_value_without_tax", "po_header_amount": "po_header_amount",
         "tax_amount": "tax_amount"},
        extra_parents=[_STOCK_CATEGORY_PARENT],
        extra_drop=["category"],
    ),
]


# ── ZEPTO PO ── city from the delivery location; category from the product.
QCOM_PO_ZEPTO_FLOW = [
    _category_flat(),
    _po_product(),
    _qcom_po(
        {"po_line_no": "po_line_no", "po_created_by": "po_created_by", "vendor_code": "vendor_code",
         "brand_name": "brand_name", "product_ean": "product_ean", "product_hsn": "product_hsn",
         "asn_qty": "asn_qty", "unit_landing_cost": "unit_landing_cost",
         "po_header_amount": "po_header_amount", "tax_pct": "tax_pct",
         "cgst_pct": "cgst_pct", "sgst_pct": "sgst_pct", "igst_pct": "igst_pct", "cess_pct": "cess_pct"},
        extra_parents=[_PO_CATEGORY_FROM_PRODUCT],
    ),
]


# Registry keyed by qcom file_type.
QCOM_FLOWS = {
    FileType.QCOM_BLINKIT: QCOM_BLINKIT_FLOW,
    FileType.QCOM_INSTAMART: QCOM_INSTAMART_FLOW,
    FileType.QCOM_ZEPTO: QCOM_ZEPTO_FLOW,
    FileType.QCOM_SOH_BLINKIT: QCOM_SOH_BLINKIT_FLOW,
    FileType.QCOM_SOH_INSTAMART: QCOM_SOH_INSTAMART_FLOW,
    FileType.QCOM_SOH_ZEPTO: QCOM_SOH_ZEPTO_FLOW,
    FileType.QCOM_PO_BLINKIT: QCOM_PO_BLINKIT_FLOW,
    FileType.QCOM_PO_INSTAMART: QCOM_PO_INSTAMART_FLOW,
    FileType.QCOM_PO_ZEPTO: QCOM_PO_ZEPTO_FLOW,
}

# Source table + platform + masterdb ERP rec code per qcom file_type. client_id
# and ext_erp_id are resolved from masterdb (CoreDataValidatorService), exactly
# like the file-upload ETL — never hardcoded.
QCOM_SOURCES = {
    FileType.QCOM_BLINKIT: {"source_table": "Hangyo_Blinkit_Overall_Sales", "platform": "BLINKIT",
                            "ext_erp_rec_code": ErpRecCode.BLINKIT_HANGYO},
    FileType.QCOM_INSTAMART: {"source_table": "Hangyo_Instamart_Overall_Sales", "platform": "INSTAMART",
                              "ext_erp_rec_code": ErpRecCode.INSTAMART_HANGYO},
    FileType.QCOM_ZEPTO: {"source_table": "hangyo_Zepto_sales", "platform": "ZEPTO",
                          "ext_erp_rec_code": ErpRecCode.ZEPTO_HANGYO},
    # stock on hand -> hangyodb.qcom_stock_on_hand
    FileType.QCOM_SOH_BLINKIT: {"source_table": "hangyo_blinkit_stock_on_hand", "platform": "BLINKIT",
                                "ext_erp_rec_code": ErpRecCode.BLINKIT_HANGYO},
    FileType.QCOM_SOH_INSTAMART: {"source_table": "hangyo_instamart_stock_on_hand", "platform": "INSTAMART",
                                  "ext_erp_rec_code": ErpRecCode.INSTAMART_HANGYO},
    FileType.QCOM_SOH_ZEPTO: {"source_table": "hangyo_zepto_vendor_inventory", "platform": "ZEPTO",
                              "ext_erp_rec_code": ErpRecCode.ZEPTO_HANGYO},
    # purchase orders -> hangyodb.qcom_po
    FileType.QCOM_PO_BLINKIT: {"source_table": "hangyo_blinkit_po", "platform": "BLINKIT",
                               "ext_erp_rec_code": ErpRecCode.BLINKIT_HANGYO},
    FileType.QCOM_PO_INSTAMART: {"source_table": "hangyo_instamart_po", "platform": "INSTAMART",
                                 "ext_erp_rec_code": ErpRecCode.INSTAMART_HANGYO},
    FileType.QCOM_PO_ZEPTO: {"source_table": "hangyo_zepto_po", "platform": "ZEPTO",
                             "ext_erp_rec_code": ErpRecCode.ZEPTO_HANGYO},
}


def get_qcom_flow(file_type: str) -> list:
    if file_type not in QCOM_FLOWS:
        raise KeyError(f"No qcom flow configured for '{file_type}'.")
    return QCOM_FLOWS[file_type]

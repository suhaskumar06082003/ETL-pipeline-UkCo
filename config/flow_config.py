"""ETL flow configuration — ported verbatim from retailverse flow_config.py.

Defines, per file type, the ordered list of target tables and, per table, the
flow steps (source->dest column maps, key/check/drop columns, parent-id
lookups). The load engine walks these in order.

Handler metadata captured alongside each table:
  mode              "NORMAL" (master dedupe) or "JUNC" (junction/transaction)
  forward_ext_erp_id  only category & product filter lookups/dedupe by ext_erp_id
  extra             table-specific post/pre step (order_status / beat_outlet_status
                    / secondary_item / uom_dimension), else None
  filter_nulls      drop rows where ALL these cols are null (before processing)
  check_notnull     keep only rows where this col is not null
"""

# ── OUTLET flow (FileType.OUTLETDUMP) ──
# order: outlet -> position -> distributor -> salesman -> beat -> beat_outlet
OUTLET_FLOW = [
    {
        "handler": "outlet", "table_name": "outlet", "mode": "NORMAL",
        "forward_ext_erp_id": False,
        "on_update": ["ext_erp_record_id"], "on_update_filters": ["ext_erp_id"],
        "flow": [{
            "key_columns": ["outlet_erp_id"], "check_cols": [], "drop_cols": [], "condition_col": [],
            "required_columns": {
                "client_id": "client_id", "ext_erp_id": "ext_erp_id", "created_by": "created_by",
                "updated_by": "updated_by", "formatted_address": "street_address",
                "zone": "zone", "region": "region",
                "territory": "territory", "outlets_name": "outlet_name",
                "outlet_erp_id": "ext_erp_record_id", "latitude": "lat", "longitude": "longi",
                "subcity": "city", "state": "state", "pincode": "pin_code", "image": "outlet_image",
                "gstin": "gst_no", "owners_name": "owner_name", "owners_number": "owner_contact_no",
                "outlets_type": "outlet_type", "outlets_channel": "channel_type", "status": "status",
                "outlet_creation_date": "outlet_creation_date",
                "outlet_deactivation_date": "outlet_deactivation_date",
            },
            "parent_columns": [
                {"col_name": "pincode", "map_col_name": "pincode_id", "DB_column": "pincode", "table": "mpincode_office", "id_col": "pincode_id"},
                {"col_name": "pincode", "map_col_name": "district_id", "DB_column": "pincode", "table": "mpincode_office", "id_col": "district_id"},
            ],
        }],
    },
    {
        "handler": "position", "table_name": "position", "mode": "NORMAL",
        "forward_ext_erp_id": False,
        "on_update": ["position_name"], "on_update_filters": [],
        "flow": [
            {"key_columns": ["l5_position_name"], "check_cols": ["l5_position_name"], "drop_cols": [], "condition_col": [],
             "required_columns": {"created_by": "created_by", "updated_by": "updated_by", "l5_position_name": "position_name"},
             "parent_columns": []},
            {"key_columns": ["l4_position_name"], "check_cols": ["l4_position_name", "parent_position_id"], "drop_cols": ["l5_position_name"], "condition_col": ["l5_position_name", "l4_position_name"],
             "required_columns": {"created_by": "created_by", "updated_by": "updated_by", "l4_position_name": "position_name", "l5_position_name": ""},
             "parent_columns": [{"col_name": "l5_position_name", "map_col_name": "parent_position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}]},
            {"key_columns": ["l3_position_name"], "check_cols": ["l3_position_name", "parent_position_id"], "drop_cols": ["l4_position_name"], "condition_col": ["l4_position_name", "l3_position_name"],
             "required_columns": {"created_by": "created_by", "updated_by": "updated_by", "l3_position_name": "position_name", "l4_position_name": ""},
             "parent_columns": [{"col_name": "l4_position_name", "map_col_name": "parent_position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}]},
            {"key_columns": ["l2_position_name"], "check_cols": ["l2_position_name", "parent_position_id"], "drop_cols": ["l3_position_name"], "condition_col": ["l3_position_name", "l2_position_name"],
             "required_columns": {"created_by": "created_by", "updated_by": "updated_by", "l2_position_name": "position_name", "l3_position_name": ""},
             "parent_columns": [{"col_name": "l3_position_name", "map_col_name": "parent_position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}]},
            {"key_columns": ["l1_position_name"], "check_cols": ["l1_position_name", "parent_position_id"], "drop_cols": ["l2_position_name"], "condition_col": ["l2_position_name", "l1_position_name"],
             "required_columns": {"created_by": "created_by", "updated_by": "updated_by", "l1_position_name": "position_name", "l2_position_name": ""},
             "parent_columns": [{"col_name": "l2_position_name", "map_col_name": "parent_position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}]},
        ],
    },
    {
        "handler": "distributor", "table_name": "distributor", "mode": "NORMAL",
        "forward_ext_erp_id": False,
        "on_update": ["ext_erp_record_id", "distributor_name"], "on_update_filters": ["ext_erp_id"],
        "flow": [
            {"key_columns": ["superstockist_erp_id", "superstockist_name"], "check_cols": ["superstockist_erp_id", "superstockist_name"], "drop_cols": [], "condition_col": [],
             "required_columns": {"client_id": "client_id", "ext_erp_id": "ext_erp_id", "created_by": "created_by", "updated_by": "updated_by", "superstockist_name": "distributor_name", "superstockist_erp_id": "ext_erp_record_id", "state": "state"},
             "parent_columns": []},
            {"key_columns": ["distributor_erp_id", "distributor"], "check_cols": ["distributor_erp_id", "distributor"], "drop_cols": ["superstockist_name", "superstockist_erp_id"], "condition_col": [],
             "required_columns": {"client_id": "client_id", "ext_erp_id": "ext_erp_id", "created_by": "created_by", "updated_by": "updated_by", "distributor": "distributor_name", "distributor_email_id": "email", "superstockist_name": "", "distributor_erp_id": "ext_erp_record_id", "distributor_place": "city", "distributor_type": "distributor_type", "superstockist_erp_id": "", "state": "state"},
             "parent_columns": [{"col_name": "superstockist_name", "map_col_name": "parent_distributor_id", "DB_column": "distributor_name", "table": "distributor", "id_col": "distributor_id"}]},
        ],
    },
    {
        "handler": "salesman", "table_name": "salesman", "mode": "NORMAL",
        "forward_ext_erp_id": False,
        "on_update": ["ext_erp_record_id", "name"], "on_update_filters": ["ext_erp_id"],
        "flow": [
            {"key_columns": ["l5_position_user_erp_id"], "check_cols": ["l5_position_user_erp_id", "position_id"], "drop_cols": ["l5_position_name"], "condition_col": [],
             "required_columns": {"state": "state", "client_id": "client_id", "ext_erp_id": "ext_erp_id", "created_by": "created_by", "updated_by": "updated_by", "l5_position_user_name": "name", "l5_position_name": "", "l5_position_user_erp_id": "ext_erp_record_id"},
             "parent_columns": [{"col_name": "l5_position_name", "map_col_name": "position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}]},
            {"key_columns": ["l4_position_user_erp_id"], "check_cols": ["l4_position_user_erp_id", "l4_position_user_name", "manager_id"], "drop_cols": ["l4_position_name", "l5_position_user_erp_id"], "condition_col": [],
             "required_columns": {"state": "state", "client_id": "client_id", "ext_erp_id": "ext_erp_id", "created_by": "created_by", "updated_by": "updated_by", "l4_position_user_name": "name", "l4_position_name": "", "l4_position_user_erp_id": "ext_erp_record_id", "l5_position_user_erp_id": ""},
             "parent_columns": [{"col_name": "l4_position_name", "map_col_name": "position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}, {"col_name": "l5_position_user_erp_id", "map_col_name": "manager_id", "DB_column": "ext_erp_record_id", "table": "salesman", "id_col": "salesman_id"}]},
            {"key_columns": ["l3_position_user_erp_id"], "check_cols": ["l3_position_user_erp_id", "l3_position_user_name", "manager_id"], "drop_cols": ["l3_position_name", "l4_position_user_erp_id"], "condition_col": [],
             "required_columns": {"state": "state", "client_id": "client_id", "ext_erp_id": "ext_erp_id", "created_by": "created_by", "updated_by": "updated_by", "l3_position_user_name": "name", "l3_position_name": "", "l3_position_user_erp_id": "ext_erp_record_id", "l4_position_user_erp_id": ""},
             "parent_columns": [{"col_name": "l3_position_name", "map_col_name": "position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}, {"col_name": "l4_position_user_erp_id", "map_col_name": "manager_id", "DB_column": "ext_erp_record_id", "table": "salesman", "id_col": "salesman_id"}]},
            {"key_columns": ["l2_position_user_erp_id"], "check_cols": ["l2_position_user_erp_id", "l2_position_user_name", "manager_id"], "drop_cols": ["l2_position_name", "l3_position_user_erp_id"], "condition_col": [],
             "required_columns": {"state": "state", "client_id": "client_id", "ext_erp_id": "ext_erp_id", "created_by": "created_by", "updated_by": "updated_by", "l2_position_user_name": "name", "l2_position_name": "", "l2_position_user_erp_id": "ext_erp_record_id", "l3_position_user_erp_id": ""},
             "parent_columns": [{"col_name": "l2_position_name", "map_col_name": "position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}, {"col_name": "l3_position_user_erp_id", "map_col_name": "manager_id", "DB_column": "ext_erp_record_id", "table": "salesman", "id_col": "salesman_id"}]},
            {"key_columns": ["l1_position_user_erp_id"], "check_cols": ["l1_position_user_erp_id", "l1_position_user_name", "manager_id"], "drop_cols": ["l1_position_name", "l2_position_user_erp_id"], "condition_col": [],
             "required_columns": {"state": "state", "client_id": "client_id", "ext_erp_id": "ext_erp_id", "created_by": "created_by", "updated_by": "updated_by", "l1_position_user_name": "name", "l1_position_name": "", "l1_position_user_erp_id": "ext_erp_record_id", "l2_position_user_erp_id": ""},
             "parent_columns": [{"col_name": "l1_position_name", "map_col_name": "position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}, {"col_name": "l2_position_user_erp_id", "map_col_name": "manager_id", "DB_column": "ext_erp_record_id", "table": "salesman", "id_col": "salesman_id"}]},
        ],
    },
    {
        "handler": "beat", "table_name": "beat", "mode": "NORMAL",
        "forward_ext_erp_id": False,
        "on_update": ["ext_erp_record_id", "beat_name"], "on_update_filters": ["ext_erp_id"],
        "flow": [{
            "key_columns": ["beaterp_id", "beats"], "check_cols": ["beaterp_id", "beats", "salesman_id"], "drop_cols": ["l1_position_user_erp_id"], "condition_col": [],
            "required_columns": {"client_id": "client_id", "ext_erp_id": "ext_erp_id", "created_by": "created_by", "updated_by": "updated_by", "beaterp_id": "ext_erp_record_id", "beats": "beat_name", "l1_position_user_erp_id": ""},
            "parent_columns": [{"col_name": "l1_position_user_erp_id", "map_col_name": "salesman_id", "DB_column": "ext_erp_record_id", "table": "salesman", "id_col": "salesman_id"}],
        }],
    },
    {
        "handler": "beat_outlet", "table_name": "beat_outlet", "mode": "JUNC",
        "forward_ext_erp_id": False, "extra": "beat_outlet_status", "filter_nulls": "key_columns",
        "on_update": ["beat_id", "outlet_id", "visited_at"], "on_update_filters": [],
        "flow": [{
            "key_columns": ["outlet_erp_id", "beaterp_id"], "check_cols": ["beat_id", "outlet_id", "visited_at"],
            "drop_cols": ["beats", "outlet_erp_id", "outlets_name", "beaterp_id", "l1_position_user_erp_id"], "condition_col": [],
            "required_columns": {"beats": "", "outlet_creation_date": "visited_at", "latitude": "visit_lat", "longitude": "visit_long", "outlet_erp_id": "ext_erp_record_id", "beaterp_id": "ext_erp_record_id", "outlets_name": "", "l1_position_user_erp_id": ""},
            "parent_columns": [
                {"col_name": "outlet_erp_id", "map_col_name": "outlet_id", "DB_column": "ext_erp_record_id", "table": "outlet", "id_col": "outlet_id"},
                {"col_name": "l1_position_user_erp_id", "map_col_name": "visited_by", "DB_column": "ext_erp_record_id", "table": "salesman", "id_col": "salesman_id"},
                {"col_name": "beaterp_id", "map_col_name": "beat_id", "DB_column": "ext_erp_record_id", "table": "beat", "id_col": "beat_id"},
            ],
        }],
    },
]


_CAT_REQ0 = {"client_id": "client_id", "ext_erp_id": "ext_erp_id", "created_by": "created_by", "updated_by": "updated_by"}


# ── PRODUCT flow (FileType.PRODUCT) ──  order: category -> product
PRODUCT_FLOW = [
    {
        "handler": "category", "table_name": "category", "mode": "NORMAL", "forward_ext_erp_id": True,
        "on_update": ["category_name"], "on_update_filters": ["ext_erp_id"],
        "flow": [
            {"key_columns": ["product_division"], "check_cols": ["product_division"], "drop_cols": [], "condition_col": [],
             "required_columns": {**_CAT_REQ0, "product_division": "category_name"}, "parent_columns": []},
            {"key_columns": ["primary_category"], "check_cols": ["primary_category"], "drop_cols": ["product_division"], "condition_col": ["primary_category", "product_division"],
             "required_columns": {**_CAT_REQ0, "primary_category": "category_name", "product_division": ""},
             "parent_columns": [{"col_name": "product_division", "map_col_name": "parent_category_id", "DB_column": "category_name", "table": "category", "id_col": "category_id"}]},
            {"key_columns": ["secondary_category"], "check_cols": ["secondary_category"], "drop_cols": ["primary_category"], "condition_col": ["primary_category", "secondary_category"],
             "required_columns": {**_CAT_REQ0, "secondary_category": "category_name", "primary_category": ""},
             "parent_columns": [{"col_name": "primary_category", "map_col_name": "parent_category_id", "DB_column": "category_name", "table": "category", "id_col": "category_id"}]},
        ],
    },
    {
        "handler": "product", "table_name": "product", "mode": "NORMAL", "forward_ext_erp_id": True,
        "on_update": ["ext_erp_record_id"], "on_update_filters": [],
        "flow": [{
            "key_columns": ["erp_id", "product_name"], "check_cols": [], "drop_cols": ["secondary_category"], "condition_col": [],
            "required_columns": {**_CAT_REQ0, "product_name": "product_name", "erp_id": "ext_erp_record_id", "skus_code": "sku_code", "secondary_category": "category_name", "ptr": "offer_price", "mrp": "price", "product_status": "product_status"},
            "parent_columns": [{"col_name": "secondary_category", "map_col_name": "category_id", "DB_column": "category_name", "table": "category", "id_col": "category_id"}],
        }],
    },
]


# ── SECONDARY DUMP flow (FileType.SECONDARYDUMP) ──
# order: category -> product -> position -> salesman -> distributor -> beat
#        -> package -> uom -> secondary_order -> secondary_order_item -> beat_outlet
SECONDARY_FLOW = [
    {
        "handler": "category", "table_name": "category", "mode": "NORMAL", "forward_ext_erp_id": True,
        "on_update": ["category_name"], "on_update_filters": ["ext_erp_id"],
        "flow": [
            {"key_columns": ["product_division"], "check_cols": ["product_division"], "drop_cols": [], "condition_col": [],
             "required_columns": {**_CAT_REQ0, "product_division": "category_name"}, "parent_columns": []},
            {"key_columns": ["primary_category"], "check_cols": ["primary_category"], "drop_cols": ["product_division"], "condition_col": [],
             "required_columns": {**_CAT_REQ0, "primary_category": "category_name", "product_division": ""},
             "parent_columns": [{"col_name": "product_division", "map_col_name": "parent_category_id", "DB_column": "category_name", "table": "category", "id_col": "category_id"}]},
            {"key_columns": ["secondary_category"], "check_cols": ["secondary_category"], "drop_cols": ["primary_category"], "condition_col": [],
             "required_columns": {**_CAT_REQ0, "secondary_category": "category_name", "primary_category": ""},
             "parent_columns": [{"col_name": "primary_category", "map_col_name": "parent_category_id", "DB_column": "category_name", "table": "category", "id_col": "category_id"}]},
        ],
    },
    {
        "handler": "product", "table_name": "product", "mode": "NORMAL", "forward_ext_erp_id": True,
        "on_update": ["ext_erp_record_id"], "on_update_filters": ["ext_erp_id"],
        "flow": [{
            "key_columns": ["product_name"], "check_cols": ["product_name", "category_id", "price", "product_mrp"], "drop_cols": ["secondary_category"], "condition_col": [],
            "required_columns": {"skus_code": "sku_code", **_CAT_REQ0, "product_erp_id": "ext_erp_record_id", "product_name": "product_name", "price": "offer_price", "product_mrp": "price", "secondary_category": ""},
            "parent_columns": [{"col_name": "secondary_category", "map_col_name": "category_id", "DB_column": "Category_Name", "table": "category", "id_col": "category_id"}],
        }],
    },
    {
        "handler": "position", "table_name": "position", "mode": "NORMAL", "forward_ext_erp_id": False,
        "on_update": ["position_name"], "on_update_filters": [],
        "flow": [
            {"key_columns": ["l5position"], "check_cols": ["l5position"], "drop_cols": [], "condition_col": [],
             "required_columns": {"l5position": "position_name"}, "parent_columns": []},
            {"key_columns": ["l4position"], "check_cols": ["l4position", "parent_position_id"], "drop_cols": ["l5position"], "condition_col": ["l4position", "l5position"],
             "required_columns": {"l4position": "position_name", "l5position": ""},
             "parent_columns": [{"col_name": "l5position", "map_col_name": "parent_position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}]},
            {"key_columns": ["l3position"], "check_cols": ["l3position", "parent_position_id"], "drop_cols": ["l4position"], "condition_col": ["l4position", "l3position"],
             "required_columns": {"l3position": "position_name", "l4position": ""},
             "parent_columns": [{"col_name": "l4position", "map_col_name": "parent_position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}]},
            {"key_columns": ["l2position"], "check_cols": ["l2position", "parent_position_id"], "drop_cols": ["l3position"], "condition_col": ["l2position", "l3position"],
             "required_columns": {"l2position": "position_name", "l3position": ""},
             "parent_columns": [{"col_name": "l3position", "map_col_name": "parent_position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}]},
            {"key_columns": ["user_position_name"], "check_cols": ["user_position_name", "parent_position_id"], "drop_cols": ["l2position"], "condition_col": ["user_position_name", "l2position"],
             "required_columns": {"user_position_name": "position_name", "l2position": ""},
             "parent_columns": [{"col_name": "l2position", "map_col_name": "parent_position_id", "DB_column": "position_name", "table": "position", "id_col": "position_id"}]},
        ],
    },
    {
        "handler": "salesman", "table_name": "salesman", "mode": "NORMAL", "forward_ext_erp_id": False,
        "on_update": ["ext_erp_record_id", "name"], "on_update_filters": [],
        "flow": [{
            "key_columns": ["esm_erp_id", "user"], "check_cols": ["esm_erp_id", "user"], "drop_cols": ["user_position_name"], "condition_col": [],
            "required_columns": {**_CAT_REQ0, "user": "name", "user_position_name": "", "esm_erp_id": "ext_erp_record_id", "state": "state"},
            "parent_columns": [{"col_name": "user_position_name", "map_col_name": "position_id", "DB_column": "Position_Name", "table": "position", "id_col": "position_id"}],
        }],
    },
    {
        "handler": "distributor", "table_name": "distributor", "mode": "NORMAL", "forward_ext_erp_id": False,
        "on_update": ["ext_erp_record_id", "distributor_name"], "on_update_filters": [],
        "flow": [
            {"key_columns": ["superstockist_erp_id", "superstockist"], "check_cols": ["superstockist", "superstockist_erp_id"], "drop_cols": [], "condition_col": [],
             "required_columns": {**_CAT_REQ0, "superstockist": "distributor_name", "superstockist_erp_id": "ext_erp_record_id"}, "parent_columns": []},
            {"key_columns": ["distributor_erp_id", "distributor"], "check_cols": ["distributor", "distributor_erp_id", "distributor_gstin"], "drop_cols": ["superstockist_erp_id", "superstockist"], "condition_col": [],
             "required_columns": {**_CAT_REQ0, "distributor": "distributor_name", "superstockist": "", "distributor_erp_id": "ext_erp_record_id", "distributor_state": "state", "distributor_gstin": "gstin", "superstockist_erp_id": ""},
             "parent_columns": [{"col_name": "superstockist", "map_col_name": "parent_distributor_id", "DB_column": "distributor_name", "table": "distributor", "id_col": "distributor_id"}]},
        ],
    },
    {
        "handler": "beat", "table_name": "beat", "mode": "NORMAL", "forward_ext_erp_id": False,
        "on_update": ["ext_erp_record_id", "beat_name"], "on_update_filters": [],
        "flow": [{
            "key_columns": ["beaterp_id", "beat"], "check_cols": ["beaterp_id", "beat", "salesman_id"], "drop_cols": ["esm_erp_id"], "condition_col": [],
            "required_columns": {**_CAT_REQ0, "esm_erp_id": "", "beaterp_id": "ext_erp_record_id", "beat": "beat_name"},
            "parent_columns": [{"col_name": "esm_erp_id", "map_col_name": "salesman_id", "DB_column": "ext_erp_record_id", "table": "salesman", "id_col": "salesman_id"}],
        }],
    },
    {
        "handler": "package", "table_name": "package", "mode": "NORMAL", "forward_ext_erp_id": False,
        "on_update": ["package_name"], "on_update_filters": [],
        "flow": [{"key_columns": ["packaging_type"], "check_cols": ["packaging_type"], "drop_cols": [], "condition_col": [],
                  "required_columns": {"packaging_type": "package_name"}, "parent_columns": []}],
    },
    {
        "handler": "uom", "table_name": "uom", "mode": "NORMAL", "forward_ext_erp_id": False, "post": "uom_dimension",
        "on_update": ["uom_code"], "on_update_filters": [],
        "flow": [
            {"key_columns": ["super_unit"], "check_cols": ["super_unit"], "drop_cols": [], "condition_col": [],
             "required_columns": {"created_by": "created_by", "updated_by": "updated_by", "super_unit": "uom_code"}, "parent_columns": []},
            {"key_columns": ["unit"], "check_cols": ["unit"], "drop_cols": [], "condition_col": [],
             "required_columns": {"created_by": "created_by", "updated_by": "updated_by", "unit": "uom_code"}, "parent_columns": []},
            {"key_columns": ["std_unit"], "check_cols": ["std_unit"], "drop_cols": [], "condition_col": [],
             "required_columns": {"created_by": "created_by", "updated_by": "updated_by", "std_unit": "uom_code"}, "parent_columns": []},
        ],
    },
    {
        "handler": "secondary_order", "table_name": "secondary_order", "mode": "NORMAL", "forward_ext_erp_id": False,
        "extra": "order_status", "filter_nulls": "key_columns",
        "on_update": ["order_no", "order_date", "outlet_id", "salesman_id", "beat_id"], "on_update_filters": [],
        "flow": [{
            "key_columns": ["order_no"], "check_cols": [], "condition_col": [],
            "drop_cols": ["sale_value", "product_erp_id", "beaterp_id", "product_name", "superstockist_erp_id", "superstockist", "distributor", "distributor_erp_id", "esm_erp_id", "user", "pincode"],
            "required_columns": {
                "deviation_from_outlet": "deviation_meters", "outlet_gstin": "outlet_gstin", "address": "address",
                "outlet_channel_name": "outlet_channel", "call_start_time": "start_time", "call_end_time": "end_time", "market": "market",
                "created_by": "created_by", "updated_by": "updated_by", "distributor": "", "esm_erp_id": "", "user": "",
                "superstockist_erp_id": "", "superstockist": "", "product_name": "product_name", "visit_latitude": "visit_latitude",
                "visit_longitude": "visit_longitude", "order_date": "order_date", "order_no": "order_no", "order_type": "order_type",
                "outlet_name": "outlet_name", "outlet_erp_id": "outlet_erp_id", "no_sales_reason_category": "no_sales_reason_cat",
                "no_sales_reason": "no_sales_reason", "is_telephonic": "is_telephonic", "packaging_type": "pkg_Type",
                "owners_name": "owner_name", "owners_no": "owner_contact", "outlet_type": "outlet_type", "beat": "beat_name",
                "city": "city", "state": "state", "zone": "zone", "region": "service_region", "territory": "territory",
                "beaterp_id": "", "distributor_erp_id": "", "sale_value": "",
            },
            "parent_columns": [
                {"col_name": "outlet_erp_id", "map_col_name": "outlet_id", "DB_column": "ext_erp_record_id", "table": "outlet", "id_col": "outlet_id"},
                {"col_name": "outlet_erp_id", "map_col_name": "pincode", "DB_column": "ext_erp_record_id", "table": "outlet", "id_col": "pin_code"},
                {"col_name": "beaterp_id", "map_col_name": "beat_id", "DB_column": "ext_erp_record_id", "table": "beat", "id_col": "beat_id"},
                {"col_name": "user", "map_col_name": "salesman_id", "DB_column": "name", "table": "salesman", "id_col": "salesman_id"},
                {"col_name": "superstockist", "map_col_name": "superstockist_id", "DB_column": "distributor_name", "table": "distributor", "id_col": "distributor_id"},
                {"col_name": "distributor", "map_col_name": "distributor_id", "DB_column": "Distributor_name", "table": "distributor", "id_col": "distributor_id"},
                {"col_name": "packaging_type", "map_col_name": "pkg_id", "DB_column": "package_name", "table": "package", "id_col": "pkg_id"},
                # retailverse: the outlet's pin_code (looked up above as `pincode`)
                # -> masterdb pincode_id / district_id
                {"col_name": "pincode", "map_col_name": "pincode_id", "DB_column": "pincode", "table": "mpincode_office", "id_col": "pincode_id"},
                {"col_name": "pincode", "map_col_name": "district_id", "DB_column": "pincode", "table": "mpincode_office", "id_col": "district_id"},
            ],
        }],
    },
    {
        "handler": "secondary_order_item", "table_name": "secondary_order_item", "mode": "JUNC", "forward_ext_erp_id": False,
        "extra": "secondary_item_pre", "post": "secondary_item_post", "check_notnull": "product_name", "filter_nulls": "key_columns",
        "on_update": ["secondary_order_id", "product_id"], "on_update_filters": [],
        "flow": [{
            "key_columns": ["order_no", "product_erp_id", "gross_qty"], "check_cols": [], "condition_col": [],
            "drop_cols": ["product_erp_id", "order_no", "super_unit", "std_unit", "unit"],
            "required_columns": {
                "super_unit": "", "std_unit": "", "unit": "", "product_name": "product_name", "order_no": "", "product_erp_id": "",
                "product_division": "product_division", "primary_category": "primary_category", "secondary_category": "secondary_category",
                "packaging_type": "packaging_type", "qty_superunit": "qty_superunit", "qty_stdunit": "qty_stdunit",
                "standard_unit_conversion_factor": "conversion_factor", "gross_qty": "gross_qty", "qty_unit": "qty_unit",
                "foc_value": "foc_value", "product_mrp": "product_mrp", "price": "price", "actual_price": "actual_price",
                "stdunit_price": "stdunit_price", "gross_value": "gross_value", "sale_value": "sale_value", "cgst": "cgst_amount",
                "sgst": "sgst_amount", "igst": "igst_amount", "net_value": "net_value", "net_value_tax_inclusive": "net_value_incl_tax",
            },
            "parent_columns": [
                {"col_name": "product_erp_id", "map_col_name": "product_id", "DB_column": "ext_erp_record_id", "table": "product", "id_col": "product_id"},
                {"col_name": "order_no", "map_col_name": "secondary_order_id", "DB_column": "order_no", "table": "secondary_order", "id_col": "secondary_order_id"},
                {"col_name": "super_unit", "map_col_name": "superunit_uom_id", "DB_column": "uom_code", "table": "uom", "id_col": "uom_id"},
                {"col_name": "std_unit", "map_col_name": "stdunit_uom_id", "DB_column": "uom_code", "table": "uom", "id_col": "uom_id"},
                {"col_name": "unit", "map_col_name": "unit_uom_id", "DB_column": "uom_code", "table": "uom", "id_col": "uom_id"},
            ],
        }],
    },
    {
        "handler": "beat_outlet", "table_name": "beat_outlet", "mode": "JUNC", "forward_ext_erp_id": False,
        "extra": "beat_outlet_status", "filter_nulls": "key_columns",
        "on_update": ["beat_id", "outlet_id", "visited_at"], "on_update_filters": [],
        "flow": [{
            "key_columns": ["outlet_erp_id", "beaterp_id"], "check_cols": ["beat_id", "outlet_id", "visited_at"], "condition_col": [],
            "drop_cols": ["product_erp_id", "outlet_erp_id", "beaterp_id", "product_name", "beat", "outlet_name", "esm_erp_id"],
            "required_columns": {"esm_erp_id": "", "order_date": "visited_at", "product_erp_id": "", "visit_latitude": "visit_lat", "visit_longitude": "visit_long", "outlet_erp_id": "", "beaterp_id": "", "product_name": "", "outlet_name": "", "beat": ""},
            "parent_columns": [
                {"col_name": "beaterp_id", "map_col_name": "beat_id", "DB_column": "ext_erp_record_id", "table": "beat", "id_col": "beat_id"},
                {"col_name": "outlet_erp_id", "map_col_name": "outlet_id", "DB_column": "ext_erp_record_id", "table": "outlet", "id_col": "outlet_id"},
                {"col_name": "esm_erp_id", "map_col_name": "visited_by", "DB_column": "ext_erp_record_id", "table": "salesman", "id_col": "salesman_id"},
            ],
        }],
    },
]


# Registry keyed by FileType value.
FLOWS = {
    "OUTLETDUMP": OUTLET_FLOW,
    "PRODUCT": PRODUCT_FLOW,
    "SECONDARYDUMP": SECONDARY_FLOW,
}


def get_flow(file_type: str) -> list:
    if file_type not in FLOWS:
        raise KeyError(f"No load flow configured for file_type '{file_type}'.")
    return FLOWS[file_type]

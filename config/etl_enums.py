from enum import StrEnum


class ClientCode(StrEnum):
    __repr__ = str.__repr__

    NATURO = "NATURO"
    MODERNKT = "MODERNKT"
    REPOSE = "REPOSE"
    ULSNIRT = "ULSNIRT"
    HANGYO = "HANGYO"


class ErpRecCode(StrEnum):
    __repr__ = str.__repr__

    #naturo
    FA_NATURO = "ERP10001"
    IBEAM_NATURO = "ERP10002"
    
    #modern kitchen
    FA_MODERNKT = "ERPDZ1001"
    TALLY_MODERNKT = "ERPIX1001"
    
    
    #redncv knsdv
    #REPOSE
    TALLY_REPOSE = "ERPSD1001"
    
    #Ullas Nirantara
    FA_ULSNIRT = "ERPNC1001"

    #Hangyo q-commerce (masterdb.external_erp)
    BLINKIT_HANGYO = "ERPMC1001"
    ZEPTO_HANGYO = "ERPHH1001"
    INSTAMART_HANGYO = "ERPAA1001"


class ErpName(StrEnum):
    __repr__ = str.__repr__

    FA_NATURO = "FA-Naturo"
    IBEAM_NATURO = "iBeam-Naturo"
    FA_MODERNKT = "FA-Modern Kitchen"
    TALLY_MODERNKT = "Tally-Modern Kitchen"
    TALLY_REPOSE = "Tally-Repose"
    FA_ULSNIRT = "FA-Ullas Nirantara"
    BLINKIT_HANGYO = "Q-com-hangyo-blinkit"
    ZEPTO_HANGYO = "Q-com-hangyo-zepto"
    INSTAMART_HANGYO = "Q-com-hangyo-instamart"


class FileType(StrEnum):
    __repr__ = str.__repr__

    PRODUCT = "PRODUCT"
    OUTLETLIST = "OUTLETDUMP"
    SECONDARYDUMP = "SECONDARYDUMP"
    
    
    PRIMARYSALES = "PRIMARYSALES"
    PRODUCTIONREGISTRATION = "PRODUCTIONREGISTRATION"
    DEBTORSMASTER = "DEBTORSMASTER"
    CREDITORS = "CREDITORS"
    INVENTORYDATA = "INVENTORYDATA"
    ITEMMASTER = "ITEMMASTER"
    FINANCEDATA = "FINANCEDATA"
    RECEIVABLES = "RECEIVABLES"
    PAYABLES = "PAYABLES"

    # q-commerce sales (DB source tables, not file uploads)
    QCOM_BLINKIT = "QCOM_BLINKIT"
    QCOM_INSTAMART = "QCOM_INSTAMART"
    QCOM_ZEPTO = "QCOM_ZEPTO"
    # q-commerce stock on hand (DB source tables)
    QCOM_SOH_BLINKIT = "QCOM_SOH_BLINKIT"
    QCOM_SOH_INSTAMART = "QCOM_SOH_INSTAMART"
    QCOM_SOH_ZEPTO = "QCOM_SOH_ZEPTO"
    # q-commerce purchase orders (DB source tables)
    QCOM_PO_BLINKIT = "QCOM_PO_BLINKIT"
    QCOM_PO_INSTAMART = "QCOM_PO_INSTAMART"
    QCOM_PO_ZEPTO = "QCOM_PO_ZEPTO"


class ConfigKey(StrEnum):
    __repr__ = str.__repr__

    EXTRACTING_TABLES = "extracting_tables"
    ALLOWED_CLIENTS = "ALLOWED_CLIENTS"
    ALLOWED_ROLES = "ALLOWED_ROLES"
    ALLOWED_ERPS = "ALLOWED_ERPS"
    ALLOWED_FILES = "ALLOWED_FILES"
    ERP_NAME = "erp_name"
    TABLE_NAME = "table_name"
    ON_UPDATE = "on_update"
    ON_UPDATE_FILTERS = "on_update_filters"
    DATABASE = "database"
    FLOW = "flow"
    CONDITION_COL = "condition_col"
    DROP_COLS = "drop_cols"
    CHECK_COLS = "check_cols"
    KEY_COLUMNS = "key_columns"
    REQUIRED_COLUMNS = "required_columns"
    PARENT_COLUMNS = "parent_columns"
    COL_NAME = "col_name"
    MAP_COL_NAME = "map_col_name"
    DB_COLUMN = "DB_column"
    TABLE = "table"
    ID_COL = "id_col"


class TableName(StrEnum):
    __repr__ = str.__repr__

    BEAT = "beat"
    BEAT_OUTLET = "beat_outlet"
    CATEGORY = "category"
    CREDITORS = "creditors"
    DEBTORS = "debtors"
    DISTRIBUTOR = "distributor"
    GENERAL_LEDGER = "general_ledger"
    INVENTORY = "inventory"
    INVOICE = "invoice"
    ITEM_TYPE = "item_type"
    OUTLET = "outlet"
    PACKAGE = "package"
    PAYABLES = "payables"
    POSITION = "position"
    PRIMARY_ORDER = "primary_order"
    PRIMARY_ORDER_ITEM = "primary_order_item"
    PRODUCT = "product"
    PRODUCTION_BATCH = "production_batch"
    RECEIVABLES = "receivables"
    SALESMAN = "salesman"
    SECONDARY_ORDER = "secondary_order"
    SECONDARY_ORDER_ITEM = "secondary_order_item"
    UOM = "uom"
    MCLIENT = "mclient"
    MDISTRICT = "mdistrict"
    MPINCODE_OFFICE = "mpincode_office"
    EXTERNAL_ERP = "external_erp"


class TableHandler(StrEnum):
    __repr__ = str.__repr__

    CATEGORY = "handle_category_table"
    PRODUCT = "handle_product_table"
    OUTLET = "handle_outlet_table"
    POSITION = "handle_position_table"
    DISTRIBUTOR = "handle_distributor_table"
    BEAT = "handle_beat_table"
    BEAT_OUTLET = "handle_beat_outlet_table"
    PACKAGE = "handle_package_table"
    SECONDARY_ORDER = "handle_secondary_order_table"
    SECONDARY_ORDER_ITEM = "handle_secondary_order_item_table"
    SALESMAN = "handle_salesman_table"
    ITEM_TYPE = "handle_item_type_table"
    UOM = "handle_uom_table"
    INVOICE = "handle_invoice_table"
    PRIMARY_ORDER = "handle_primary_order_table"
    PRIMARY_ORDER_ITEM = "handle_primary_order_item_table"
    INVENTORY = "handle_inventory_table"
    FINANCE = "handle_finance_table"
    RECEIVABLES = "handle_receivables_table"
    PAYABLES = "handle_payables_table"
    PRODUCTION = "handle_production_table"
    DEBTORS_MASTER = "handle_debtors_master_table"
    CREDITORS_MASTER = "handle_creditors_master_table"

import os
from datetime import datetime, timedelta, timezone

# Import all required column-schema models (copied from retailverse ETL)
from .file_config import *
from .etl_enums import ClientCode, ConfigKey, FileType

# Complete CLIENT_REQUIREMENTS dictionary
# (identical to the retailverse ETL config — defines the required columns
#  that a file must contain, per client and per file type).
CLIENT_REQUIREMENTS = {
    ClientCode.NATURO: {
        FileType.PRODUCT: {ConfigKey.REQUIRED_COLUMNS: ProductModel.get_config_feild_assist()},
        FileType.OUTLETLIST: {ConfigKey.REQUIRED_COLUMNS: OutletModel.get_config_feild_assist()},
        FileType.SECONDARYDUMP: {ConfigKey.REQUIRED_COLUMNS: SecondaryDumModel.get_config_feild_assist()},
        FileType.PRIMARYSALES: {ConfigKey.REQUIRED_COLUMNS: PrimarySalesModel.get_config_ibeam()},
        FileType.PRODUCTIONREGISTRATION: {ConfigKey.REQUIRED_COLUMNS: ProductionRegistrationModel.get_config_ibeam()},
        FileType.DEBTORSMASTER: {ConfigKey.REQUIRED_COLUMNS: DebtorsMasterModel.get_config_ibeam()},
        FileType.CREDITORS: {ConfigKey.REQUIRED_COLUMNS: CreditorsModel.get_config_ibeam()},
        FileType.INVENTORYDATA: {ConfigKey.REQUIRED_COLUMNS: InventoryDataModel.get_config_ibeam()},
        FileType.ITEMMASTER: {ConfigKey.REQUIRED_COLUMNS: ItemMasterModel.get_config_ibeam()},
        FileType.FINANCEDATA: {ConfigKey.REQUIRED_COLUMNS: FinanceDataModel.get_config_ibeam()},
        FileType.RECEIVABLES: {ConfigKey.REQUIRED_COLUMNS: ReceivablesModel.get_config_ibeam()},
        FileType.PAYABLES: {ConfigKey.REQUIRED_COLUMNS: PayablesModel.get_config_ibeam()},
    },
    ClientCode.MODERNKT: {
        FileType.PRODUCT: {ConfigKey.REQUIRED_COLUMNS: ProductModel.get_config_feild_assist()},
        FileType.OUTLETLIST: {ConfigKey.REQUIRED_COLUMNS: OutletModel.get_config_feild_assist()},
        FileType.SECONDARYDUMP: {ConfigKey.REQUIRED_COLUMNS: SecondaryDumModel.get_config_feild_assist()},
        FileType.PRIMARYSALES: {ConfigKey.REQUIRED_COLUMNS: PrimarySalesModel.get_config_ibeam()},
        FileType.ITEMMASTER: {ConfigKey.REQUIRED_COLUMNS: ItemMasterModel.get_config_ibeam()},
    },
    ClientCode.ULSNIRT: {
        FileType.PRODUCT: {ConfigKey.REQUIRED_COLUMNS: ProductModel.get_config_feild_assist()},
        FileType.OUTLETLIST: {ConfigKey.REQUIRED_COLUMNS: OutletModel.get_config_feild_assist()},
        FileType.SECONDARYDUMP: {ConfigKey.REQUIRED_COLUMNS: SecondaryDumModel.get_config_feild_assist()},
    },
    ClientCode.REPOSE: {
        FileType.PRIMARYSALES: {ConfigKey.REQUIRED_COLUMNS: PrimarySalesModel.get_repose_config_tally()},
        FileType.ITEMMASTER: {ConfigKey.REQUIRED_COLUMNS: ItemMasterModel.get_repose_config_tally()},
    },
    # q-commerce: source is a MySQL table (pbisalesanalysisdb), not a file —
    # same required-column validation + rename-once schema as file uploads.
    ClientCode.HANGYO: {
        FileType.QCOM_BLINKIT: {ConfigKey.REQUIRED_COLUMNS: QcomSalesModel.get_config_blinkit()},
        FileType.QCOM_INSTAMART: {ConfigKey.REQUIRED_COLUMNS: QcomSalesModel.get_config_instamart()},
        FileType.QCOM_ZEPTO: {ConfigKey.REQUIRED_COLUMNS: QcomSalesModel.get_config_zepto()},
        FileType.QCOM_SOH_BLINKIT: {ConfigKey.REQUIRED_COLUMNS: QcomStockModel.get_config_blinkit()},
        FileType.QCOM_SOH_INSTAMART: {ConfigKey.REQUIRED_COLUMNS: QcomStockModel.get_config_instamart()},
        FileType.QCOM_SOH_ZEPTO: {ConfigKey.REQUIRED_COLUMNS: QcomStockModel.get_config_zepto()},
        FileType.QCOM_PO_BLINKIT: {ConfigKey.REQUIRED_COLUMNS: QcomPoModel.get_config_blinkit()},
        FileType.QCOM_PO_INSTAMART: {ConfigKey.REQUIRED_COLUMNS: QcomPoModel.get_config_instamart()},
        FileType.QCOM_PO_ZEPTO: {ConfigKey.REQUIRED_COLUMNS: QcomPoModel.get_config_zepto()},
    },
}

# ─────────────────────────────────────────────────────────────
# Client -> destination database name (one DB per client).
# Mirrors retailverse apps/ETL/etl_config/clients_config.CLIENTS_DB_CONFIG,
# but the DB names come from .env (MYSQL_DB_<CLIENT>) so they're configurable.
# ─────────────────────────────────────────────────────────────
from core.settings import get_settings as _get_settings

_dbs = _get_settings().client_databases()
CLIENTS_DB_CONFIG = {
    ClientCode.NATURO: _dbs["NATURO"],
    ClientCode.MODERNKT: _dbs["MODERNKT"],
    ClientCode.ULSNIRT: _dbs["ULSNIRT"],
    ClientCode.REPOSE: _dbs["REPOSE"],
    ClientCode.HANGYO: _dbs["HANGYO"],
}


# ─────────────────────────────────────────────────────────────
# Blob folder layout:  <container>/<client_code>/<category>/<file>
# Maps each FileType to the folder name used inside the blob container.
# (matches the requested outlet / secondary / product structure)
# ─────────────────────────────────────────────────────────────
FILE_TYPE_FOLDER = {
    FileType.PRODUCT: "product",
    FileType.OUTLETLIST: "outlet",
    FileType.SECONDARYDUMP: "secondary",
    FileType.PRIMARYSALES: "primary_sales",
    FileType.PRODUCTIONREGISTRATION: "production_registration",
    FileType.DEBTORSMASTER: "debtors_master",
    FileType.CREDITORS: "creditors",
    FileType.INVENTORYDATA: "inventory",
    FileType.ITEMMASTER: "item_master",
    FileType.FINANCEDATA: "finance",
    FileType.RECEIVABLES: "receivables",
    FileType.PAYABLES: "payables",
}


def folder_for_file_type(file_type: str) -> str:
    """Return the blob folder name for a file type (fallback: lowercased type)."""
    return FILE_TYPE_FOLDER.get(file_type, str(file_type).lower())


# Master/snapshot file types: each upload OVERWRITES the previous blob (fixed
# name, latest wins). Transactional types keep a versioned name per upload.
OVERWRITE_FILE_TYPES = {FileType.PRODUCT, FileType.OUTLETLIST}


def is_overwrite_file_type(file_type: str) -> bool:
    return file_type in OVERWRITE_FILE_TYPES


# India time (fixed +05:30) for the daily upload folder
_IST = timezone(timedelta(hours=5, minutes=30))


def blob_location(client_code: str, file_type: str, filename: str, task_id: str,
                  now: datetime | None = None) -> tuple[str, str, bool]:
    """Where an uploaded file is stored: (folder, parquet_name, overwrite).

    master (outlet / product):  <client>/<category>/<category>.parquet
                                one fixed file, every upload OVERWRITES it
    transactional (secondary…): <client>/<category>/<YYYY-MM-DD>/<file>_<task_id>.parquet
                                one folder per upload day (IST); every upload is
                                kept — never overwritten
    """
    category = folder_for_file_type(file_type)
    if is_overwrite_file_type(file_type):
        return f"{client_code}/{category}", f"{category}.parquet", True
    day = (now or datetime.now(_IST)).astimezone(_IST).strftime("%Y-%m-%d")
    name = f"{os.path.splitext(filename)[0]}_{task_id}.parquet"
    return f"{client_code}/{category}/{day}", name, False

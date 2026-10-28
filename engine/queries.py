"""Raw SQL used by the load engine repository (``etl.repositories``).

The load engine builds SQL dynamically from the flow config (table/column names
are validated against the flow, not user input). These helpers return the exact
SQL text the repository executed inline before — kept identical, extracted here
so all engine SQL lives in one place.
"""


def insert_record_sql(table_name: str, collist: str, placeholders: str) -> str:
    return f"INSERT INTO `{table_name}` ({collist}) VALUES ({placeholders})"


def on_duplicate_key_update_clause(sets: str) -> str:
    return f" ON DUPLICATE KEY UPDATE {sets}"


def id_mapping_sql(id_column: str, name_column: str, table_name: str, inlist: str) -> str:
    return f"SELECT `{id_column}`, `{name_column}` FROM `{table_name}` WHERE TRIM(`{name_column}`) IN ({inlist})"


ID_MAPPING_EXT_ERP_FILTER = " AND `ext_erp_id` = :eid"


def filter_new_entries_sql(sel: str, table_name: str) -> str:
    return f"SELECT DISTINCT {sel} FROM `{table_name}`"


FILTER_NEW_ENTRIES_EXT_ERP_FILTER = " WHERE `ext_erp_id` = :eid"


# ─────────────────────────────────────────────────────────────
# Master-DB (user_db) id resolution — mclient + external_erp.
# Ported from retailverse CoreDataValidatorQueries.
# ─────────────────────────────────────────────────────────────
class CoreDataValidatorQueries:

    @staticmethod
    def get_client_id(client_code: str):
        sql = "SELECT client_id FROM mclient WHERE shortcode = :code AND is_active = 1"
        return sql, {"code": client_code}

    @staticmethod
    def get_erp_details(ext_erp_rec_code: str):
        sql = ("SELECT erp_id AS ext_erp_id, erp_name AS ext_erp_name "
               "FROM external_erp WHERE rec_code = :code AND is_active = 1")
        return sql, {"code": ext_erp_rec_code}

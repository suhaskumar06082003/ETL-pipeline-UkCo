"""Application settings, loaded from environment / .env file."""
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Reads configuration from the `.env` file (or real env vars)."""

    # ---- Azure Blob (raw / landing zone) ----
    azure_storage_connection_string: str = ""
    azure_storage_container_name: str = "etl-uploads"

    # ---- Upload limits ----
    max_upload_size_mb: int = 100

    # ---- Airflow (auto-trigger the ETL DAG after upload) ----
    airflow_base_url: str = ""          # e.g. http://etl-airflow:8080 (empty = no auto-trigger)
    airflow_username: str = "admin"
    airflow_password: str = ""

    # ---- Logging ----
    log_file: str = "logs/etl.log"
    log_level: str = "INFO"

    # ---- MySQL — ONE host/user/password/port for every database; databases
    # differ only by NAME (retailverse-style). The DB is picked dynamically per
    # client (or the master DB), all on this single connection. ----
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_user: str = ""
    mysql_password: str = ""
    mysql_database: str = ""            # default DB (falls back to master_db_name)

    # Master DB — holds mclient + external_erp for id resolution.
    master_db_name: str = "masterdb"

    # Per-client destination databases: client_code -> DB name (from .env
    # MYSQL_DB_<CLIENT>). Add a client here + in .env and it resolves dynamically.
    mysql_db_naturo: str = "naturodb"
    mysql_db_modernkt: str = "modernkitchendb"
    mysql_db_ulsnirt: str = "ulsnirt"
    mysql_db_repose: str = "repose"
    mysql_db_hangyo: str = "hangyodb"

    # Q-commerce source DB (Blinkit/Instamart/Zepto sales source tables)
    qcom_source_db: str = "pbisalesanalysisdb"

    # ---- BigQuery (analytics warehouse) ----
    # Service-account JSON key file — kept in secrets/ (git-ignored), never in git.
    gcp_credentials_file: str = "secrets/gcp-key.json"
    bq_project_id: str = ""                 # e.g. ecstatic-splice-392612
    bq_location: str = "asia-south1"        # the datasets' region
    # client -> BigQuery dataset
    bq_dataset_naturo: str = "naturoanalyticsdb"
    bq_dataset_modernkt: str = "modernkitchenanalyticsdb"
    bq_dataset_hangyo: str = "hangyoanalyticsdb"
    bq_dataset_ulsnirt: str = "ullasnirantaraanalyticsdb"

    def bq_dataset_for(self, client_code: str) -> str:
        datasets = {"NATURO": self.bq_dataset_naturo, "MODERNKT": self.bq_dataset_modernkt,
                    "HANGYO": self.bq_dataset_hangyo, "ULSNIRT": self.bq_dataset_ulsnirt}
        code = str(client_code).upper()
        if code not in datasets:
            raise ValueError(f"No BigQuery dataset configured for client {client_code!r}")
        return datasets[code]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def max_upload_size_bytes(self) -> int:
        return self.max_upload_size_mb * 1024 * 1024

    def mysql_url_for(self, database: str) -> str:
        """SQLAlchemy connection URL for a specific MySQL database.

        User/password are URL-encoded so special characters (e.g. '@' in
        'user@pass') don't break URL parsing / mangle the host.
        """
        from urllib.parse import quote_plus
        return (
            f"mysql+pymysql://{quote_plus(self.mysql_user)}:{quote_plus(self.mysql_password)}"
            f"@{self.mysql_host}:{self.mysql_port}/{database}"
        )

    @property
    def mysql_url(self) -> str:
        """SQLAlchemy connection URL for the default MySQL destination."""
        return self.mysql_url_for(self.default_database())

    def default_database(self) -> str:
        """DB used when no client_code is given (falls back to the master DB)."""
        return self.mysql_database or self.master_db_name

    def client_databases(self) -> dict[str, str]:
        """client_code -> destination database NAME (from .env MYSQL_DB_<CLIENT>)."""
        return {
            "NATURO": self.mysql_db_naturo,
            "MODERNKT": self.mysql_db_modernkt,
            "ULSNIRT": self.mysql_db_ulsnirt,
            "REPOSE": self.mysql_db_repose,
            "HANGYO": self.mysql_db_hangyo,
        }

    def _base_conn(self, database: str) -> dict:
        """One shared host/user/password/port; only the database NAME varies."""
        return {"host": self.mysql_host, "port": self.mysql_port,
                "user": self.mysql_user, "password": self.mysql_password,
                "database": database}

    def client_connection(self, client_code: str | None = None) -> dict:
        """Connection for a client's destination DB (or the default/master DB
        when no client_code) — resolved dynamically by client_code."""
        db = self.client_databases().get(str(client_code)) if client_code else None
        return self._base_conn(db or self.default_database())

    def master_connection(self) -> dict:
        """Connection for the master DB (mclient + external_erp)."""
        return self._base_conn(self.master_db_name)


@lru_cache
def get_settings() -> Settings:
    """Cached settings instance (read once per process)."""
    return Settings()

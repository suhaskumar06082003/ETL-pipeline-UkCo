"""MySQL connection (the ETL destination).

Scaffold for the Airflow load stage. The SQLAlchemy engine is created lazily
so importing this module never requires a live MySQL server (safe for the API
and for Airflow DAG parsing).

Requires: sqlalchemy, pymysql  (see requirements.txt).
"""
import logging
from functools import lru_cache

from core.settings import get_settings

logger = logging.getLogger("ETL_MYSQL")

def db_for_client(client_code: str | None = None) -> str:
    """Resolve the MySQL database name for a client.

    Driven by CLIENTS_DB_CONFIG (client_code -> database), the same map
    retailverse uses. No client_code -> the default DB (MYSQL_DATABASE).
    """
    settings = get_settings()
    if not client_code:
        return settings.default_database()
    from config.clients_config import CLIENTS_DB_CONFIG
    return CLIENTS_DB_CONFIG.get(client_code, settings.default_database())


@lru_cache
def _engine_for_db(database: str):
    """Cached SQLAlchemy engine for one database on the DEFAULT host/creds."""
    from sqlalchemy import create_engine

    settings = get_settings()
    if not settings.mysql_user or not database:
        raise RuntimeError(
            "MySQL is not configured. Set MYSQL_USER / MYSQL_PASSWORD / "
            "MYSQL_DATABASE (and host/port) in your .env file."
        )
    logger.info("Creating MySQL engine for %s", database)
    return create_engine(settings.mysql_url_for(database), pool_pre_ping=True, future=True)


@lru_cache
def _engine_for_conn(host: str, port: int, user: str, password: str, database: str):
    """Cached engine for a full connection (per-client host/creds/db).

    Uses URL.create so special characters in the password (e.g. '@' in
    'user@pass') are handled correctly — a raw f-string URL would split
    on the '@' and mangle the host.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    if not user or not database or not host:
        raise RuntimeError("MySQL is not configured for this client (host/user/database missing).")
    logger.info("Creating MySQL engine for %s@%s/%s", user, host, database)
    url = URL.create(
        "mysql+pymysql", username=user, password=password,
        host=host, port=port, database=database,
    )
    return create_engine(url, pool_pre_ping=True, future=True)


def get_engine(client_code: str | None = None):
    """Return the cached engine for a client's connection.

    A client with a per-client host override (e.g. ULLASNIRANTARA on the dev
    server) connects there; everyone else uses the global MYSQL_* host + their
    own database name. No client_code -> the default DB (naturo).
    """
    c = get_settings().client_connection(client_code)
    return _engine_for_conn(c["host"], c["port"], c["user"], c["password"], c["database"])


def get_master_engine():
    """Engine for the master DB — same host/creds as the clients, database =
    master_db_name. Holds mclient + external_erp used to resolve client_id (by
    shortcode) and ext_erp_id/name (by rec_code)."""
    c = get_settings().master_connection()
    return _engine_for_conn(c["host"], c["port"], c["user"], c["password"], c["database"])

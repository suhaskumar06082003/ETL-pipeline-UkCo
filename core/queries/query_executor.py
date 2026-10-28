"""Reusable SQL execution engine (SQLAlchemy) for the ETL project.

Centralizes raw SQL execution on top of the per-client engines from
``core.queries.db_connection``. Supports BOTH standalone execution (each call
opens/closes its own connection or transaction) AND execution on a
caller-supplied connection, so the load engine can run many statements inside
ONE shared transaction that the caller owns and can roll back atomically.

All params are SQLAlchemy-style named dict params (``:name``).
"""
import logging

from sqlalchemy import text

from core.queries.db_connection import get_engine

logger = logging.getLogger("ETL_QUERY_EXECUTOR")


class QueryExecutor:
    """Query execution engine with multi-client (per-DB) support.

    Two families of methods:
      * standalone (``fetch_all`` / ``fetch_one`` / ``scalar`` / ``execute``)
        resolve the client's engine and own their connection/transaction.
      * ``*_on`` methods run a single statement on a CALLER-supplied connection
        and never commit — the caller owns the transaction.
    """

    # ── standalone execution (owns its own connection / transaction) ──

    @staticmethod
    def fetch_all(sql, params=None, client_code=None) -> list:
        """Open a connection on the client's engine, run, return ``.fetchall()``."""
        try:
            with get_engine(client_code).connect() as conn:
                result = conn.execute(text(sql), params or {})
                rows = result.fetchall()
            logger.info("fetch_all OK (%s rows): %s", len(rows), sql)
            return rows
        except Exception as e:
            logger.error("fetch_all failed: %s | sql=%s", e, sql)
            raise

    @staticmethod
    def fetch_one(sql, params=None, client_code=None):
        """Open a connection on the client's engine, run, return ``.first()``."""
        try:
            with get_engine(client_code).connect() as conn:
                row = conn.execute(text(sql), params or {}).first()
            logger.info("fetch_one OK: %s", sql)
            return row
        except Exception as e:
            logger.error("fetch_one failed: %s | sql=%s", e, sql)
            raise

    @staticmethod
    def scalar(sql, params=None, client_code=None):
        """Open a connection on the client's engine, run, return ``.scalar()``."""
        try:
            with get_engine(client_code).connect() as conn:
                value = conn.execute(text(sql), params or {}).scalar()
            logger.info("scalar OK: %s", sql)
            return value
        except Exception as e:
            logger.error("scalar failed: %s | sql=%s", e, sql)
            raise

    @staticmethod
    def execute(sql, params=None, client_code=None) -> int:
        """Run DML inside ``engine.begin()`` (its own transaction, commits).

        Returns the affected rowcount.
        """
        try:
            with get_engine(client_code).begin() as conn:
                result = conn.execute(text(sql), params or {})
            logger.info("execute OK (rowcount=%s): %s", result.rowcount, sql)
            return result.rowcount
        except Exception as e:
            logger.error("execute failed: %s | sql=%s", e, sql)
            raise

    # ── execution on a CALLER-supplied connection (caller owns the txn) ──

    @staticmethod
    def execute_on(conn, sql, params=None):
        """Run one statement on a caller-supplied connection (no commit).

        Returns the raw SQLAlchemy ``Result`` so the caller can chain
        ``.mappings()`` / ``.rowcount`` / ``.first()`` as needed.
        """
        try:
            result = conn.execute(text(sql), params or {})
            logger.info("execute_on OK: %s", sql)
            return result
        except Exception as e:
            logger.error("execute_on failed: %s | sql=%s", e, sql)
            raise

    @staticmethod
    def executemany_on(conn, sql, records):
        """Run one statement for a list of param dicts on a caller connection.

        Mirrors ``conn.execute(text(sql), records)`` (SQLAlchemy executemany).
        No commit — the caller owns the transaction.
        """
        try:
            result = conn.execute(text(sql), records)
            logger.info(
                "executemany_on OK (%s rows): %s",
                len(records) if records is not None else 0,
                sql,
            )
            return result
        except Exception as e:
            logger.error("executemany_on failed: %s | sql=%s", e, sql)
            raise

    @staticmethod
    def fetch_all_on(conn, sql, params=None):
        """Read on a caller-supplied connection; return ``.fetchall()``."""
        try:
            rows = conn.execute(text(sql), params or {}).fetchall()
            logger.info("fetch_all_on OK (%s rows): %s", len(rows), sql)
            return rows
        except Exception as e:
            logger.error("fetch_all_on failed: %s | sql=%s", e, sql)
            raise

    @staticmethod
    def fetch_one_on(conn, sql, params=None):
        """Read on a caller-supplied connection; return ``.first()``."""
        try:
            row = conn.execute(text(sql), params or {}).first()
            logger.info("fetch_one_on OK: %s", sql)
            return row
        except Exception as e:
            logger.error("fetch_one_on failed: %s | sql=%s", e, sql)
            raise

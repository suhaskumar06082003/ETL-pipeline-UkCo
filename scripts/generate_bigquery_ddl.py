"""Generate BigQuery CREATE TABLE statements from the LIVE MySQL tables of a client DB
(read-only: only information_schema is read).

Type mapping (same style as sql/bigqry/naturodb_tables.sql):
    tinyint(1) / bool -> BOOL          int types        -> INT64
    decimal(p,s) s<=9 -> NUMERIC       decimal s>9 (lat/long) / float / double -> FLOAT64
    char/varchar/text/enum/set -> STRING               json -> JSON
    date -> DATE   datetime -> DATETIME   timestamp -> TIMESTAMP   time -> TIME
    blob/binary -> BYTES

Usage (from D:\\etl, PYTHONPATH=D:\\etl):
    python scripts/generate_bigquery_ddl.py --client HANGYO --tables qcom_sales,qcom_stock_on_hand,qcom_po,product,category --out sql/bigqry/hangyodb_tables.sql
"""
import argparse
import re

from sqlalchemy import text

from core.queries.db_connection import db_for_client, get_engine


def bq_type(data_type: str, column_type: str, scale) -> str:
    dt, ct = data_type.lower(), column_type.lower()
    if dt in ("tinyint", "bool", "boolean") and ct.startswith("tinyint(1)"):
        return "BOOL"
    if dt in ("tinyint", "smallint", "mediumint", "int", "integer", "bigint", "year"):
        return "INT64"
    if dt in ("decimal", "numeric"):
        return "NUMERIC" if (scale or 0) <= 9 else "FLOAT64"
    if dt in ("float", "double", "real"):
        return "FLOAT64"
    if dt == "json":
        return "JSON"
    if dt == "date":
        return "DATE"
    if dt == "datetime":
        return "DATETIME"
    if dt == "timestamp":
        return "TIMESTAMP"
    if dt == "time":
        return "TIME"
    if dt in ("blob", "tinyblob", "mediumblob", "longblob", "binary", "varbinary"):
        return "BYTES"
    return "STRING"


def columns(conn, table: str) -> list[tuple[str, str]]:
    rows = conn.execute(text(
        "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, NUMERIC_SCALE FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = :t ORDER BY ORDINAL_POSITION"), {"t": table}).fetchall()
    return [(r[0], bq_type(r[1], r[2], r[3])) for r in rows]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", required=True)
    ap.add_argument("--tables", required=True, help="comma-separated MySQL table names")
    ap.add_argument("--out", required=True)
    a = ap.parse_args()
    db = db_for_client(a.client)
    parts = [f"-- BigQuery DDL for {db} (generated from the live MySQL tables)", ""]
    with get_engine(a.client).connect() as conn:
        for i, table in enumerate(t.strip() for t in a.tables.split(",")):
            cols = columns(conn, table)
            if not cols:
                raise SystemExit(f"table {db}.{table} not found")
            body = ",\n".join(f"  {c} {t}" for c, t in cols)
            parts += [f"-- {i + 1}. {table}", f"CREATE TABLE IF NOT EXISTS {db}.{table} (", body, ");", ""]
            print(f"   {table:22} {len(cols):>3} columns")
    open(a.out, "w", encoding="utf-8", newline="\n").write("\n".join(parts))
    print(f"written {a.out}")


if __name__ == "__main__":
    main()

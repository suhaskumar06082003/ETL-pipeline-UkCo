"""Create the BigQuery tables for a client from a DDL file.

Every `CREATE TABLE IF NOT EXISTS <anything>.<table>` in the file is pointed at
`<BQ_PROJECT_ID>.<client's dataset from .env>.<table>` and executed. Existing
tables are left untouched (IF NOT EXISTS) — nothing is altered or dropped.

Usage (from D:\\etl, PYTHONPATH=D:\\etl):
    python scripts/create_bigquery_tables.py --client NATURO --ddl sql/bigqry/naturodb_tables.sql
    python scripts/create_bigquery_tables.py --client NATURO --ddl sql/bigqry/naturodb_tables.sql --dry-run
"""
import argparse
import re

from core.services.bigquery_service import get_bigquery_client
from core.settings import get_settings

_CREATE = re.compile(r"CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+`?(?:[\w-]+\.)?(?:\w+\.)?(\w+)`?", re.I)


def statements(path: str) -> list[tuple[str, str]]:
    """[(table, sql)] from a DDL file (comments stripped, split on ';')."""
    text = "\n".join(line for line in open(path, encoding="utf-8").read().splitlines()
                     if not line.strip().startswith("--"))
    out = []
    for stmt in (s.strip() for s in text.split(";")):
        m = _CREATE.search(stmt)
        if m:
            out.append((m.group(1), stmt))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--client", required=True, help="NATURO / MODERNKT / HANGYO")
    ap.add_argument("--ddl", required=True, help="BigQuery DDL file, e.g. sql/bigqry/naturodb_tables.sql")
    ap.add_argument("--dry-run", action="store_true", help="print what would be created, run nothing")
    a = ap.parse_args()

    s = get_settings()
    dataset = s.bq_dataset_for(a.client)
    stmts = statements(a.ddl)
    print(f"{len(stmts)} CREATE TABLE statements in {a.ddl} -> {s.bq_project_id}.{dataset}")
    if a.dry_run:
        for table, _ in stmts:
            print(f"   would create {s.bq_project_id}.{dataset}.{table}")
        return

    client = get_bigquery_client()
    existing = {t.table_id for t in client.list_tables(f"{client.project}.{dataset}")}
    created, skipped, failed = [], [], []
    for table, stmt in stmts:
        target = f"`{client.project}.{dataset}.{table}`"
        sql = _CREATE.sub(f"CREATE TABLE IF NOT EXISTS {target}", stmt, count=1)
        if table in existing:
            skipped.append(table)
            continue
        try:
            client.query(sql).result()
            created.append(table)
            print(f"   created  {dataset}.{table}")
        except Exception as e:  # noqa: BLE001 — report every table, keep going
            failed.append(table)
            print(f"   FAILED   {dataset}.{table}: {str(e).splitlines()[0][:200]}")

    tables = {t.table_id: t for t in client.list_tables(f"{client.project}.{dataset}")}
    print(f"\ncreated {len(created)} | already existed {len(skipped)} {skipped or ''} | failed {len(failed)} {failed or ''}")
    print(f"tables now in {dataset}: {len(tables)}")
    for name in sorted(tables):
        t = client.get_table(f"{client.project}.{dataset}.{name}")
        print(f"   {name:24} {len(t.schema):>3} columns  expires={t.expires or 'never'}")


if __name__ == "__main__":
    main()

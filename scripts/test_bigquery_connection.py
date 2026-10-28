"""Check that the ETL project can talk to BigQuery. Read-only: it lists the
datasets and runs `SELECT 1` — it creates / writes / deletes nothing.

Run from D:\\etl (PowerShell):
    $env:PYTHONPATH="D:\\etl"; python scripts\\test_bigquery_connection.py
"""
from core.services.bigquery_service import get_bigquery_client
from core.settings import get_settings

s = get_settings()
client = get_bigquery_client()
print(f"1) connected to project : {client.project}  (location {s.bq_location})")

datasets = sorted(d.dataset_id for d in client.list_datasets())
print(f"2) datasets visible      : {datasets or 'none'}")

for client_code in ("NATURO", "MODERNKT", "HANGYO"):
    ds = s.bq_dataset_for(client_code)
    print(f"   {client_code:9} -> {ds:26} {'OK' if ds in datasets else 'NOT FOUND'}")

row = list(client.query("SELECT 1 AS ok").result())[0]
print(f"3) test query SELECT 1   : {row.ok}  -> BigQuery connection works")

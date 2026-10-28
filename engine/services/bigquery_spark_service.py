"""MySQL -> BigQuery FULL load with PySpark (manual DAG <client>_bigqry_fullload_spark).

Same run / tracking / publish as the incremental sync (bigquery_sync_service):
start_run(full=True) -> spark_full_table() per table -> publish -> finish_run
(fail_run on any error). Only the per-table EXTRACT + BUILD differs:

    EXTRACT  Spark reads the table spec's SELECT from MySQL over JDBC (parallel
             partitions on the first numeric id column), writes Parquet files
             to a local temp dir, and a BigQuery LOAD job puts them into
             <table>__delta (load jobs work in the BigQuery Sandbox).
    BUILD    one query job: SELECT every live-table column cast to the live
             BigQuery type FROM __delta -> <table>__next (WRITE_TRUNCATE).
             FULL load = __next is exactly what MySQL has now (deletes mirrored).

Times: MySQL DATETIME / TIMESTAMP are read as wall-clock values (no timezone
shift) - JVM + Spark session in UTC, Connector/J connectionTimeZone=SERVER,
preserveInstants=false, Spark TIMESTAMP_NTZ -> Parquet isAdjustedToUTC=false
-> BigQuery DATETIME.  Memory: local[2], driver SPARK_DRIVER_MEMORY (1g).
"""
import glob
import logging
import os
import shutil
import tempfile

from sqlalchemy import text

from config.bigquery_sync_config import get_bigquery_pipeline
from core.queries.db_connection import get_engine
from core.services.bigquery_service import get_bigquery_client
from core.settings import get_settings
from core.utils.logger import configure_etl_logging
from engine.services.bigquery_sync_service import _ref, _stage, _task

logger = logging.getLogger("BQ_SPARK")

MYSQL_JAR = os.getenv("SPARK_MYSQL_JAR", "/opt/spark-jars/mysql-connector-j.jar")
DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "1g")
ROWS_PER_PARTITION = 250_000          # JDBC read split (max 8 partitions)
MODE = "FULL_SPARK"                   # etl_task_stage.load_mode

# BigQuery field_type -> the name used in SQL
_SQL_TYPE = {"INTEGER": "INT64", "INT64": "INT64", "FLOAT": "FLOAT64", "FLOAT64": "FLOAT64",
             "BOOLEAN": "BOOL", "BOOL": "BOOL", "NUMERIC": "NUMERIC", "BIGNUMERIC": "BIGNUMERIC",
             "STRING": "STRING", "BYTES": "BYTES", "DATE": "DATE", "DATETIME": "DATETIME",
             "TIMESTAMP": "TIMESTAMP", "TIME": "TIME", "JSON": "JSON"}


# ─────────────────────────────────────────────────────────────
# Spark
# ─────────────────────────────────────────────────────────────
def _spark(app: str):
    from pyspark.sql import SparkSession
    utc = "-Duser.timezone=UTC"
    spark = (SparkSession.builder.appName(app).master("local[2]")
             .config("spark.driver.memory", DRIVER_MEMORY)
             .config("spark.driver.extraJavaOptions", utc)
             .config("spark.executor.extraJavaOptions", utc)
             .config("spark.jars", MYSQL_JAR)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.timestampType", "TIMESTAMP_NTZ")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _jdbc(client_code: str) -> tuple[str, dict]:
    c = get_settings().client_connection(client_code)
    url = (f"jdbc:mysql://{c['host']}:{c['port']}/{c['database']}"
           "?zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=true&useCursorFetch=true"
           "&connectionTimeZone=SERVER&preserveInstants=false&forceConnectionTimeZoneToSession=false")
    props = {"user": c["user"], "password": c["password"], "driver": "com.mysql.cj.jdbc.Driver",
             "fetchsize": "10000", "preferTimestampNTZ": "true"}
    return url, props


def _partition_opts(client_code: str, sql: str, key: str | None) -> tuple[int, dict]:
    """(row count, JDBC partition options) — split on the key when it is numeric."""
    with get_engine(client_code).connect() as c:
        if key:
            n, lo, hi = c.execute(text(f"SELECT COUNT(*), MIN(`{key}`), MAX(`{key}`) FROM ({sql}) src")).fetchone()
        else:
            n, lo, hi = c.execute(text(f"SELECT COUNT(*) FROM ({sql}) src")).scalar(), None, None
    parts = min(8, max(1, (n or 0) // ROWS_PER_PARTITION + 1))
    if parts > 1 and isinstance(lo, int) and isinstance(hi, int) and hi > lo:
        return n, {"partitionColumn": key, "lowerBound": str(lo), "upperBound": str(hi), "numPartitions": str(parts)}
    return n, {}


# ─────────────────────────────────────────────────────────────
# BigQuery helpers
# ─────────────────────────────────────────────────────────────
def _load_parquet(bq, files: list[str], ref: str) -> None:
    from google.cloud import bigquery
    for i, path in enumerate(files):
        cfg = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE" if i == 0 else "WRITE_APPEND",
            decimal_target_types=["NUMERIC", "BIGNUMERIC", "STRING"])
        with open(path, "rb") as f:
            bq.load_table_from_file(f, ref, job_config=cfg).result()


def _cast(name: str, live_type: str, src_type: str | None) -> str:
    """SQL for one __next column: the __delta value converted to the live type."""
    c, lt = f"`{name}`", _SQL_TYPE.get(live_type.upper(), live_type.upper())
    st = _SQL_TYPE.get((src_type or "").upper(), (src_type or "").upper())
    if not src_type:
        return f"CAST(NULL AS {lt}) AS {c}"
    if lt == st and lt != "JSON":
        return f"{c} AS {c}"
    if lt == "JSON":                                   # SQL NULL stays NULL (not JSON 'null')
        return f"SAFE.PARSE_JSON({c}) AS {c}" if st == "STRING" else f"TO_JSON({c}) AS {c}"
    if lt == "DATETIME":
        return {"TIMESTAMP": f"DATETIME({c}, 'UTC') AS {c}", "DATE": f"DATETIME({c}) AS {c}"}.get(st, f"SAFE_CAST({c} AS DATETIME) AS {c}")
    if lt == "TIMESTAMP":
        return {"DATETIME": f"TIMESTAMP({c}, 'UTC') AS {c}", "DATE": f"TIMESTAMP({c}, 'UTC') AS {c}"}.get(st, f"SAFE_CAST({c} AS TIMESTAMP) AS {c}")
    if lt == "DATE":
        return {"DATETIME": f"DATE({c}) AS {c}", "TIMESTAMP": f"DATE({c}, 'UTC') AS {c}"}.get(st, f"SAFE_CAST({c} AS DATE) AS {c}")
    if lt == "TIME":
        return {"DATETIME": f"TIME({c}) AS {c}", "TIMESTAMP": f"TIME({c}, 'UTC') AS {c}"}.get(st, f"SAFE_CAST({c} AS TIME) AS {c}")
    if lt == "BOOL" and st in ("INT64", "NUMERIC", "BIGNUMERIC", "FLOAT64"):
        return f"({c} != 0) AS {c}"
    return f"SAFE_CAST({c} AS {lt}) AS {c}"


# ─────────────────────────────────────────────────────────────
# DAG step: one table, FULL, with Spark
# ─────────────────────────────────────────────────────────────
def spark_full_table(client_code: str, task_id: str, table: str) -> dict:
    """EXTRACT (Spark JDBC -> Parquet -> BigQuery __delta) + BUILD (__next)."""
    configure_etl_logging()
    spec = get_bigquery_pipeline(client_code)["tables"][table]
    bq = get_bigquery_client()
    live_ref = _ref(bq, client_code, table)
    delta, nxt = live_ref + "__delta", live_ref + "__next"
    stage, tmp, spark = "EXTRACT", tempfile.mkdtemp(prefix=f"bq_spark_{table}_"), None
    try:
        from google.cloud import bigquery
        live = bq.get_table(live_ref)
        _task(client_code, task_id, current_stage=f"SPARK_{table}"[:50])

        # EXTRACT: MySQL -> Spark -> Parquet
        sql = f"SELECT {', '.join(spec['select'])} FROM {spec['from']}"
        key = (spec.get("id_columns") or [None])[0]
        n_mysql, part = _partition_opts(client_code, sql, key)
        url, props = _jdbc(client_code)
        spark = _spark(f"bq_full_{client_code.lower()}_{table}")
        reader = spark.read.format("jdbc").option("url", url).option("dbtable", f"({sql}) src")
        for k, v in {**props, **part}.items():
            reader = reader.option(k, v)
        df = reader.load()
        keep = [f.name for f in live.schema if f.name in df.columns]       # columns BigQuery has
        df.select(*keep).write.mode("overwrite").parquet(tmp)
        extracted = spark.read.parquet(tmp).count()
        files = sorted(glob.glob(os.path.join(tmp, "*.parquet")))
        logger.info("----> [BQ SPARK] %s: %s rows (MySQL said %s) -> %s parquet file(s), partitions=%s",
                    table, extracted, n_mysql, len(files), part.get("numPartitions", 1))
        if extracted == 0 and (live.num_rows or 0) > 0:   # never empty a filled BigQuery table
            raise RuntimeError(f"MySQL returned 0 rows for {table} but BigQuery has {live.num_rows} — refusing to empty it")
        if extracted:
            _load_parquet(bq, files, delta)
        _stage(client_code, task_id, "EXTRACT", "SUCCESS", table=table, mode=MODE, rows=extracted)

        # BUILD: __delta -> __next with the live table's exact column types
        stage = "BUILD"
        if extracted:
            src = {f.name: f.field_type for f in bq.get_table(delta).schema}
            cols = ",\n  ".join(_cast(f.name, f.field_type, src.get(f.name)) for f in live.schema)
            bq.query(f"SELECT\n  {cols}\nFROM `{delta}`", job_config=bigquery.QueryJobConfig(
                destination=nxt, write_disposition="WRITE_TRUNCATE")).result()
        else:                                            # empty in MySQL and in BigQuery
            bq.delete_table(nxt, not_found_ok=True)
            bq.create_table(bigquery.Table(nxt, schema=live.schema))
        built = bq.get_table(nxt).num_rows
        if built != extracted:
            raise RuntimeError(f"{table}: built {built} rows but Spark extracted {extracted}")
        _stage(client_code, task_id, "BUILD", "SUCCESS", table=table, mode=MODE, rows=built)
        with get_engine(client_code).begin() as c:
            c.execute(text("UPDATE etl_task SET row_count = row_count + :n WHERE task_id = :t"),
                      {"n": extracted, "t": task_id})
        logger.info("----> [BQ SPARK] %s built: %s rows in the new table (live was %s)", table, built, live.num_rows)
        return {"table": table, "mode": MODE, "extracted": extracted, "rows": built}
    except Exception as e:
        _stage(client_code, task_id, stage, "FAILED", table=table, mode=MODE, error=e)
        logger.error("----> [BQ SPARK FAILED] %s at %s: %s", table, stage, str(e).splitlines()[0][:300])
        raise
    finally:
        if spark is not None:
            spark.stop()
        shutil.rmtree(tmp, ignore_errors=True)

"""Central logging configuration: console + rotating log file.

All logs (app + uvicorn) are written to the log file so everything is visible
in one place. The file rotates at 10 MB, keeping 5 backups.
"""
import logging
import os
from logging.handlers import RotatingFileHandler

_FILE_HANDLER_NAME = "etl_file_handler"
_FORMAT = "[%(asctime)s] [%(levelname)s] %(name)s - %(message)s"
_DATEFMT = "%Y-%m-%d %H:%M:%S"

# uvicorn's own loggers (so access + error logs also land in the file)
_UVICORN_LOGGERS = ("uvicorn", "uvicorn.error", "uvicorn.access")


def configure_logging(log_file: str = "logs/etl.log", level: str = "INFO") -> None:
    """Configure root + uvicorn logging with a console and a rotating file handler."""
    log_level = getattr(logging, str(level).upper(), logging.INFO)
    formatter = logging.Formatter(_FORMAT, datefmt=_DATEFMT)

    # Ensure the log directory exists.
    os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)

    file_handler = RotatingFileHandler(
        log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.set_name(_FILE_HANDLER_NAME)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)

    # Root logger: reset handlers so re-runs (reload) don't duplicate lines.
    root = logging.getLogger()
    root.setLevel(log_level)
    root.handlers.clear()
    root.addHandler(console_handler)
    root.addHandler(file_handler)

    # Attach the file handler to uvicorn's loggers too (avoid duplicates).
    for name in _UVICORN_LOGGERS:
        lg = logging.getLogger(name)
        if not any(getattr(h, "name", None) == _FILE_HANDLER_NAME for h in lg.handlers):
            lg.addHandler(file_handler)


_ETL_FILE_HANDLER_NAME = "etl_pipeline_file_handler"


def configure_etl_logging(log_file: str = "/opt/etl/logs/etl_pipeline.log", level: str = "INFO") -> None:
    """Route EVERY ETL log line into ONE consolidated file (in addition to the
    terminal / Airflow task log).

    Called at the start of each Airflow task so a whole run's steps — across all
    pipeline modules — are traceable in a single file. Idempotent and best-effort
    (never fails the ETL if the file can't be opened).
    """
    log_level = getattr(logging, str(level).upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(log_level)
    # quiet noisy third-party loggers so the ETL trace stays readable
    for noisy in ("azure", "azure.core.pipeline.policies.http_logging_policy",
                  "urllib3", "httpx", "httpcore", "snowflake"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    # already attached? don't duplicate lines on re-entry.
    if any(getattr(h, "name", None) == _ETL_FILE_HANDLER_NAME for h in root.handlers):
        return
    try:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        fh = RotatingFileHandler(log_file, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
        fh.set_name(_ETL_FILE_HANDLER_NAME)
        fh.setFormatter(logging.Formatter(_FORMAT, datefmt=_DATEFMT))
        fh.setLevel(log_level)
        root.addHandler(fh)
    except Exception:
        pass  # logging must never break the pipeline


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

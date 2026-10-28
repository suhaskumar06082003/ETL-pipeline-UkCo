"""BigQuery connection (analytics warehouse).

Authenticates with a Google Cloud service-account JSON key file
(settings.gcp_credentials_file, default secrets/gcp-key.json — git-ignored).
Framework-agnostic, usable from the API and from Airflow.
"""
import logging
import os

from core.settings import get_settings
from core.utils.exceptions import ConfigurationError

logger = logging.getLogger("ETL_BIGQUERY")


def _key_path(settings) -> str:
    path = settings.gcp_credentials_file
    if not os.path.isabs(path):
        # relative paths are resolved from the project root (D:\etl, /opt/etl, /app)
        root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        path = os.path.join(root, path)
    return path


def get_bigquery_client():
    """google.cloud.bigquery.Client for settings.bq_project_id / bq_location."""
    from google.cloud import bigquery
    from google.oauth2 import service_account

    s = get_settings()
    if not s.bq_project_id:
        raise ConfigurationError("BigQuery is not configured: set BQ_PROJECT_ID in .env")
    key = _key_path(s)
    if not os.path.exists(key):
        raise ConfigurationError(f"BigQuery key file not found: {key} "
                                 f"(put the service-account JSON there, or set GCP_CREDENTIALS_FILE)")
    creds = service_account.Credentials.from_service_account_file(
        key, scopes=["https://www.googleapis.com/auth/cloud-platform"])
    logger.info("----> [BIGQUERY] project=%s location=%s key=%s", s.bq_project_id, s.bq_location, key)
    return bigquery.Client(project=s.bq_project_id, credentials=creds, location=s.bq_location)

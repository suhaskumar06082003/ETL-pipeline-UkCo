"""Azure Blob Storage connection + upload (the raw / landing zone).

Ported from retailverse ETL `core/services/blob_service.py`. Framework-agnostic
(raises domain LoadError) so it can be used from the API and from Airflow.
"""
import logging

from azure.storage.blob import BlobServiceClient, ContentSettings

from core.settings import Settings
from core.utils.exceptions import ConfigurationError, LoadError

logger = logging.getLogger("ETL_BLOB")
# the SDK logs every HTTP request's headers at INFO — keep only warnings
logging.getLogger("azure.core.pipeline.policies.http_logging_policy").setLevel(logging.WARNING)

# Large files (e.g. a 26 MB outlet parquet) go up in 4 MB blocks, several in
# parallel, each with its own timeout + retry — a single 26 MB PUT timed out
# on a slow uplink ("The write operation timed out").
_BLOCK_SIZE = 4 * 1024 * 1024
_UPLOAD_CONCURRENCY = 4


class AzureBlobConnection:
    """Thin wrapper around the Azure Blob SDK."""

    def __init__(self, settings: Settings) -> None:
        self.connection_string = settings.azure_storage_connection_string
        self.container_name = settings.azure_storage_container_name
        if not self.connection_string:
            raise ConfigurationError(
                "Azure Blob Storage is not configured. "
                "Set AZURE_STORAGE_CONNECTION_STRING in your .env file."
            )

    def get_blob_service(self) -> BlobServiceClient:
        """Client for Azure Blob Storage."""
        try:
            return BlobServiceClient.from_connection_string(
                self.connection_string,
                max_single_put_size=_BLOCK_SIZE,   # bigger than this -> block upload
                max_block_size=_BLOCK_SIZE,
                connection_timeout=60,             # seconds to connect
                read_timeout=300,                  # seconds per block request
            )
        except Exception as e:
            logger.error(f"Azure Blob Storage: {e}")
            raise LoadError(f"Could not connect to Azure Blob Storage: {e}") from e

    def _ensure_container(self, blob_service_client: BlobServiceClient) -> None:
        container_client = blob_service_client.get_container_client(self.container_name)
        if not container_client.exists():
            container_client.create_container()

    def upload_blob(
        self,
        file_data: bytes,
        blob_name: str,
        content_type: str,
        folder_path: str | None = None,
        overwrite: bool = True,
    ) -> str:
        """Upload a blob and return its URL.

        With ``folder_path`` the blob is stored at ``<folder_path>/<blob_name>``.
        ``overwrite=False`` refuses to replace an existing blob (daily-folder
        uploads must never overwrite an earlier file).
        """
        if not file_data or not blob_name or not content_type:
            raise LoadError("file_data, blob_name and content_type are required")
        if folder_path:
            blob_name = f"{folder_path}/{blob_name}"
        try:
            blob_service_client = self.get_blob_service()
            self._ensure_container(blob_service_client)
            blob_client = blob_service_client.get_blob_client(
                container=self.container_name, blob=blob_name
            )
            blob_client.upload_blob(
                file_data,
                overwrite=overwrite,
                content_settings=ContentSettings(content_type=content_type),
                max_concurrency=_UPLOAD_CONCURRENCY,
            )
            return blob_client.url
        except LoadError:
            raise
        except Exception as e:
            raise LoadError(f"Error uploading to {self.container_name}: {e}") from e

    def download_blob(self, blob_name: str) -> bytes:
        """Download a blob's bytes (used by the Airflow load stage to read Parquet)."""
        try:
            blob_service_client = self.get_blob_service()
            blob_client = blob_service_client.get_blob_client(
                container=self.container_name, blob=blob_name
            )
            return blob_client.download_blob().readall()
        except LoadError:
            raise
        except Exception as e:
            raise LoadError(f"Error downloading {blob_name} from {self.container_name}: {e}") from e

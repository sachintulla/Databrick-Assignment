"""
Azure Blob Storage Client — SuperStore Analytics System.

Handles all raw file uploads/downloads to Azure Blob Storage.
The blob container mirrors the local data/ layout:

  superstore-analytics/
  ├── raw/superstore_sales.csv
  ├── processed/superstore_clean.parquet
  └── output/
      ├── charts/kpi_summary_card.png
      └── analytics/monthly_trend.csv

Authentication priority (first available wins):
  1. AZURE_STORAGE_ACCOUNT_KEY  (storage key)
  2. AZURE_CLIENT_ID + AZURE_CLIENT_SECRET + AZURE_TENANT_ID  (service principal)
  3. DefaultAzureCredential  (managed identity / CLI login)

Usage:
    from src.storage.azure_blob import AzureBlobClient
    client = AzureBlobClient()
    client.upload_raw_data()
    client.upload_processed_data()
    client.upload_outputs()
    client.download_file("raw/superstore_sales.csv", "/tmp/sales.csv")
"""

import os
from pathlib import Path
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _load_env():
    """Load .env file if present (dev convenience)."""
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parents[2] / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass


class AzureBlobClient:
    """
    Azure Blob Storage operations for the SuperStore Analytics System.

    Args:
        account_name: Override AZURE_STORAGE_ACCOUNT_NAME env var.
        container_name: Override AZURE_BLOB_CONTAINER env var.
        account_key: Override AZURE_STORAGE_ACCOUNT_KEY env var.
        config_path: Optional path to config.yaml.
    """

    def __init__(
        self,
        account_name: Optional[str] = None,
        container_name: Optional[str] = None,
        account_key: Optional[str] = None,
        config_path: Optional[str] = None,
    ):
        _load_env()

        from src.utils.config import load_config, get_path
        self.config = load_config(config_path)
        self.project_root = Path(__file__).resolve().parents[2]

        self.account_name = account_name or os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
        self.container_name = container_name or os.environ.get(
            "AZURE_BLOB_CONTAINER", "superstore-analytics"
        )
        self._account_key = account_key or os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

        self._client = self._build_service_client()
        self._container = self._ensure_container()

        logger.info(
            f"AzureBlobClient connected: "
            f"account={self.account_name}, container={self.container_name}"
        )

    # ── Connection ────────────────────────────────────────────────────────

    def _build_service_client(self):
        """Build BlobServiceClient using the best available credential."""
        from azure.storage.blob import BlobServiceClient

        url = f"https://{self.account_name}.blob.core.windows.net"

        if self._account_key:
            logger.debug("Authenticating with storage account key.")
            return BlobServiceClient(account_url=url, credential=self._account_key)

        client_id = os.environ.get("AZURE_CLIENT_ID")
        client_secret = os.environ.get("AZURE_CLIENT_SECRET")
        tenant_id = os.environ.get("AZURE_TENANT_ID")

        if client_id and client_secret and tenant_id:
            from azure.identity import ClientSecretCredential
            logger.debug("Authenticating with Service Principal.")
            cred = ClientSecretCredential(tenant_id, client_id, client_secret)
            return BlobServiceClient(account_url=url, credential=cred)

        from azure.identity import DefaultAzureCredential
        logger.debug("Authenticating with DefaultAzureCredential.")
        return BlobServiceClient(account_url=url, credential=DefaultAzureCredential())

    def _ensure_container(self):
        """Return container client, creating the container if it does not exist."""
        from azure.core.exceptions import ResourceExistsError
        container_client = self._client.get_container_client(self.container_name)
        try:
            container_client.create_container()
            logger.info(f"Container '{self.container_name}' created.")
        except ResourceExistsError:
            pass
        return container_client

    # ── Upload helpers ────────────────────────────────────────────────────

    def upload_file(
        self,
        local_path: str,
        blob_path: str,
        overwrite: bool = True,
    ) -> str:
        """
        Upload a single local file to blob storage.

        Args:
            local_path: Absolute local file path.
            blob_path: Target blob name (path within container).
            overwrite: Overwrite existing blob.

        Returns:
            Full blob URL.
        """
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        blob_client = self._container.get_blob_client(blob_path)
        with open(local_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=overwrite)

        url = blob_client.url
        logger.info(f"Uploaded: {local_path.name} → {blob_path}  ({local_path.stat().st_size:,} bytes)")
        return url

    def upload_directory(
        self,
        local_dir: str,
        blob_prefix: str,
        pattern: str = "*",
        overwrite: bool = True,
    ) -> list[str]:
        """
        Upload all files matching a glob pattern from a local directory.

        Args:
            local_dir: Local directory path.
            blob_prefix: Blob path prefix (folder within container).
            pattern: Glob pattern, e.g. "*.csv" or "*.png".
            overwrite: Overwrite existing blobs.

        Returns:
            List of uploaded blob URLs.
        """
        local_dir = Path(local_dir)
        files = list(local_dir.glob(pattern))
        if not files:
            logger.warning(f"No files matched '{pattern}' in {local_dir}")
            return []

        urls = []
        for f in files:
            blob_path = f"{blob_prefix}/{f.name}"
            url = self.upload_file(str(f), blob_path, overwrite=overwrite)
            urls.append(url)

        logger.info(f"Uploaded {len(urls)} files to blob prefix '{blob_prefix}'.")
        return urls

    # ── Convenience upload methods ────────────────────────────────────────

    def upload_raw_data(self) -> str:
        """Upload raw CSV to blob raw/ folder."""
        raw_dir = self.project_root / self.config["paths"]["raw_data_dir"]
        raw_file = self.config["data"]["raw_file"]
        return self.upload_file(str(raw_dir / raw_file), f"raw/{raw_file}")

    def upload_processed_data(self) -> str:
        """Upload cleaned Parquet to blob processed/ folder."""
        proc_dir = self.project_root / self.config["paths"]["processed_data_dir"]
        proc_file = self.config["data"]["processed_file"]
        return self.upload_file(str(proc_dir / proc_file), f"processed/{proc_file}")

    def upload_outputs(self) -> dict[str, list[str]]:
        """Upload all analytics CSVs and dashboard PNGs to blob output/."""
        output_dir = self.project_root / self.config["paths"]["output_dir"]
        results = {
            "charts": self.upload_directory(str(output_dir), "output/charts", "*.png"),
            "analytics": self.upload_directory(str(output_dir), "output/analytics", "*.csv"),
        }
        return results

    def upload_all(self) -> dict:
        """Full upload: raw + processed + outputs."""
        logger.info("Starting full blob upload ...")
        result = {
            "raw": self.upload_raw_data(),
            "processed": self.upload_processed_data(),
            "outputs": self.upload_outputs(),
        }
        logger.info("Full blob upload complete.")
        return result

    # ── Download ──────────────────────────────────────────────────────────

    def download_file(self, blob_path: str, local_path: str) -> str:
        """
        Download a blob to a local file.

        Args:
            blob_path: Blob name within the container.
            local_path: Local destination path.

        Returns:
            Local path where file was written.
        """
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        blob_client = self._container.get_blob_client(blob_path)
        with open(local_path, "wb") as f:
            stream = blob_client.download_blob()
            stream.readinto(f)
        logger.info(f"Downloaded: {blob_path} → {local_path}")
        return local_path

    # ── Listing ───────────────────────────────────────────────────────────

    def list_blobs(self, prefix: str = "") -> list[str]:
        """List all blob names under the given prefix."""
        return [b.name for b in self._container.list_blobs(name_starts_with=prefix)]

    def get_blob_url(self, blob_path: str) -> str:
        """Return the public URL for a blob (works if container is public)."""
        return self._container.get_blob_client(blob_path).url

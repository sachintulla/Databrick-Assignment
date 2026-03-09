"""
Azure Data Lake Storage Gen2 Client — SuperStore Analytics System.

Implements the Medallion Architecture on ADLS Gen2:

  superstore/  (filesystem / container)
  ├── bronze/   ← raw ingested CSV (as-is from source)
  ├── silver/   ← cleaned Parquet (schema enforced, nulls filled, types cast)
  ├── gold/     ← analytics aggregations (monthly_trend, category_perf, etc.)
  └── dashboard/← dashboard PNG charts

ADLS Gen2 uses a hierarchical namespace which enables:
  - Directory-level access control (POSIX ACLs)
  - Atomic rename / move operations
  - Native Spark / Databricks integration via abfss:// protocol

Authentication priority (same as AzureBlobClient):
  1. AZURE_STORAGE_ACCOUNT_KEY
  2. Service Principal (AZURE_CLIENT_ID + AZURE_CLIENT_SECRET + AZURE_TENANT_ID)
  3. DefaultAzureCredential

Usage:
    from src.storage.adls_client import ADLSClient
    adls = ADLSClient()
    adls.upload_bronze()          # raw CSV  → bronze/
    adls.upload_silver()          # Parquet  → silver/
    adls.upload_gold()            # CSVs     → gold/
    adls.upload_dashboard()       # PNGs     → dashboard/
    adls.upload_all()             # all four layers

    # Get abfss:// paths for Databricks notebooks
    paths = adls.get_databricks_paths()
"""

import io
import os
from pathlib import Path
from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


def _load_env():
    try:
        from dotenv import load_dotenv
        env_path = Path(__file__).resolve().parents[2] / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        pass


class ADLSClient:
    """
    Azure Data Lake Storage Gen2 client using the Medallion Architecture.

    Args:
        account_name: Override AZURE_ADLS_ACCOUNT_NAME env var.
        filesystem: Override AZURE_ADLS_FILESYSTEM env var (= container name).
        account_key: Override AZURE_STORAGE_ACCOUNT_KEY env var.
        config_path: Optional path to config.yaml.
    """

    def __init__(
        self,
        account_name: Optional[str] = None,
        filesystem: Optional[str] = None,
        account_key: Optional[str] = None,
        config_path: Optional[str] = None,
    ):
        _load_env()

        from src.utils.config import load_config
        self.config = load_config(config_path)
        self.project_root = Path(__file__).resolve().parents[2]

        self.account_name = (
            account_name
            or os.environ.get("AZURE_ADLS_ACCOUNT_NAME")
            or os.environ["AZURE_STORAGE_ACCOUNT_NAME"]
        )
        self.filesystem = filesystem or os.environ.get("AZURE_ADLS_FILESYSTEM", "superstore")
        self._account_key = (
            account_key
            or os.environ.get("AZURE_ADLS_ACCOUNT_KEY")
            or os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
        )

        self._service_client = self._build_service_client()
        self._fs_client = self._ensure_filesystem()

        logger.info(
            f"ADLSClient connected: account={self.account_name}, "
            f"filesystem={self.filesystem}"
        )

    # ── Connection ────────────────────────────────────────────────────────

    def _build_service_client(self):
        from azure.storage.filedatalake import DataLakeServiceClient

        url = f"https://{self.account_name}.dfs.core.windows.net"

        if self._account_key:
            logger.debug("ADLS: authenticating with storage account key.")
            return DataLakeServiceClient(account_url=url, credential=self._account_key)

        client_id = os.environ.get("AZURE_CLIENT_ID")
        client_secret = os.environ.get("AZURE_CLIENT_SECRET")
        tenant_id = os.environ.get("AZURE_TENANT_ID")

        if client_id and client_secret and tenant_id:
            from azure.identity import ClientSecretCredential
            logger.debug("ADLS: authenticating with Service Principal.")
            cred = ClientSecretCredential(tenant_id, client_id, client_secret)
            return DataLakeServiceClient(account_url=url, credential=cred)

        from azure.identity import DefaultAzureCredential
        logger.debug("ADLS: authenticating with DefaultAzureCredential.")
        return DataLakeServiceClient(
            account_url=url, credential=DefaultAzureCredential()
        )

    def _ensure_filesystem(self):
        """Return filesystem client, creating it if it does not exist."""
        from azure.core.exceptions import ResourceExistsError
        fs_client = self._service_client.get_file_system_client(self.filesystem)
        try:
            fs_client.create_file_system()
            logger.info(f"ADLS filesystem '{self.filesystem}' created.")
        except ResourceExistsError:
            pass
        return fs_client

    # ── Core upload method ────────────────────────────────────────────────

    def upload_file(
        self,
        local_path: str,
        adls_path: str,
        overwrite: bool = True,
    ) -> str:
        """
        Upload a single local file to ADLS Gen2.

        Args:
            local_path: Absolute local path.
            adls_path: Target path within the filesystem (e.g. "bronze/sales.csv").
            overwrite: Overwrite if exists.

        Returns:
            abfss:// URI for use in Databricks / Spark.
        """
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        # Ensure parent directory exists in ADLS
        parent = str(Path(adls_path).parent)
        if parent and parent != ".":
            try:
                self._fs_client.create_directory(parent)
            except Exception:
                pass  # directory may already exist

        file_client = self._fs_client.get_file_client(adls_path)
        with open(local_path, "rb") as f:
            data = f.read()

        file_client.upload_data(data, overwrite=overwrite)
        size = local_path.stat().st_size

        abfss_uri = f"abfss://{self.filesystem}@{self.account_name}.dfs.core.windows.net/{adls_path}"
        logger.info(
            f"ADLS upload: {local_path.name} → {adls_path}  "
            f"({size:,} bytes)  [{abfss_uri}]"
        )
        return abfss_uri

    def upload_directory(
        self,
        local_dir: str,
        adls_prefix: str,
        pattern: str = "*",
        overwrite: bool = True,
    ) -> list[str]:
        """Upload all files matching pattern from a local directory."""
        local_dir = Path(local_dir)
        files = list(local_dir.glob(pattern))
        if not files:
            logger.warning(f"No files matched '{pattern}' in {local_dir}")
            return []

        uris = []
        for f in files:
            uri = self.upload_file(str(f), f"{adls_prefix}/{f.name}", overwrite=overwrite)
            uris.append(uri)

        logger.info(f"ADLS: uploaded {len(uris)} files to '{adls_prefix}'.")
        return uris

    # ── Medallion layer uploads ───────────────────────────────────────────

    def upload_bronze(self) -> str:
        """
        Bronze layer — upload raw CSV as-is.
        Path: bronze/superstore_sales.csv
        """
        raw_dir = self.project_root / self.config["paths"]["raw_data_dir"]
        raw_file = self.config["data"]["raw_file"]
        uri = self.upload_file(str(raw_dir / raw_file), f"bronze/{raw_file}")
        logger.info(f"Bronze layer populated: {uri}")
        return uri

    def upload_silver(self) -> str:
        """
        Silver layer — upload cleaned Parquet.
        Path: silver/superstore_clean.parquet
        """
        proc_dir = self.project_root / self.config["paths"]["processed_data_dir"]
        proc_file = self.config["data"]["processed_file"]
        uri = self.upload_file(str(proc_dir / proc_file), f"silver/{proc_file}")
        logger.info(f"Silver layer populated: {uri}")
        return uri

    def upload_gold(self) -> list[str]:
        """
        Gold layer — upload all analytics CSVs (aggregations).
        Path: gold/<filename>.csv
        """
        output_dir = self.project_root / self.config["paths"]["output_dir"]
        uris = self.upload_directory(str(output_dir), "gold", "*.csv")
        logger.info(f"Gold layer populated: {len(uris)} aggregation files.")
        return uris

    def upload_dashboard(self) -> list[str]:
        """
        Dashboard layer — upload PNG charts.
        Path: dashboard/<filename>.png
        """
        output_dir = self.project_root / self.config["paths"]["output_dir"]
        uris = self.upload_directory(str(output_dir), "dashboard", "*.png")
        logger.info(f"Dashboard layer populated: {len(uris)} charts.")
        return uris

    def upload_all(self) -> dict:
        """
        Full medallion upload: Bronze → Silver → Gold → Dashboard.

        Returns:
            Dict mapping layer name to uploaded URI(s).
        """
        logger.info("Starting ADLS Medallion upload (Bronze → Silver → Gold → Dashboard) ...")
        result = {
            "bronze": self.upload_bronze(),
            "silver": self.upload_silver(),
            "gold":   self.upload_gold(),
            "dashboard": self.upload_dashboard(),
        }
        logger.info(
            f"Medallion upload complete. "
            f"Gold files: {len(result['gold'])}, "
            f"Dashboard charts: {len(result['dashboard'])}"
        )
        return result

    # ── Download ──────────────────────────────────────────────────────────

    def download_file(self, adls_path: str, local_path: str) -> str:
        """Download a file from ADLS Gen2 to local disk."""
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        file_client = self._fs_client.get_file_client(adls_path)
        with open(local_path, "wb") as f:
            download = file_client.download_file()
            download.readinto(f)
        logger.info(f"ADLS download: {adls_path} → {local_path}")
        return local_path

    def read_parquet_to_df(self, adls_path: str):
        """
        Read a Parquet file from ADLS directly into a pandas DataFrame
        without writing to local disk.
        """
        import pandas as pd

        file_client = self._fs_client.get_file_client(adls_path)
        download = file_client.download_file()
        buffer = io.BytesIO(download.readall())
        df = pd.read_parquet(buffer)
        logger.info(f"Read Parquet from ADLS: {adls_path}  ({len(df):,} rows)")
        return df

    # ── Listing & path helpers ────────────────────────────────────────────

    def list_paths(self, directory: str = "", recursive: bool = True) -> list[str]:
        """List all paths under a directory in the filesystem."""
        return [
            p.name
            for p in self._fs_client.get_paths(path=directory, recursive=recursive)
        ]

    def get_databricks_paths(self) -> dict:
        """
        Return the abfss:// URIs for each medallion layer.
        Use these directly in Databricks notebooks or PySpark code.

        Example:
            spark.read.parquet(paths["silver"])
        """
        base = f"abfss://{self.filesystem}@{self.account_name}.dfs.core.windows.net"
        return {
            "bronze":    f"{base}/bronze/",
            "silver":    f"{base}/silver/",
            "gold":      f"{base}/gold/",
            "dashboard": f"{base}/dashboard/",
        }

    def get_abfss_uri(self, adls_path: str) -> str:
        """Build the full abfss:// URI for a given ADLS path."""
        return (
            f"abfss://{self.filesystem}@{self.account_name}.dfs.core.windows.net"
            f"/{adls_path}"
        )

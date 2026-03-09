"""
Azure Databricks Client — SuperStore Analytics System.

Provides:
  1. DBFS (Databricks File System) uploads — sync local files to /dbfs/
  2. Notebook execution — trigger analytics notebooks via Jobs API
  3. Cluster management — start/stop cluster, get cluster status
  4. Delta Lake helpers — create / refresh Delta tables from ADLS silver layer
  5. Mount helpers — mount ADLS Gen2 onto Databricks /mnt/ path
  6. SQL analytics — run SQL queries via the Databricks SQL Connector

DBFS path layout mirrors ADLS medallion:
  /dbfs/superstore/
  ├── bronze/superstore_sales.csv
  ├── silver/superstore_clean.parquet
  ├── gold/<analytics CSVs>
  └── dashboard/<PNG charts>

Authentication:
  - DATABRICKS_HOST  (https://<workspace>.azuredatabricks.net)
  - DATABRICKS_TOKEN (personal access token or service principal token)

Usage:
    from src.storage.databricks_client import DatabricksClient
    dbc = DatabricksClient()
    dbc.upload_to_dbfs_all()
    dbc.run_notebook("/Shared/superstore/03_analytics", timeout=600)
"""

import os
import time
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


class DatabricksClient:
    """
    Azure Databricks workspace operations for the SuperStore Analytics System.

    Args:
        host: Override DATABRICKS_HOST env var.
        token: Override DATABRICKS_TOKEN env var.
        dbfs_root: Override DATABRICKS_DBFS_ROOT env var.
        config_path: Optional path to config.yaml.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        dbfs_root: Optional[str] = None,
        config_path: Optional[str] = None,
    ):
        _load_env()

        from src.utils.config import load_config
        self.config = load_config(config_path)
        self.project_root = Path(__file__).resolve().parents[2]

        self.host = (host or os.environ["DATABRICKS_HOST"]).rstrip("/")
        self.token = token or os.environ["DATABRICKS_TOKEN"]
        self.dbfs_root = dbfs_root or os.environ.get(
            "DATABRICKS_DBFS_ROOT", "/dbfs/superstore"
        )
        self.cluster_id = os.environ.get("DATABRICKS_CLUSTER_ID")

        self._sdk_client = self._build_sdk_client()
        logger.info(f"DatabricksClient connected: {self.host}")

    # ── SDK client ────────────────────────────────────────────────────────

    def _build_sdk_client(self):
        """Build Databricks SDK WorkspaceClient."""
        from databricks.sdk import WorkspaceClient
        return WorkspaceClient(host=self.host, token=self.token)

    # ── DBFS uploads ──────────────────────────────────────────────────────

    def upload_to_dbfs(self, local_path: str, dbfs_path: str, overwrite: bool = True) -> str:
        """
        Upload a single local file to DBFS.

        Args:
            local_path: Absolute local path.
            dbfs_path: Target DBFS path (e.g. /dbfs/superstore/bronze/sales.csv).
            overwrite: Overwrite if exists.

        Returns:
            DBFS path where file was written.
        """
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Local file not found: {local_path}")

        import base64
        MAX_BLOCK = 1 * 1024 * 1024  # 1 MB per block

        with open(local_path, "rb") as f:
            data = f.read()

        if len(data) <= MAX_BLOCK:
            # Small file — single put call
            self._sdk_client.dbfs.put(
                path=dbfs_path,
                contents=base64.b64encode(data).decode(),
                overwrite=overwrite,
            )
        else:
            # Large file — streaming upload (create → addBlock* → close)
            handle_resp = self._sdk_client.dbfs.create(path=dbfs_path, overwrite=overwrite)
            handle = handle_resp.handle
            try:
                offset = 0
                while offset < len(data):
                    chunk = data[offset: offset + MAX_BLOCK]
                    self._sdk_client.dbfs.add_block(
                        handle=handle,
                        data=base64.b64encode(chunk).decode(),
                    )
                    offset += MAX_BLOCK
            finally:
                self._sdk_client.dbfs.close(handle=handle)

        size = local_path.stat().st_size
        logger.info(f"DBFS upload: {local_path.name} → {dbfs_path}  ({size:,} bytes)")
        return dbfs_path

    def upload_directory_to_dbfs(
        self,
        local_dir: str,
        dbfs_prefix: str,
        pattern: str = "*",
        overwrite: bool = True,
    ) -> list[str]:
        """Upload all files matching pattern from a local directory to DBFS."""
        local_dir = Path(local_dir)
        files = list(local_dir.glob(pattern))
        if not files:
            logger.warning(f"No files matched '{pattern}' in {local_dir}")
            return []

        paths = []
        for f in files:
            dbfs_path = f"{dbfs_prefix}/{f.name}"
            self.upload_to_dbfs(str(f), dbfs_path, overwrite=overwrite)
            paths.append(dbfs_path)

        logger.info(f"DBFS: uploaded {len(paths)} files to '{dbfs_prefix}'.")
        return paths

    def upload_to_dbfs_all(self) -> dict:
        """
        Upload all data layers to DBFS mirroring the medallion layout.

        Returns:
            Dict of layer → list of DBFS paths.
        """
        logger.info("Uploading all layers to DBFS ...")
        raw_dir    = self.project_root / self.config["paths"]["raw_data_dir"]
        proc_dir   = self.project_root / self.config["paths"]["processed_data_dir"]
        output_dir = self.project_root / self.config["paths"]["output_dir"]

        raw_file  = self.config["data"]["raw_file"]
        proc_file = self.config["data"]["processed_file"]

        result = {
            "bronze": [self.upload_to_dbfs(
                str(raw_dir / raw_file),
                f"{self.dbfs_root}/bronze/{raw_file}",
            )],
            "silver": [self.upload_to_dbfs(
                str(proc_dir / proc_file),
                f"{self.dbfs_root}/silver/{proc_file}",
            )],
            "gold": self.upload_directory_to_dbfs(
                str(output_dir), f"{self.dbfs_root}/gold", "*.csv"
            ),
            "dashboard": self.upload_directory_to_dbfs(
                str(output_dir), f"{self.dbfs_root}/dashboard", "*.png"
            ),
        }
        logger.info("DBFS full upload complete.")
        return result

    # ── Cluster management ────────────────────────────────────────────────

    def get_cluster_state(self, cluster_id: Optional[str] = None) -> str:
        """
        Get the current state of a Databricks cluster.

        Returns:
            State string: RUNNING, TERMINATED, PENDING, RESTARTING, etc.
        """
        cid = cluster_id or self.cluster_id
        if not cid:
            raise ValueError("cluster_id not set — provide via env DATABRICKS_CLUSTER_ID")
        info = self._sdk_client.clusters.get(cluster_id=cid)
        state = info.state.value
        logger.info(f"Cluster {cid} state: {state}")
        return state

    def start_cluster(self, cluster_id: Optional[str] = None, wait: bool = True) -> str:
        """
        Start a terminated cluster.

        Args:
            cluster_id: Cluster ID to start.
            wait: Block until cluster reaches RUNNING state.

        Returns:
            Final cluster state.
        """
        cid = cluster_id or self.cluster_id
        state = self.get_cluster_state(cid)

        if state == "RUNNING":
            logger.info(f"Cluster {cid} is already RUNNING.")
            return state

        logger.info(f"Starting cluster {cid} ...")
        self._sdk_client.clusters.start(cluster_id=cid)

        if wait:
            for _ in range(60):  # wait up to 10 minutes
                time.sleep(10)
                state = self.get_cluster_state(cid)
                if state == "RUNNING":
                    logger.info(f"Cluster {cid} is RUNNING.")
                    return state
                logger.info(f"  Waiting for cluster ... current state: {state}")
            raise TimeoutError(f"Cluster {cid} did not reach RUNNING within 10 minutes.")

        return self.get_cluster_state(cid)

    def list_clusters(self) -> list[dict]:
        """List all clusters in the workspace."""
        clusters = list(self._sdk_client.clusters.list())
        result = [
            {
                "cluster_id": c.cluster_id,
                "cluster_name": c.cluster_name,
                "state": c.state.value if c.state else "UNKNOWN",
                "spark_version": c.spark_version,
            }
            for c in clusters
        ]
        logger.info(f"Found {len(result)} clusters.")
        return result

    # ── Notebook execution ────────────────────────────────────────────────

    def run_notebook(
        self,
        notebook_path: str,
        cluster_id: Optional[str] = None,
        parameters: Optional[dict] = None,
        timeout: int = 1800,
    ) -> dict:
        """
        Run a Databricks notebook as a one-time job run.

        Args:
            notebook_path: Workspace path (e.g. /Shared/superstore/03_analytics).
            cluster_id: Use existing cluster. If None, creates a new job cluster.
            parameters: Dict of notebook widget parameters.
            timeout: Max wait time in seconds.

        Returns:
            Dict with run_id, state, result_state, run_page_url.
        """
        from databricks.sdk.service.jobs import (
            NotebookTask,
            RunSubmitTaskSettings,
            TaskDependency,
        )

        cid = cluster_id or self.cluster_id
        task_settings = {
            "task_key": "superstore_notebook",
            "notebook_task": NotebookTask(
                notebook_path=notebook_path,
                base_parameters=parameters or {},
            ),
        }
        if cid:
            task_settings["existing_cluster_id"] = cid

        logger.info(f"Submitting notebook run: {notebook_path}")
        run = self._sdk_client.jobs.submit(
            run_name=f"SuperStore: {Path(notebook_path).name}",
            tasks=[RunSubmitTaskSettings(**task_settings)],
        )
        run_id = run.run_id
        logger.info(f"Run submitted: run_id={run_id}")

        # Poll for completion
        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(15)
            run_info = self._sdk_client.jobs.get_run(run_id=run_id)
            life_cycle = run_info.state.life_cycle_state.value
            logger.info(f"  Run {run_id} state: {life_cycle}")

            if life_cycle in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
                result_state = run_info.state.result_state.value if run_info.state.result_state else "UNKNOWN"
                outcome = {
                    "run_id": run_id,
                    "state": life_cycle,
                    "result_state": result_state,
                    "run_page_url": run_info.run_page_url,
                }
                if result_state != "SUCCESS":
                    logger.error(f"Notebook run failed: {outcome}")
                else:
                    logger.info(f"Notebook run succeeded: {outcome}")
                return outcome

        raise TimeoutError(f"Notebook run {run_id} did not complete within {timeout}s.")

    def run_analytics_pipeline(self) -> list[dict]:
        """
        Sequentially run all four SuperStore notebooks in the Databricks workspace.
        Notebooks must be imported at /Shared/superstore/.

        Returns:
            List of run result dicts.
        """
        notebooks = [
            "/Shared/superstore/01_data_exploration",
            "/Shared/superstore/02_etl_pipeline",
            "/Shared/superstore/03_analytics",
            "/Shared/superstore/04_dashboard",
        ]
        results = []
        for nb in notebooks:
            logger.info(f"Running notebook: {nb}")
            result = self.run_notebook(nb)
            results.append(result)
            if result.get("result_state") != "SUCCESS":
                logger.error(f"Pipeline stopped at {nb}: {result}")
                break
        return results

    # ── ADLS Mount helper ─────────────────────────────────────────────────

    def generate_mount_notebook_code(
        self,
        adls_account: Optional[str] = None,
        adls_filesystem: Optional[str] = None,
        mount_point: str = "/mnt/superstore",
    ) -> str:
        """
        Generate PySpark code to mount ADLS Gen2 onto Databricks /mnt/.

        This code should be pasted into a Databricks notebook cell and run once
        to mount the data lake. After mounting, use /mnt/superstore/ paths
        instead of abfss:// URIs.

        Returns:
            Python string containing the mount code.
        """
        _load_env()
        account = adls_account or os.environ.get("AZURE_ADLS_ACCOUNT_NAME", "<YOUR_ACCOUNT>")
        fs = adls_filesystem or os.environ.get("AZURE_ADLS_FILESYSTEM", "superstore")
        tenant_id = os.environ.get("AZURE_TENANT_ID", "<YOUR_TENANT_ID>")
        client_id = os.environ.get("AZURE_CLIENT_ID", "<YOUR_CLIENT_ID>")

        code = f'''# ── Mount ADLS Gen2 to Databricks /mnt/superstore ─────────────────────
# Run this cell ONCE in a Databricks notebook to mount the data lake.

configs = {{
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type":
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "{client_id}",
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(
        scope="superstore-secrets", key="adls-client-secret"
    ),
    "fs.azure.account.oauth2.client.endpoint":
        "https://login.microsoftonline.com/{tenant_id}/oauth2/token",
}}

# Unmount if already mounted
for m in dbutils.fs.mounts():
    if m.mountPoint == "{mount_point}":
        dbutils.fs.unmount("{mount_point}")
        print("Unmounted existing mount.")
        break

dbutils.fs.mount(
    source="abfss://{fs}@{account}.dfs.core.windows.net/",
    mount_point="{mount_point}",
    extra_configs=configs,
)
print("ADLS Gen2 mounted at {mount_point}")

# Verify
display(dbutils.fs.ls("{mount_point}"))
'''
        return code

    # ── Delta Lake helpers ────────────────────────────────────────────────

    def generate_delta_table_notebook_code(self) -> str:
        """
        Generate PySpark code to create Delta Lake tables from ADLS silver layer.
        Run this in a Databricks notebook after mounting ADLS.

        Returns:
            Python string containing the Delta table creation code.
        """
        adls_account = os.environ.get("AZURE_ADLS_ACCOUNT_NAME", "<YOUR_ACCOUNT>")
        adls_fs = os.environ.get("AZURE_ADLS_FILESYSTEM", "superstore")

        code = f'''# ── Create Delta Lake tables from ADLS Silver layer ──────────────────
# Requires: ADLS mounted at /mnt/superstore  OR  abfss:// URI

silver_path = "abfss://{adls_fs}@{adls_account}.dfs.core.windows.net/silver/"
gold_path   = "abfss://{adls_fs}@{adls_account}.dfs.core.windows.net/gold/"
delta_path  = "abfss://{adls_fs}@{adls_account}.dfs.core.windows.net/delta/"

# 1. Read silver Parquet and register as Delta table
df_silver = spark.read.parquet(silver_path + "superstore_clean.parquet")
df_silver.write.format("delta").mode("overwrite").save(delta_path + "superstore_clean/")

spark.sql("""
    CREATE TABLE IF NOT EXISTS superstore_clean
    USING DELTA
    LOCATION '{delta_path}superstore_clean/'
""")
print("Delta table superstore_clean created.")

# 2. Create gold aggregation Delta tables
monthly = df_silver.groupBy(
    F.date_format("Order Date", "yyyy-MM").alias("year_month")
).agg(
    F.round(F.sum("Sales"), 2).alias("sales"),
    F.round(F.sum("Profit"), 2).alias("profit"),
    F.countDistinct("Order ID").alias("orders"),
)
monthly.write.format("delta").mode("overwrite").save(delta_path + "monthly_trend/")

spark.sql("""
    CREATE TABLE IF NOT EXISTS monthly_trend
    USING DELTA
    LOCATION '{delta_path}monthly_trend/'
""")
print("Delta table monthly_trend created.")

# 3. Optimize Delta tables
spark.sql("OPTIMIZE superstore_clean ZORDER BY (Category, Region)")
spark.sql("OPTIMIZE monthly_trend")

print("All Delta tables created and optimized.")
spark.sql("SHOW TABLES").show()
'''
        return code

    # ── Listing ───────────────────────────────────────────────────────────

    def list_dbfs(self, path: str = "/dbfs/superstore") -> list[dict]:
        """List files and directories in DBFS."""
        try:
            items = list(self._sdk_client.dbfs.list(path=path))
            return [
                {
                    "path": i.path,
                    "is_dir": i.is_dir,
                    "file_size": i.file_size,
                }
                for i in items
            ]
        except Exception as exc:
            logger.warning(f"Could not list DBFS path {path}: {exc}")
            return []

    def list_workspace_notebooks(self, path: str = "/Shared") -> list[dict]:
        """List notebooks in the Databricks workspace."""
        try:
            items = list(self._sdk_client.workspace.list(path=path))
            return [
                {"path": i.path, "type": i.object_type.value}
                for i in items or []
            ]
        except Exception as exc:
            logger.warning(f"Could not list workspace path {path}: {exc}")
            return []

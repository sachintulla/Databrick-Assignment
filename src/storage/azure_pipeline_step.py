"""
Azure Pipeline Step — SuperStore Analytics System.

Single callable that runs all Azure uploads in order:
  1. Azure Blob Storage  → raw + processed + outputs
  2. ADLS Gen2           → Bronze / Silver / Gold / Dashboard (Medallion)
  3. Databricks DBFS     → mirrors medallion layout under /dbfs/superstore/
  4. (optional) Trigger Databricks notebooks

Called from run_pipeline.py and the Airflow DAG.
Skips gracefully when azure.enabled = false in config.yaml or SDK not installed.
"""

from pathlib import Path
from typing import Optional

from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


def run_azure_uploads(config_path: Optional[str] = None) -> dict:
    """
    Execute all Azure storage uploads based on config.yaml settings.

    Returns a summary dict. Safe to call even when Azure is disabled —
    returns {"skipped": True} in that case.
    """
    cfg = load_config(config_path)
    azure_cfg = cfg.get("azure", {})

    if not azure_cfg.get("enabled", False):
        logger.info("Azure integration disabled (azure.enabled = false in config.yaml). Skipping.")
        return {"skipped": True, "reason": "azure.enabled = false"}

    summary = {}

    # ── 1. Azure Blob Storage ─────────────────────────────────────────────
    try:
        from src.storage.azure_blob import AzureBlobClient
        blob = AzureBlobClient()
        blob_cfg = azure_cfg.get("blob", {})

        blob_result = {}
        if blob_cfg.get("upload_raw", True):
            blob_result["raw"] = blob.upload_raw_data()
        if blob_cfg.get("upload_processed", True):
            blob_result["processed"] = blob.upload_processed_data()
        if blob_cfg.get("upload_outputs", True):
            blob_result["outputs"] = blob.upload_outputs()

        summary["blob"] = blob_result
        logger.info("Azure Blob Storage uploads complete.")

    except ImportError as e:
        logger.warning(f"Azure Blob SDK not installed: {e}. Skipping Blob uploads.")
        summary["blob"] = {"error": str(e)}
    except Exception as e:
        logger.error(f"Azure Blob upload failed: {e}")
        summary["blob"] = {"error": str(e)}

    # ── 2. ADLS Gen2 (Medallion) ──────────────────────────────────────────
    try:
        from src.storage.adls_client import ADLSClient
        adls = ADLSClient()
        adls_result = adls.upload_all()
        summary["adls"] = {
            "bronze": adls_result["bronze"],
            "silver": adls_result["silver"],
            "gold_files": len(adls_result["gold"]),
            "dashboard_charts": len(adls_result["dashboard"]),
            "databricks_paths": adls.get_databricks_paths(),
        }
        logger.info("ADLS Gen2 Medallion upload complete.")

    except ImportError as e:
        logger.warning(f"Azure ADLS SDK not installed: {e}. Skipping ADLS uploads.")
        summary["adls"] = {"error": str(e)}
    except Exception as e:
        logger.error(f"ADLS upload failed: {e}")
        summary["adls"] = {"error": str(e)}

    # ── 3. Databricks DBFS ────────────────────────────────────────────────
    try:
        from src.storage.databricks_client import DatabricksClient
        dbc_cfg = azure_cfg.get("databricks", {})

        dbc = DatabricksClient()

        if dbc_cfg.get("upload_to_dbfs", True):
            dbfs_result = dbc.upload_to_dbfs_all()
            summary["databricks_dbfs"] = {
                "bronze": len(dbfs_result["bronze"]),
                "silver": len(dbfs_result["silver"]),
                "gold_files": len(dbfs_result["gold"]),
                "dashboard_charts": len(dbfs_result["dashboard"]),
            }
            logger.info("Databricks DBFS upload complete.")

        if dbc_cfg.get("run_notebooks", False):
            logger.info("Triggering Databricks notebook pipeline ...")
            nb_results = dbc.run_analytics_pipeline()
            summary["databricks_notebooks"] = nb_results
            logger.info("Databricks notebook pipeline complete.")

    except ImportError as e:
        logger.warning(f"Databricks SDK not installed: {e}. Skipping DBFS upload.")
        summary["databricks_dbfs"] = {"error": str(e)}
    except KeyError as e:
        logger.warning(f"Databricks env var not set: {e}. Skipping DBFS upload.")
        summary["databricks_dbfs"] = {"error": f"Missing env var: {e}"}
    except Exception as e:
        logger.error(f"Databricks upload failed: {e}")
        summary["databricks_dbfs"] = {"error": str(e)}

    logger.info(f"Azure pipeline step complete. Summary: {_safe_summary(summary)}")
    return summary


def _safe_summary(summary: dict) -> str:
    """Return a compact log-safe string of the summary."""
    parts = []
    for layer, val in summary.items():
        if isinstance(val, dict) and "error" in val:
            parts.append(f"{layer}=ERROR")
        elif isinstance(val, dict) and val.get("skipped"):
            parts.append(f"{layer}=SKIPPED")
        else:
            parts.append(f"{layer}=OK")
    return ", ".join(parts)

"""
Apache Airflow DAG — SuperStore ETL Pipeline.

Schedule: Daily (@daily)
Pipeline tasks in order:
  1. validate_raw_data       — Check file exists and schema is correct
  2. ingest_data             — Load raw CSV, run quality summary
  3. clean_transform_data    — Remove dupes, handle nulls, create derived metrics
  4. run_pandas_analytics    — Compute all Pandas-based aggregations → CSV/Parquet
  5. run_spark_analytics     — PySpark aggregations (Databricks / local Spark)
  6. generate_dashboard      — Generate all visualization charts
  7. data_quality_checks     — Post-ETL accuracy and consistency assertions
  8. notify_success          — Log final pipeline success message

Task dependencies:
  validate → ingest → clean → pandas_analytics → dashboard → quality_checks → notify
                                  ↓
                          spark_analytics

Retry config: 3 retries with 5-min delay on each task.

Run locally (without Airflow scheduler):
    python airflow/dags/superstore_etl_dag.py --run-local
"""

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to sys.path so imports work both locally and in Airflow
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ── Default args ─────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "superstore-analytics",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# ── Task callables ────────────────────────────────────────────────────────────


def task_validate_raw_data(**context):
    """
    Task 1: Validate that the raw CSV exists and passes schema checks.
    Uses DataLoader's validation without loading the full DataFrame into XCom.
    """
    from src.ingestion.data_loader import DataLoader
    from src.utils.config import load_config

    cfg = load_config()
    raw_dir = str(PROJECT_ROOT / cfg["paths"]["raw_data_dir"])
    raw_file = cfg["data"]["raw_file"]
    full_path = os.path.join(raw_dir, raw_file)

    if not Path(full_path).exists():
        raise FileNotFoundError(
            f"Raw data file not found: {full_path}. "
            f"Run: python data/generate_dataset.py"
        )

    loader = DataLoader()
    df = loader.load(full_path)

    summary = {
        "rows": len(df),
        "columns": list(df.columns),
        "nulls": int(df.isnull().sum().sum()),
        "duplicates": int(df.duplicated().sum()),
    }
    log.info(f"Validation summary: {summary}")
    context["ti"].xcom_push(key="validation_summary", value=summary)
    return summary


def task_ingest_data(**context):
    """
    Task 2: Load raw data and push row count to XCom.
    In a real pipeline this would persist to a staging table/blob.
    """
    from src.ingestion.data_loader import DataLoader

    loader = DataLoader()
    df = loader.load()
    row_count = len(df)
    log.info(f"Ingested {row_count:,} rows.")
    context["ti"].xcom_push(key="ingested_rows", value=row_count)
    return row_count


def task_clean_transform_data(**context):
    """
    Task 3: Clean and transform raw data → save as Parquet.
    """
    from src.cleaning.data_cleaner import DataCleaner
    from src.ingestion.data_loader import DataLoader

    loader = DataLoader()
    raw_df = loader.load()

    cleaner = DataCleaner()
    clean_df = cleaner.clean(raw_df)
    saved_path = cleaner.save(clean_df)

    result = {
        "clean_rows": len(clean_df),
        "saved_path": saved_path,
        "loss_items": int(clean_df["is_loss"].sum()),
    }
    log.info(f"Cleaning result: {result}")
    context["ti"].xcom_push(key="clean_result", value=result)
    return result


def task_run_pandas_analytics(**context):
    """
    Task 4: Run all Pandas analytics and save results as CSVs.
    """
    import pandas as pd

    from src.analytics.profitability_analytics import ProfitabilityAnalytics
    from src.analytics.sales_analytics import SalesAnalytics
    from src.utils.config import load_config

    cfg = load_config()
    processed_path = str(
        PROJECT_ROOT / cfg["paths"]["processed_data_dir"] / cfg["data"]["processed_file"]
    )
    output_dir = Path(PROJECT_ROOT / cfg["paths"]["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    clean_df = pd.read_parquet(processed_path)

    sa = SalesAnalytics(clean_df)
    pa = ProfitabilityAnalytics(clean_df)

    # Save key analytics results as CSV for audit / dashboard consumption
    results = {
        "kpis": sa.kpi_summary(),
        "exec_summary": pa.executive_summary(),
    }

    analytics_outputs = {
        "monthly_trend": sa.monthly_sales_trend(),
        "category_performance": sa.category_performance(),
        "subcategory_performance": sa.subcategory_performance(),
        "regional_performance": sa.regional_performance(),
        "loss_products": sa.loss_making_products(),
        "segment_performance": sa.segment_performance(),
        "discount_impact": sa.discount_impact_analysis(),
        "yoy_growth": pa.yoy_profit_growth(),
    }

    for name, df in analytics_outputs.items():
        path = output_dir / f"{name}.csv"
        df.to_csv(path, index=False)
        log.info(f"Saved analytics result → {path}")

    context["ti"].xcom_push(key="analytics_kpis", value=results["kpis"])
    return results


def task_run_spark_analytics(**context):
    """
    Task 5: PySpark analytics (runs in local mode here; replace master with
    Databricks cluster URL / connect string in production).
    """
    from pyspark.sql import SparkSession

    from src.analytics.spark_analytics import SparkAnalytics
    from src.utils.config import load_config

    cfg = load_config()
    parquet_path = str(
        PROJECT_ROOT / cfg["paths"]["processed_data_dir"] / cfg["data"]["processed_file"]
    )
    output_dir = Path(PROJECT_ROOT / cfg["paths"]["output_dir"])

    spark = (
        SparkSession.builder
        .appName("SuperStoreAnalytics")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        sa = SparkAnalytics(spark, parquet_path)

        # Compute and save key Spark results
        monthly = sa.monthly_trend()
        monthly.write.mode("overwrite").option("header", "true").csv(
            str(output_dir / "spark_monthly_trend")
        )

        category = sa.category_performance()
        category.write.mode("overwrite").option("header", "true").csv(
            str(output_dir / "spark_category_performance")
        )

        loss_products = sa.loss_making_products()
        loss_products.write.mode("overwrite").option("header", "true").csv(
            str(output_dir / "spark_loss_products")
        )

        kpis = sa.kpi_summary()
        log.info(f"Spark KPIs: {kpis}")
        context["ti"].xcom_push(key="spark_kpis", value=kpis)

    finally:
        spark.stop()

    return kpis


def task_generate_dashboard(**context):
    """
    Task 6: Generate all visualization charts.
    """
    import pandas as pd

    from src.analytics.profitability_analytics import ProfitabilityAnalytics
    from src.analytics.sales_analytics import SalesAnalytics
    from src.utils.config import load_config
    from src.visualization.dashboard import Dashboard

    cfg = load_config()
    processed_path = str(
        PROJECT_ROOT / cfg["paths"]["processed_data_dir"] / cfg["data"]["processed_file"]
    )

    clean_df = pd.read_parquet(processed_path)

    sa = SalesAnalytics(clean_df)
    pa = ProfitabilityAnalytics(clean_df)
    dash = Dashboard(sa, pa)
    saved_charts = dash.generate_all()

    log.info(f"Generated {len(saved_charts)} charts.")
    return {"charts_generated": len(saved_charts), "paths": saved_charts}


def task_data_quality_checks(**context):
    """
    Task 7: Post-ETL data accuracy and consistency validation.

    Checks:
      1. No null values in critical columns after cleaning.
      2. Sales > 0 for all rows.
      3. Discount in [0, 1].
      4. Ship Date >= Order Date.
      5. Profit margin calculation consistency.
      6. Total rows within expected bounds.
    """
    import pandas as pd

    from src.utils.config import load_config

    cfg = load_config()
    processed_path = str(
        PROJECT_ROOT / cfg["paths"]["processed_data_dir"] / cfg["data"]["processed_file"]
    )

    df = pd.read_parquet(processed_path)
    failures = []

    # Check 1: Critical columns not null
    critical_cols = ["Order ID", "Customer ID", "Product ID", "Sales", "Profit", "Region"]
    for col in critical_cols:
        nulls = df[col].isnull().sum()
        if nulls > 0:
            failures.append(f"CHECK FAILED: {nulls} null values in critical column '{col}'")

    # Check 2: Sales > 0
    neg_sales = (df["Sales"] <= 0).sum()
    if neg_sales > 0:
        failures.append(f"CHECK FAILED: {neg_sales} rows with Sales <= 0")

    # Check 3: Discount in [0, 1]
    bad_discount = ((df["Discount"] < 0) | (df["Discount"] > 1)).sum()
    if bad_discount > 0:
        failures.append(f"CHECK FAILED: {bad_discount} rows with Discount outside [0, 1]")

    # Check 4: Ship Date >= Order Date
    if "shipping_days" in df.columns:
        bad_ship = (df["shipping_days"] < 0).sum()
        if bad_ship > 0:
            failures.append(f"CHECK FAILED: {bad_ship} rows where Ship Date < Order Date")

    # Check 5: Profit margin recalculation consistency
    recalc_margin = (df["Profit"] / df["Sales"] * 100).round(1)
    stored_margin = df["profit_margin"].round(1)
    mismatch = (abs(recalc_margin - stored_margin) > 0.5).sum()
    if mismatch > 0:
        failures.append(f"CHECK FAILED: {mismatch} rows with profit_margin calculation mismatch")

    # Check 6: Row count sanity
    if len(df) < 1000:
        failures.append(f"CHECK FAILED: Only {len(df)} rows — expected at least 1000")

    if failures:
        for f in failures:
            log.error(f)
        raise ValueError(f"Data quality checks failed:\n" + "\n".join(failures))

    log.info(
        f"All data quality checks PASSED. "
        f"Rows: {len(df):,}, Columns: {len(df.columns)}"
    )
    return {"status": "PASSED", "rows": len(df), "columns": len(df.columns)}


def task_upload_to_azure(**context):
    """
    Task 8: Upload all data layers to Azure storage (Blob + ADLS + Databricks DBFS).

    Layer mapping:
      Blob Storage : raw/  processed/  output/charts/  output/analytics/
      ADLS Gen2    : bronze/  silver/  gold/  dashboard/   (Medallion Architecture)
      Databricks   : /dbfs/superstore/bronze|silver|gold|dashboard/

    Skips gracefully when azure.enabled = false in config.yaml.
    """
    from src.storage.azure_pipeline_step import run_azure_uploads

    result = run_azure_uploads()
    context["ti"].xcom_push(key="azure_upload_result", value=str(result))

    if result.get("skipped"):
        log.info(f"Azure uploads skipped: {result['reason']}")
    else:
        adls = result.get("adls", {})
        if adls and "error" not in adls:
            log.info(f"ADLS Bronze: {adls.get('bronze')}")
            log.info(f"ADLS Silver: {adls.get('silver')}")
            log.info(f"ADLS Gold files: {adls.get('gold_files', 0)}")
            log.info(f"ADLS Dashboard charts: {adls.get('dashboard_charts', 0)}")
            for layer, uri in adls.get("databricks_paths", {}).items():
                log.info(f"  abfss:// {layer}: {uri}")

    return result


def task_notify_success(**context):
    """
    Task 9: Final success notification.
    In production, replace with email/Slack/Teams notification.
    """
    ti = context["ti"]
    validation   = ti.xcom_pull(task_ids="validate_raw_data",    key="validation_summary") or {}
    clean_result = ti.xcom_pull(task_ids="clean_transform_data",  key="clean_result") or {}
    kpis         = ti.xcom_pull(task_ids="run_pandas_analytics",  key="analytics_kpis") or {}
    azure_result = ti.xcom_pull(task_ids="upload_to_azure",       key="azure_upload_result") or "skipped"

    message = (
        f"\n{'='*60}\n"
        f"SuperStore ETL Pipeline — SUCCESS\n"
        f"Run date    : {context.get('ds', 'N/A')}\n"
        f"Raw rows    : {validation.get('rows', 'N/A')}\n"
        f"Clean rows  : {clean_result.get('clean_rows', 'N/A')}\n"
        f"Total Sales : ${kpis.get('total_sales', 0):,.2f}\n"
        f"Total Profit: ${kpis.get('total_profit', 0):,.2f}\n"
        f"Azure Upload: {azure_result[:80]}\n"
        f"{'='*60}"
    )
    log.info(message)
    return message


# ── DAG Definition ────────────────────────────────────────────────────────────
#
# Full dependency graph:
#
#   validate → ingest → clean ──┬── pandas_analytics ──┬── spark_analytics
#                                │                      │
#                                │                      └── dashboard ──┐
#                                │                                      │
#                                └──────────────────────────────────────┴──▶ azure_upload
#                                                                                  │
#                                                                         quality_checks
#                                                                                  │
#                                                                          notify_success

with DAG(
    dag_id="superstore_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="SuperStore Sales Analytics End-to-End ETL Pipeline with Azure",
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["superstore", "etl", "analytics", "azure", "capstone"],
) as dag:

    t1_validate = PythonOperator(
        task_id="validate_raw_data",
        python_callable=task_validate_raw_data,
    )

    t2_ingest = PythonOperator(
        task_id="ingest_data",
        python_callable=task_ingest_data,
    )

    t3_clean = PythonOperator(
        task_id="clean_transform_data",
        python_callable=task_clean_transform_data,
    )

    t4_pandas = PythonOperator(
        task_id="run_pandas_analytics",
        python_callable=task_run_pandas_analytics,
    )

    t5_spark = PythonOperator(
        task_id="run_spark_analytics",
        python_callable=task_run_spark_analytics,
    )

    t6_dashboard = PythonOperator(
        task_id="generate_dashboard",
        python_callable=task_generate_dashboard,
    )

    t7_azure = PythonOperator(
        task_id="upload_to_azure",
        python_callable=task_upload_to_azure,
    )

    t8_quality = PythonOperator(
        task_id="data_quality_checks",
        python_callable=task_data_quality_checks,
    )

    t9_notify = PythonOperator(
        task_id="notify_success",
        python_callable=task_notify_success,
    )

    # Task dependency graph:
    # validate → ingest → clean → [pandas_analytics, spark_analytics]
    # pandas_analytics → dashboard
    # [spark_analytics, dashboard] → azure_upload → quality_checks → notify
    t1_validate >> t2_ingest >> t3_clean >> [t4_pandas, t5_spark]
    t4_pandas >> t6_dashboard
    [t5_spark, t6_dashboard] >> t7_azure >> t8_quality >> t9_notify


# ── Local test runner (without Airflow) ───────────────────────────────────────

def run_pipeline_locally():
    """
    Execute the full pipeline locally without the Airflow scheduler.
    Useful for development and CI/CD verification.
    """
    import traceback

    class MockTI:
        """Minimal XCom mock for local runs."""
        _store = {}
        def xcom_push(self, key, value): self._store[key] = value
        def xcom_pull(self, task_ids, key): return self._store.get(key)

    class MockContext:
        def __init__(self): self.ti = MockTI(); self.ds = datetime.now().strftime("%Y-%m-%d")
        def get(self, k, d=None): return getattr(self, k, d)
        def __getitem__(self, k): return getattr(self, k)

    tasks = [
        ("validate_raw_data",   task_validate_raw_data),
        ("ingest_data",         task_ingest_data),
        ("clean_transform_data",task_clean_transform_data),
        ("run_pandas_analytics",task_run_pandas_analytics),
        # Spark is optional locally (comment out if PySpark not installed)
        # ("run_spark_analytics", task_run_spark_analytics),
        ("generate_dashboard",  task_generate_dashboard),
        ("data_quality_checks", task_data_quality_checks),
        ("notify_success",      task_notify_success),
    ]

    ctx = MockContext()
    print("\n" + "="*60)
    print("SuperStore ETL Pipeline — LOCAL RUN")
    print("="*60)

    for name, fn in tasks:
        print(f"\n[TASK] {name}")
        try:
            result = fn(**{"ti": ctx.ti, "ds": ctx.ds})
            print(f"  ✓ Completed: {str(result)[:120]}")
        except Exception as exc:
            print(f"  ✗ FAILED: {exc}")
            traceback.print_exc()
            break


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-local", action="store_true",
                        help="Run pipeline locally without Airflow")
    args = parser.parse_args()
    if args.run_local:
        run_pipeline_locally()

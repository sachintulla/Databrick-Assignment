"""
SuperStore Analytics System — Main Pipeline Runner.

Runs the full ETL pipeline locally WITHOUT needing Apache Airflow.
Use this for:
  - Local development and testing
  - CI/CD verification
  - Demo / presentation runs

Usage:
    # Full pipeline
    python run_pipeline.py

    # Skip Spark (if PySpark not installed locally)
    python run_pipeline.py --no-spark

    # Skip dashboard generation
    python run_pipeline.py --no-dashboard

    # Generate dataset first
    python data/generate_dataset.py && python run_pipeline.py
"""

import argparse
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.utils.logger import get_logger

logger = get_logger("run_pipeline")


def run_step(name: str, fn, *args, **kwargs):
    """Execute a pipeline step with timing and error reporting."""
    print(f"\n{'='*60}")
    print(f"STEP: {name}")
    print(f"{'='*60}")
    t0 = time.time()
    try:
        result = fn(*args, **kwargs)
        elapsed = time.time() - t0
        print(f"  ✓ Completed in {elapsed:.1f}s")
        return result
    except Exception as exc:
        elapsed = time.time() - t0
        print(f"  ✗ FAILED after {elapsed:.1f}s: {exc}")
        logger.exception(f"Step '{name}' failed")
        raise


def main(skip_spark: bool = False, skip_dashboard: bool = False):
    """Execute the complete SuperStore ETL & Analytics pipeline."""

    total_start = time.time()
    print("\n" + "="*60)
    print("SUPERSTORE ANALYTICS SYSTEM — PIPELINE START")
    print("="*60)

    # ── Step 1: Generate dataset if needed ───────────────────────────────
    raw_path = PROJECT_ROOT / "data" / "raw" / "superstore_sales.csv"
    if not raw_path.exists():
        def generate():
            import runpy
            runpy.run_path(str(PROJECT_ROOT / "data" / "generate_dataset.py"), run_name="__main__")
        run_step("Generate Synthetic Dataset", generate)

    # ── Step 2: Ingest & Validate ─────────────────────────────────────────
    from src.ingestion.data_loader import DataLoader

    def ingest():
        loader = DataLoader()
        df = loader.load()
        print(f"  Rows loaded: {len(df):,}")
        return df

    raw_df = run_step("Data Ingestion & Validation", ingest)

    # ── Step 3: Clean & Transform ─────────────────────────────────────────
    from src.cleaning.data_cleaner import DataCleaner

    def clean():
        cleaner = DataCleaner()
        clean = cleaner.clean(raw_df)
        path = cleaner.save(clean)
        print(f"  Clean rows: {len(clean):,}")
        print(f"  Loss items: {clean['is_loss'].sum():,}")
        print(f"  Saved to  : {path}")
        return clean

    clean_df = run_step("Data Cleaning & Transformation", clean)

    # ── Step 4: Pandas Analytics ──────────────────────────────────────────
    from src.analytics.profitability_analytics import ProfitabilityAnalytics
    from src.analytics.sales_analytics import SalesAnalytics
    from src.utils.config import load_config

    def pandas_analytics():
        cfg = load_config()
        output_dir = PROJECT_ROOT / cfg["paths"]["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)

        sa = SalesAnalytics(clean_df)
        pa = ProfitabilityAnalytics(clean_df)

        kpis = sa.kpi_summary()
        exec_sum = pa.executive_summary()

        # Save analytics CSVs
        sa.monthly_sales_trend().to_csv(output_dir / "monthly_trend.csv", index=False)
        sa.category_performance().to_csv(output_dir / "category_performance.csv", index=False)
        sa.subcategory_performance().to_csv(output_dir / "subcategory_performance.csv", index=False)
        sa.regional_performance().to_csv(output_dir / "regional_performance.csv", index=False)
        sa.loss_making_products().to_csv(output_dir / "loss_products.csv", index=False)
        sa.segment_performance().to_csv(output_dir / "segment_performance.csv", index=False)
        sa.discount_impact_analysis().to_csv(output_dir / "discount_impact.csv", index=False)
        pa.yoy_profit_growth().to_csv(output_dir / "yoy_growth.csv", index=False)
        pa.subcategory_return_on_sales().to_csv(output_dir / "subcategory_ros.csv", index=False)
        pa.profitability_by_state().to_csv(output_dir / "state_profitability.csv", index=False)

        print(f"  Total Sales   : ${kpis['total_sales']:,.2f}")
        print(f"  Total Profit  : ${kpis['total_profit']:,.2f}")
        print(f"  Profit Margin : {kpis['avg_profit_margin_pct']:.1f}%")
        print(f"  Best Category : {exec_sum['best_performing_category']}")
        print(f"  Best Region   : {exec_sum['best_performing_region']}")
        print(f"  Loss SKUs     : {exec_sum['loss_making_sku_count']}")
        return sa, pa

    sa, pa = run_step("Pandas Analytics & Business Insights", pandas_analytics)

    # ── Step 5: Spark Analytics (optional) ───────────────────────────────
    if not skip_spark:
        def spark_analytics():
            from pyspark.sql import SparkSession
            from src.analytics.spark_analytics import SparkAnalytics
            from src.utils.config import load_config

            cfg = load_config()
            parquet_path = str(
                PROJECT_ROOT / cfg["paths"]["processed_data_dir"] / cfg["data"]["processed_file"]
            )

            spark = (
                SparkSession.builder
                .appName("SuperStoreAnalytics")
                .master("local[*]")
                .config("spark.driver.memory", "2g")
                .config("spark.sql.shuffle.partitions", "8")
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("ERROR")

            try:
                sa_spark = SparkAnalytics(spark, parquet_path)
                kpis = sa_spark.kpi_summary()
                print(f"  Spark Total Sales : ${kpis.get('total_sales', 0):,.2f}")
                print(f"  Spark Total Orders: {kpis.get('total_orders', 0):,}")

                cfg2 = load_config()
                output_dir = PROJECT_ROOT / cfg2["paths"]["output_dir"]

                sa_spark.category_performance().toPandas().to_csv(
                    output_dir / "spark_category.csv", index=False
                )
                sa_spark.regional_performance().toPandas().to_csv(
                    output_dir / "spark_regional.csv", index=False
                )
                sa_spark.loss_making_products().toPandas().to_csv(
                    output_dir / "spark_loss_products.csv", index=False
                )
            finally:
                spark.stop()

        try:
            run_step("PySpark Analytics", spark_analytics)
        except ImportError:
            print("  ℹ PySpark not installed — skipping Spark step (use --no-spark to suppress)")

    # ── Step 6: Dashboard ─────────────────────────────────────────────────
    if not skip_dashboard:
        from src.visualization.dashboard import Dashboard

        def generate_dashboard():
            dash = Dashboard(sa, pa)
            charts = dash.generate_all()
            print(f"  Charts generated: {len(charts)}")
            for p in charts:
                print(f"    → {p}")
            return charts

        run_step("Visualization & Dashboard Generation", generate_dashboard)

    # ── Step 7: Data Quality Checks ───────────────────────────────────────
    def quality_checks():
        import pandas as pd
        from src.utils.config import load_config

        cfg = load_config()
        pq_path = str(
            PROJECT_ROOT / cfg["paths"]["processed_data_dir"] / cfg["data"]["processed_file"]
        )
        df = pd.read_parquet(pq_path)
        failures = []

        checks = {
            "No nulls in Order ID": df["Order ID"].isnull().sum() == 0,
            "No nulls in Sales": df["Sales"].isnull().sum() == 0,
            "No nulls in Profit": df["Profit"].isnull().sum() == 0,
            "Sales > 0": (df["Sales"] > 0).all(),
            "Discount in [0,1]": ((df["Discount"] >= 0) & (df["Discount"] <= 1)).all(),
            "shipping_days >= 0": (df["shipping_days"] >= 0).all(),
            "profit_margin exists": "profit_margin" in df.columns,
            "is_loss is bool": df["is_loss"].dtype == bool,
            "Row count >= 1000": len(df) >= 1000,
        }

        for check_name, passed in checks.items():
            status = "PASS" if passed else "FAIL"
            print(f"  [{status}] {check_name}")
            if not passed:
                failures.append(check_name)

        if failures:
            raise AssertionError(f"Quality checks failed: {failures}")
        return True

    run_step("Post-ETL Data Quality Checks", quality_checks)

    # ── Step 8: Azure Storage Uploads ────────────────────────────────────
    def azure_uploads():
        from src.storage.azure_pipeline_step import run_azure_uploads
        from src.utils.config import load_config

        result = run_azure_uploads()

        if result.get("skipped"):
            print(f"  Skipped — {result['reason']}")
            print("  To enable: set azure.enabled: true in config.yaml and fill .env")
            return result

        # Blob Storage summary
        blob = result.get("blob", {})
        if "error" not in blob:
            print(f"  Blob Storage : raw ✓  processed ✓  outputs ✓")
        else:
            print(f"  Blob Storage : ERROR — {blob['error']}")

        # ADLS summary
        adls = result.get("adls", {})
        if "error" not in adls:
            print(f"  ADLS (Bronze → Silver → Gold → Dashboard) : ✓")
            print(f"    Bronze  : {adls.get('bronze', 'N/A')}")
            print(f"    Silver  : {adls.get('silver', 'N/A')}")
            print(f"    Gold    : {adls.get('gold_files', 0)} CSV files")
            print(f"    Dashboard: {adls.get('dashboard_charts', 0)} PNG charts")
            paths = adls.get("databricks_paths", {})
            if paths:
                print(f"  abfss:// paths for Databricks:")
                for layer, uri in paths.items():
                    print(f"    {layer:12s}: {uri}")
        else:
            print(f"  ADLS : ERROR — {adls['error']}")

        # DBFS summary
        dbfs = result.get("databricks_dbfs", {})
        if "error" not in dbfs:
            print(f"  Databricks DBFS : ✓  ({dbfs.get('gold_files', 0)} gold + "
                  f"{dbfs.get('dashboard_charts', 0)} charts)")
        else:
            print(f"  Databricks DBFS : ERROR — {dbfs['error']}")

        return result

    run_step("Azure Storage Uploads (Blob + ADLS + Databricks DBFS)", azure_uploads)

    # ── Summary ───────────────────────────────────────────────────────────
    total_elapsed = time.time() - total_start
    print(f"\n{'='*60}")
    print(f"PIPELINE COMPLETE — Total time: {total_elapsed:.1f}s")
    print(f"Local output : {PROJECT_ROOT / 'data' / 'output'}")
    print(f"Logs         : {PROJECT_ROOT / 'logs'}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SuperStore Analytics Pipeline Runner")
    parser.add_argument("--no-spark", action="store_true", help="Skip PySpark analytics step")
    parser.add_argument("--no-dashboard", action="store_true", help="Skip dashboard generation")
    args = parser.parse_args()
    main(skip_spark=args.no_spark, skip_dashboard=args.no_dashboard)

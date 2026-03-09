"""
PySpark Analytics Module — SuperStore Analytics System.

Designed to run inside a Databricks Notebook (or local Spark session).
Provides the same analytics as SalesAnalytics but using PySpark,
demonstrating distributed / big-data patterns.

Usage in Databricks Notebook:
    from src.analytics.spark_analytics import SparkAnalytics
    sa = SparkAnalytics(spark, parquet_path="/dbfs/superstore/processed/")
    kpis = sa.kpi_summary()

For local testing:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("test").master("local[*]").getOrCreate()
    sa = SparkAnalytics(spark, parquet_path="data/processed/superstore_clean.parquet")
"""

from typing import Optional

from src.utils.logger import get_logger

logger = get_logger(__name__)


class SparkAnalytics:
    """
    PySpark-based analytics engine for the SuperStore dataset.

    Demonstrates:
      - SparkSession usage
      - DataFrame read (Parquet)
      - groupBy + agg + joins
      - Window functions (rolling totals, ranking)
      - Writing results back to storage
    """

    def __init__(self, spark, parquet_path: str, config_path: Optional[str] = None):
        """
        Args:
            spark: Active SparkSession (inject from notebook or test).
            parquet_path: Path to the cleaned Parquet file or directory.
            config_path: Optional config.yaml override.
        """
        self.spark = spark
        self.parquet_path = parquet_path

        # Import PySpark types lazily to avoid import errors on non-Spark environments
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window
        self.F = F
        self.Window = Window

        self.df = self._load()
        logger.info(f"SparkAnalytics: loaded {self.df.count():,} rows from {parquet_path}")

    # ── Load ──────────────────────────────────────────────────────────────

    def _load(self):
        """Read Parquet into a Spark DataFrame."""
        sdf = (
            self.spark.read
            .option("header", "true")
            .parquet(self.parquet_path)
        )
        return sdf

    # ── KPI Summary ───────────────────────────────────────────────────────

    def kpi_summary(self) -> dict:
        """
        Compute top-level KPIs using Spark aggregations.

        Returns:
            Dictionary of KPI name → value.
        """
        F = self.F
        row = (
            self.df.agg(
                F.round(F.sum("Sales"), 2).alias("total_sales"),
                F.round(F.sum("Profit"), 2).alias("total_profit"),
                F.countDistinct("Order ID").alias("total_orders"),
                F.countDistinct("Customer ID").alias("total_customers"),
                F.round(F.avg("Discount") * 100, 2).alias("avg_discount_pct"),
            )
            .first()
        )
        return row.asDict()

    # ── Trend Analysis ────────────────────────────────────────────────────

    def monthly_trend(self):
        """
        Monthly sales & profit using Spark date functions and groupBy.

        Returns:
            Spark DataFrame: year_month, sales, profit, orders.
        """
        F = self.F
        return (
            self.df
            .withColumn("year_month", F.date_format(F.col("Order Date"), "yyyy-MM"))
            .groupBy("year_month")
            .agg(
                F.round(F.sum("Sales"), 2).alias("sales"),
                F.round(F.sum("Profit"), 2).alias("profit"),
                F.countDistinct("Order ID").alias("orders"),
            )
            .orderBy("year_month")
        )

    # ── Category Analysis ─────────────────────────────────────────────────

    def category_performance(self):
        """
        Sales and profit by Category using groupBy + agg.

        Returns:
            Spark DataFrame: Category, sales, profit, avg_discount.
        """
        F = self.F
        return (
            self.df
            .groupBy("Category")
            .agg(
                F.round(F.sum("Sales"), 2).alias("sales"),
                F.round(F.sum("Profit"), 2).alias("profit"),
                F.sum("Quantity").alias("quantity"),
                F.round(F.avg("Discount") * 100, 2).alias("avg_discount_pct"),
                F.countDistinct("Order ID").alias("orders"),
            )
            .orderBy(F.desc("sales"))
        )

    def subcategory_performance(self):
        """Sub-category breakdown using groupBy."""
        F = self.F
        return (
            self.df
            .groupBy("Category", "Sub-Category")
            .agg(
                F.round(F.sum("Sales"), 2).alias("sales"),
                F.round(F.sum("Profit"), 2).alias("profit"),
                F.sum("Quantity").alias("quantity"),
            )
            .orderBy(F.desc("profit"))
        )

    # ── Regional Analysis ─────────────────────────────────────────────────

    def regional_performance(self):
        """Region-level aggregation using groupBy."""
        F = self.F
        return (
            self.df
            .groupBy("Region")
            .agg(
                F.round(F.sum("Sales"), 2).alias("sales"),
                F.round(F.sum("Profit"), 2).alias("profit"),
                F.countDistinct("Order ID").alias("orders"),
                F.countDistinct("Customer ID").alias("customers"),
            )
            .orderBy(F.desc("sales"))
        )

    # ── Joins Example ─────────────────────────────────────────────────────

    def join_order_with_product_summary(self):
        """
        Demonstrates a PySpark join:
          - Left side: order-level summary (Order ID → total order value)
          - Right side: per-order product count
          - JOIN on Order ID
        Returns:
            Spark DataFrame with order value + product diversity.
        """
        F = self.F

        order_totals = (
            self.df
            .groupBy("Order ID")
            .agg(
                F.round(F.sum("Sales"), 2).alias("order_total_sales"),
                F.round(F.sum("Profit"), 2).alias("order_total_profit"),
                F.first("Customer ID").alias("Customer ID"),
                F.first("Region").alias("Region"),
            )
        )

        product_diversity = (
            self.df
            .groupBy("Order ID")
            .agg(
                F.countDistinct("Product ID").alias("distinct_products"),
                F.sum("Quantity").alias("total_quantity"),
            )
        )

        return order_totals.join(product_diversity, on="Order ID", how="left")

    # ── Window Functions ──────────────────────────────────────────────────

    def running_monthly_sales(self):
        """
        Running total of sales per year using Spark Window functions.

        Returns:
            Spark DataFrame: year, month, monthly_sales, cumulative_sales.
        """
        F = self.F
        Window = self.Window

        monthly = (
            self.df
            .withColumn("year", F.year(F.col("Order Date")))
            .withColumn("month", F.month(F.col("Order Date")))
            .groupBy("year", "month")
            .agg(F.round(F.sum("Sales"), 2).alias("monthly_sales"))
        )

        window_spec = (
            Window
            .partitionBy("year")
            .orderBy("month")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )

        return monthly.withColumn(
            "cumulative_sales", F.round(F.sum("monthly_sales").over(window_spec), 2)
        ).orderBy("year", "month")

    def product_sales_rank_by_category(self):
        """
        Rank products within each category by total sales using Window dense_rank.

        Returns:
            Spark DataFrame: Category, Product Name, sales, rank_in_category.
        """
        F = self.F
        Window = self.Window

        product_sales = (
            self.df
            .groupBy("Category", "Product Name")
            .agg(F.round(F.sum("Sales"), 2).alias("sales"))
        )

        window_spec = (
            Window
            .partitionBy("Category")
            .orderBy(F.desc("sales"))
        )

        return product_sales.withColumn(
            "rank_in_category", F.dense_rank().over(window_spec)
        ).orderBy("Category", "rank_in_category")

    # ── Loss Detection ────────────────────────────────────────────────────

    def loss_making_products(self):
        """
        Products with negative total profit — PySpark version.

        Returns:
            Spark DataFrame filtered to loss-making SKUs.
        """
        F = self.F
        product_profit = (
            self.df
            .groupBy("Product ID", "Product Name", "Category", "Sub-Category")
            .agg(
                F.round(F.sum("Sales"), 2).alias("total_sales"),
                F.round(F.sum("Profit"), 2).alias("total_profit"),
                F.round(F.avg("Discount") * 100, 2).alias("avg_discount_pct"),
            )
        )
        return (
            product_profit
            .filter(F.col("total_profit") < 0)
            .orderBy("total_profit")
        )

    # ── Save Results ──────────────────────────────────────────────────────

    def save_results(self, sdf, output_path: str, format: str = "parquet") -> None:
        """
        Write Spark DataFrame to storage.

        Args:
            sdf: Spark DataFrame to write.
            output_path: Target path (DBFS, S3, local, etc.)
            format: Output format — 'parquet', 'csv', 'delta'.
        """
        (
            sdf.write
            .format(format)
            .mode("overwrite")
            .option("header", "true")
            .save(output_path)
        )
        logger.info(f"Results saved → {output_path}  (format: {format})")

"""
Sales Analytics Module — SuperStore Analytics System.

Provides Pandas-based aggregations and insights for:
  - Monthly / quarterly / yearly sales trends
  - Category & sub-category performance
  - Regional sales & profit comparison
  - Top-N products by sales and profit
  - Loss-making product identification
  - Customer segment analysis
  - Discount impact analysis
  - Key Performance Indicators (KPIs) summary

Usage:
    from src.analytics.sales_analytics import SalesAnalytics
    sa = SalesAnalytics(clean_df)
    kpis = sa.kpi_summary()
    trends = sa.monthly_sales_trend()
"""

from typing import Optional

import numpy as np
import pandas as pd

from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SalesAnalytics:
    """
    Encapsulates all Pandas-based sales and profitability analyses.

    Args:
        df: Cleaned DataFrame produced by DataCleaner.
        config_path: Optional path to config.yaml override.
    """

    def __init__(self, df: pd.DataFrame, config_path: Optional[str] = None):
        self.df = df.copy()
        self.config = load_config(config_path)
        self._top_n = self.config["analytics"]["top_n_products"]
        logger.info(f"SalesAnalytics initialized with {len(self.df):,} rows.")

    # ── KPI Summary ───────────────────────────────────────────────────────

    def kpi_summary(self) -> dict:
        """
        Compute high-level KPIs for the full dataset.

        Returns:
            Dict with keys: total_sales, total_profit, total_orders,
            total_customers, avg_order_value, avg_profit_margin,
            overall_discount_rate, loss_order_pct.
        """
        df = self.df
        total_sales = df["Sales"].sum()
        total_profit = df["Profit"].sum()
        total_orders = df["Order ID"].nunique()
        total_customers = df["Customer ID"].nunique()

        # Average order value — sum sales per order, then mean
        avg_order_value = df.groupby("Order ID")["Sales"].sum().mean()
        avg_profit_margin = df["profit_margin"].mean() if "profit_margin" in df else np.nan
        overall_discount_rate = df["Discount"].mean() * 100  # as %
        loss_order_pct = (df["is_loss"].sum() / len(df) * 100) if "is_loss" in df else np.nan

        kpis = {
            "total_sales": round(total_sales, 2),
            "total_profit": round(total_profit, 2),
            "total_orders": total_orders,
            "total_customers": total_customers,
            "avg_order_value": round(avg_order_value, 2),
            "avg_profit_margin_pct": round(avg_profit_margin, 2),
            "overall_discount_rate_pct": round(overall_discount_rate, 2),
            "loss_line_items_pct": round(loss_order_pct, 2),
        }

        logger.info("KPI Summary computed.")
        for k, v in kpis.items():
            logger.info(f"  {k}: {v}")
        return kpis

    # ── Trend Analysis ────────────────────────────────────────────────────

    def monthly_sales_trend(self) -> pd.DataFrame:
        """
        Sales and profit aggregated by year-month.

        Returns:
            DataFrame with columns: year_month, sales, profit, orders, profit_margin.
        """
        df = self.df.copy()
        df["year_month"] = df["Order Date"].dt.to_period("M").astype(str)
        grouped = (
            df.groupby("year_month")
            .agg(
                sales=("Sales", "sum"),
                profit=("Profit", "sum"),
                orders=("Order ID", "nunique"),
            )
            .reset_index()
            .sort_values("year_month")
        )
        grouped["profit_margin"] = (grouped["profit"] / grouped["sales"] * 100).round(2)
        logger.info(f"Monthly trend: {len(grouped)} months.")
        return grouped

    def quarterly_sales_trend(self) -> pd.DataFrame:
        """Sales & profit by year-quarter."""
        df = self.df.copy()
        df["year_quarter"] = (
            df["order_year"].astype(str) + "-Q" + df["order_quarter"].astype(str)
        )
        grouped = (
            df.groupby("year_quarter")
            .agg(
                sales=("Sales", "sum"),
                profit=("Profit", "sum"),
                orders=("Order ID", "nunique"),
            )
            .reset_index()
            .sort_values("year_quarter")
        )
        grouped["profit_margin"] = (grouped["profit"] / grouped["sales"] * 100).round(2)
        logger.info(f"Quarterly trend: {len(grouped)} quarters.")
        return grouped

    def yearly_sales_trend(self) -> pd.DataFrame:
        """Sales & profit by year."""
        grouped = (
            self.df.groupby("order_year")
            .agg(
                sales=("Sales", "sum"),
                profit=("Profit", "sum"),
                orders=("Order ID", "nunique"),
                customers=("Customer ID", "nunique"),
            )
            .reset_index()
            .sort_values("order_year")
        )
        grouped["profit_margin"] = (grouped["profit"] / grouped["sales"] * 100).round(2)
        return grouped

    # ── Category & Sub-Category ───────────────────────────────────────────

    def category_performance(self) -> pd.DataFrame:
        """
        Sales, profit, and margin by Category.

        Returns:
            DataFrame sorted by sales descending.
        """
        grouped = (
            self.df.groupby("Category")
            .agg(
                sales=("Sales", "sum"),
                profit=("Profit", "sum"),
                quantity=("Quantity", "sum"),
                orders=("Order ID", "nunique"),
                avg_discount=("Discount", "mean"),
            )
            .reset_index()
        )
        grouped["profit_margin"] = (grouped["profit"] / grouped["sales"] * 100).round(2)
        grouped["avg_discount_pct"] = (grouped["avg_discount"] * 100).round(2)
        return grouped.sort_values("sales", ascending=False)

    def subcategory_performance(self) -> pd.DataFrame:
        """Sales, profit, margin by Sub-Category."""
        grouped = (
            self.df.groupby(["Category", "Sub-Category"])
            .agg(
                sales=("Sales", "sum"),
                profit=("Profit", "sum"),
                quantity=("Quantity", "sum"),
                avg_discount=("Discount", "mean"),
            )
            .reset_index()
        )
        grouped["profit_margin"] = (grouped["profit"] / grouped["sales"] * 100).round(2)
        return grouped.sort_values("profit", ascending=False)

    # ── Regional Analysis ─────────────────────────────────────────────────

    def regional_performance(self) -> pd.DataFrame:
        """Sales & profit comparison across regions."""
        grouped = (
            self.df.groupby("Region")
            .agg(
                sales=("Sales", "sum"),
                profit=("Profit", "sum"),
                orders=("Order ID", "nunique"),
                customers=("Customer ID", "nunique"),
                avg_discount=("Discount", "mean"),
            )
            .reset_index()
        )
        grouped["profit_margin"] = (grouped["profit"] / grouped["sales"] * 100).round(2)
        grouped["avg_discount_pct"] = (grouped["avg_discount"] * 100).round(2)
        return grouped.sort_values("sales", ascending=False)

    def regional_category_heatmap_data(self) -> pd.DataFrame:
        """Pivot: Region × Category → Sales (for heatmap visualization)."""
        pivot = (
            self.df.groupby(["Region", "Category"])["Sales"]
            .sum()
            .unstack(fill_value=0)
            .round(2)
        )
        return pivot

    def state_performance(self) -> pd.DataFrame:
        """Top states by sales."""
        grouped = (
            self.df.groupby(["State", "Region"])
            .agg(sales=("Sales", "sum"), profit=("Profit", "sum"))
            .reset_index()
        )
        grouped["profit_margin"] = (grouped["profit"] / grouped["sales"] * 100).round(2)
        return grouped.sort_values("sales", ascending=False)

    # ── Product Analysis ──────────────────────────────────────────────────

    def top_products_by_sales(self, n: int = None) -> pd.DataFrame:
        """Top N products ranked by total sales."""
        n = n or self._top_n
        grouped = (
            self.df.groupby(["Product ID", "Product Name", "Category", "Sub-Category"])
            .agg(
                sales=("Sales", "sum"),
                profit=("Profit", "sum"),
                quantity=("Quantity", "sum"),
            )
            .reset_index()
        )
        grouped["profit_margin"] = (grouped["profit"] / grouped["sales"] * 100).round(2)
        return grouped.nlargest(n, "sales")

    def loss_making_products(self) -> pd.DataFrame:
        """
        Products with negative total profit — filtered and sorted by worst loss.

        Returns:
            DataFrame of loss-making products sorted by profit ascending.
        """
        grouped = (
            self.df.groupby(["Product ID", "Product Name", "Category", "Sub-Category"])
            .agg(
                sales=("Sales", "sum"),
                profit=("Profit", "sum"),
                quantity=("Quantity", "sum"),
                avg_discount=("Discount", "mean"),
            )
            .reset_index()
        )
        loss = grouped[grouped["profit"] < 0].copy()
        loss["profit_margin"] = (loss["profit"] / loss["sales"] * 100).round(2)
        loss["avg_discount_pct"] = (loss["avg_discount"] * 100).round(2)
        result = loss.sort_values("profit")
        logger.info(f"Loss-making products found: {len(result)}")
        return result

    # ── Customer Segment ──────────────────────────────────────────────────

    def segment_performance(self) -> pd.DataFrame:
        """Sales & profit by customer segment."""
        grouped = (
            self.df.groupby("Segment")
            .agg(
                sales=("Sales", "sum"),
                profit=("Profit", "sum"),
                orders=("Order ID", "nunique"),
                customers=("Customer ID", "nunique"),
            )
            .reset_index()
        )
        grouped["profit_margin"] = (grouped["profit"] / grouped["sales"] * 100).round(2)
        grouped["avg_order_value"] = (grouped["sales"] / grouped["orders"]).round(2)
        return grouped.sort_values("sales", ascending=False)

    def top_customers_by_sales(self, n: int = None) -> pd.DataFrame:
        """Top N customers by total sales."""
        n = n or self.config["analytics"]["top_n_customers"]
        grouped = (
            self.df.groupby(["Customer ID", "Customer Name", "Segment"])
            .agg(
                sales=("Sales", "sum"),
                profit=("Profit", "sum"),
                orders=("Order ID", "nunique"),
            )
            .reset_index()
        )
        return grouped.nlargest(n, "sales")

    # ── Discount Impact ───────────────────────────────────────────────────

    def discount_impact_analysis(self) -> pd.DataFrame:
        """
        Bin discount into buckets and show average profit margin per bucket.
        Demonstrates how deep discounts erode profitability.
        """
        df = self.df.copy()
        bins = [-0.01, 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.01]
        labels = [
            "0%", "0-10%", "10-20%", "20-30%", "30-40%",
            "40-50%", "50-60%", "60-70%", "70-80%", "80%+",
        ]
        df["discount_bucket"] = pd.cut(df["Discount"], bins=bins, labels=labels)
        grouped = (
            df.groupby("discount_bucket", observed=True)
            .agg(
                avg_profit_margin=("profit_margin", "mean"),
                total_sales=("Sales", "sum"),
                total_profit=("Profit", "sum"),
                n_transactions=("Order ID", "count"),
            )
            .reset_index()
        )
        grouped["avg_profit_margin"] = grouped["avg_profit_margin"].round(2)
        return grouped

    # ── Ship Mode Analysis ────────────────────────────────────────────────

    def shipping_analysis(self) -> pd.DataFrame:
        """Avg shipping days and order share by ship mode."""
        grouped = (
            self.df.groupby("Ship Mode")
            .agg(
                orders=("Order ID", "nunique"),
                avg_shipping_days=("shipping_days", "mean"),
                sales=("Sales", "sum"),
            )
            .reset_index()
        )
        total = grouped["orders"].sum()
        grouped["order_share_pct"] = (grouped["orders"] / total * 100).round(2)
        grouped["avg_shipping_days"] = grouped["avg_shipping_days"].round(1)
        return grouped.sort_values("orders", ascending=False)

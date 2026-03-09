"""
Profitability Analytics Module — SuperStore Analytics System.

Provides deeper profitability insights:
  - Profitability heatmaps (State × Category)
  - Discount-to-profit correlation
  - Return-on-sales per sub-category
  - Year-over-year profit growth
  - High-discount / low-profit risk products
  - Executive summary DataFrame

Usage:
    from src.analytics.profitability_analytics import ProfitabilityAnalytics
    pa = ProfitabilityAnalytics(clean_df)
    summary = pa.executive_summary()
"""

from typing import Optional

import numpy as np
import pandas as pd

from src.utils.config import load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class ProfitabilityAnalytics:
    """
    Advanced profitability computations on the cleaned SuperStore DataFrame.

    Args:
        df: Cleaned DataFrame produced by DataCleaner.
        config_path: Optional path to config.yaml override.
    """

    def __init__(self, df: pd.DataFrame, config_path: Optional[str] = None):
        self.df = df.copy()
        self.config = load_config(config_path)
        logger.info(f"ProfitabilityAnalytics initialized with {len(self.df):,} rows.")

    # ── Core Profitability Breakdowns ─────────────────────────────────────

    def profitability_by_state(self) -> pd.DataFrame:
        """
        Profit, profit margin, and sales by state — sorted worst to best margin.
        Highlights underperforming geographies.
        """
        grouped = (
            self.df.groupby(["State", "Region"])
            .agg(
                sales=("Sales", "sum"),
                profit=("Profit", "sum"),
                orders=("Order ID", "nunique"),
            )
            .reset_index()
        )
        grouped["profit_margin"] = (grouped["profit"] / grouped["sales"] * 100).round(2)
        return grouped.sort_values("profit_margin")

    def subcategory_return_on_sales(self) -> pd.DataFrame:
        """
        Return on Sales (RoS) = Profit / Sales per sub-category.
        Negative RoS indicates a money-losing sub-category.
        """
        grouped = (
            self.df.groupby(["Category", "Sub-Category"])
            .agg(
                sales=("Sales", "sum"),
                profit=("Profit", "sum"),
                avg_discount=("Discount", "mean"),
                quantity=("Quantity", "sum"),
            )
            .reset_index()
        )
        grouped["return_on_sales"] = (grouped["profit"] / grouped["sales"] * 100).round(2)
        grouped["avg_discount_pct"] = (grouped["avg_discount"] * 100).round(2)
        return grouped.sort_values("return_on_sales")

    def yoy_profit_growth(self) -> pd.DataFrame:
        """
        Year-over-year profit growth rate per category.

        Returns:
            DataFrame with year, category, profit, prior_year_profit, yoy_growth_pct.
        """
        df = self.df.copy()
        yearly = (
            df.groupby(["order_year", "Category"])
            .agg(profit=("Profit", "sum"), sales=("Sales", "sum"))
            .reset_index()
            .sort_values(["Category", "order_year"])
        )
        yearly["prior_year_profit"] = yearly.groupby("Category")["profit"].shift(1)
        yearly["yoy_growth_pct"] = (
            (yearly["profit"] - yearly["prior_year_profit"])
            / yearly["prior_year_profit"].abs()
            * 100
        ).round(2)
        return yearly

    def high_discount_low_profit_risk(
        self,
        discount_threshold: float = 0.3,
        margin_threshold: float = 5.0,
    ) -> pd.DataFrame:
        """
        Identify line items where discount ≥ threshold AND profit margin < threshold.
        These represent pricing risk and require attention.

        Args:
            discount_threshold: Minimum discount rate (e.g. 0.3 = 30%).
            margin_threshold: Maximum acceptable profit margin %.

        Returns:
            Risk DataFrame sorted by profit ascending.
        """
        risk = self.df[
            (self.df["Discount"] >= discount_threshold)
            & (self.df["profit_margin"] < margin_threshold)
        ].copy()

        risk_summary = (
            risk.groupby(["Product Name", "Sub-Category", "Category"])
            .agg(
                total_sales=("Sales", "sum"),
                total_profit=("Profit", "sum"),
                avg_discount=("Discount", "mean"),
                avg_margin=("profit_margin", "mean"),
                occurrences=("Order ID", "count"),
            )
            .reset_index()
            .sort_values("total_profit")
        )
        logger.info(
            f"High-discount / low-profit risk products: {len(risk_summary)}"
        )
        return risk_summary

    def profitability_pivot(self) -> pd.DataFrame:
        """
        Pivot table: Region (rows) × Category (cols) → Profit.
        Useful for heatmap visualization.
        """
        pivot = (
            self.df.pivot_table(
                values="Profit",
                index="Region",
                columns="Category",
                aggfunc="sum",
            )
            .round(2)
        )
        return pivot

    def monthly_profit_trend(self) -> pd.DataFrame:
        """Profit trend month over month with rolling 3-month average."""
        df = self.df.copy()
        df["year_month"] = df["Order Date"].dt.to_period("M").astype(str)
        monthly = (
            df.groupby("year_month")
            .agg(profit=("Profit", "sum"), sales=("Sales", "sum"))
            .reset_index()
            .sort_values("year_month")
        )
        monthly["profit_margin"] = (monthly["profit"] / monthly["sales"] * 100).round(2)
        monthly["profit_rolling_3m"] = (
            monthly["profit"].rolling(window=3, min_periods=1).mean().round(2)
        )
        return monthly

    # ── Executive Summary ─────────────────────────────────────────────────

    def executive_summary(self) -> dict:
        """
        Produce an executive-level profitability summary.

        Returns:
            Dict suitable for display in a dashboard header/KPI card.
        """
        df = self.df
        total_sales = df["Sales"].sum()
        total_profit = df["Profit"].sum()
        overall_margin = (total_profit / total_sales * 100) if total_sales else 0

        best_cat = (
            df.groupby("Category")["Profit"].sum().idxmax()
        )
        worst_cat = (
            df.groupby("Category")["Profit"].sum().idxmin()
        )

        best_region = df.groupby("Region")["Profit"].sum().idxmax()
        worst_region = df.groupby("Region")["Profit"].sum().idxmin()

        loss_amount = df[df["Profit"] < 0]["Profit"].sum()
        loss_sku_count = (
            df.groupby("Product ID")["Profit"].sum() < 0
        ).sum()

        summary = {
            "total_sales": round(total_sales, 2),
            "total_profit": round(total_profit, 2),
            "overall_profit_margin_pct": round(overall_margin, 2),
            "best_performing_category": best_cat,
            "worst_performing_category": worst_cat,
            "best_performing_region": best_region,
            "worst_performing_region": worst_region,
            "total_loss_amount": round(loss_amount, 2),
            "loss_making_sku_count": int(loss_sku_count),
        }

        logger.info("Executive profitability summary computed.")
        for k, v in summary.items():
            logger.info(f"  {k}: {v}")

        return summary

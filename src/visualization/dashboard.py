"""
Visualization & Dashboard Module — SuperStore Analytics System.

Generates all charts saved as PNG files in data/output/.
Covers:
  - KPI summary card (text-based)
  - Monthly sales & profit trends
  - Category performance bar charts
  - Regional heatmap
  - Discount impact on profit margin
  - Loss-making products chart
  - Sub-category return on sales
  - Shipping mode distribution

Usage:
    from src.visualization.dashboard import Dashboard
    from src.analytics.sales_analytics import SalesAnalytics
    from src.analytics.profitability_analytics import ProfitabilityAnalytics

    sa = SalesAnalytics(clean_df)
    pa = ProfitabilityAnalytics(clean_df)
    dash = Dashboard(sa, pa, output_dir="data/output")
    dash.generate_all()
"""

import os
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd
import seaborn as sns

from src.utils.config import get_path, load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Default style
sns.set_theme(style="whitegrid", palette="husl", font_scale=1.1)
plt.rcParams.update({"figure.dpi": 150, "savefig.bbox": "tight"})

_CURRENCY_FMT = lambda x, _: f"${x:,.0f}"
_PCT_FMT = lambda x, _: f"{x:.1f}%"


class Dashboard:
    """
    Generates and saves all visualization artifacts.

    Args:
        sa: Initialized SalesAnalytics instance.
        pa: Initialized ProfitabilityAnalytics instance.
        output_dir: Directory where chart PNGs will be written.
        config_path: Optional config.yaml override.
    """

    def __init__(
        self,
        sa,
        pa,
        output_dir: Optional[str] = None,
        config_path: Optional[str] = None,
    ):
        self.sa = sa
        self.pa = pa
        self.config = load_config(config_path)
        self.output_dir = output_dir or get_path("output_dir", self.config)
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        self.figsize = tuple(self.config["visualization"]["figure_size"])
        logger.info(f"Dashboard initialized. Output → {self.output_dir}")

    # ── Orchestrator ──────────────────────────────────────────────────────

    def generate_all(self) -> list[str]:
        """
        Generate and save all dashboard charts.

        Returns:
            List of saved file paths.
        """
        saved = []

        charts = [
            ("kpi_summary_card", self.kpi_summary_card),
            ("monthly_sales_trend", self.monthly_sales_trend_chart),
            ("quarterly_trend", self.quarterly_trend_chart),
            ("yearly_trend", self.yearly_trend_chart),
            ("category_performance", self.category_performance_chart),
            ("subcategory_performance", self.subcategory_performance_chart),
            ("regional_performance", self.regional_performance_chart),
            ("regional_heatmap", self.regional_heatmap_chart),
            ("loss_making_products", self.loss_making_products_chart),
            ("discount_impact", self.discount_impact_chart),
            ("segment_performance", self.segment_performance_chart),
            ("shipping_analysis", self.shipping_analysis_chart),
            ("profitability_by_state", self.state_profitability_chart),
            ("subcategory_ros", self.subcategory_ros_chart),
        ]

        for name, fn in charts:
            try:
                path = fn()
                saved.append(path)
                logger.info(f"Chart saved: {path}")
            except Exception as exc:
                logger.error(f"Failed to generate chart '{name}': {exc}")

        logger.info(f"Dashboard generation complete. {len(saved)}/{len(charts)} charts saved.")
        return saved

    # ── KPI Summary ───────────────────────────────────────────────────────

    def kpi_summary_card(self) -> str:
        """Text KPI card rendered as a matplotlib figure."""
        kpis = self.sa.kpi_summary()
        exec_sum = self.pa.executive_summary()

        fig, ax = plt.subplots(figsize=(14, 5))
        ax.axis("off")
        fig.patch.set_facecolor("#1a1a2e")

        lines = [
            ("SuperStore Sales & Business Performance KPIs", 0.92, 20, "#e94560", "bold"),
            (f"Total Sales: ${kpis['total_sales']:,.2f}", 0.75, 15, "#f5f5f5", "normal"),
            (f"Total Profit: ${kpis['total_profit']:,.2f}", 0.62, 15, "#4ecca3", "normal"),
            (f"Total Orders: {kpis['total_orders']:,}", 0.49, 15, "#f5f5f5", "normal"),
            (f"Total Customers: {kpis['total_customers']:,}", 0.36, 15, "#f5f5f5", "normal"),
            (
                f"Avg Order Value: ${kpis['avg_order_value']:,.2f}   |   "
                f"Avg Profit Margin: {kpis['avg_profit_margin_pct']:.1f}%   |   "
                f"Overall Discount Rate: {kpis['overall_discount_rate_pct']:.1f}%",
                0.22, 12, "#a8dadc", "normal",
            ),
            (
                f"Best Category: {exec_sum['best_performing_category']}   |   "
                f"Best Region: {exec_sum['best_performing_region']}   |   "
                f"Loss-making SKUs: {exec_sum['loss_making_sku_count']}",
                0.10, 12, "#e9c46a", "normal",
            ),
        ]

        for text, y, size, color, weight in lines:
            ax.text(0.5, y, text, transform=ax.transAxes, fontsize=size,
                    color=color, ha="center", va="center", fontweight=weight)

        return self._save(fig, "kpi_summary_card")

    # ── Trend Charts ──────────────────────────────────────────────────────

    def monthly_sales_trend_chart(self) -> str:
        """Dual-axis chart: monthly sales (bar) + profit (line)."""
        data = self.sa.monthly_sales_trend()
        fig, ax1 = plt.subplots(figsize=self.figsize)

        x = range(len(data))
        bars = ax1.bar(x, data["sales"], color="#4a90d9", alpha=0.75, label="Sales")
        ax1.set_ylabel("Sales ($)", color="#4a90d9")
        ax1.yaxis.set_major_formatter(mticker.FuncFormatter(_CURRENCY_FMT))
        ax1.tick_params(axis="y", colors="#4a90d9")

        ax2 = ax1.twinx()
        ax2.plot(x, data["profit"], color="#e94560", marker="o", linewidth=2, label="Profit")
        ax2.set_ylabel("Profit ($)", color="#e94560")
        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(_CURRENCY_FMT))
        ax2.tick_params(axis="y", colors="#e94560")

        ax1.set_xticks(list(x))
        tick_labels = [d if i % 3 == 0 else "" for i, d in enumerate(data["year_month"])]
        ax1.set_xticklabels(tick_labels, rotation=45, ha="right", fontsize=8)

        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

        plt.title("Monthly Sales & Profit Trend", fontsize=14, fontweight="bold")
        plt.tight_layout()
        return self._save(fig, "monthly_sales_trend")

    def quarterly_trend_chart(self) -> str:
        """Quarterly sales + profit margin trend."""
        data = self.sa.quarterly_sales_trend()
        fig, ax1 = plt.subplots(figsize=self.figsize)

        x = range(len(data))
        ax1.bar(x, data["sales"], color="#4ecca3", alpha=0.8, label="Sales")
        ax1.set_ylabel("Sales ($)")
        ax1.yaxis.set_major_formatter(mticker.FuncFormatter(_CURRENCY_FMT))

        ax2 = ax1.twinx()
        ax2.plot(x, data["profit_margin"], color="#e94560", marker="s",
                 linewidth=2, label="Profit Margin %")
        ax2.set_ylabel("Profit Margin (%)")
        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(_PCT_FMT))

        ax1.set_xticks(list(x))
        ax1.set_xticklabels(data["year_quarter"], rotation=45, ha="right", fontsize=9)

        lines1, _ = ax1.get_legend_handles_labels()
        lines2, _ = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, ["Sales", "Profit Margin %"], loc="upper left")

        plt.title("Quarterly Sales & Profit Margin", fontsize=14, fontweight="bold")
        plt.tight_layout()
        return self._save(fig, "quarterly_trend")

    def yearly_trend_chart(self) -> str:
        """Yearly grouped bar: Sales vs Profit."""
        data = self.sa.yearly_sales_trend()
        x = np.arange(len(data))
        width = 0.35

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(x - width / 2, data["sales"], width, label="Sales", color="#4a90d9")
        ax.bar(x + width / 2, data["profit"], width, label="Profit", color="#4ecca3")
        ax.set_xticks(x)
        ax.set_xticklabels(data["order_year"].astype(str))
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(_CURRENCY_FMT))
        ax.set_ylabel("Amount ($)")
        ax.legend()
        plt.title("Yearly Sales vs Profit Comparison", fontsize=14, fontweight="bold")
        plt.tight_layout()
        return self._save(fig, "yearly_trend")

    # ── Category Charts ───────────────────────────────────────────────────

    def category_performance_chart(self) -> str:
        """Horizontal grouped bar: Sales + Profit per Category."""
        data = self.sa.category_performance()
        x = np.arange(len(data))
        width = 0.35

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(x + width / 2, data["sales"], width, label="Sales", color="#4a90d9")
        ax.barh(x - width / 2, data["profit"], width, label="Profit", color="#4ecca3")
        ax.set_yticks(x)
        ax.set_yticklabels(data["Category"])
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_CURRENCY_FMT))
        ax.set_xlabel("Amount ($)")
        ax.legend()
        plt.title("Category Performance: Sales vs Profit", fontsize=14, fontweight="bold")
        plt.tight_layout()
        return self._save(fig, "category_performance")

    def subcategory_performance_chart(self) -> str:
        """Horizontal bar chart: profit per sub-category, colored red/green."""
        data = self.sa.subcategory_performance().head(20)
        colors = ["#e94560" if p < 0 else "#4ecca3" for p in data["profit"]]

        fig, ax = plt.subplots(figsize=(12, 8))
        ax.barh(data["Sub-Category"], data["profit"], color=colors)
        ax.axvline(0, color="gray", linewidth=0.8, linestyle="--")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_CURRENCY_FMT))
        ax.set_xlabel("Total Profit ($)")
        plt.title("Sub-Category Profitability (Red = Loss)", fontsize=14, fontweight="bold")
        plt.tight_layout()
        return self._save(fig, "subcategory_performance")

    # ── Regional Charts ───────────────────────────────────────────────────

    def regional_performance_chart(self) -> str:
        """Pie chart of regional sales share + bar of regional profit."""
        data = self.sa.regional_performance()

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

        ax1.pie(data["sales"], labels=data["Region"], autopct="%1.1f%%",
                colors=sns.color_palette("husl", len(data)), startangle=90)
        ax1.set_title("Regional Sales Share")

        colors = ["#4ecca3" if p > 0 else "#e94560" for p in data["profit"]]
        ax2.bar(data["Region"], data["profit"], color=colors)
        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(_CURRENCY_FMT))
        ax2.set_ylabel("Total Profit ($)")
        ax2.set_title("Regional Profit Comparison")

        plt.suptitle("Regional Performance", fontsize=16, fontweight="bold")
        plt.tight_layout()
        return self._save(fig, "regional_performance")

    def regional_heatmap_chart(self) -> str:
        """Heatmap: Region × Category → Sales."""
        data = self.sa.regional_category_heatmap_data()

        fig, ax = plt.subplots(figsize=(10, 6))
        sns.heatmap(
            data,
            annot=True,
            fmt=",.0f",
            cmap="YlOrRd",
            linewidths=0.5,
            ax=ax,
        )
        ax.set_title("Sales Heatmap: Region × Category", fontsize=14, fontweight="bold")
        plt.tight_layout()
        return self._save(fig, "regional_heatmap")

    # ── Loss & Risk Charts ────────────────────────────────────────────────

    def loss_making_products_chart(self) -> str:
        """Bottom-10 products by profit (loss-making)."""
        data = self.sa.loss_making_products().head(10)
        if data.empty:
            logger.info("No loss-making products — skipping chart.")
            return ""

        fig, ax = plt.subplots(figsize=(12, 6))
        labels = [n[:35] + "…" if len(n) > 35 else n for n in data["Product Name"]]
        ax.barh(labels, data["profit"], color="#e94560")
        ax.axvline(0, color="gray", linewidth=0.8, linestyle="--")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_CURRENCY_FMT))
        ax.set_xlabel("Total Profit ($)")
        plt.title("Top 10 Loss-Making Products", fontsize=14, fontweight="bold")
        plt.tight_layout()
        return self._save(fig, "loss_making_products")

    def discount_impact_chart(self) -> str:
        """Line chart: discount bucket vs average profit margin."""
        data = self.sa.discount_impact_analysis()

        fig, ax1 = plt.subplots(figsize=self.figsize)
        ax1.bar(
            data["discount_bucket"].astype(str),
            data["total_sales"],
            color="#4a90d9",
            alpha=0.6,
            label="Total Sales",
        )
        ax1.set_ylabel("Total Sales ($)")
        ax1.yaxis.set_major_formatter(mticker.FuncFormatter(_CURRENCY_FMT))

        ax2 = ax1.twinx()
        ax2.plot(
            data["discount_bucket"].astype(str),
            data["avg_profit_margin"],
            color="#e94560",
            marker="o",
            linewidth=2,
            label="Avg Profit Margin %",
        )
        ax2.axhline(0, color="gray", linewidth=0.8, linestyle="--")
        ax2.set_ylabel("Avg Profit Margin (%)")

        lines1, l1 = ax1.get_legend_handles_labels()
        lines2, l2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, l1 + l2, loc="upper right")

        plt.title("Discount Impact on Profit Margin", fontsize=14, fontweight="bold")
        ax1.set_xlabel("Discount Bucket")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        return self._save(fig, "discount_impact")

    # ── Segment & Shipping ────────────────────────────────────────────────

    def segment_performance_chart(self) -> str:
        """Grouped bar: Sales, Profit per customer segment."""
        data = self.sa.segment_performance()
        x = np.arange(len(data))
        width = 0.35

        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(x - width / 2, data["sales"], width, label="Sales", color="#4a90d9")
        ax.bar(x + width / 2, data["profit"], width, label="Profit", color="#4ecca3")
        ax.set_xticks(x)
        ax.set_xticklabels(data["Segment"])
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(_CURRENCY_FMT))
        ax.legend()
        plt.title("Customer Segment Performance", fontsize=14, fontweight="bold")
        plt.tight_layout()
        return self._save(fig, "segment_performance")

    def shipping_analysis_chart(self) -> str:
        """Donut chart of order share by ship mode + avg days table."""
        data = self.sa.shipping_analysis()

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 6))

        wedges, texts, autotexts = ax1.pie(
            data["order_share_pct"],
            labels=data["Ship Mode"],
            autopct="%1.1f%%",
            colors=sns.color_palette("Set2", len(data)),
            startangle=90,
            wedgeprops={"width": 0.5},
        )
        ax1.set_title("Order Share by Ship Mode")

        ax2.axis("off")
        table_data = [
            [row["Ship Mode"], f"{row['avg_shipping_days']:.1f} days", f"{row['order_share_pct']:.1f}%"]
            for _, row in data.iterrows()
        ]
        tbl = ax2.table(
            cellText=table_data,
            colLabels=["Ship Mode", "Avg Days", "% Orders"],
            cellLoc="center",
            loc="center",
        )
        tbl.scale(1.2, 1.5)
        ax2.set_title("Shipping Mode Details")

        plt.suptitle("Shipping Analysis", fontsize=16, fontweight="bold")
        plt.tight_layout()
        return self._save(fig, "shipping_analysis")

    # ── Profitability Charts ───────────────────────────────────────────────

    def state_profitability_chart(self) -> str:
        """Bottom-15 states by profit margin (most underperforming)."""
        data = self.pa.profitability_by_state().head(15)

        colors = ["#e94560" if m < 0 else "#4a90d9" for m in data["profit_margin"]]
        fig, ax = plt.subplots(figsize=(12, 7))
        ax.barh(data["State"], data["profit_margin"], color=colors)
        ax.axvline(0, color="gray", linewidth=0.8, linestyle="--")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_PCT_FMT))
        ax.set_xlabel("Profit Margin (%)")
        plt.title("Bottom 15 States by Profit Margin", fontsize=14, fontweight="bold")
        plt.tight_layout()
        return self._save(fig, "profitability_by_state")

    def subcategory_ros_chart(self) -> str:
        """Return on Sales per sub-category, sorted ascending."""
        data = self.pa.subcategory_return_on_sales()

        colors = ["#e94560" if r < 0 else "#4ecca3" for r in data["return_on_sales"]]
        fig, ax = plt.subplots(figsize=(12, 8))
        ax.barh(data["Sub-Category"], data["return_on_sales"], color=colors)
        ax.axvline(0, color="gray", linewidth=0.8, linestyle="--")
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_PCT_FMT))
        ax.set_xlabel("Return on Sales (%)")
        plt.title("Sub-Category Return on Sales (RoS)", fontsize=14, fontweight="bold")
        plt.tight_layout()
        return self._save(fig, "subcategory_ros")

    # ── Helper ────────────────────────────────────────────────────────────

    def _save(self, fig: plt.Figure, name: str) -> str:
        """Save a figure to the output directory and close it."""
        fmt = self.config["visualization"].get("output_format", "png")
        path = os.path.join(self.output_dir, f"{name}.{fmt}")
        fig.savefig(path, format=fmt, bbox_inches="tight")
        plt.close(fig)
        return path

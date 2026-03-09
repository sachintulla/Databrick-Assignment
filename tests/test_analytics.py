"""
Unit tests for Sales & Profitability Analytics modules.

Coverage:
  - KPI summary completeness and value ranges
  - Monthly / quarterly trend shape
  - Category / regional grouping correctness
  - Loss-making product detection
  - Discount impact analysis
  - Profitability executive summary
  - YoY growth computation
"""

import numpy as np
import pandas as pd
import pytest

from src.analytics.profitability_analytics import ProfitabilityAnalytics
from src.analytics.sales_analytics import SalesAnalytics


@pytest.fixture(scope="module")
def sa(clean_df):
    return SalesAnalytics(clean_df)


@pytest.fixture(scope="module")
def pa(clean_df):
    return ProfitabilityAnalytics(clean_df)


class TestKPISummary:

    def test_kpi_summary_has_required_keys(self, sa):
        kpis = sa.kpi_summary()
        required = [
            "total_sales", "total_profit", "total_orders",
            "total_customers", "avg_order_value",
            "avg_profit_margin_pct", "overall_discount_rate_pct",
            "loss_line_items_pct",
        ]
        for key in required:
            assert key in kpis, f"Missing KPI key: '{key}'"

    def test_total_sales_positive(self, sa, clean_df):
        kpis = sa.kpi_summary()
        assert kpis["total_sales"] == round(clean_df["Sales"].sum(), 2)
        assert kpis["total_sales"] > 0

    def test_total_profit_matches(self, sa, clean_df):
        kpis = sa.kpi_summary()
        assert kpis["total_profit"] == round(clean_df["Profit"].sum(), 2)

    def test_total_orders_positive(self, sa):
        assert sa.kpi_summary()["total_orders"] > 0

    def test_total_customers_positive(self, sa):
        assert sa.kpi_summary()["total_customers"] > 0


class TestTrends:

    def test_monthly_trend_returns_dataframe(self, sa):
        result = sa.monthly_sales_trend()
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_monthly_trend_columns(self, sa):
        result = sa.monthly_sales_trend()
        assert "year_month" in result.columns
        assert "sales" in result.columns
        assert "profit" in result.columns
        assert "profit_margin" in result.columns

    def test_quarterly_trend_returns_dataframe(self, sa):
        result = sa.quarterly_sales_trend()
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_yearly_trend_returns_dataframe(self, sa):
        result = sa.yearly_sales_trend()
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0


class TestCategoryAnalysis:

    def test_category_performance_has_3_categories(self, sa):
        """Our fixture has 3 categories: Furniture, Office Supplies, Technology."""
        result = sa.category_performance()
        assert len(result) == 3

    def test_category_performance_columns(self, sa):
        result = sa.category_performance()
        for col in ["Category", "sales", "profit", "profit_margin"]:
            assert col in result.columns

    def test_category_sales_sum_matches_total(self, sa, clean_df):
        cat_total = sa.category_performance()["sales"].sum()
        assert abs(cat_total - clean_df["Sales"].sum()) < 0.01

    def test_subcategory_performance_not_empty(self, sa):
        result = sa.subcategory_performance()
        assert len(result) > 0


class TestRegionalAnalysis:

    def test_regional_performance_has_4_regions(self, sa):
        result = sa.regional_performance()
        assert len(result) == 4

    def test_regional_sales_sum_matches_total(self, sa, clean_df):
        regional_total = sa.regional_performance()["sales"].sum()
        assert abs(regional_total - clean_df["Sales"].sum()) < 0.01

    def test_regional_heatmap_pivot_shape(self, sa):
        pivot = sa.regional_category_heatmap_data()
        assert isinstance(pivot, pd.DataFrame)
        assert pivot.shape[0] == 4  # 4 regions
        assert pivot.shape[1] == 3  # 3 categories


class TestLossDetection:

    def test_loss_products_are_negative(self, sa):
        """All returned products must have profit < 0."""
        result = sa.loss_making_products()
        if len(result) > 0:
            assert (result["profit"] < 0).all()

    def test_loss_products_columns(self, sa):
        result = sa.loss_making_products()
        assert "Product Name" in result.columns
        assert "profit" in result.columns
        assert "avg_discount_pct" in result.columns

    def test_loss_products_sorted_ascending(self, sa):
        """Loss products should be sorted worst (most negative) first."""
        result = sa.loss_making_products()
        if len(result) > 1:
            assert result["profit"].iloc[0] <= result["profit"].iloc[-1]


class TestDiscountImpact:

    def test_discount_impact_returns_dataframe(self, sa):
        result = sa.discount_impact_analysis()
        assert isinstance(result, pd.DataFrame)

    def test_discount_buckets_present(self, sa):
        result = sa.discount_impact_analysis()
        assert "discount_bucket" in result.columns
        assert "avg_profit_margin" in result.columns


class TestSegmentAnalysis:

    def test_segment_performance_has_3_segments(self, sa):
        result = sa.segment_performance()
        assert len(result) == 3

    def test_top_customers_returns_dataframe(self, sa):
        result = sa.top_customers_by_sales(n=5)
        assert len(result) <= 5


class TestShippingAnalysis:

    def test_shipping_analysis_not_empty(self, sa):
        result = sa.shipping_analysis()
        assert len(result) > 0

    def test_order_share_sums_to_100(self, sa):
        result = sa.shipping_analysis()
        assert abs(result["order_share_pct"].sum() - 100.0) < 0.1


class TestProfitabilityAnalytics:

    def test_executive_summary_keys(self, pa):
        summary = pa.executive_summary()
        required = [
            "total_sales", "total_profit", "overall_profit_margin_pct",
            "best_performing_category", "worst_performing_category",
            "best_performing_region", "worst_performing_region",
            "total_loss_amount", "loss_making_sku_count",
        ]
        for key in required:
            assert key in summary, f"Missing exec summary key: '{key}'"

    def test_best_region_is_string(self, pa):
        summary = pa.executive_summary()
        assert isinstance(summary["best_performing_region"], str)

    def test_loss_sku_count_non_negative(self, pa):
        summary = pa.executive_summary()
        assert summary["loss_making_sku_count"] >= 0

    def test_subcategory_ros_sorted(self, pa):
        """Return on Sales should be sorted ascending (worst first)."""
        result = pa.subcategory_return_on_sales()
        assert result["return_on_sales"].iloc[0] <= result["return_on_sales"].iloc[-1]

    def test_yoy_growth_has_growth_column(self, pa):
        result = pa.yoy_profit_growth()
        assert "yoy_growth_pct" in result.columns

    def test_profitability_pivot_shape(self, pa):
        pivot = pa.profitability_pivot()
        assert isinstance(pivot, pd.DataFrame)
        assert pivot.shape[0] == 4  # 4 regions
        assert pivot.shape[1] == 3  # 3 categories

    def test_monthly_profit_trend_rolling(self, pa):
        result = pa.monthly_profit_trend()
        assert "profit_rolling_3m" in result.columns
        assert len(result) > 0

    def test_high_discount_risk_products(self, pa):
        result = pa.high_discount_low_profit_risk(discount_threshold=0.3, margin_threshold=20.0)
        assert isinstance(result, pd.DataFrame)
        if len(result) > 0:
            assert "total_profit" in result.columns

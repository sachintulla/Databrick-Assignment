"""
Unit tests for the Data Cleaning module (DataCleaner).

Coverage:
  - Duplicate removal
  - Null handling (Postal Code, numeric, categorical)
  - Type enforcement (dates, numerics)
  - Derived metric creation (profit_margin, discount_amount, shipping_days, is_loss)
  - Categorical standardization
"""

import pandas as pd
import pytest

from src.cleaning.data_cleaner import DataCleaner


class TestDataCleaner:

    def test_removes_exact_duplicates(self, sample_df):
        """Duplicate row injected in fixture must be removed."""
        cleaner = DataCleaner()
        clean = cleaner.clean(sample_df)
        # Original sample_df has one duplicate appended
        assert len(clean) < len(sample_df)

    def test_no_duplicates_after_clean(self, clean_df):
        """No duplicate rows remain after cleaning."""
        assert clean_df.duplicated().sum() == 0

    def test_postal_code_nulls_filled(self, clean_df):
        """Null Postal Code values replaced with '00000'."""
        assert clean_df["Postal Code"].isnull().sum() == 0

    def test_no_critical_nulls_remain(self, clean_df):
        """No nulls in Sales, Profit, Order ID after cleaning."""
        for col in ["Sales", "Profit", "Order ID", "Region", "Category"]:
            assert clean_df[col].isnull().sum() == 0, f"Nulls remain in '{col}'"

    def test_order_date_is_datetime(self, clean_df):
        """Order Date column should be datetime dtype."""
        assert pd.api.types.is_datetime64_any_dtype(clean_df["Order Date"])

    def test_ship_date_is_datetime(self, clean_df):
        """Ship Date column should be datetime dtype."""
        assert pd.api.types.is_datetime64_any_dtype(clean_df["Ship Date"])

    def test_sales_is_float(self, clean_df):
        """Sales column should be float."""
        assert pd.api.types.is_float_dtype(clean_df["Sales"])

    def test_profit_is_float(self, clean_df):
        """Profit column should be float."""
        assert pd.api.types.is_float_dtype(clean_df["Profit"])

    def test_quantity_is_int(self, clean_df):
        """Quantity should be integer dtype."""
        assert pd.api.types.is_integer_dtype(clean_df["Quantity"])

    # ── Derived metrics ──────────────────────────────────────────────────

    def test_profit_margin_column_created(self, clean_df):
        """profit_margin column must exist."""
        assert "profit_margin" in clean_df.columns

    def test_profit_margin_formula(self, clean_df):
        """profit_margin = Profit / Sales * 100 (within rounding tolerance)."""
        mask = clean_df["Sales"] > 0
        recalc = (clean_df.loc[mask, "Profit"] / clean_df.loc[mask, "Sales"] * 100).round(1)
        stored = clean_df.loc[mask, "profit_margin"].round(1)
        assert (abs(recalc - stored) < 0.6).all(), "profit_margin formula mismatch"

    def test_discount_amount_column_created(self, clean_df):
        """discount_amount column must exist."""
        assert "discount_amount" in clean_df.columns

    def test_discount_amount_formula(self, clean_df):
        """discount_amount = Sales * Discount (within rounding tolerance)."""
        recalc = (clean_df["Sales"] * clean_df["Discount"]).round(1)
        stored = clean_df["discount_amount"].round(1)
        assert (abs(recalc - stored) < 0.1).all()

    def test_shipping_days_column_created(self, clean_df):
        """shipping_days column must exist."""
        assert "shipping_days" in clean_df.columns

    def test_shipping_days_non_negative(self, clean_df):
        """shipping_days must be >= 0 for all rows."""
        assert (clean_df["shipping_days"] >= 0).all()

    def test_is_loss_column_created(self, clean_df):
        """is_loss boolean column must exist."""
        assert "is_loss" in clean_df.columns

    def test_is_loss_correctly_flags_negatives(self, clean_df):
        """is_loss = True iff Profit < 0."""
        expected = clean_df["Profit"] < 0
        assert (clean_df["is_loss"] == expected).all()

    def test_order_year_column(self, clean_df):
        """order_year derived from Order Date."""
        assert "order_year" in clean_df.columns
        assert clean_df["order_year"].isin([2021, 2022, 2023, 2024, 2025]).all() or \
               clean_df["order_year"].notna().all()

    def test_order_month_column(self, clean_df):
        """order_month must be 1–12."""
        assert "order_month" in clean_df.columns
        assert clean_df["order_month"].between(1, 12).all()

    def test_order_quarter_column(self, clean_df):
        """order_quarter must be 1–4."""
        assert "order_quarter" in clean_df.columns
        assert clean_df["order_quarter"].between(1, 4).all()

    # ── Categoricals ─────────────────────────────────────────────────────

    def test_region_title_case(self, clean_df):
        """Region values should be title-cased."""
        assert clean_df["Region"].str.istitle().all() or \
               clean_df["Region"].isin(["East", "West", "Central", "South"]).all()

    def test_category_no_leading_whitespace(self, clean_df):
        """Category values should have no leading/trailing whitespace."""
        assert (clean_df["Category"] == clean_df["Category"].str.strip()).all()

    # ── Save / load ───────────────────────────────────────────────────────

    def test_save_creates_parquet_file(self, clean_df, tmp_path):
        """DataCleaner.save() creates a Parquet file at the given path."""
        cleaner = DataCleaner()
        out = tmp_path / "output.parquet"
        saved = cleaner.save(clean_df, file_path=str(out))
        import os
        assert os.path.exists(saved)
        loaded = pd.read_parquet(saved)
        assert loaded.shape == clean_df.shape

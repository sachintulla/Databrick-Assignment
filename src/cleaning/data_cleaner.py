"""
Data Cleaning & Transformation Module — SuperStore Analytics System.

Responsibilities:
  - Remove duplicate records.
  - Handle missing / null values.
  - Standardize region, category, date, and ship-mode fields.
  - Enforce correct data types.
  - Create derived/engineered metrics:
      * profit_margin       = Profit / Sales  (rounded %)
      * discount_amount     = Sales * Discount
      * shipping_days       = Ship Date − Order Date
      * order_year, order_month, order_quarter
      * is_loss             = True when Profit < 0
  - Persist cleaned data as Parquet for downstream use.

Usage:
    from src.cleaning.data_cleaner import DataCleaner
    cleaner = DataCleaner()
    clean_df = cleaner.clean(raw_df)
    cleaner.save(clean_df)
"""

import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from src.utils.config import get_path, load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataCleaner:
    """
    Cleans and enriches the raw SuperStore DataFrame.

    Args:
        config_path: Optional path to config.yaml override.
    """

    def __init__(self, config_path: Optional[str] = None):
        self.config = load_config(config_path)
        self.processed_dir = get_path("processed_data_dir", self.config)
        self.processed_file = self.config["data"]["processed_file"]
        self.date_format = self.config["data"]["date_format"]
        self.date_cols = self.config["etl"]["date_columns"]
        self.numeric_cols = self.config["etl"]["numeric_columns"]
        self.cat_cols = self.config["etl"]["categorical_columns"]
        self.dup_subset = self.config["etl"]["duplicate_subset"]

    # ── Public API ────────────────────────────────────────────────────────

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run the full cleaning pipeline.

        Steps executed in order:
          1. Remove duplicates
          2. Handle missing values
          3. Enforce data types (dates, numerics)
          4. Standardize categorical fields
          5. Create derived metrics

        Args:
            df: Raw DataFrame from DataLoader.

        Returns:
            Cleaned and enriched DataFrame.
        """
        logger.info("Starting data cleaning pipeline ...")
        df = df.copy()

        df = self._remove_duplicates(df)
        df = self._handle_missing(df)
        df = self._enforce_types(df)
        df = self._standardize_categoricals(df)
        df = self._create_derived_metrics(df)

        logger.info(f"Cleaning complete. Final shape: {df.shape}")
        return df

    def save(self, df: pd.DataFrame, file_path: Optional[str] = None) -> str:
        """
        Persist cleaned DataFrame as Parquet.

        Args:
            df: Cleaned DataFrame.
            file_path: Optional output path override.

        Returns:
            Path where the file was saved.
        """
        Path(self.processed_dir).mkdir(parents=True, exist_ok=True)
        path = file_path or os.path.join(self.processed_dir, self.processed_file)
        df.to_parquet(path, index=False, engine="pyarrow")
        logger.info(f"Cleaned data saved → {path}  ({len(df):,} rows)")
        return path

    # ── Pipeline steps ────────────────────────────────────────────────────

    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove exact and business-key duplicates."""
        before = len(df)

        # Exact row duplicates
        df = df.drop_duplicates()

        # Business-key duplicates (same order + product + customer appearing twice)
        dup_subset = [c for c in self.dup_subset if c in df.columns]
        df = df.drop_duplicates(subset=dup_subset, keep="first")

        after = len(df)
        removed = before - after
        logger.info(f"Duplicates removed: {removed:,}  ({before:,} → {after:,} rows)")
        return df

    def _handle_missing(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Strategy per column type:
          - Numeric   → fill with median of that column
          - Postal Code → fill with '00000' (unknown)
          - Categorical → fill with mode
          - Other string → fill with 'Unknown'
        """
        for col in df.columns:
            n_null = df[col].isna().sum()
            if n_null == 0:
                continue

            if col in self.numeric_cols:
                fill_val = df[col].median()
                df[col] = df[col].fillna(fill_val)
                logger.info(f"'{col}': filled {n_null} nulls with median ({fill_val:.4f})")

            elif col == "Postal Code":
                df[col] = df[col].fillna("00000").astype(str)
                logger.info(f"'{col}': filled {n_null} nulls with '00000'")

            elif col in self.cat_cols:
                mode_val = df[col].mode(dropna=True)
                fill_val = mode_val.iloc[0] if not mode_val.empty else "Unknown"
                df[col] = df[col].fillna(fill_val)
                logger.info(f"'{col}': filled {n_null} nulls with mode ('{fill_val}')")

            else:
                df[col] = df[col].fillna("Unknown")
                logger.info(f"'{col}': filled {n_null} nulls with 'Unknown'")

        return df

    def _enforce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Cast columns to the correct pandas dtype."""
        # Dates
        for col in self.date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format=self.date_format, errors="coerce")
                logger.debug(f"'{col}' cast to datetime64")

        # Numerics
        for col in self.numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
                logger.debug(f"'{col}' cast to float64")

        # Postal Code → string (may be numeric in CSV)
        if "Postal Code" in df.columns:
            df["Postal Code"] = df["Postal Code"].astype(str).str.zfill(5)

        # Quantity → integer
        if "Quantity" in df.columns:
            df["Quantity"] = df["Quantity"].fillna(0).astype(int)

        # Row ID → integer
        if "Row ID" in df.columns:
            df["Row ID"] = df["Row ID"].fillna(0).astype(int)

        return df

    def _standardize_categoricals(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalize text in categorical columns:
          - Strip whitespace
          - Title-case Region, Category, Sub-Category, Segment, Ship Mode
        """
        title_cols = ["Region", "Category", "Sub-Category", "Segment", "Ship Mode"]
        for col in title_cols:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.title()

        # Normalize known region aliases
        region_map = {
            "East": "East", "West": "West",
            "Central": "Central", "South": "South",
        }
        if "Region" in df.columns:
            df["Region"] = df["Region"].map(
                lambda r: region_map.get(r, r)
            )

        logger.info("Categorical columns standardized.")
        return df

    def _create_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Engineer new columns from existing fields:
          - profit_margin       (%)
          - discount_amount     (currency)
          - shipping_days       (int)
          - order_year          (int)
          - order_month         (int 1-12)
          - order_quarter       (int 1-4)
          - order_day_of_week   (str: Monday…Sunday)
          - is_loss             (bool)
        """
        # Profit margin — guard against zero sales
        df["profit_margin"] = np.where(
            df["Sales"] > 0,
            (df["Profit"] / df["Sales"] * 100).round(2),
            0.0,
        )

        # Discount in currency terms
        df["discount_amount"] = (df["Sales"] * df["Discount"]).round(2)

        # Shipping days
        if "Order Date" in df.columns and "Ship Date" in df.columns:
            df["shipping_days"] = (
                df["Ship Date"] - df["Order Date"]
            ).dt.days.fillna(0).astype(int)

        # Time decomposition
        if "Order Date" in df.columns:
            df["order_year"] = df["Order Date"].dt.year
            df["order_month"] = df["Order Date"].dt.month
            df["order_quarter"] = df["Order Date"].dt.quarter
            df["order_day_of_week"] = df["Order Date"].dt.day_name()

        # Loss flag
        df["is_loss"] = df["Profit"] < self.config["etl"].get("loss_threshold", 0.0)

        logger.info(
            f"Derived metrics created: profit_margin, discount_amount, "
            f"shipping_days, order_year/month/quarter, is_loss"
        )
        return df

"""
Data Ingestion Module — SuperStore Analytics System.

Responsibilities:
  - Load raw CSV / Parquet files from disk.
  - Validate schema (column presence, dtypes).
  - Validate date format and numeric fields.
  - Report data quality summary.
  - Return a clean, typed DataFrame ready for the cleaning stage.

Usage:
    from src.ingestion.data_loader import DataLoader
    loader = DataLoader()
    df = loader.load()
"""

import os
from pathlib import Path
from typing import Optional

import pandas as pd

from src.utils.config import get_path, get_value, load_config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class SchemaValidationError(Exception):
    """Raised when the loaded DataFrame fails schema validation."""


class DataLoader:
    """
    Handles loading and initial validation of the SuperStore raw dataset.

    Args:
        config_path: Optional path to config.yaml override.
    """

    def __init__(self, config_path: Optional[str] = None):
        self.config = load_config(config_path)
        self.raw_dir = get_path("raw_data_dir", self.config)
        self.raw_file = self.config["data"]["raw_file"]
        self.expected_columns = self.config["data"]["expected_columns"]
        self.date_format = self.config["data"]["date_format"]
        self.date_columns = self.config["etl"]["date_columns"]
        self.numeric_columns = self.config["etl"]["numeric_columns"]

    # ── Public API ────────────────────────────────────────────────────────

    def load(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """
        Load the raw SuperStore dataset and run validation.

        Args:
            file_path: Explicit path to the file; defaults to config path.

        Returns:
            Validated pandas DataFrame.

        Raises:
            FileNotFoundError: If the data file does not exist.
            SchemaValidationError: If the schema is invalid.
        """
        path = file_path or os.path.join(self.raw_dir, self.raw_file)
        logger.info(f"Loading dataset from: {path}")

        df = self._read_file(path)
        logger.info(f"Loaded {len(df):,} rows × {len(df.columns)} columns")

        self._validate_schema(df)
        self._validate_dates(df)
        self._validate_numerics(df)
        self._log_quality_summary(df)

        return df

    def load_parquet(self, file_path: Optional[str] = None) -> pd.DataFrame:
        """Load an already-processed Parquet file."""
        processed_dir = get_path("processed_data_dir", self.config)
        processed_file = self.config["data"]["processed_file"]
        path = file_path or os.path.join(processed_dir, processed_file)
        logger.info(f"Loading processed Parquet from: {path}")
        df = pd.read_parquet(path)
        logger.info(f"Loaded {len(df):,} rows from Parquet")
        return df

    # ── Private helpers ───────────────────────────────────────────────────

    def _read_file(self, path: str) -> pd.DataFrame:
        """Read CSV or Parquet based on extension."""
        if not Path(path).exists():
            raise FileNotFoundError(f"Data file not found: {path}")

        ext = Path(path).suffix.lower()
        try:
            if ext == ".csv":
                df = pd.read_csv(
                    path,
                    encoding=self.config["data"].get("encoding", "utf-8"),
                    low_memory=False,
                )
            elif ext in (".parquet", ".pq"):
                df = pd.read_parquet(path)
            else:
                raise ValueError(f"Unsupported file format: {ext}")
        except Exception as exc:
            logger.error(f"Failed to read file {path}: {exc}")
            raise

        return df

    def _validate_schema(self, df: pd.DataFrame) -> None:
        """
        Verify all expected columns are present.

        Raises:
            SchemaValidationError: On missing columns.
        """
        missing = set(self.expected_columns) - set(df.columns)
        if missing:
            msg = f"Schema validation failed. Missing columns: {sorted(missing)}"
            logger.error(msg)
            raise SchemaValidationError(msg)
        logger.info("Schema validation PASSED — all expected columns present.")

    def _validate_dates(self, df: pd.DataFrame) -> None:
        """
        Check that date columns can be parsed with the expected format.
        Logs warnings for unparseable values but does NOT raise.
        """
        for col in self.date_columns:
            if col not in df.columns:
                continue
            try:
                parsed = pd.to_datetime(df[col], format=self.date_format, errors="coerce")
                n_bad = parsed.isna().sum() - df[col].isna().sum()
                if n_bad > 0:
                    logger.warning(
                        f"Date column '{col}': {n_bad} values could not be parsed "
                        f"with format '{self.date_format}'."
                    )
                else:
                    logger.info(f"Date column '{col}' validated OK.")
            except Exception as exc:
                logger.warning(f"Date validation error on '{col}': {exc}")

    def _validate_numerics(self, df: pd.DataFrame) -> None:
        """
        Check that numeric columns contain no non-numeric values.
        Logs warnings for issues but does NOT raise.
        """
        for col in self.numeric_columns:
            if col not in df.columns:
                continue
            n_bad = pd.to_numeric(df[col], errors="coerce").isna().sum() - df[col].isna().sum()
            if n_bad > 0:
                logger.warning(f"Numeric column '{col}': {n_bad} non-numeric values found.")
            else:
                logger.info(f"Numeric column '{col}' validated OK.")

    def _log_quality_summary(self, df: pd.DataFrame) -> None:
        """Print a concise data quality summary to the log."""
        total = len(df)
        null_summary = df[self.expected_columns].isnull().sum()
        null_cols = null_summary[null_summary > 0]

        logger.info("=" * 60)
        logger.info("DATA QUALITY SUMMARY")
        logger.info(f"  Total rows       : {total:,}")
        logger.info(f"  Total columns    : {len(df.columns)}")
        logger.info(f"  Duplicate rows   : {df.duplicated().sum():,}")
        if len(null_cols):
            for col, cnt in null_cols.items():
                pct = (cnt / total) * 100
                logger.info(f"  Nulls in '{col}': {cnt:,} ({pct:.2f}%)")
        else:
            logger.info("  No null values detected in expected columns.")
        logger.info("=" * 60)

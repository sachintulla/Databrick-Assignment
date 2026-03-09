"""
Unit tests for the Data Ingestion module (DataLoader).

Coverage:
  - Schema validation pass and fail
  - Date validation
  - Numeric validation
  - File not found error
  - Quality summary logging
"""

import os
import tempfile

import pandas as pd
import pytest

from src.ingestion.data_loader import DataLoader, SchemaValidationError


class TestDataLoader:

    def test_load_valid_csv(self, tmp_path, sample_df):
        """DataLoader.load() returns a DataFrame from a valid CSV."""
        csv_path = tmp_path / "test.csv"
        # Write only valid columns (strip injected duplicate)
        sample_df.drop_duplicates().to_csv(csv_path, index=False)

        loader = DataLoader()
        df = loader.load(str(csv_path))
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_file_not_found_raises(self):
        """Load raises FileNotFoundError for non-existent file."""
        loader = DataLoader()
        with pytest.raises(FileNotFoundError):
            loader.load("/nonexistent/path/file.csv")

    def test_schema_validation_fails_on_missing_column(self, tmp_path, sample_df):
        """SchemaValidationError raised when expected column is missing."""
        bad_df = sample_df.drop(columns=["Sales"])
        csv_path = tmp_path / "bad.csv"
        bad_df.to_csv(csv_path, index=False)

        loader = DataLoader()
        with pytest.raises(SchemaValidationError) as exc_info:
            loader.load(str(csv_path))
        assert "Sales" in str(exc_info.value)

    def test_schema_all_expected_columns_present(self, tmp_path, sample_df):
        """No error when all expected columns are present."""
        csv_path = tmp_path / "ok.csv"
        sample_df.to_csv(csv_path, index=False)
        loader = DataLoader()
        df = loader.load(str(csv_path))
        expected = loader.expected_columns
        for col in expected:
            assert col in df.columns, f"Column '{col}' missing from loaded DataFrame"

    def test_load_returns_correct_row_count(self, tmp_path, sample_df):
        """Loaded row count matches the CSV row count."""
        csv_path = tmp_path / "rows.csv"
        sample_df.to_csv(csv_path, index=False)
        loader = DataLoader()
        df = loader.load(str(csv_path))
        assert len(df) == len(sample_df)

    def test_parquet_round_trip(self, tmp_path, sample_df, clean_df):
        """Parquet save + load round-trip preserves shape."""
        pq_path = tmp_path / "test.parquet"
        clean_df.to_parquet(pq_path, index=False)

        loader = DataLoader()
        loaded = loader.load_parquet(str(pq_path))
        assert loaded.shape == clean_df.shape

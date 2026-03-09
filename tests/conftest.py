"""
pytest fixtures shared across all test modules.
Generates a minimal in-memory SuperStore DataFrame for fast testing.
"""

import pandas as pd
import pytest


@pytest.fixture(scope="session")
def sample_df() -> pd.DataFrame:
    """
    Minimal 20-row SuperStore-like DataFrame for unit tests.
    Contains intentional nulls and duplicates for cleaning tests.
    """
    data = {
        "Row ID": list(range(1, 21)),
        "Order ID": [f"CA-2023-{1000 + i}" for i in range(1, 21)],
        "Order Date": ["2023-01-15"] * 10 + ["2023-06-20"] * 10,
        "Ship Date": ["2023-01-18"] * 10 + ["2023-06-24"] * 10,
        "Ship Mode": ["Standard Class"] * 7 + ["First Class"] * 7 + ["Same Day"] * 6,
        "Customer ID": [f"CUS-{100 + i}" for i in range(1, 21)],
        "Customer Name": [f"Customer {i}" for i in range(1, 21)],
        "Segment": ["Consumer"] * 8 + ["Corporate"] * 7 + ["Home Office"] * 5,
        "Country": ["United States"] * 20,
        "City": ["New York"] * 5 + ["Los Angeles"] * 5 + ["Chicago"] * 5 + ["Houston"] * 5,
        "State": ["New York"] * 5 + ["California"] * 5 + ["Illinois"] * 5 + ["Texas"] * 5,
        "Postal Code": ["10001"] * 9 + [None] * 2 + ["90001"] * 9,  # intentional nulls
        "Region": ["East"] * 5 + ["West"] * 5 + ["Central"] * 5 + ["South"] * 5,
        "Product ID": [f"PROD-{i:03d}" for i in range(1, 21)],
        "Category": ["Furniture"] * 7 + ["Office Supplies"] * 7 + ["Technology"] * 6,
        "Sub-Category": ["Chairs"] * 7 + ["Binders"] * 7 + ["Phones"] * 6,
        "Product Name": [f"Product {i}" for i in range(1, 21)],
        "Sales": [100.0, 250.0, 50.0, 800.0, 120.0,
                  300.0, 75.0, 450.0, 90.0, 1200.0,
                  60.0, 180.0, 320.0, 85.0, 500.0,
                  220.0, 640.0, 110.0, 380.0, 950.0],
        "Quantity": [2, 5, 1, 8, 3, 6, 1, 4, 2, 10, 1, 3, 5, 2, 7, 4, 6, 2, 5, 9],
        "Discount": [0.0, 0.1, 0.2, 0.0, 0.3, 0.0, 0.5, 0.1, 0.0, 0.2,
                     0.4, 0.0, 0.1, 0.6, 0.0, 0.2, 0.0, 0.3, 0.1, 0.0],
        "Profit": [30.0, 45.0, -10.0, 200.0, -25.0,
                   80.0, -30.0, 110.0, 20.0, 350.0,
                   -15.0, 50.0, 95.0, -50.0, 130.0,
                   60.0, 200.0, -8.0, 90.0, 280.0],
    }
    df = pd.DataFrame(data)
    # Add a duplicate row for testing
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    return df


@pytest.fixture(scope="session")
def clean_df(sample_df) -> pd.DataFrame:
    """Return the cleaned version of the sample_df fixture."""
    from src.cleaning.data_cleaner import DataCleaner
    cleaner = DataCleaner()
    return cleaner.clean(sample_df)

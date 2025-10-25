# tests/test_validators.py
import pandas as pd
import pytest

from app.utils.validators import (
    resolve_required_columns_from_df,
    ensure_not_empty_df,
    drop_rows_with_empty_requireds,
    normalise_dataframe,
)

# --- resolve_required_columns_from_df -----------------------------------------

def test_resolve_required_columns_ok_with_variants():
    df = pd.DataFrame(columns=["Transaction-ID", "User", "Product", "DateTime", "Amount"])
    col_map = resolve_required_columns_from_df(df)

    # Should map all 5 standard column names to actual column names present
    assert set(col_map.keys()) == {
        "transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"
    }
    assert col_map["transaction_id"] == "Transaction-ID"
    assert col_map["user_id"] == "User"
    assert col_map["product_id"] == "Product"
    assert col_map["timestamp"] == "DateTime"
    assert col_map["transaction_amount"] == "Amount"


def test_resolve_required_columns_missing_raises():
    df = pd.DataFrame(columns=["transaction_id", "user_id", "product_id", "timestamp"])  # missing amount
    with pytest.raises(Exception) as exc:
        resolve_required_columns_from_df(df)

    # The error is raised via raise_error(...) -> HTTPException under the hood
    assert "MISSING_COLUMNS" in str(exc.value)

# --- ensure_not_empty_df ------------------------------------------------------

def test_ensure_not_empty_df_raises_on_empty():
    df = pd.DataFrame(columns=["a", "b"])
    assert df.empty
    with pytest.raises(Exception) as exc:
        ensure_not_empty_df(df)
    assert "EMPTY_CSV" in str(exc.value)

def test_ensure_not_empty_df_passes_with_rows():
    df = pd.DataFrame({"a": [1], "b": [2]})

    # Should not raise
    ensure_not_empty_df(df)

# --- drop_rows_with_empty_requireds -------------------------------------------

def test_drop_rows_with_empty_requireds_trims_and_drops():
    # Mixed rows: one with missing required field (spaces), one valid
    df = pd.DataFrame({
        "transaction_id": ["  t1  ", "t2"],
        "user_id": ["u1", "  "],               # second row empty after trim -> drop
        "product_id": ["p1", "p2"],
        "timestamp": ["2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"],
        "transaction_amount": ["10.00", "12.00"],
    })

    # Identity map (already standard column headers)
    col_map = {c: c for c in df.columns}
    out = drop_rows_with_empty_requireds(df, col_map)
    assert out.shape[0] == 1
    assert out.iloc[0]["transaction_id"].strip() == "t1"

# --- normalise_dataframe ------------------------------------------------------

def test_normalise_dataframe_drops_invalid_and_formats():
    # Row 1: bad timestamp; Row 2: bad amount; Row 3: valid
    df = pd.DataFrame({
        "transaction_id": ["1", "2", "3"],
        "user_id": ["u", "u", "u"],
        "product_id": ["p", "p", "p"],
        "timestamp": ["not-a-date", "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z"],
        "transaction_amount": ["9.99", "abc", "12.345"],
    })
    col_map = {c: c for c in df.columns}
    clean = normalise_dataframe(df, col_map)

    # Only the third row survives
    assert list(clean.columns) == [
        "transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"
    ]
    assert clean.shape[0] == 1
    assert clean.iloc[0]["transaction_id"] == "3"
    assert clean.iloc[0]["timestamp"] == "2025-01-01T00:00:00Z"
    assert float(clean.iloc[0]["transaction_amount"]) == 12.34

def test_normalise_dataframe_renames_from_colmap():
    # Use header variants to ensure renaming to canonical happens
    df = pd.DataFrame({
        "Transaction-ID": ["t1"],
        "User": ["u1"],
        "Product": ["p1"],
        "DateTime": ["2025-02-02T12:00:00Z"],
        "Amount": ["10"],
    })
    # Simulate resolve_required_columns_from_df result
    col_map = {
        "transaction_id": "Transaction-ID",
        "user_id": "User",
        "product_id": "Product",
        "timestamp": "DateTime",
        "transaction_amount": "Amount",
    }
    clean = normalise_dataframe(df, col_map)
    assert list(clean.columns) == [
        "transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"
    ]
    assert clean.iloc[0]["transaction_id"] == "t1"
    assert clean.iloc[0]["timestamp"] == "2025-02-02T12:00:00Z"
    assert float(clean.iloc[0]["transaction_amount"]) == 10.00

def test_normalise_dataframe_all_invalid_raises_emptycsv():
    df = pd.DataFrame({
        "transaction_id": ["a"],
        "user_id": ["u"],
        "product_id": ["p"],
        "timestamp": ["not-a-date"],
        "transaction_amount": ["xyz"],
    })
    col_map = {c: c for c in df.columns}
    with pytest.raises(Exception) as exc:
        normalise_dataframe(df, col_map)
    assert "EMPTY_CSV" in str(exc.value)

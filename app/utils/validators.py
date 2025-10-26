from typing import Dict
import pandas as pd
from app.utils.error_utils import raise_error, ErrorType
import hashlib

# Acceptable header variants for each standard column
REQUIRED_COLUMNS: Dict[str, set[str]] = {
    "transaction_id": {"transaction_id", "transactionid", "transaction-id", "transaction id"},
    "user_id": {"user_id", "userid", "user-id", "user id", "user"},
    "product_id": {"product_id", "productid", "product-id", "product id", "product"},
    "timestamp": {"timestamp", "time_stamp", "date", "datetime", "time stamp"},
    "transaction_amount": {"transaction_amount", "amount", "value", "price", "transaction amount"},
}

def _lower_map(cols: list[str]) -> Dict[str, str]:
    return {c.lower(): c for c in cols}

def resolve_required_columns_from_df(df: pd.DataFrame) -> Dict[str, str]:
    # Map standard column names to actual columns given
    lower_cols = _lower_map(list(df.columns))
    resolved: Dict[str, str] = {}
    for target, variants in REQUIRED_COLUMNS.items():
        found = next((lower_cols[v] for v in variants if v in lower_cols), None)
        if not found:
            raise_error(
                ErrorType.MISSING_COLUMNS,
                detail={"missing": [target], "expected": list(REQUIRED_COLUMNS.keys())},
            )
        resolved[target] = found
    return resolved

def ensure_not_empty_df(df: pd.DataFrame) -> None:
    # Reject header only or empty files
    if df.empty:
        raise_error(ErrorType.EMPTY_CSV, detail="CSV has headers but zero rows")

def drop_rows_with_empty_requireds(df: pd.DataFrame, col_map: Dict[str, str]) -> pd.DataFrame:
    # Drop rows that are empty across any required field after trimming
    req_actual = [col_map[k] for k in ["transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"]]
    trimmed = df[req_actual].astype(str).apply(lambda col: col.str.strip())
    mask_all_present = (trimmed != "").all(axis=1)
    return df.loc[mask_all_present].copy()

def normalise_dataframe(df: pd.DataFrame, col_map: Dict[str, str]) -> pd.DataFrame:
    """Rename columns, coerce data types, drop invalids, and standardise format."""
    df = df.rename(columns={
        col_map["transaction_id"]: "transaction_id",
        col_map["user_id"]: "user_id",
        col_map["product_id"]: "product_id",
        col_map["timestamp"]: "timestamp",
        col_map["transaction_amount"]: "transaction_amount",
    })

    # IDs as trimmed strings
    for col in ["transaction_id", "user_id", "product_id"]:
        df[col] = df[col].astype(str).str.strip()

    # Parse once
    amt = pd.to_numeric(df["transaction_amount"], errors="coerce")
    ts = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    # Keep only valid rows
    valid_mask = amt.notna() & ts.notna()
    df = df.loc[valid_mask].copy()
    if df.empty:
        raise_error(ErrorType.EMPTY_CSV, detail="All rows invalid after coercion")

    # Reuse parsed columns
    df["transaction_amount"] = amt.loc[df.index].astype("float64").round(2)
    df["timestamp"] = ts.loc[df.index].dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Reset index for clean row order (important before converting to Polars)
    df = df.reset_index(drop=True)

    # Final column order
    return df[["transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"]]



def sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()

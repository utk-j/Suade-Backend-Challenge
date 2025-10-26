from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
from app.utils import state
from app.utils.response_models import ApiResponse
from app.utils.error_utils import raise_error, ErrorType
from typing import Optional
import pandas as pd

router = APIRouter()

@router.get("/summary/{user_id}")
def summary_for_user(
    user_id: str,
    from_date: Optional[str] = Query(None, description="Filter transactions from this date (YYYY-MM-DD)"),
    to_date: Optional[str] = Query(None, description="Filter transactions up to this date (YYYY-MM-DD)"),
):
    # Ensure dataset is available
    if state.DATASET_PATH is None or not state.DATASET_PATH.exists():
        raise_error(
            ErrorType.UNREADABLE_CSV,
            detail="No dataset available. Please upload a file using POST /upload first."
        )

    # Read Parquet dataset
    try:
        df = pd.read_parquet(state.DATASET_PATH)
    except Exception as e:
        raise_error(ErrorType.UNREADABLE_CSV, detail=f"Failed to read dataset: {e}")

    # Validate required columns
    required = {"transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"}
    lower_map = {c.lower(): c for c in df.columns}
    missing = [c for c in required if c not in lower_map]
    if missing:
        raise_error(ErrorType.MISSING_COLUMNS, detail=f"Missing required columns: {missing}")

    uid_col = lower_map["user_id"]
    amt_col = lower_map["transaction_amount"]
    ts_col = lower_map["timestamp"]

    # Filter by user_id
    df = df[df[uid_col].astype(str) == str(user_id)]

    # Apply optional date filters
    if from_date:
        try:
            from_dt = pd.to_datetime(from_date)
            df = df[df[ts_col] >= from_dt]
        except Exception:
            raise_error(ErrorType.INVALID_TIMESTAMP, detail=f"Invalid 'from_date' format: {from_date}")

    if to_date:
        try:
            to_dt = pd.to_datetime(to_date)
            df = df[df[ts_col] <= to_dt]
        except Exception:
            raise_error(ErrorType.INVALID_TIMESTAMP, detail=f"Invalid 'to_date' format: {to_date}")

    # Handle no matching records
    if df.empty:
        raise_error(
            ErrorType.USER_NOT_FOUND,
            detail=f"No records found for user_id {user_id} in the given date range."
        )

    # Compute statistics
    df[amt_col] = pd.to_numeric(df[amt_col], errors="coerce")
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce")

    amounts = df[amt_col].dropna()
    timestamps = df[ts_col].dropna()

    result = {
        "user_id": user_id,
        "count": int(amounts.size),
        "min": float(amounts.min()) if not amounts.empty else None,
        "max": float(amounts.max()) if not amounts.empty else None,
        "mean": float(amounts.mean()) if not amounts.empty else None,
        "total": float(amounts.sum()) if not amounts.empty else None,
        "first_transaction": timestamps.min().isoformat() if not timestamps.empty else None,
        "last_transaction": timestamps.max().isoformat() if not timestamps.empty else None,
    }

    response = ApiResponse(
        status="success",
        code=200,
        message="Summary computed successfully",
        data=result,
    )
    return JSONResponse(content=response.model_dump(), status_code=200)

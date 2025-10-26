# app/routes/summary.py
from datetime import datetime, timezone
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
import pandas as pd
from typing import Optional

from app.utils import state
from app.utils.response_models import ApiResponse
from app.utils.error_utils import raise_error, ErrorType

router = APIRouter()

@router.get("/summary/{user_id}")
def summary_for_user(
    user_id: str,
    date_from: Optional[datetime] = Query(None, alias="from"),  # FastAPI parses
    date_to:   Optional[datetime] = Query(None, alias="to"),
):
    # dataset must exist
    if not state.MASTER_PARQUET.exists():
        raise_error(ErrorType.UNREADABLE_CSV, detail="No dataset. Upload via POST /upload first.")

    # read
    try:
        df = pd.read_parquet(state.MASTER_PARQUET)
    except Exception as e:
        raise_error(ErrorType.UNREADABLE_CSV, detail=f"Failed to read dataset: {e}")

    # columns
    required = {"transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"}
    lower_map = {c.lower(): c for c in df.columns}
    missing = [c for c in required if c not in lower_map]
    if missing:
        raise_error(ErrorType.MISSING_COLUMNS, detail=f"Missing required columns: {missing}")

    uid_col = lower_map["user_id"]
    amt_col = lower_map["transaction_amount"]
    ts_col  = lower_map["timestamp"]

    # filter user
    df = df[df[uid_col].astype(str) == str(user_id)]
    if df.empty:
        return JSONResponse(
            content=ApiResponse.ok("No data in range", {"user_id": user_id, "count": 0}).model_dump(),
            status_code=200,
        )

    # parse timestamps once (UTC)
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df = df.dropna(subset=[ts_col])

    # normalize query bounds to UTC
    def _to_utc(dt: datetime | None) -> datetime | None:
        if dt is None:
            return None
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)

    dt_from = _to_utc(date_from)
    dt_to   = _to_utc(date_to)

    # date range filter
    if dt_from is not None:
        df = df[df[ts_col] >= dt_from]
    if dt_to is not None:
        df = df[df[ts_col] <= dt_to]

    if df.empty:
        return JSONResponse(
            content=ApiResponse.ok("No data in range", {"user_id": user_id, "count": 0}).model_dump(),
            status_code=200,
        )

    # amounts
    df[amt_col] = pd.to_numeric(df[amt_col], errors="coerce")
    df = df.dropna(subset=[amt_col])

    amounts = df[amt_col]
    first_ts = df[ts_col].min()
    last_ts  = df[ts_col].max()

    def _iso_z(ts: pd.Timestamp | None) -> Optional[str]:
        if ts is None or pd.isna(ts):
            return None
        return ts.isoformat().replace("+00:00", "Z")

    result = {
        "user_id": user_id,
        "count": int(amounts.size),
        "min": float(amounts.min()) if not amounts.empty else None,
        "max": float(amounts.max()) if not amounts.empty else None,
        "mean": float(amounts.mean()) if not amounts.empty else None,
        "total": float(amounts.sum()) if not amounts.empty else None,
        "first_transaction": _iso_z(first_ts),
        "last_transaction": _iso_z(last_ts),
    }

    return JSONResponse(
        content=ApiResponse.ok("Summary computed successfully", result).model_dump(),
        status_code=200,
    )

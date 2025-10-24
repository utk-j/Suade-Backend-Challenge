from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from app.utils import state
from app.utils.response_models import ApiResponse
import pandas as pd

router = APIRouter()

@router.get("/summary/{user_id}")
def summary_for_user(user_id: str):
    # Check if dataset is there
    if state.DATA_CSV_PATH is None or not state.DATA_CSV_PATH.exists():
        err = ApiResponse(
            status="error",
            code=400,
            message="No dataset available. Please upload a CSV first using POST /upload.",
            data=None,
        ).model_dump()
        raise HTTPException(status_code=400, detail=err)

    # 2) Read the CSV 
    try:
        df = pd.read_csv(state.DATA_CSV_PATH)
    except Exception as e:
        err = ApiResponse(
            status="error",
            code=500,
            message=f"Failed to read dataset: {e}",
            data=None,
        ).model_dump()
        raise HTTPException(status_code=500, detail=err)

    # 3) Map required columns case-insensitively (assume upload was validated)
    required = {"transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"}
    lower_map = {c.lower(): c for c in df.columns}
    missing = [c for c in required if c not in lower_map]
    if missing:
        err = ApiResponse(
            status="error",
            code=400,
            message=f"Missing required columns: {missing}",
            data=None,
        ).model_dump()
        raise HTTPException(status_code=400, detail=err)

    uid_col = lower_map["user_id"]
    amt_col = lower_map["transaction_amount"]

    # 4) Filter to this user (treat IDs as strings)
    user_mask = df[uid_col].astype(str) == str(user_id)
    user_df = df.loc[user_mask, [amt_col]]

    # If no rows for this user: return zeroed stats
    if user_df.empty:
        resp = ApiResponse(
            status="success",
            code=200,
            message="Summary computed successfully",
            data={"user_id": user_id, "count": 0, "min": None, "max": None, "mean": None},
        )
        return JSONResponse(content=resp.model_dump(), status_code=200)

    # 5) Amounts -> numeric, drop NaN, compute stats
    amounts = pd.to_numeric(user_df[amt_col], errors="coerce").dropna()
    count = int(amounts.size)
    if count == 0:
        result = {"user_id": user_id, "count": 0, "min": None, "max": None, "mean": None}
    else:
        result = {
            "user_id": user_id,
            "count": count,
            "min": float(amounts.min()),
            "max": float(amounts.max()),
            "mean": float(amounts.mean()),
        }

    resp = ApiResponse(
        status="success",
        code=200,
        message="Summary computed successfully",
        data=result,
    )
    return JSONResponse(content=resp.model_dump(), status_code=200)

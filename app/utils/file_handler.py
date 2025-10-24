from pathlib import Path
from fastapi import UploadFile, HTTPException
from app.utils.response_models import ApiResponse

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

def ensure_csv(filename: str):
    if not filename or not filename.lower().endswith(".csv"):
        error = ApiResponse(
            status="error",
            code=400,
            message="Only .csv files are accepted",
            data=None,
        ).model_dump()
        raise HTTPException(status_code=400, detail=error)

def save_upload_to_disk(upload: UploadFile) -> Path:
    ensure_csv(upload.filename or "")
    target = DATA_DIR / "transactions.csv"   
    with target.open("wb") as f:
        for chunk in iter(lambda: upload.file.read(1024 * 1024), b""):
            f.write(chunk)
    return target

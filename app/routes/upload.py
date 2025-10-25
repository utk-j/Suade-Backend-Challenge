from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
from app.utils.file_handler import save_upload_to_disk
from app.utils import state
from app.utils.response_models import ApiResponse

router = APIRouter()

@router.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    path = save_upload_to_disk(file)
    state.DATA_CSV_PATH = path

    # Try to read optional meta counts if present
    rows_cleaned = None
    rows_dropped = None
    try:
        import json
        meta = json.loads((path.with_suffix(".meta.json")).read_text(encoding="utf-8"))
        rows_cleaned = meta.get("rows_cleaned")
        rows_dropped = meta.get("rows_dropped")
    except Exception:
        pass

    response = ApiResponse(
        status="success",
        code=201,
        message="File uploaded, validated, and normalised",
        data={
            "stored_at": str(path.resolve()),
            "rows_cleaned": rows_cleaned,
            "rows_dropped": rows_dropped,
        },
    )
    return JSONResponse(content=response.model_dump(), status_code=201)

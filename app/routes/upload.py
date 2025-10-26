from fastapi import APIRouter, UploadFile, File
from fastapi.responses import JSONResponse
from app.utils.file_handler import save_upload_to_disk
from app.utils import state
from app.utils.response_models import ApiResponse

router = APIRouter()

@router.post("/upload")
async def upload_csv(file: UploadFile = File(...)):
    # Validate, normalise, and append uploaded CSV data into master Parquet storage.
    
    out_path = save_upload_to_disk(file)
    state.DATASET_PATH = out_path  

    response = ApiResponse(
        status="success",
        code=201,
        message="Upload validated and appended to master dataset",
        data={
            "stored_at": str(out_path.resolve()),
        },
    )
    return JSONResponse(content=response.model_dump(), status_code=201)

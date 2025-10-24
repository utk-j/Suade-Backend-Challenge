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

    response = ApiResponse(
        status="success",
        code=200,
        message="File uploaded successfully",
        data={"stored_at": str(path.resolve())}
    )
    return JSONResponse(content=response.model_dump(), status_code=200)

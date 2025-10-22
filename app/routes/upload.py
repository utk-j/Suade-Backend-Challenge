from fastapi import APIRouter

router = APIRouter()

@router.post("/upload")
async def upload_placeholder():
    return {"status": "not_implemented_yet"}

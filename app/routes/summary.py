from fastapi import APIRouter

router = APIRouter()

@router.get("/summary/{user_id}")
async def summary_placeholder(user_id: int):
    return {"user_id": user_id, "status": "not_implemented_yet"}

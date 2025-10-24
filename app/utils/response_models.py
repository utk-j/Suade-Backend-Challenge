from pydantic import BaseModel
from typing import Optional, Any

class ApiResponse(BaseModel):
    status: str
    code: int
    message: str
    data: Optional[Any] = None

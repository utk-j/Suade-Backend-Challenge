from enum import Enum
from fastapi import HTTPException

class ErrorType(str, Enum):
    INVALID_FILE_TYPE = "INVALID_FILE_TYPE"
    FILE_TOO_LARGE = "FILE_TOO_LARGE"
    UNREADABLE_CSV = "UNREADABLE_CSV"
    MISSING_COLUMNS = "MISSING_COLUMNS"
    EMPTY_CSV = "EMPTY_CSV"
    INVALID_AMOUNT = "INVALID_AMOUNT"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    USER_NOT_FOUND = "USER_NOT_FOUND" 


_STATUS_MAP = {
    ErrorType.INVALID_FILE_TYPE: 400,
    ErrorType.FILE_TOO_LARGE: 413,
    ErrorType.UNREADABLE_CSV: 400,
    ErrorType.MISSING_COLUMNS: 422,
    ErrorType.EMPTY_CSV: 422,
    ErrorType.INVALID_AMOUNT: 422,
    ErrorType.INVALID_TIMESTAMP: 422,
    ErrorType.USER_NOT_FOUND: 404,  
}


def raise_error(err: ErrorType, detail: object | None = None) -> None:
    status = _STATUS_MAP.get(err, 400)
    payload = {
        "status": "error",
        "code": status,
        "error": {
            "type": err,
            "detail": detail,
        },
    }
    raise HTTPException(status_code=status, detail=payload)

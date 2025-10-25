from pathlib import Path
from fastapi import UploadFile
import io
import os
import pandas as pd
import tempfile
import os as _os
import shutil

from app.utils.error_utils import raise_error, ErrorType
from app.utils.validators import (
    resolve_required_columns_from_df,
    ensure_not_empty_df,
    drop_rows_with_empty_requireds,
    normalise_dataframe,
)

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)
FINAL_PATH = DATA_DIR / "transactions.csv"

# Configurable safety cap, default 200 MB
MAX_BYTES = int(os.getenv("MAX_CSV_BYTES", str(200 * 1024 * 1024)))

def _read_upload_into_memory(upload: UploadFile) -> bytes:
    """
    Stream the upload into memory up to MAX_BYTES.
    We do not persist raw bytes to disk.
    """
    total = 0
    chunks: list[bytes] = []
    while True:
        chunk = upload.file.read(1024 * 1024)  # 1 MB
        if not chunk:
            break
        total += len(chunk)
        if total > MAX_BYTES:
            raise_error(ErrorType.FILE_TOO_LARGE, detail=f"limit {MAX_BYTES} bytes")
        chunks.append(chunk)
    return b"".join(chunks)

def _atomic_write_csv(df: pd.DataFrame, final_path: Path) -> None:
    """
    Write to a temp file in the same directory, then replace in a single operation.
    This ensures readers see either the old file or the new file, never a half file.
    """
    final_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        dir=str(final_path.parent),
        delete=False,
        encoding="utf-8",
        newline=""
    ) as tmp:
        tmp_path = Path(tmp.name)
        df.to_csv(tmp_path, index=False, encoding="utf-8", float_format="%.2f")
    _os.replace(tmp_path, final_path)  # atomic on the same filesystem

def save_upload_to_disk(upload: UploadFile) -> Path:
    """
    Validate in memory, build a clean normalised CSV, then persist atomically.
    Strict policy: any failure raises and nothing is written.
    """
    # Cheap fail on extension
    if not upload.filename or not upload.filename.lower().endswith(".csv"):
        raise_error(ErrorType.INVALID_FILE_TYPE)

    # Read bytes with cap
    raw_bytes = _read_upload_into_memory(upload)
    if not raw_bytes:
        raise_error(ErrorType.EMPTY_CSV)

    # Parse CSV as strings so IDs keep leading zeros
    try:
        buf = io.BytesIO(raw_bytes)
        df = pd.read_csv(buf, dtype=str, keep_default_na=False)
    except Exception as e:
        raise_error(ErrorType.UNREADABLE_CSV, detail=str(e))

    # Header presence and empty checks
    col_map = resolve_required_columns_from_df(df)
    ensure_not_empty_df(df)

    # Drop rows that are missing any required field after trimming
    original_rows = int(df.shape[0])
    df = drop_rows_with_empty_requireds(df, col_map)

    # Build the clean dataframe and apply coercions and row dropping
    clean_df = normalise_dataframe(df, col_map)
    cleaned_rows = int(clean_df.shape[0])
    if cleaned_rows == 0:
        raise_error(ErrorType.EMPTY_CSV, detail="No valid rows after validation")

    # Save atomically
    _atomic_write_csv(clean_df, FINAL_PATH)

    # Optional: store a tiny metadata file with counts
    meta_path = FINAL_PATH.with_suffix(".meta.json")
    try:
        import json
        meta = {
            "rows_incoming": original_rows,
            "rows_cleaned": cleaned_rows,
            "rows_dropped": max(0, original_rows - cleaned_rows),
        }
        with open(meta_path, "w", encoding="utf-8") as fh:
            json.dump(meta, fh)
    except Exception:
        # Non critical
        pass

    return FINAL_PATH

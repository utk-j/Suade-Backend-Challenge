from pathlib import Path
from fastapi import UploadFile
import io
import os
import tempfile
from contextlib import contextmanager

import pandas as pd
import polars as pl
from filelock import FileLock

from app.utils.state import (
    find_by_checksum,
    append_ingest,
    LOCK_PATH,
    LOCKS_DIR,
    ensure_data_layout,
    MASTER_PARQUET,
)
from app.utils.validators import sha256_bytes
from app.utils.error_utils import raise_error, ErrorType
from app.utils.validators import (
    resolve_required_columns_from_df,
    ensure_not_empty_df,
    drop_rows_with_empty_requireds,
    normalise_dataframe,
)

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Max upload size (default 200 MB)
MAX_BYTES = int(os.getenv("MAX_CSV_BYTES", str(200 * 1024 * 1024)))


def _read_upload_into_memory(upload: UploadFile) -> bytes:
    # Stream upload into memory with a hard size cap
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


def _append_to_master_parquet(clean_df: pd.DataFrame) -> Path:
    # Atomically append rows to master Parquet
    MASTER_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    new_pl = pl.from_pandas(clean_df)

    # Serialize writes via a global file lock
    with upload_lock():
        with tempfile.TemporaryDirectory(dir=str(MASTER_PARQUET.parent)) as td:
            tmp_parquet = Path(td) / "merged.parquet"

            if MASTER_PARQUET.exists():
                existing = pl.read_parquet(MASTER_PARQUET.as_posix())
                combined = pl.concat([existing, new_pl], how="vertical_relaxed")
                combined.write_parquet(tmp_parquet.as_posix())
                os.replace(tmp_parquet, MASTER_PARQUET)
            else:
                new_pl.write_parquet(tmp_parquet.as_posix())
                os.replace(tmp_parquet, MASTER_PARQUET)

    return MASTER_PARQUET


def save_upload_to_disk(upload: UploadFile) -> Path:
    # Validate, normalise, and append CSV; dedupe by checksum
    if not upload.filename or not upload.filename.lower().endswith(".csv"):
        raise_error(ErrorType.INVALID_FILE_TYPE)

    ensure_data_layout()

    raw_bytes = _read_upload_into_memory(upload)
    if not raw_bytes:
        raise_error(ErrorType.EMPTY_CSV)

    checksum = sha256_bytes(raw_bytes)

    # Fast path for sequential duplicate uploads
    if find_by_checksum(checksum):
        return MASTER_PARQUET

    # Parse + validate outside lock
    try:
        try:
            buf = io.BytesIO(raw_bytes)
            df = pd.read_csv(buf, dtype=str, keep_default_na=False)
        except Exception as e:
            append_ingest(status="failed", checksum=checksum, rows=None, error=str(e))
            raise_error(ErrorType.UNREADABLE_CSV, detail=str(e))

        col_map = resolve_required_columns_from_df(df)
        ensure_not_empty_df(df)

        df = drop_rows_with_empty_requireds(df, col_map)
        clean_df = normalise_dataframe(df, col_map)
        cleaned_rows = int(clean_df.shape[0])
        if cleaned_rows == 0:
            append_ingest(
                status="failed",
                checksum=checksum,
                rows=None,
                error="No valid rows after validation",
            )
            raise_error(ErrorType.EMPTY_CSV, detail="No valid rows after validation")

    except Exception:
        # Validation errors are logged already
        raise

    # Atomic dedupe section: recheck + append + mark ready
    with checksum_lock(checksum):
        if find_by_checksum(checksum):
            return MASTER_PARQUET

        try:
            out_path = _append_to_master_parquet(clean_df)
        except Exception as e:
            append_ingest(status="failed", checksum=checksum, rows=None, error=str(e))
            raise

        append_ingest(status="ready", checksum=checksum, rows=cleaned_rows)
        return out_path


@contextmanager
def upload_lock():
    # Global file-level lock for Parquet writes
    ensure_data_layout()
    lock = FileLock(str(LOCK_PATH))
    with lock:
        yield


@contextmanager
def checksum_lock(checksum: str):
    # Per-checksum lock for atomic dedupe
    ensure_data_layout()
    lock_path = LOCKS_DIR / f"{checksum}.lock"
    lock = FileLock(str(lock_path))
    with lock:
        yield

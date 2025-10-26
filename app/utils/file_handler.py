from pathlib import Path
from fastapi import UploadFile
import io
import pandas as pd
import polars as pl
import tempfile
import os
from contextlib import contextmanager
from filelock import FileLock

from .state import LOCK_PATH, ensure_data_layout, MASTER_PARQUET
from app.utils.state import find_by_checksum, append_ingest
from app.utils.validators import sha256_bytes  # tiny helper to hash bytes

from app.utils.error_utils import raise_error, ErrorType
from app.utils.validators import (
    resolve_required_columns_from_df,
    ensure_not_empty_df,
    drop_rows_with_empty_requireds,
    normalise_dataframe,
)

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Optional: keep writing a CSV snapshot (overwrites). Leave False to avoid conflicts with "never delete".
WRITE_CSV_LEGACY = False
FINAL_CSV_PATH = DATA_DIR / "transactions.csv"

# Configurable safety cap, default 200 MB
MAX_BYTES = int(os.getenv("MAX_CSV_BYTES", str(200 * 1024 * 1024)))


def _read_upload_into_memory(upload: UploadFile) -> bytes:
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
    os.replace(tmp_path, final_path)  # atomic on the same filesystem


def _append_to_master_parquet(clean_df: pd.DataFrame) -> Path:
    """Append rows to the single master Parquet atomically."""
    MASTER_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    # Convert pandas -> polars
    new_pl = pl.from_pandas(clean_df)

    with upload_lock():
        with tempfile.TemporaryDirectory(dir=str(MASTER_PARQUET.parent)) as td:
            tmp_parquet = Path(td) / "merged.parquet"

            if MASTER_PARQUET.exists():
                # Read existing, append new, write to temp, atomic replace
                existing = pl.read_parquet(MASTER_PARQUET.as_posix())
                combined = pl.concat([existing, new_pl], how="vertical_relaxed")
                combined.write_parquet(tmp_parquet.as_posix())
                os.replace(tmp_parquet, MASTER_PARQUET)
            else:
                new_pl.write_parquet(tmp_parquet.as_posix())
                os.replace(tmp_parquet, MASTER_PARQUET)

    return MASTER_PARQUET


def save_upload_to_disk(upload: UploadFile) -> Path:
    # Cheap fail on extension
    if not upload.filename or not upload.filename.lower().endswith(".csv"):
        raise_error(ErrorType.INVALID_FILE_TYPE)

    # Read bytes with cap
    raw_bytes = _read_upload_into_memory(upload)
    if not raw_bytes:
        raise_error(ErrorType.EMPTY_CSV)

    # Checksum for idempotency + audit
    checksum = sha256_bytes(raw_bytes)

    # Duplicate upload short-circuit
    existing = find_by_checksum(checksum)
    if existing:
        # Already ingested; return current master parquet path
        return MASTER_PARQUET if MASTER_PARQUET.exists() else FINAL_CSV_PATH

    try:
        # Parse CSV as strings so IDs keep leading zeros
        try:
            buf = io.BytesIO(raw_bytes)
            df = pd.read_csv(buf, dtype=str, keep_default_na=False)
        except Exception as e:
            append_ingest(status="failed", checksum=checksum, rows=None, error=str(e))
            raise_error(ErrorType.UNREADABLE_CSV, detail=str(e))

        # Header presence and empty checks
        col_map = resolve_required_columns_from_df(df)
        ensure_not_empty_df(df)

        # Drop rows missing any required field after trimming
        original_rows = int(df.shape[0])
        df = drop_rows_with_empty_requireds(df, col_map)

        # Build the clean dataframe and apply coercions and row dropping
        clean_df = normalise_dataframe(df, col_map)
        cleaned_rows = int(clean_df.shape[0])
        if cleaned_rows == 0:
            append_ingest(status="failed", checksum=checksum, rows=None, error="No valid rows after validation")
            raise_error(ErrorType.EMPTY_CSV, detail="No valid rows after validation")

        # Append to master Parquet (append-only, atomic, locked)
        out_path = _append_to_master_parquet(clean_df)

        # Optional legacy CSV snapshot (overwrites — keep disabled for "never delete")
        if WRITE_CSV_LEGACY:
            with upload_lock():
                _atomic_write_csv(clean_df, FINAL_CSV_PATH)

    except Exception as e:
        append_ingest(status="failed", checksum=checksum, rows=None, error=str(e))
        raise

    append_ingest(status="ready", checksum=checksum, rows=cleaned_rows)

    return out_path


@contextmanager
def upload_lock():
    ensure_data_layout()
    lock = FileLock(str(LOCK_PATH))
    with lock:
        yield

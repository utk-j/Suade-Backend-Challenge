from pathlib import Path
from datetime import datetime, timezone
from typing import Iterable, Dict, Optional
import json

DATASET_PATH: Optional[Path] = None

# Base data paths
DATA_DIR = Path("data")
MASTER_PARQUET = DATA_DIR / "transactions.parquet"
MANIFEST_PATH = DATA_DIR / "manifest.jsonl"
LOCK_PATH = DATA_DIR / "upload.lock"             
LOCKS_DIR = DATA_DIR / "locks"   

def ensure_data_layout() -> None:
    # Make sure the data directories and manifest file exist
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    LOCKS_DIR.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.touch(exist_ok=True)

# ---- manifest helpers ----
def _iter_manifest() -> Iterable[Dict]:
    # Yield each JSON line from the manifest
    if not MANIFEST_PATH.exists():
        return []
    with MANIFEST_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue

def find_by_checksum(checksum: str) -> Optional[Dict]:
    # Find a ready entry with this checksum
    for rec in _iter_manifest():
        if rec.get("checksum_sha256") == checksum and rec.get("status") == "ready":
            return rec
    return None

def append_ingest(*, status: str, checksum: str, rows: int | None, error: str | None = None) -> str:
    # Append one ingest record to manifest and return its id
    ensure_data_layout()
    ingest_id = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    rec = {
        "ingest_id": ingest_id,
        "status": status,
        "checksum_sha256": checksum,
        "rows_appended": rows,
        "error": error,
    }
    with MANIFEST_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    return ingest_id

def try_restore_dataset_path() -> None:
    # Set DATASET_PATH to the master Parquet file if the latest manifest entry is ready
    global DATASET_PATH

    if not MANIFEST_PATH.exists() or not MASTER_PARQUET.exists():
        return

    # Find the latest record with status == "ready"
    latest_ready = None
    for rec in _iter_manifest():
        if rec.get("status") == "ready":
            latest_ready = rec

    if latest_ready:
        DATASET_PATH = MASTER_PARQUET


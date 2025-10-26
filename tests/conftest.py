# tests/conftest.py

import os
import io
import json
import shutil
import tempfile
import threading
import pytest
from pathlib import Path

import polars as pl
from fastapi.testclient import TestClient

from app.main import app
from app.utils import state, file_handler


@pytest.fixture(autouse=True)
def temp_data_dir(monkeypatch):
    tmpdir = Path(tempfile.mkdtemp(prefix="suade-test-"))
    tmp_data = tmpdir / "data"
    tmp_data.mkdir(exist_ok=True)

    # Patch file_handler paths that actually exist in the Parquet workflow
    if hasattr(file_handler, "DATA_DIR"):
        monkeypatch.setattr(file_handler, "DATA_DIR", tmp_data)
    if hasattr(file_handler, "MASTER_PARQUET"):
        monkeypatch.setattr(file_handler, "MASTER_PARQUET", tmp_data / "transactions.parquet")
    if hasattr(file_handler, "MANIFEST_PATH"):
        monkeypatch.setattr(file_handler, "MANIFEST_PATH", tmp_data / "manifest.json")
    if hasattr(file_handler, "LOCK_PATH"):
        monkeypatch.setattr(file_handler, "LOCK_PATH", tmp_data / ".ingest.lock")
    if hasattr(file_handler, "STAGING_DIR"):
        staging = tmp_data / "staging"
        staging.mkdir(exist_ok=True)
        monkeypatch.setattr(file_handler, "STAGING_DIR", staging)

    # Patch state paths the app reads
    if hasattr(state, "DATA_DIR"):
        monkeypatch.setattr(state, "DATA_DIR", tmp_data)
    if hasattr(state, "MASTER_PARQUET"):
        monkeypatch.setattr(state, "MASTER_PARQUET", tmp_data / "transactions.parquet")
    if hasattr(state, "MANIFEST_PATH"):
        monkeypatch.setattr(state, "MANIFEST_PATH", tmp_data / "manifest.json")

    # Reset size cap
    monkeypatch.delenv("MAX_CSV_BYTES", raising=False)
    max_bytes_env = os.getenv("MAX_CSV_BYTES", str(200 * 1024 * 1024))
    if hasattr(file_handler, "MAX_BYTES"):
        monkeypatch.setattr(file_handler, "MAX_BYTES", int(max_bytes_env))

    try:
        yield tmp_data
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture()
def client():
    return TestClient(app)


def _csv_bytes(text: str) -> bytes:
    return text.encode("utf-8")


@pytest.fixture()
def sample_ok_csv():
    return _csv_bytes(
        "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
        "001,u1,p1,2025-01-01T10:00:00Z,12.345\n"
        "002,u2,p2,2025-01-01T11:00:00Z,9.9\n"
    )


# ---------- helpers used by test_upload_api.py ----------

def _post_file(client, name, content: bytes):
    files = {"file": (name, io.BytesIO(content), "text/csv")}
    return client.post("/upload", files=files)


def _err_type(resp):
    try:
        detail = resp.json().get("detail", {})
        if isinstance(detail, dict):
            # Support both {detail: {error: {type}}} and {detail: {type}}
            if "error" in detail and isinstance(detail["error"], dict):
                return detail["error"].get("type")
            return detail.get("type")
        return detail
    except Exception:
        return "UNKNOWN"


def _read_parquet():
    assert state.MASTER_PARQUET.exists(), "Parquet file missing"
    return pl.read_parquet(state.MASTER_PARQUET)


def _read_manifest():
    assert state.MANIFEST_PATH.exists(), "Manifest file missing"
    return [json.loads(l) for l in state.MANIFEST_PATH.read_text(encoding="utf-8").splitlines() if l.strip()]

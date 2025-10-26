import io
import json
import pytest
import threading
from app.utils import state
from app.utils.response_models import ApiResponse

import polars as pl


def _post_file(client, name, content: bytes):
    files = {"file": (name, io.BytesIO(content), "text/csv")}
    return client.post("/upload", files=files)


def _err_type(resp):
    try:
        detail = resp.json().get("detail", {})
        if isinstance(detail, dict):
            return detail.get("error", {}).get("type")
        return detail
    except Exception:
        return "UNKNOWN"


def _read_parquet():
    assert state.MASTER_PARQUET.exists(), "Parquet file missing"
    return pl.read_parquet(state.MASTER_PARQUET)


def _read_manifest():
    assert state.MANIFEST_PATH.exists(), "Manifest file missing"
    return [json.loads(l) for l in state.MANIFEST_PATH.read_text(encoding="utf-8").splitlines() if l.strip()]


def test_upload_success(client, sample_ok_csv):
    r = _post_file(client, "valid.csv", sample_ok_csv)
    assert r.status_code == 201

    # Check manifest
    manifest = _read_manifest()
    assert len(manifest) == 1
    assert manifest[0]["status"] == "ready"
    assert manifest[0]["rows_appended"] == 2

    # Check Parquet
    df = _read_parquet()
    assert df.shape[0] == 2
    assert set(df.columns) == {"transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"}


def test_idempotent_upload(client, sample_ok_csv):
    # First upload
    r1 = _post_file(client, "dup.csv", sample_ok_csv)
    assert r1.status_code == 201
    before = _read_parquet().shape[0]

    # Re-upload same bytes (same checksum)
    r2 = _post_file(client, "dup.csv", sample_ok_csv)
    assert r2.status_code == 201

    # Manifest should still have a single ready entry for this checksum
    manifest = _read_manifest()
    ready = [m for m in manifest if m["status"] == "ready"]
    assert len(ready) == 1

    # Parquet rowcount must be unchanged (no duplicates appended)
    after = _read_parquet().shape[0]
    assert after == before


def test_empty_file(client):
    r = _post_file(client, "empty.csv", b"")
    assert r.status_code == 422
    assert _err_type(r) == "EMPTY_CSV"


def test_missing_column(client):
    csv = b"transaction_id,user_id,product_id,timestamp\n1,u1,p1,2025-01-01T00:00:00Z"
    r = _post_file(client, "bad.csv", csv)
    assert r.status_code == 422
    assert _err_type(r) == "MISSING_COLUMNS"


def test_invalid_rows_dropped(client):
    csv = (
        "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
        "1,u,p,not-a-date,9.99\n"
        "2,u,p,2025-01-01T00:00:00Z,abc\n"
        "3,u,p,2025-01-01T00:00:00Z,12.5\n"
    ).encode()

    r = _post_file(client, "mixed.csv", csv)
    assert r.status_code == 201
    df = _read_parquet()
    assert df.shape[0] == 1
    assert df["transaction_amount"][0] == 12.5


def test_all_rows_invalid(client):
    csv = (
        "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
        "1,u,p,not-a-date,abc\n"
    ).encode()

    r = _post_file(client, "allbad.csv", csv)
    assert r.status_code == 422
    assert _err_type(r) == "EMPTY_CSV"


def test_header_variants(client):
    csv = (
        "Transaction-ID,User,Product,DateTime,Amount\n"
        "t1,u1,p1,2025-01-01T00:00:00Z,10\n"
    ).encode()

    r = _post_file(client, "headermap.csv", csv)
    assert r.status_code == 201
    df = _read_parquet()
    assert df.shape[0] == 1
    assert df["transaction_id"][0] == "t1"
    assert df["transaction_amount"][0] == 10.0


def test_file_too_large(client, monkeypatch):
    from app.utils import file_handler
    monkeypatch.setattr(file_handler, "MAX_BYTES", 64)
    r = _post_file(client, "big.csv", b"x" * 512)
    assert r.status_code == 413
    assert _err_type(r) == "FILE_TOO_LARGE"


def test_parallel_upload_atomic(client):
    csv1 = (
        "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
        "A,u1,p1,2025-01-01T00:00:00Z,1.11\n"
    ).encode()
    csv2 = (
        "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
        "B,u2,p2,2025-01-02T00:00:00Z,2.22\n"
    ).encode()

    threads = [
        threading.Thread(target=_post_file, args=(client, "a.csv", csv1)),
        threading.Thread(target=_post_file, args=(client, "b.csv", csv2)),
    ]
    for t in threads: t.start()
    for t in threads: t.join()

    df = _read_parquet()
    # Both uploads should be present, and no corruption
    assert df.shape[0] == 2
    assert set(df["transaction_id"].to_list()) == {"A", "B"}



def test_high_volume_parallel_uploads(client):
    threads = []
    seen_ids = set()

    for i in range(5):
        txid = f"T{i:03}"
        seen_ids.add(txid)
        csv = (
            "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
            f"{txid},U{i},P{i},2025-01-01T00:00:00Z,{i + 0.99}\n"
        ).encode()
        threads.append(threading.Thread(target=_post_file, args=(client, f"{txid}.csv", csv)))

    for t in threads: t.start()
    for t in threads: t.join()

    df = _read_parquet()
    assert df.shape[0] == 5
    assert set(df["transaction_id"].to_list()) == seen_ids

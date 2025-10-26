import os
import io
import csv
import time
import json
import threading
import secrets
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

import pytest
import polars as pl

from tests.conftest import _post_file, _read_parquet
from app.utils import state


# sizes (override via env)
ROWS1 = int(os.getenv("ROWS1", "50000"))
ROWS2 = int(os.getenv("ROWS2", "50000"))
PAR_THREADS = int(os.getenv("PAR_THREADS", "6"))
PAR_ROWS_PER_FILE = int(os.getenv("PAR_ROWS_PER_FILE", "5000"))


def _iso_utc(rng) -> str:
    """Random UTC timestamp within ~6 months."""
    days = rng.randrange(0, 181)
    hours = rng.randrange(0, 24)
    dt = datetime.now(timezone.utc) - timedelta(days=days, hours=hours)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def make_csv_bytes(n: int, *, seed: int, prefix: str = "") -> bytes:
    """Deterministic CSV bytes for a given seed."""
    import random
    rng = random.Random(seed)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"])
    for i in range(n):
        txid = f"{prefix or seed}-{i}"
        writer.writerow([
            txid,
            f"u{rng.randint(1, 999)}",
            f"p{rng.randint(1, 499)}",
            _iso_utc(rng),
            round(rng.uniform(1.0, 500.0), 2),
        ])
    return buf.getvalue().encode("utf-8")


def assert_status(resp, ok=(201,)):
    """Ensure upload response is OK."""
    try:
        body = resp.json()
    except Exception:
        body = resp.text
    assert resp.status_code in ok, f"{resp.status_code}: {body}"


def _has_parquet() -> bool:
    try:
        _read_parquet()
        return True
    except AssertionError:
        return False


def measure_upload(client, name: str, b: bytes, expected_add: int | None = None) -> float:
    """Upload and measure throughput."""
    before = _read_parquet().shape[0] if _has_parquet() else 0
    t0 = time.perf_counter()
    r = _post_file(client, name, b)
    dt = time.perf_counter() - t0
    assert_status(r)
    after = _read_parquet().shape[0]
    if expected_add is not None:
        delta = after - before
        assert delta == expected_add, f"{name}: added {delta}, expected {expected_add}"
    return dt


def _safe_read_manifest() -> list[dict]:
    """Read manifest; return [] if not present yet."""
    if not state.MANIFEST_PATH.exists():
        return []
    lines = state.MANIFEST_PATH.read_text(encoding="utf-8").splitlines()
    out = []
    for l in lines:
        l = l.strip()
        if not l:
            continue
        try:
            out.append(json.loads(l))
        except json.JSONDecodeError:
            pass
    return out


@pytest.mark.upload
@pytest.mark.smoke
def test_upload_smoke_suite(client):
    """End-to-end smoke for /upload."""
    run_seed = secrets.randbits(64)
    print(f"[seed] RUN_SEED={run_seed}")

    # derive seeds
    import random
    rng = random.Random(run_seed)
    s1, s2 = rng.getrandbits(64), rng.getrandbits(64)
    par_seeds = [rng.getrandbits(64) for _ in range(max(PAR_THREADS - 1, 0))]
    dup_seed = par_seeds[0] if par_seeds else rng.getrandbits(64)
    print(f"[seeds] CSV1={s1}, CSV2={s2}, PARALLEL={par_seeds}, DUP={dup_seed}")

    # 0) baseline manifest
    base_manifest = _safe_read_manifest()
    ready_base = sum(1 for m in base_manifest if m.get("status") == "ready")

    # 1) large upload
    print(f"[1] Upload CSV1 ({ROWS1:,} rows, seed={s1})")
    csv1 = make_csv_bytes(ROWS1, seed=s1)
    t1 = measure_upload(client, "csv1.csv", csv1, expected_add=ROWS1)
    print(f"    OK in {t1:.2f}s (~{ROWS1/t1:,.0f} r/s)")

    df = _read_parquet()
    assert df.shape[0] == ROWS1
    assert set(df.columns) == {"transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"}

    # 2) idempotent re-upload
    print("[2] Re-upload CSV1 (expect 0 new rows)")
    t1b = measure_upload(client, "csv1.csv", csv1, expected_add=0)
    print(f"    OK in {t1b:.2f}s")
    ready_now = sum(1 for m in _safe_read_manifest() if m.get("status") == "ready")
    assert ready_now - ready_base == 1, "should log exactly one ready ingest"

    # 3) append second batch
    print(f"[3] Upload CSV2 ({ROWS2:,} rows, seed={s2})")
    csv2 = make_csv_bytes(ROWS2, seed=s2)
    t2 = measure_upload(client, "csv2.csv", csv2, expected_add=ROWS2)
    print(f"    OK in {t2:.2f}s (~{ROWS2/t2:,.0f} r/s)")
    assert _read_parquet().shape[0] == ROWS1 + ROWS2

    # 4) header variant
    print("[4] Header variant (1 row)")
    hv = b"Transaction-ID,User,Product,DateTime,Amount\nt1,u1,p1,2025-01-02T09:00:00Z,10\n"
    t4 = measure_upload(client, "headers.csv", hv, expected_add=1)
    print(f"    OK in {t4:.2f}s")

    # 5) invalid rows dropped
    print("[5] Mixed file (2 invalid, 1 valid)")
    mixed = (
        "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
        "1,u,p,not-a-date,9.99\n"
        "2,u,p,2025-01-01T00:00:00Z,abc\n"
        "3,u,p,2025-01-01T00:00:00Z,12.5\n"
    ).encode()
    t5 = measure_upload(client, "mixed.csv", mixed, expected_add=1)
    print(f"    OK in {t5:.2f}s")

    # 6) negative cases
    print("[6] Negative uploads")
    def neg(name, data, exp=(400, 415, 422)):
        r = _post_file(client, name, data)
        assert r.status_code in exp, f"{name}: got {r.status_code}, expected {exp}"

    neg("readme.md", b"# not csv", (400, 415))
    neg("empty.csv", b"", (422,))
    neg("missing.csv", b"transaction_id,user_id\n1,u\n", (422,))
    neg("allbad.csv", b"transaction_id,user_id,product_id,timestamp,transaction_amount\n1,u,p,not-a-date,abc\n", (422,))
    print("    OK (expected 4xx)")

    # 7) parallel uploads (+dup of first)
    print(f"[7] Parallel uploads x{PAR_THREADS} ({PAR_ROWS_PER_FILE:,} rows each)")
    
    # Build N-1 unique payloads
    payloads = [
        (f"par-{i}.csv", make_csv_bytes(PAR_ROWS_PER_FILE, seed=s))
        for i, s in enumerate(par_seeds)
    ]
    
    # Exact duplicate of the first one: identical bytes ⇒ identical checksum
    assert len(payloads) >= 1, "PAR_THREADS must be >= 2 for the duplicate check"
    dup_bytes = payloads[0][1]
    payloads.append(("par-dup.csv", dup_bytes))
    
    print(f"    parallel seeds: {par_seeds} (dup reuses bytes of par-0)")
    
    start_rows = _read_parquet().shape[0]
    t0 = time.perf_counter()
    errors: List[str] = []
    
    def worker(name, b):
        try:
            assert_status(_post_file(client, name, b))
        except AssertionError as e:
            errors.append(f"{name}: {e}")
    
    threads = [threading.Thread(target=worker, args=(n, b)) for n, b in payloads]
    for t in threads: t.start()
    for t in threads: t.join()
    assert not errors, f"parallel errors: {errors}"
    
    dt = time.perf_counter() - t0
    added = _read_parquet().shape[0] - start_rows
    expected = (PAR_THREADS - 1) * PAR_ROWS_PER_FILE  # one duplicate ignored
    assert added == expected, f"expected {expected}, got {added}"
    print(f"    OK in {dt:.2f}s (~{expected/dt:,.0f} r/s)")


    # 8) aggregate sanity
    df_final = _read_parquet()
    assert set(df_final.columns) == {"transaction_id", "user_id", "product_id", "timestamp", "transaction_amount"}
    agg = df_final.select([
        pl.len().alias("count"),
        pl.col("transaction_amount").sum().alias("sum"),
        pl.col("transaction_amount").mean().alias("mean"),
        pl.col("transaction_amount").min().alias("min"),
        pl.col("transaction_amount").max().alias("max"),
    ])
    print("Aggregate:\n", agg)
    print(f"✅ Upload smoke passed ({df_final.shape[0]:,} total rows)")

# tests/test_upload_api.py
import io
from app.utils import file_handler


def _err_type(resp):
    """Extracts the error type regardless of whether FastAPI returns {'error':{}} or {'detail':{}}."""
    body = resp.json()
    if isinstance(body, dict):
        if "error" in body and isinstance(body["error"], dict):
            return body["error"].get("type")
        if "detail" in body:
            det = body["detail"]
            if isinstance(det, dict) and "error" in det and isinstance(det["error"], dict):
                return det["error"].get("type")
            if isinstance(det, str):
                return det
    return None


def test_upload_success(client, sample_ok_csv):
    files = {"file": ("ok.csv", io.BytesIO(sample_ok_csv), "text/csv")}
    r = client.post("/upload", files=files)

    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "success"
    assert "stored_at" in body["data"]

    assert file_handler.FINAL_PATH.exists()
    content = file_handler.FINAL_PATH.read_text(encoding="utf-8").splitlines()
    assert content[0] == "transaction_id,user_id,product_id,timestamp,transaction_amount"

    # Adjust rounding to match your current .round(2)
    assert "001,u1,p1,2025-01-01T10:00:00Z,12.34" in content[1]
    assert "002,u2,p2,2025-01-01T11:00:00Z,9.90" in content[2]


def test_reject_non_csv_extension(client):
    files = {"file": ("readme.md", io.BytesIO(b"# not csv"), "text/markdown")}
    r = client.post("/upload", files=files)
    assert r.status_code == 400
    assert _err_type(r) == "INVALID_FILE_TYPE"


def test_empty_payload(client):
    files = {"file": ("empty.csv", io.BytesIO(b""), "text/csv")}
    r = client.post("/upload", files=files)
    assert r.status_code == 422
    assert _err_type(r) == "EMPTY_CSV"


def test_missing_required_column(client):
    csv = (
        "transaction_id,user_id,product_id,timestamp\n"
        "1,u,p,2025-01-01T00:00:00Z\n"
    ).encode()
    files = {"file": ("bad.csv", io.BytesIO(csv), "text/csv")}
    r = client.post("/upload", files=files)
    assert r.status_code == 422
    assert _err_type(r) == "MISSING_COLUMNS"


def test_invalid_rows_dropped(client):
    csv = (
        "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
        "1,u,p,not-a-date,9.99\n"
        "2,u,p,2025-01-01T00:00:00Z,abc\n"
        "3,u,p,2025-01-01T00:00:00Z,12.5\n"
    ).encode()
    files = {"file": ("mixed.csv", io.BytesIO(csv), "text/csv")}
    r = client.post("/upload", files=files)
    assert r.status_code == 201

    text = file_handler.FINAL_PATH.read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    assert len(lines) == 2  # header + one valid row
    assert lines[1].endswith(",2025-01-01T00:00:00Z,12.50")


def test_all_rows_invalid(client):
    csv = (
        "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
        "1,u,p,not-a-date,abc\n"
    ).encode()
    files = {"file": ("bad.csv", io.BytesIO(csv), "text/csv")}
    r = client.post("/upload", files=files)
    assert r.status_code == 422
    assert _err_type(r) == "EMPTY_CSV"


def test_preserve_leading_zeros(client):
    csv = (
        "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
        "000123,000045,007,2025-01-01T00:00:00Z,1\n"
    ).encode()
    files = {"file": ("ok.csv", io.BytesIO(csv), "text/csv")}
    r = client.post("/upload", files=files)
    assert r.status_code == 201
    text = file_handler.FINAL_PATH.read_text(encoding="utf-8")
    assert "000123,000045,007,2025-01-01T00:00:00Z,1.00" in text


def test_header_variants_accepted(client):
    csv = (
        "Transaction-ID,User,Product,DateTime,Amount\n"
        "t1,u1,p1,2025-01-02T09:00:00Z,10\n"
    ).encode()
    files = {"file": ("ok.csv", io.BytesIO(csv), "text/csv")}
    r = client.post("/upload", files=files)
    assert r.status_code == 201
    text = file_handler.FINAL_PATH.read_text(encoding="utf-8")
    assert "t1,u1,p1,2025-01-02T09:00:00Z,10.00" in text


def test_file_too_large_limit(client, monkeypatch):
    from app.utils import file_handler
    monkeypatch.setattr(file_handler, "MAX_BYTES", 32)  # 32 bytes max
    big = b"x" * 1024  # 1 KB
    files = {"file": ("big.csv", io.BytesIO(big), "text/csv")}
    r = client.post("/upload", files=files)
    assert r.status_code == 413
    assert _err_type(r) == "FILE_TOO_LARGE"

def _post_file(client, name, content: bytes):
    files = {"file": (name, io.BytesIO(content), "text/csv")}
    return client.post("/upload", files=files)

def test_two_quick_uploads_atomic(client):
    # Concurrency tests with 2 requests
    csv1 = (
        "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
        "A,u1,p1,2025-01-01T00:00:00Z,1\n"
    ).encode()
    csv2 = (
        "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
        "B,u2,p2,2025-01-02T00:00:00Z,2\n"
    ).encode()

    import threading
    t1 = threading.Thread(target=_post_file, args=(client, "a.csv", csv1))
    t2 = threading.Thread(target=_post_file, args=(client, "b.csv", csv2))
    t1.start(); t2.start()
    t1.join(); t2.join()

    # Read final file: must be either the cleaned output of csv1 OR csv2
    text = file_handler.FINAL_PATH.read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    assert len(lines) == 2  # header + one row
    assert lines[0] == "transaction_id,user_id,product_id,timestamp,transaction_amount"

    row = lines[1]
    expect_row_1 = "A,u1,p1,2025-01-01T00:00:00Z,1.00"
    expect_row_2 = "B,u2,p2,2025-01-02T00:00:00Z,2.00"
    assert row in (expect_row_1, expect_row_2), f"Unexpected row: {row}"

def test_many_quick_uploads_still_consistent(client):
    # Testing for concurrency with several uploads quickly --> final file should always be a complete dataset (last write wins)
    import threading
    payloads = []
    for i in range(5):
        csv = (
            "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
            f"T{i},U{i},P{i},2025-01-01T00:00:00Z,{i + 0.5}\n"
        ).encode()
        payloads.append(("t.csv", csv))

    threads = [
        threading.Thread(target=_post_file, args=(client, name, content))
        for (name, content) in payloads
    ]
    for t in threads: t.start()
    for t in threads: t.join()

    text = file_handler.FINAL_PATH.read_text(encoding="utf-8")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    assert len(lines) == 2
    assert lines[0] == "transaction_id,user_id,product_id,timestamp,transaction_amount"

    # Final row must be one of the expected cleaned rows
    expected = {
        f"T{i},U{i},P{i},2025-01-01T00:00:00Z,{(i + 0.5):.2f}"
        for i in range(5)
    }
    assert lines[1] in expected

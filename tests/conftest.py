import io
import os
import shutil
import tempfile
import pytest
from fastapi.testclient import TestClient
from app.main import app
from app.utils import file_handler


@pytest.fixture(autouse=True)
def temp_data_dir(monkeypatch):
    # Create a temp directory for each test
    tmpdir = tempfile.mkdtemp(prefix="suade-data-")

    # Override file_handler constants to use this temp dir
    monkeypatch.setattr(file_handler, "DATA_DIR", file_handler.Path(tmpdir))
    monkeypatch.setattr(file_handler, "FINAL_PATH", file_handler.Path(os.path.join(tmpdir, "transactions.csv")))

    # Reset MAX_BYTES to default
    monkeypatch.delenv("MAX_CSV_BYTES", raising=False)
    monkeypatch.setattr(file_handler, "MAX_BYTES", int(os.getenv("MAX_CSV_BYTES", str(200 * 1024 * 1024))))

    yield file_handler.Path(tmpdir)
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture()
def client():
    # FastAPI test client for making requests to the app
    return TestClient(app)


def _csv_bytes(text: str) -> bytes:
    # Convert CSV text into UTF-8 bytes for upload simulation
    return text.encode("utf-8")


@pytest.fixture()
def sample_ok_csv():
    # Cretae a minimal valid CSV for upload tests
    return _csv_bytes(
        "transaction_id,user_id,product_id,timestamp,transaction_amount\n"
        "001,u1,p1,2025-01-01T10:00:00Z,12.345\n"
        "002,u2,p2,2025-01-01T11:00:00Z,9.9\n"
    )

# Suade Backend Challenge

This project implements a FastAPI backend for the Suade Graduate Engineer Challenge.  
It provides two main endpoints:  
- **`/upload`** – for validating, normalising, and storing transaction data  
- **`/summary/{user_id}`** – for computing per-user transaction statistics

The implementation focuses on production-grade design: atomic writes, data integrity, and deterministic testing.

---

## Features

- **Validated ingestion pipeline**
  - Full CSV validation in-memory before any persistence
  - Strict schema normalisation and column mapping
  - Leading-zero preservation for IDs
  - Deduplication via SHA-256 checksums

- **Efficient storage**
  - All data is stored as a clean, validated Parquet dataset  
  - Atomic writes using temp directories and file locks  
  - Manifest log for ingestion tracking (`manifest.jsonl`)

- **Concurrency safety**
  - Global file locks to prevent race conditions  
  - Per-checksum locks for idempotent uploads  
  - Thread-safe uploads and deterministic parallel tests

- **Summary endpoint**
  - Returns per-user transaction statistics (`count`, `min`, `max`, `mean`, `total`)
  - Optional `from` and `to` date filters
  - Built-in error handling for invalid dates or empty ranges

- **Full automated testing**
  - `pytest` suite covering validation, upload, deduplication, and summary behaviour
  - Smoke and concurrency tests (`test_upload_endpoint.py`)
  - Seeded summary test dataset (`tests/summary_test_dataset.csv`)

---

## Implementation Details

- **Checksum-based deduplication:**  
  Uses Python’s built-in `hashlib` (SHA-256) to generate unique fingerprints for uploaded files.  
  Duplicate uploads with the same checksum are skipped automatically.
- **Atomic file handling:**  
  Writes occur via temporary directories to ensure files are only committed once validation succeeds.
- **Manifest logging:**  
  Each upload (whether accepted or skipped) is appended to a JSON Lines manifest for traceability.
- **Validation and cleaning:**  
  Data is loaded and normalised using `pandas` before conversion to Parquet for efficient storage.
- **Summary computation:**  
  Performed via lightweight aggregations on Parquet data; can be upgraded to Polars’ lazy queries in future for better scalability.

---

## Project Structure
<pre>
app/
├── main.py # FastAPI app entrypoint
├── routes/
│ ├── upload.py # /upload endpoint
│ └── summary.py # /summary endpoint
└── utils/
├── file_handler.py # Upload handling, validation, atomic writes
├── validators.py # Schema validation & normalisation
├── state.py # Manifest & dataset state management
├── error_utils.py # Standardised error responses
└── response_models.py # API response schema

tests/
├── test_upload_api.py
├── test_upload_endpoint.py
├── test_summary_endpoint.py
├── test_validator.py
└── summary_test_dataset.csv
  </pre>

---

## Setup & Installation

1. Clone the repository:
   ```bash
   https://github.com/utk-j/Suade-Backend-Challenge.git
   cd suade-backend-challenge
   ```
2. Install dependencies
   ```bash
   pip install -r requirements.txt
   ```
4. Start the FastAPI Server
   ```bash
   python -m uvicorn app.main:app --reload --host 127.0.0.1 --port 8001
   ```
   The API will be available at http://127.0.0.1:8001.

---

##  Using the API

Once the server is running, you can interact with the API in a few ways.

### 1. Swagger UI (Recommended)

FastAPI automatically provides an interactive interface for exploring and testing endpoints.  

Open your browser and visit:
```bash
http://127.0.0.1:8001/docs
```
From there, you can upload CSV files and query user summaries directly — no code required.

---

### 2. `/upload`

Upload a CSV file containing transaction data.  
The file is fully validated in memory before being stored as a clean Parquet dataset.

**Example usage (via Swagger UI):**
- Choose **POST /upload**
- Select a `.csv` file from your local system
- Click **Execute**

**Key points:**
- Duplicate files (same checksum) are automatically ignored  
- Invalid rows are dropped during validation  
- Successfully uploaded files are logged in the manifest (`data/manifest.jsonl`)

**Upload Flow Fiagram**
Below is a visual summary of the `/upload` pipeline, showing how the backend validates, deduplicates, and safely stores incoming files.
<img width="1496" height="4112" alt="Upload Diagram" src="https://github.com/user-attachments/assets/c260bd21-5c51-481f-afcc-61df131a93b5" />

---

### 3. `/summary/{user_id}`

Retrieve aggregated transaction statistics for a specific user.  
You can optionally add date filters (`from` and `to`) to narrow down the range.

**Example usage (via Swagger UI):**
- Choose **GET /summary/{user_id}**
- Enter a user ID (e.g. `200`)
- Optionally provide `from` and `to` date filters
- Click **Execute** to view the computed summary

  ** Summary Flow Diagram**
  The diagram below illustrates how the /summary/{user_id} endpoint processes requests — from parameter validation and dataset filtering to computing user-level transaction statistics.
  <img width="1466" height="3963" alt="Summary Diagram" src="https://github.com/user-attachments/assets/d8d0b436-9b01-4afe-bd02-f1e0571e3acb" />


---

### 4. Root Endpoint

Verify that the API is running:
```bash
http://127.0.0.1:8001/docs
```
Returns a simple confirmation message when the backend is live.

## Running Tests

This project includes a **comprehensive pytest suite** that fully validates functionality, performance, and data integrity.  
All tests are isolated — they use temporary directories to avoid touching real data.

To run all tests:

```bash
pytest tests/ -v
```

---

### What’s Covered

- **Unit tests** – verify validators, schema handling, and normalisation logic  
- **Integration tests** – confirm `/upload` and `/summary` endpoints work end-to-end  
- **Concurrency tests** – ensure thread safety and idempotency across parallel uploads  
- **Smoke tests** – simulate large file uploads and high-volume ingestion  
- **Seeded dataset tests** – check real summary computations using `summary_test_dataset.csv`

In total, there are **19 automated tests** that run in under 5 seconds on a local machine.

---

### Expected Output

If everything passes, you’ll see a result similar to:

```bash
================================================ test session starts ================================================
platform win32 -- Python 3.13.5, pytest-8.4.2, pluggy-1.6.0 -- C:\Users\UtkarshJain\Desktop\Personal\Projects\Suade-Backend-Challenge\venv\Scripts\python.exe
cachedir: .pytest_cache
rootdir: C:\Users\UtkarshJain\Desktop\Personal\Projects\Suade-Backend-Challenge
configfile: pytest.ini
plugins: anyio-4.11.0, Faker-37.11.0
collected 19 items                                                                                                   

tests/test_upload_api.py::test_upload_success PASSED                                                           [  5%]
tests/test_upload_api.py::test_idempotent_upload PASSED                                                        [ 10%]
tests/test_upload_api.py::test_empty_file PASSED                                                               [ 15%]
tests/test_upload_api.py::test_missing_column PASSED                                                           [ 21%]
tests/test_upload_api.py::test_invalid_rows_dropped PASSED                                                     [ 26%]
tests/test_upload_api.py::test_all_rows_invalid PASSED                                                         [ 31%]
tests/test_upload_api.py::test_header_variants PASSED                                                          [ 36%]
tests/test_upload_api.py::test_file_too_large PASSED                                                           [ 42%]
tests/test_upload_api.py::test_parallel_upload_atomic PASSED                                                   [ 47%]
tests/test_upload_api.py::test_high_volume_parallel_uploads PASSED                                             [ 52%]
tests/test_upload_endpoint.py::test_upload_smoke_suite PASSED                                                  [ 57%]
tests/test_validator.py::test_resolve_required_columns_ok_with_variants PASSED                                 [ 63%]
tests/test_validator.py::test_resolve_required_columns_missing_raises PASSED                                   [ 68%]
tests/test_validator.py::test_ensure_not_empty_df_raises_on_empty PASSED                                       [ 73%]
tests/test_validator.py::test_ensure_not_empty_df_passes_with_rows PASSED                                      [ 78%]
tests/test_validator.py::test_drop_rows_with_empty_requireds_trims_and_drops PASSED                            [ 84%]
tests/test_validator.py::test_normalise_dataframe_drops_invalid_and_formats PASSED                             [ 89%]
tests/test_validator.py::test_normalise_dataframe_renames_from_colmap PASSED                                   [ 94%]
tests/test_validator.py::test_normalise_dataframe_all_invalid_raises_emptycsv PASSED                           [100%]

================================================ 19 passed in 2.62s =================================================
```

All tests should finish with **PASSED with 0 failures**.

---

## Future Improvements

While the current implementation is fully functional and production-ready for a small-scale setup, several improvements can make it more scalable, secure, and maintainable if this were to be deployed in a real-world environment.

### 1. Storage & Performance
- **Move from local Parquet files to a database** such as PostgreSQL or a cloud data warehouse.  
  This would allow for faster reads and writes, better query performance, and more flexible filtering (e.g., by date, product, or amount).
- **Cloud-based storage** (e.g., AWS S3, Azure Blob) could replace local disk to make uploads accessible from multiple servers and environments.
- **Parallel processing** could be improved so that multiple uploads can be validated and appended concurrently, reducing queue time for large workloads.
- **Store invalid rows separately** instead of discarding them, to support auditing or debugging later.

### 2. API & Functionality
- **Add authentication and authorisation** using tokens or role-based access control (RBAC).  
  This ensures only authorised users can upload data or view summaries.
- **Detailed response messages** for uploads (e.g., “X rows uploaded, Y rows discarded”) would improve transparency.
- **Additional filters for `/summary`**, such as filtering by product ID or amount range, could make the API more versatile.

### 3. Reliability & Monitoring
- **Enhanced logging** to track which user uploaded which file and when, improving traceability.  
- **Versioned manifest records** could provide a full audit trail of uploads, updates, and failures.
- **Error reporting** could be extended to log invalid file attempts, malformed CSVs, and rejected rows.

### 4. CI/CD & Testing
- **Automated testing pipelines** (e.g., GitHub Actions) can run the pytest suite on every commit or pull request to prevent regressions.
- **Load testing** can be added to measure throughput and performance for larger datasets.
- **Integration tests** for authentication and cloud storage could be added once those features exist.

### 5. Security & Deployment
- **Authentication tokens or OAuth2** to protect endpoints from unauthorised access.  
- **HTTPS and secure configuration management** to protect sensitive data.  
- If deployed publicly, a **reverse proxy** (e.g., Nginx) and **rate limiting** would help prevent abuse.
- Eventually, the API could be **containerised and hosted on a cloud platform** (AWS, Azure, or GCP) for global accessibility.

---

These improvements would move the project from a robust local prototype to a cloud-ready, scalable backend service — capable of handling higher concurrency, larger datasets, and enterprise-grade security.

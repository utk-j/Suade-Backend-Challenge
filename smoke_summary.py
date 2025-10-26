import requests
from urllib.parse import urlencode
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "http://127.0.0.1:8001"
SUMMARY_ENDPOINT = f"{BASE_URL}/summary"

# Replace with known real user IDs
known_users = {
    "has_data": "200",          # This user has data in the range
    "out_of_range": "200",      # Same user but using wrong dates
    "no_data": "999",           # No data for this user at all
    "fake_user": "9999999999",  # Totally fake user
}

# Define test cases
test_cases = [
    {
        "label": "Valid user with data",
        "user_id": known_users["has_data"],
        "params": {
            "from": "2024-01-01T00:00:00+00:00",
            "to": "2025-12-31T23:59:59+00:00"
        },
    },
    {
        "label": "Valid user but no data in range",
        "user_id": known_users["out_of_range"],
        "params": {
            "from": "2010-01-01T00:00:00+00:00",
            "to": "2010-12-31T23:59:59+00:00"
        },
    },
    {
        "label": "User with no records",
        "user_id": known_users["no_data"],
        "params": {
            "from": "2024-01-01T00:00:00+00:00",
            "to": "2025-12-31T23:59:59+00:00"
        },
    },
    {
        "label": "Fake user ID",
        "user_id": known_users["fake_user"],
        "params": {
            "from": "2024-01-01T00:00:00+00:00",
            "to": "2025-12-31T23:59:59+00:00"
        },
    },
    {
        "label": "Valid user with no date filter",
        "user_id": known_users["has_data"],
        "params": None,
    },
]

def run_test(label, user_id, params=None):
    try:
        if params:
            url = f"{SUMMARY_ENDPOINT}/{user_id}?{urlencode(params)}"
        else:
            url = f"{SUMMARY_ENDPOINT}/{user_id}"

        response = requests.get(url)
        print(f"[{label}]")
        print("URL:", url)
        print("Status:", response.status_code)
        print("Response:", response.json())
        print("-" * 50)
    except Exception as e:
        print(f"[{label}] FAILED with error: {e}")
        print("-" * 50)

def run_all_tests_parallel():
    with ThreadPoolExecutor(max_workers=5) as executor:
        for case in test_cases:
            executor.submit(run_test, case["label"], case["user_id"], case.get("params"))

if __name__ == "__main__":
    print("Starting /summary smoke tests...")

    run_all_tests_parallel()

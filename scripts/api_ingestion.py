import requests
import time
import json

BASE_URL = "https://fakestoreapi.com/products"

# ----------------------------
# AUTH OPTIONS (choose one)
# ----------------------------
API_KEY = None                     # x-api-key or query key
BEARER_TOKEN = None                # Bearer token auth

# Retry settings
MAX_RETRIES = 3
REQUEST_TIMEOUT = 10  # seconds


def call_api(url, params=None):
    headers = {}

    # ----------------------------
    # API KEY AUTH
    # ----------------------------
    if API_KEY:
        headers["x-api-key"] = API_KEY            # common name
        # OR query form:
        if params is None:
            params = {}
        params["apikey"] = API_KEY

    # ----------------------------
    # BEARER TOKEN AUTH
    # ----------------------------
    if BEARER_TOKEN:
        headers["Authorization"] = f"Bearer {BEARER_TOKEN}"

    attempt = 1

    while attempt <= MAX_RETRIES:
        try:
            response = requests.get(
                url,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            # Handle rate limit
            if response.status_code == 429:
                print("Rate limited — waiting 5 seconds")
                time.sleep(5)
                continue

            # Retry on server failure
            if response.status_code >= 500:
                print(f"Server error {response.status_code} — retrying…")
                time.sleep(2)
                attempt += 1
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            print("Timeout — retrying…")
            attempt += 1
            time.sleep(2)

    raise Exception("API request failed after retries")


def fetch_paginated(endpoint):
    page = 1
    page_size = 50
    all_data = []
    last_page_data = None
    MAX_PAGES = 20

    while True:
        if page > MAX_PAGES:
            print("Max page limit reached — stopping")
            break

        params = {"page": page, "limit": page_size}
        data = call_api(endpoint, params)

        if not data:
            print("No more data — stopping")
            break

        if last_page_data == data:
            print("API not paginated — stopping pagination loop")
            break

        all_data.extend(data)
        last_page_data = data

        print(f"Fetched page {page} — {len(data)} records")
        page += 1

        time.sleep(1)

    return all_data


def save_raw_json(data, path="/opt/airflow/data/staging/api_raw.json"):
    with open(path, "w") as f:
        json.dump(data, f, indent=4)

    print(f"Saved {len(data)} records → {path}")


def run_api_ingestion():
    try:
        data = fetch_paginated(BASE_URL)
    except Exception as e:
        print(f"Pagination failed ({e}) — fetching once")
        data = call_api(BASE_URL)

    save_raw_json(data)


if __name__ == "__main__":
    run_api_ingestion()

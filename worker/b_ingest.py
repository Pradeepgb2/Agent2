from dotenv import load_dotenv, main
import os
import logging
from datetime import datetime
from pathlib import Path
import requests
import time
load_dotenv()
input_dir = Path(os.getenv("BD_INPUT_PATH", "/data/input"))
raw_dir = Path(os.getenv("BD_RAW_PATH", "/data/raw"))
raw_dir.mkdir(parents=True, exist_ok=True)
# ================== CONFIGURATION ==================

API_TOKEN = os.getenv("BRIGHTDATA_API")        # <-- put your Bright Data API key here
DATASET_ID = os.getenv("BRIGHTDATA_DATASET_ID")   # <-- your Web Scraper API dataset_id
INPUT_CSV_PATH = input_dir / "urls.csv"            # <-- your CSV file

POLL_INTERVAL_SECONDS = 60             # how often to check job status
MAX_WAIT_MINUTES = 120                 # max time to wait for job to finish

TRIGGER_URL = "https://api.brightdata.com/datasets/v3/trigger"
PROGRESS_URL_TEMPLATE = "https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
SNAPSHOT_URL_TEMPLATE = "https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"


def trigger_job_with_csv():
    if not os.path.exists(INPUT_CSV_PATH):
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV_PATH}")

    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
    }

    params = {
        "dataset_id": DATASET_ID,
        # no need for format here; trigger response is always JSON
    }

    with open(INPUT_CSV_PATH, "rb") as f:
        files = {
            "data": (os.path.basename(INPUT_CSV_PATH), f, "text/csv")
        }

        print(f"[TRIGGER] Uploading CSV: {INPUT_CSV_PATH} ...")
        response = requests.post(TRIGGER_URL, headers=headers, params=params, files=files)

    response.raise_for_status()

    # Trigger response is JSON
    data = response.json()
    snapshot_id = data.get("snapshot_id")

    if not snapshot_id:
        raise RuntimeError(f"No snapshot_id returned. Response: {data}")

    print(f"[TRIGGER] Job triggered. snapshot_id = {snapshot_id}")
    return snapshot_id
def wait_for_snapshot_ready(snapshot_id):
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
    }
    
    url = PROGRESS_URL_TEMPLATE.format(snapshot_id=snapshot_id)
    start_time = time.time()
    max_wait_seconds = MAX_WAIT_MINUTES * 60

    print(f"[WAIT] Waiting for snapshot {snapshot_id} to be ready...")
    # logging.info(f"[WAIT] Waiting for snapshot {snapshot_id} to be ready...")
    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # Progress endpoint returns JSON
        info = response.json()
        status = info.get("status")

        now = datetime.utcnow().isoformat(timespec="seconds")
        print(f"[WAIT] {now} | status = {status}")

        if status == "ready":
            print(f"[WAIT] Snapshot {snapshot_id} is ready.")
            return
        elif status == "failed":
            raise RuntimeError(f"Snapshot {snapshot_id} failed. Full response: {info}")

        elapsed = time.time() - start_time
        if elapsed > max_wait_seconds:
            raise TimeoutError(
                f"Timed out after {MAX_WAIT_MINUTES} minutes waiting for snapshot {snapshot_id}"
            )

        time.sleep(POLL_INTERVAL_SECONDS)
def download_snapshot_csv(snapshot_id):
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
    }

    base_url = SNAPSHOT_URL_TEMPLATE.format(snapshot_id=snapshot_id)
    # CSV download
    url = f"{base_url}?format=csv"

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H_%M_%S")
    new_path = raw_dir / f"employees_linkedin_data_{timestamp}.csv"
    print(new_path)
    print(f"[DOWNLOAD] Downloading CSV for snapshot {snapshot_id} ...")
    response = requests.get(url, headers=headers)
    response.raise_for_status()

    # CSV is binary/text, do NOT call .json()
    with open(new_path, "wb") as f:
        f.write(response.content)

    print(f"[DOWNLOAD] Saved CSV to: {new_path}")
    return {
        "status": "success",
        "file_path": str(new_path),
        "week_label": str(timestamp)
    }


# ===================================================


def run_ingestion():
    if API_TOKEN == "YOUR_API_KEY_HERE":
        raise ValueError("Please set API_TOKEN to your actual Bright Data API key.")
    if DATASET_ID == "YOUR_DATASET_ID_HERE":
        raise ValueError("Please set DATASET_ID to your actual Web Scraper API dataset_id.")

    snapshot_id = trigger_job_with_csv()
    wait_for_snapshot_ready(snapshot_id)
    download_snapshot_csv(snapshot_id)


if __name__ == "__main__":
    print(run_ingestion())
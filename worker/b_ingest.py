from dotenv import load_dotenv
import os
import requests
import time
from datetime import datetime
from pathlib import Path

from worker.s3_utils import upload_file

load_dotenv()

# ================== PATHS ==================
input_dir = Path(os.getenv("BD_INPUT_PATH", "/data/input"))
raw_dir = Path(os.getenv("BD_RAW_PATH", "/data/raw"))
raw_dir.mkdir(parents=True, exist_ok=True)

# ================== CONFIGURATION ==================
API_TOKEN = os.getenv("BRIGHTDATA_API")
DATASET_ID = os.getenv("BRIGHTDATA_DATASET_ID")
INPUT_CSV_PATH = input_dir / "urls.csv"

POLL_INTERVAL_SECONDS = 60
MAX_WAIT_MINUTES = 120

TRIGGER_URL = "https://api.brightdata.com/datasets/v3/trigger"
PROGRESS_URL_TEMPLATE = "https://api.brightdata.com/datasets/v3/progress/{snapshot_id}"
SNAPSHOT_URL_TEMPLATE = "https://api.brightdata.com/datasets/v3/snapshot/{snapshot_id}"


def trigger_job_with_csv():
    if not INPUT_CSV_PATH.exists():
        raise FileNotFoundError(f"Input CSV not found: {INPUT_CSV_PATH}")

    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
    }

    params = {
        "dataset_id": DATASET_ID,
    }

    with open(INPUT_CSV_PATH, "rb") as f:
        files = {
            "data": (INPUT_CSV_PATH.name, f, "text/csv")
        }

        print(f"[TRIGGER] Uploading CSV: {INPUT_CSV_PATH} ...")
        response = requests.post(TRIGGER_URL, headers=headers, params=params, files=files)

    response.raise_for_status()

    data = response.json()
    snapshot_id = data.get("snapshot_id")

    if not snapshot_id:
        raise RuntimeError(f"No snapshot_id returned. Response: {data}")

    print(f"[TRIGGER] Job triggered. snapshot_id = {snapshot_id}")
    return snapshot_id


def wait_for_snapshot_ready(snapshot_id: str):
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
    }

    url = PROGRESS_URL_TEMPLATE.format(snapshot_id=snapshot_id)
    start_time = time.time()
    max_wait_seconds = MAX_WAIT_MINUTES * 60

    print(f"[WAIT] Waiting for snapshot {snapshot_id} to be ready...")

    while True:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        info = response.json()
        status = info.get("status")

        now = datetime.utcnow().isoformat(timespec="seconds")
        print(f"[WAIT] {now} | status = {status}")

        if status == "ready":
            print(f"[WAIT] Snapshot {snapshot_id} is ready.")
            return
        if status == "failed":
            raise RuntimeError(f"Snapshot {snapshot_id} failed. Full response: {info}")

        elapsed = time.time() - start_time
        if elapsed > max_wait_seconds:
            raise TimeoutError(
                f"Timed out after {MAX_WAIT_MINUTES} minutes waiting for snapshot {snapshot_id}"
            )

        time.sleep(POLL_INTERVAL_SECONDS)


def download_snapshot_csv(snapshot_id: str):
    headers = {
        "Authorization": f"Bearer {API_TOKEN}",
    }

    base_url = SNAPSHOT_URL_TEMPLATE.format(snapshot_id=snapshot_id)
    url = f"{base_url}?format=csv"

    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H_%M_%S")
    new_path = raw_dir / f"employees_linkedin_data_{timestamp}.csv"

    print(new_path)
    print(f"[DOWNLOAD] Downloading CSV for snapshot {snapshot_id} ...")

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    with open(new_path, "wb") as f:
        f.write(response.content)

    print(f"[DOWNLOAD] Saved CSV to: {new_path}")

    # Upload raw file to S3
    upload_file(str(new_path), f"raw/{new_path.name}")

    return {
        "status": "success",
        "file_path": str(new_path),
        "week_label": timestamp,
    }


def run_ingestion():
    if not API_TOKEN:
        raise ValueError("BRIGHTDATA_API is not set in environment variables.")
    if not DATASET_ID:
        raise ValueError("BRIGHTDATA_DATASET_ID is not set in environment variables.")

    snapshot_id = trigger_job_with_csv()
    wait_for_snapshot_ready(snapshot_id)
    return download_snapshot_csv(snapshot_id)


if __name__ == "__main__":
    print(run_ingestion())

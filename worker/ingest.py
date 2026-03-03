from dotenv import load_dotenv
import os
import logging
from datetime import datetime
from pathlib import Path

load_dotenv()



def run_ingestion():
    # Set env vars BEFORE importing kaggle
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError("API_KEY not found in environment variables")

    os.environ["KAGGLE_KEY"] = api_key
    os.environ["KAGGLE_USERNAME"] = os.getenv("KAGGLE_USERNAME", "pradeepgubbala")

    import kaggle  # import after env vars are set

    """
    Downloads latest dataset from Kaggle
    Renames it with today's date
    Returns ingestion metadata
    """

    raw_dir = Path(os.getenv("DATA_RAW_PATH", "/data/raw"))
    raw_dir.mkdir(parents=True, exist_ok=True)

    dataset = "pradeepgubbala/employees-data"
    file_name = "employees_linkedin_data.csv"

    logging.info("Authenticating Kaggle")
    kaggle.api.authenticate()

    logging.info("Downloading dataset from Kaggle")
    kaggle.api.dataset_download_file(
        dataset,
        file_name=file_name,
        path=raw_dir,
    )

    # Rename file with today's date
    today_date = datetime.now().date()
    old_path = raw_dir / file_name
    new_path = raw_dir / f"employees_linkedin_data_{today_date}.csv"

    if not old_path.exists():
        raise FileNotFoundError("Downloaded file not found.")

    old_path.rename(new_path)

    logging.info(f"Ingestion completed successfully. Saved as {new_path.name}")

    return {
        "status": "success",
        "week_label": str(today_date),
        "file_path": str(new_path)
    }


if __name__ == "__main__":
    print(run_ingestion())
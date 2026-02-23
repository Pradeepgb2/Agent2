import os
import logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

RAW_PATH = os.getenv("DATA_RAW_PATH", "./data/raw")
SIGNALS_PATH = os.getenv("DATA_SIGNALS_PATH", "./data/signals")
RUNS_PATH = os.getenv("DATA_RUNS_PATH", "./data/runs")
LOG_PATH = os.getenv("LOG_PATH", "./logs")

os.makedirs(RAW_PATH, exist_ok=True)
os.makedirs(SIGNALS_PATH, exist_ok=True)
os.makedirs(RUNS_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)

logging.basicConfig(
    filename=os.path.join(LOG_PATH, "pipeline.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def run():
    logging.info("Pipeline started")
    print("Pipeline started")

    # Placeholder logic
    print("Ingestion step placeholder")
    print("Comparison step placeholder")

    logging.info("Pipeline completed")
    print("Pipeline completed")

if __name__ == "__main__":
    run()

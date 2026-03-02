import os
import logging
import json
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from worker.ingest import run_ingestion
from worker.compare import run_comparison
import re
from typing import List, Tuple
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

DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")

def _extract_date(filename: str):
    m = DATE_PATTERN.search(filename)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d")
    except ValueError:
        return None

def enforce_retention(folder: str, prefix: str, keep_count: int):
    p = Path(folder)
    if not p.exists():
        return

    dated = []
    for f in p.glob(f"{prefix}*"):
        if f.is_file():
            d = _extract_date(f.name)
            if d:
                dated.append((f, d))

    dated.sort(key=lambda x: x[1], reverse=True)
    to_delete = dated[keep_count:]

    for f, _ in to_delete:
        try:
            f.unlink()
            logging.info(f"Retention deleted: {f}")
        except Exception as e:
            logging.warning(f"Retention failed deleting {f}: {e}")
            
def run():
    start_time = datetime.now()
    logging.info("Pipeline started")
    print("Pipeline started")

    run_status = "failed"
    error_message = None

    ingestion_result = None
    comparison_result = None

    try:
        # Step 1: Ingestion
        ingestion_result = run_ingestion()

        # Step 2: Comparison
        comparison_result = run_comparison()

        run_status = "success"
        logging.info("Pipeline completed successfully")
        print("Pipeline completed successfully")

    except Exception as e:
        run_status = "failed"
        error_message = str(e)
        logging.exception("Pipeline failed")
        print("Pipeline failed:", error_message)

    finally:
        duration_seconds = int((datetime.now() - start_time).total_seconds())
        logging.info(f"Pipeline duration: {duration_seconds} seconds")

        # -------------------------
        # Build Run Summary (for Summary Card)
        # -------------------------
        week_present = None
        week_past = None

        # Try to take week info from comparison first, else ingestion, else today
        if comparison_result and comparison_result.get("week_present"):
            week_present = comparison_result.get("week_present")
            week_past = comparison_result.get("week_past")
        elif ingestion_result and ingestion_result.get("week_label"):
            week_present = ingestion_result.get("week_label")

        if not week_present:
            week_present = datetime.now().strftime("%Y-%m-%d")

        # Counts (safe defaults)
        signals_count = int(comparison_result.get("signals_count", 0)) if comparison_result else 0
        company_change_count = int(comparison_result.get("company_change_count", 0)) if comparison_result else 0
        role_change_count = int(comparison_result.get("role_change_count", 0)) if comparison_result else 0
        both_change_count = int(comparison_result.get("role_and_company_change_count", 0)) if comparison_result else 0

        # Row counts (you’ll fill these properly once ingestion returns them)
        # For now, store placeholders or 0
        total_rows_fetched = 0
        valid_rows_processed = 0
        invalid_rows_skipped = 0
        skipped_reason_counts = {}

        # If you later return these from ingestion/compare, plug them here.
        # Example:
        # total_rows_fetched = ingestion_result.get("total_rows_fetched", 0)
        # valid_rows_processed = ingestion_result.get("valid_rows_processed", 0)
        # invalid_rows_skipped = ingestion_result.get("invalid_rows_skipped", 0)
        # skipped_reason_counts = ingestion_result.get("skipped_reason_counts", {})

        # Partial success rule (based on skipped rows percentage)
        if run_status == "success" and total_rows_fetched > 0:
            skipped_ratio = invalid_rows_skipped / total_rows_fetched
            if skipped_ratio > 0.30:
                run_status = "partial_success"

        run_summary = {
            "week_present": week_present,
            "week_past": week_past,
            "total_rows_fetched": total_rows_fetched,
            "valid_rows_processed": valid_rows_processed,
            "invalid_rows_skipped": invalid_rows_skipped,
            "skipped_reason_counts": skipped_reason_counts,
            "signals_count": signals_count,
            "company_change_count": company_change_count,
            "role_change_count": role_change_count,
            "role_and_company_change_count": both_change_count,
            "run_status": run_status,
            "started_at": start_time.isoformat(timespec="seconds"),
            "ended_at": datetime.now().isoformat(timespec="seconds"),
            "duration_seconds": duration_seconds,
            "error_message": error_message,
        }
        # Raw: keep 5 weeks
        enforce_retention(RAW_PATH, "employees_linkedin_data_", keep_count=5)

        # Signals: keep 8 weeks
        enforce_retention(SIGNALS_PATH, "employee_changes_report_", keep_count=8)

        # Run summaries: keep 8 weeks
        enforce_retention(RUNS_PATH, "run_summary_", keep_count=8)

        summary_path = write_run_summary(run_summary, RUNS_PATH)
        logging.info(f"Run summary written to: {summary_path}")
        print(f"Run summary written to: {summary_path}")

def write_run_summary(summary: dict, runs_path: str) -> str:
    """
    Writes run summary JSON to /data/runs/run_summary_YYYY-MM-DD.json
    Returns the file path as string.
    """
    Path(runs_path).mkdir(parents=True, exist_ok=True)

    week_present = summary.get("week_present", datetime.now().strftime("%Y-%m-%d"))
    out_path = Path(runs_path) / f"run_summary_{week_present}.json"

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return str(out_path)

if __name__ == "__main__":
    run()
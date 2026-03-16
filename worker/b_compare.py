from pathlib import Path
from datetime import datetime
import re
import json
import ast
import pandas as pd
import os
from dotenv import load_dotenv


def extract_company_details(value):
    """
    Extract company name and link from current_company JSON-like column.

    Example:
    {"link":"https://www.linkedin.com/company/us-bank?trk=...","name":"U.S. Bank","company_id":"us-bank","location":null}

    Returns:
        (company_name, company_url)
    """
    if pd.isna(value):
        return pd.NA, pd.NA

    value = str(value).strip()
    if not value:
        return pd.NA, pd.NA

    try:
        obj = json.loads(value)
        if isinstance(obj, dict):
            return obj.get("name", pd.NA), obj.get("link", pd.NA)
    except Exception:
        pass

    try:
        obj = ast.literal_eval(value)
        if isinstance(obj, dict):
            return obj.get("name", pd.NA), obj.get("link", pd.NA)
    except Exception:
        pass

    return pd.NA, pd.NA


def normalize_missing(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        df[c] = df[c].astype("string").str.strip()
        df.loc[df[c].isin(["", "none", "null", "nan", "None", "<NA>"]), c] = pd.NA
    return df


def extract_date_from_filename(filename: str) -> datetime:
    match_dash = re.search(r"(\d{4}-\d{2}-\d{2})", filename)
    if match_dash:
        return datetime.strptime(match_dash.group(1), "%Y-%m-%d")

    match_underscore = re.search(r"(\d{4}_\d{2}_\d{2})", filename)
    if match_underscore:
        return datetime.strptime(match_underscore.group(1), "%Y_%m_%d")

    raise ValueError(f"Could not extract date from filename: {filename}")


def extract_week_label(filename: str) -> str:
    return extract_date_from_filename(filename).strftime("%Y-%m-%d")


def run_comparison():
    load_dotenv()

    RAW_DIR = Path(os.getenv("DATA_RAW_PATH", "/data/raw"))
    SIGNALS_DIR = Path(os.getenv("DATA_SIGNALS_PATH", "/data/signals"))

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    DATASET_PREFIX = "employees_linkedin_data_"
    files_with_dates = []

    for file in RAW_DIR.glob(f"{DATASET_PREFIX}*.csv"):
        try:
            file_date = extract_date_from_filename(file.name)
            files_with_dates.append((file, file_date))
        except ValueError:
            continue

    files_with_dates.sort(key=lambda x: x[1], reverse=True)

    if not files_with_dates:
        raise FileNotFoundError(
            f"No valid files found in {RAW_DIR} matching {DATASET_PREFIX}*.csv"
        )

    if len(files_with_dates) < 2:
        raise FileNotFoundError(
            f"Need at least 2 valid files in {RAW_DIR} to compare, but found only 1."
        )

    new_file = files_with_dates[0][0]
    prev_file = files_with_dates[1][0]

    # 0=id, 1=name, 2=city, 7=current_company
    USECOLS = [0, 1, 2, 7]

    old_df = pd.read_csv(prev_file, usecols=USECOLS, dtype="string", engine="c")
    new_df = pd.read_csv(new_file, usecols=USECOLS, dtype="string", engine="c")

    old_df.columns = ["id", "name", "city", "current_company"]
    new_df.columns = ["id", "name", "city", "current_company"]

    for df in (old_df, new_df):
        df["id"] = df["id"].astype("string").str.strip().str.lower()
        df["name"] = df["name"].astype("string").str.strip()
        df["city"] = df["city"].astype("string").str.strip()

        extracted = df["current_company"].apply(extract_company_details)
        df["company_name"] = extracted.apply(lambda x: x[0])
        df["company_url"] = extracted.apply(lambda x: x[1])

    REQUIRED_COLS = ["id", "name", "city", "company_name"]

    total_rows_fetched = len(new_df)

    old_df = normalize_missing(old_df, REQUIRED_COLS + ["company_url"])
    new_df = normalize_missing(new_df, REQUIRED_COLS + ["company_url"])

    old_before = len(old_df)
    new_before = len(new_df)

    old_df_valid = old_df.dropna(subset=REQUIRED_COLS).copy()
    new_df_valid = new_df.dropna(subset=REQUIRED_COLS).copy()

    old_skipped = old_before - len(old_df_valid)
    new_skipped = new_before - len(new_df_valid)

    old_df = old_df_valid
    new_df = new_df_valid

    valid_rows_processed = len(new_df)
    invalid_rows_skipped = new_skipped
    skipped_reason_counts = {
        "missing_required_fields": invalid_rows_skipped
    }

    merged = old_df[["id", "name", "city", "company_name", "company_url"]].merge(
        new_df[["id", "company_name", "company_url"]],
        on="id",
        how="inner",
        suffixes=("_old", "_new"),
    )

    company_changed = merged["company_name_old"] != merged["company_name_new"]
    changed_rows = merged.loc[company_changed].copy()

    week_present = extract_week_label(new_file.name)
    week_past = extract_week_label(prev_file.name)

    changed_rows["signal_type"] = "company_change"
    changed_rows["week_present"] = week_present
    changed_rows["week_past"] = week_past
    changed_rows["detected_at"] = datetime.now().isoformat(timespec="seconds")

    signals_df = changed_rows[
        [
            "signal_type",
            "id",
            "name",
            "company_name_old",
            "company_url_old",
            "company_name_new",
            "company_url_new",
            "city",
            "week_past",
            "week_present",
            "detected_at",
        ]
    ].rename(
        columns={
            "id": "linkedin_id",
            "company_name_old": "past_company",
            "company_url_old": "past_company_url",
            "company_name_new": "new_company",
            "company_url_new": "new_company_url",
        }
    )

    signals_output_filename = f"signals_{week_present}.csv"
    signals_output_path = SIGNALS_DIR / signals_output_filename
    signals_df.to_csv(signals_output_path, index=False)

    changed_rows["Status"] = "company changed"
    changed_rows["Company (City)"] = (
        changed_rows["company_name_old"].fillna("Unknown")
        + " ("
        + changed_rows["city"].fillna("Unknown")
        + ")"
    )

    report_df = (
        changed_rows[
            [
                "name",
                "company_url_old",
                "company_name_old",
                "company_name_new",
                "Company (City)",
                "Status",
            ]
        ]
        .rename(
            columns={
                "name": "Name",
                "company_url_old": "Past Company URL",
                "company_name_old": "Past Company",
                "company_name_new": "New Company",
            }
        )
        .reset_index(drop=True)
    )

    report_output_filename = f"employee_changes_report_{datetime.now().strftime('%Y-%m-%d')}.csv"
    report_output_path = SIGNALS_DIR / report_output_filename
    report_df.to_csv(report_output_path, index=False)

    company_change_count = int(len(signals_df))

    return {
        "status": "success",
        "new_file": str(new_file),
        "prev_file": str(prev_file),
        "week_present": week_present,
        "week_past": week_past,
        "signals_count": company_change_count,
        "company_change_count": company_change_count,
        "signals_output_path": str(signals_output_path),
        "report_output_path": str(report_output_path),
        "total_rows_fetched": total_rows_fetched,
        "valid_rows_processed": valid_rows_processed,
        "invalid_rows_skipped": invalid_rows_skipped,
        "skipped_reason_counts": skipped_reason_counts,
        "old_rows_skipped": old_skipped,
        "new_rows_skipped": new_skipped,
    }


if __name__ == "__main__":
    print(run_comparison())
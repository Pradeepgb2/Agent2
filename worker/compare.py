from pathlib import Path
from datetime import datetime
import re
import pandas as pd
import os
from dotenv import load_dotenv


def run_comparison():
    """
    Compares the two most recent weekly datasets stored in data/raw/
    and generates a signals report in data/signals/employee_changes_report_YYYY_MM_DD.csv

    Returns:
        dict: comparison metadata (files used, counts, output path)
    """
    # -------------------------
    # PATH SETUP (ENV-BASED)
    # -------------------------
    load_dotenv()

    RAW_DIR = Path(os.getenv("DATA_RAW_PATH", "./data/raw"))
    SIGNALS_DIR = Path(os.getenv("DATA_SIGNALS_PATH", "./data/signals"))

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)
    # -------------------------
    # IDENTIFY TWO MOST RECENT DATASETS
    # -------------------------
    DATASET_PREFIX = "employees_linkedin_data_"
    date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})")
    files_with_dates = []

    for file in RAW_DIR.glob(f"{DATASET_PREFIX}*.csv"):
        match = date_pattern.search(file.name)
        if match:
            file_date = datetime.strptime(match.group(1), "%Y-%m-%d")
            files_with_dates.append((file, file_date))

    files_with_dates.sort(key=lambda x: x[1], reverse=True)

    if not files_with_dates:
        raise FileNotFoundError(
            f"No files found in {RAW_DIR} matching {DATASET_PREFIX}YYYY-MM-DD.csv"
        )

    if len(files_with_dates) < 2:
        new_file = files_with_dates[0][0]
        prev_file = files_with_dates[0][0]
    else:
        new_file = files_with_dates[0][0]
        prev_file = files_with_dates[1][0]

    # -------------------------
    # LOAD CSVs (FAST)
    # -------------------------
    USECOLS = [0, 1, 2, 3, 4]  # only first 5 columns
    DTYPES = {0: "string", 1: "string", 2: "string", 3: "string", 4: "string"}

    old_df = pd.read_csv(prev_file, usecols=USECOLS, dtype=DTYPES, engine="c")
    new_df = pd.read_csv(new_file, usecols=USECOLS, dtype=DTYPES, engine="c")

    old_df.columns = ["email_id", "name", "role", "company_name", "company_location"]
    new_df.columns = ["email_id", "name", "role", "company_name", "company_location"]

    # -------------------------
    # CLEAN DATA (VECTORIZED)
    # -------------------------
    for df in (old_df, new_df):
        df["email_id"] = df["email_id"].str.strip().str.lower()
        df["role"] = df["role"].str.strip()
        df["company_name"] = df["company_name"].str.strip()
        df["company_location"] = df["company_location"].str.strip().fillna("")

    # -------------------------
    # ROW VALIDATION (SKIP INVALID ROWS)
    # -------------------------
    REQUIRED_COLS = ["email_id", "name", "role", "company_name", "company_location"]
    # total rows fetched
    total_rows_fetched = len(new_df)

    # find invalid rows (missing required values)
    invalid_mask = new_df[REQUIRED_COLS].isna().any(axis=1)

    invalid_rows_skipped = int(invalid_mask.sum())

    # keep only valid rows
    new_df = new_df[~invalid_mask]

    valid_rows_processed = len(new_df)
    skipped_reason_counts = {
        "missing_required_fields": invalid_rows_skipped
    }

    def normalize_missing(df: pd.DataFrame) -> pd.DataFrame:
        # Convert empty/whitespace strings to <NA> for required cols
        for c in REQUIRED_COLS:
            df[c] = df[c].astype("string")
            df[c] = df[c].str.strip()
            df.loc[df[c].isin(["", "none", "null", "nan"]), c] = pd.NA
        return df

    old_df = normalize_missing(old_df)
    new_df = normalize_missing(new_df)

    old_before = len(old_df)
    new_before = len(new_df)

    old_df_valid = old_df.dropna(subset=REQUIRED_COLS).copy()
    new_df_valid = new_df.dropna(subset=REQUIRED_COLS).copy()

    old_skipped = old_before - len(old_df_valid)
    new_skipped = new_before - len(new_df_valid)

    # Optional: log counts (or return these counts in metadata)
    # print(f"Old skipped: {old_skipped}, New skipped: {new_skipped}")

    old_df = old_df_valid
    new_df = new_df_valid

    # -------------------------
    # MERGE ONLY NEEDED COLUMNS
    # -------------------------
    merged = old_df[["email_id", "name", "role", "company_name", "company_location"]].merge(
        new_df[["email_id", "role", "company_name"]],
        on="email_id",
        how="inner",
        suffixes=("_old", "_new"),
    )

    # -------------------------
    # FILTER CHANGED ROWS (SAFE + VECTORIZED)
    # -------------------------
    company_changed = merged["company_name_old"] != merged["company_name_new"]
    role_changed = merged["role_old"] != merged["role_new"]

    changed_rows = merged.loc[company_changed | role_changed].copy()

    # Recompute masks on changed_rows (prevents index misalignment)
    company_changed_cr = changed_rows["company_name_old"] != changed_rows["company_name_new"]
    role_changed_cr = changed_rows["role_old"] != changed_rows["role_new"]

    # -------------------------
    # SIGNAL TYPE (PROFESSIONAL)
    # -------------------------
    changed_rows["signal_type"] = "role_change"
    changed_rows.loc[company_changed_cr, "signal_type"] = "company_change"
    changed_rows.loc[role_changed_cr & company_changed_cr, "signal_type"] = "role_and_company_change"

    # -------------------------
    # BUILD SIGNALS OUTPUT (PAST + NEW VALUES)
    # -------------------------
    week_present = date_pattern.search(new_file.name).group(1)  # YYYY-MM-DD from filename
    week_past = date_pattern.search(prev_file.name).group(1)    # YYYY-MM-DD from filename

    changed_rows["week_present"] = week_present
    changed_rows["week_past"] = week_past
    changed_rows["detected_at"] = datetime.now().isoformat(timespec="seconds")

    signals_df = changed_rows[[
        "signal_type",
        "email_id",
        "name",
        "role_old", "role_new",
        "company_name_old", "company_name_new",
        "company_location",
        "week_past", "week_present",
        "detected_at"
    ]].rename(columns={
        "role_old": "past_role",
        "role_new": "new_role",
        "company_name_old": "past_company",
        "company_name_new": "new_company",
        "company_location": "past_company_location",  # keep simple in v0.1
    })

    # If you want new location too later, you must merge company_location_new as well.

    # -------------------------
    # OUTPUT FILE (signals_YYYY-MM-DD.csv)
    # -------------------------
    output_filename = f"signals_{week_present}.csv"
    output_path = SIGNALS_DIR / output_filename

    signals_df.to_csv(output_path, index=False)

    # -------------------------
    # STATUS (handles both changes)
    # -------------------------
    changed_rows["Status"] = ""
    changed_rows.loc[role_changed, "Status"] = "role changed"
    changed_rows.loc[company_changed, "Status"] = "company changed"
    changed_rows.loc[role_changed & company_changed, "Status"] = "role and company changed"

    # -------------------------
    # FINAL REPORT
    # -------------------------
    changed_rows["Company (Location)"] = (
        changed_rows["company_name_old"]
        + " ("
        + changed_rows["company_location"].replace("", "Unknown")
        + ")"
    )

    report_df = (
        changed_rows[["email_id", "Company (Location)", "role_old", "Status"]]
        .rename(columns={"email_id": "Email", "role_old": "Position"})
        .reset_index(drop=True)
    )

    report_df.index += 1

    # -------------------------
    # OUTPUT FILE WITH DATE: employee_changes_report_YYYY_MM_DD.csv
    # -------------------------
    today_str = datetime.now().strftime("%Y-%m-%d")
    output_filename = f"employee_changes_report_{today_str}.csv"
    output_path = SIGNALS_DIR / output_filename

    report_df.to_csv(output_path, index=False)

    company_change_count = int((signals_df["signal_type"] == "company_change").sum())
    role_change_count = int((signals_df["signal_type"] == "role_change").sum())
    both_change_count = int((signals_df["signal_type"] == "role_and_company_change").sum())

    return {
        "status": "success",
        "new_file": str(new_file),
        "prev_file": str(prev_file),
        "week_present": week_present,
        "week_past": week_past,
        "signals_count": int(len(signals_df)),
        "company_change_count": company_change_count,
        "role_change_count": role_change_count,
        "role_and_company_change_count": both_change_count,
        "output_path": str(output_path),
        "total_rows_fetched": total_rows_fetched,
        "valid_rows_processed": valid_rows_processed,
        "invalid_rows_skipped": invalid_rows_skipped,
        "skipped_reason_counts": skipped_reason_counts,
    }
import os
import re
import json
import io
import secrets
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.middleware.cors import CORSMiddleware

load_dotenv()

app = FastAPI(title="Agent2 API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "https://agent2-git-main-pradeep-gubbalas-projects.vercel.app/"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Paths
SIGNALS_PATH = os.getenv("DATA_SIGNALS_PATH", "./data/signals")
RUNS_PATH = os.getenv("DATA_RUNS_PATH", "./data/runs")
DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")

# Basic Auth
security = HTTPBasic()
BASIC_AUTH_USER = os.getenv("BASIC_AUTH_USER", "")
BASIC_AUTH_PASSWORD = os.getenv("BASIC_AUTH_PASSWORD", "")

def require_basic_auth(credentials: HTTPBasicCredentials = Depends(security)):
    # if not configured, show clear error (you can keep 500 or change to 401)
    if not BASIC_AUTH_USER or not BASIC_AUTH_PASSWORD:
        raise HTTPException(status_code=500, detail="Basic Auth is not configured")

    ok_user = secrets.compare_digest(credentials.username, BASIC_AUTH_USER)
    ok_pass = secrets.compare_digest(credentials.password, BASIC_AUTH_PASSWORD)

    if not (ok_user and ok_pass):
        raise HTTPException(
            status_code=401,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )
    return True


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/")
def root():
    return JSONResponse(content={"message": "Agent2 backend running successfully"})


def _get_available_weeks() -> list[str]:
    signals_dir = Path(SIGNALS_PATH)
    if not signals_dir.exists():
        return []
    weeks = []
    for f in signals_dir.glob("employee_changes_report_*.csv"):
        m = DATE_PATTERN.search(f.name)
        if m:
            weeks.append(m.group(1))
    return sorted(set(weeks), reverse=True)


def _get_latest_week() -> str | None:
    weeks = _get_available_weeks()
    return weeks[0] if weeks else None


@app.get("/weeks")
def weeks(_: bool = Depends(require_basic_auth)):
    signals_dir = Path(SIGNALS_PATH)
    if not signals_dir.exists():
        raise HTTPException(status_code=404, detail="Signals directory not found")
    return {"weeks": _get_available_weeks()}


@app.get("/runs/summary")
def runs_summary(week: str | None = None, _: bool = Depends(require_basic_auth)):
    if not week:
        week = _get_latest_week()
        if not week:
            raise HTTPException(status_code=404, detail="No weeks found")

    summary_file = Path(RUNS_PATH) / f"run_summary_{week}.json"
    if not summary_file.exists():
        raise HTTPException(status_code=404, detail=f"No run summary found for week {week}")

    with open(summary_file, "r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/signals")
def signals(
    week: str | None = None,
    q: str | None = None,
    status: str | None = None,
    role: str | None = None,
    company_name: str | None = None,
    company_location: str | None = None,
    limit: int = 100,
    offset: int = 0,
    _: bool = Depends(require_basic_auth),
):
    if not week:
        week = _get_latest_week()
        if not week:
            raise HTTPException(status_code=404, detail="No weeks found")

    signals_file = Path(SIGNALS_PATH) / f"employee_changes_report_{week}.csv"
    if not signals_file.exists():
        raise HTTPException(status_code=404, detail=f"No signals file found for week {week}")

    df = pd.read_csv(signals_file, dtype="string").fillna("")

    def contains(series, text):
        return series.str.lower().str.contains(text.lower(), na=False)

    if q:
        mask = (
            contains(df["Email"], q)
            | contains(df["Company (Location)"], q)
            | contains(df["Position"], q)
            | contains(df["Status"], q)
        )
        df = df[mask]

    if status:
        df = df[contains(df["Status"], status)]

    if role:
        df = df[contains(df["Position"], role)]

    if company_name:
        df = df[contains(df["Company (Location)"], company_name)]

    if company_location:
        df = df[contains(df["Company (Location)"], company_location)]

    total = len(df)
    df_page = df.iloc[offset: offset + limit].copy()
    return {"week": week, "total": total, "items": df_page.to_dict(orient="records")}


@app.get("/signals/export")
def signals_export(
    week: str | None = None,
    q: str | None = None,
    status: str | None = None,
    role: str | None = None,
    company_name: str | None = None,
    company_location: str | None = None,
    _: bool = Depends(require_basic_auth),
):
    result = signals(
        week=week, q=q, status=status, role=role,
        company_name=company_name, company_location=company_location,
        limit=10**9, offset=0
    )
    df = pd.DataFrame(result["items"])

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    filename = f"signals_export_{result['week']}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'}
    )

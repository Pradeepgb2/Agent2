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
from backend.db_utils import get_connection

load_dotenv()

app = FastAPI(title="Agent2 API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "https://agent2-git-main-pradeep-gubbalas-projects.vercel.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SIGNALS_PATH = os.getenv("DATA_SIGNALS_PATH", "./data/signals")
RUNS_PATH = os.getenv("DATA_RUNS_PATH", "./data/runs")
DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})")

security = HTTPBasic()
BASIC_AUTH_USER = os.getenv("BASIC_AUTH_USER", "")
BASIC_AUTH_PASSWORD = os.getenv("BASIC_AUTH_PASSWORD", "")


def require_basic_auth(credentials: HTTPBasicCredentials = Depends(security)):
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
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT week_present
        FROM pipeline_runs
        WHERE week_present IS NOT NULL
        ORDER BY week_present DESC
    """)

    rows = cur.fetchall()

    cur.close()
    conn.close()

    return [row["week_present"] for row in rows]


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
    conn = get_connection()
    cur = conn.cursor()

    if week:
        cur.execute("""
            SELECT *
            FROM pipeline_runs
            WHERE week_present = %s
            ORDER BY id DESC
            LIMIT 1
        """, (week,))
    else:
        cur.execute("""
            SELECT *
            FROM pipeline_runs
            ORDER BY week_present DESC, id DESC
            LIMIT 1
        """)

    row = cur.fetchone()

    cur.close()
    conn.close()

    if not row:
        raise HTTPException(status_code=404, detail="No run summary found")

    return row


@app.get("/signals")
def signals(
    week: str | None = None,
    q: str | None = None,
    status: str | None = None,
    company_name: str | None = None,
    city: str | None = None,
    limit: int = 100,
    offset: int = 0,
    _: bool = Depends(require_basic_auth),
):
    conn = get_connection()
    cur = conn.cursor()

    filters = []
    values = []

    if week:
        filters.append("week_present = %s")
        values.append(week)
    else:
        latest_week = _get_latest_week()
        if not latest_week:
            raise HTTPException(status_code=404, detail="No weeks found")
        filters.append("week_present = %s")
        values.append(latest_week)
        week = latest_week

    if status:
        filters.append("signal_type ILIKE %s")
        values.append(f"%{status}%")

    if company_name:
        filters.append("(past_company ILIKE %s OR new_company ILIKE %s)")
        values.extend([f"%{company_name}%", f"%{company_name}%"])

    if city:
        filters.append("city ILIKE %s")
        values.append(f"%{city}%")

    if q:
        filters.append("""
            (
                name ILIKE %s OR
                past_company ILIKE %s OR
                new_company ILIKE %s OR
                city ILIKE %s OR
                past_company_url ILIKE %s
            )
        """)
        values.extend([f"%{q}%"] * 5)

    where_clause = " AND ".join(filters) if filters else "TRUE"

    count_sql = f"SELECT COUNT(*) AS total FROM signals WHERE {where_clause}"
    cur.execute(count_sql, values)
    total = cur.fetchone()["total"]

    data_sql = f"""
        SELECT
            name AS "Name",
            past_company_url AS "Past Company URL",
            past_company AS "Past Company",
            new_company AS "New Company",
            CONCAT(COALESCE(past_company, 'Unknown'), ' (', COALESCE(city, 'Unknown'), ')') AS "Company (City)",
            'company changed' AS "Status"
        FROM signals
        WHERE {where_clause}
        ORDER BY detected_at DESC
        LIMIT %s OFFSET %s
    """

    cur.execute(data_sql, values + [limit, offset])
    items = cur.fetchall()

    cur.close()
    conn.close()

    return {
        "week": week,
        "total": total,
        "items": items,
    }
    

@app.get("/signals/export")
def signals_export(
    week: str | None = None,
    q: str | None = None,
    status: str | None = None,
    company_name: str | None = None,
    city: str | None = None,
    _: bool = Depends(require_basic_auth),
):
    result = signals(
        week=week,
        q=q,
        status=status,
        company_name=company_name,
        city=city,
        limit=10**9,
        offset=0,
    )

    df = pd.DataFrame(result["items"])

    buf = io.StringIO()
    df.to_csv(buf, index=False)
    buf.seek(0)

    filename = f"signals_export_{result['week']}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
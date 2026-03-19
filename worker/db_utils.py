import os
import json
import psycopg2
from psycopg2.extras import Json


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


def create_tables():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id SERIAL PRIMARY KEY,
            linkedin_id TEXT,
            name TEXT,
            past_company TEXT,
            past_company_url TEXT,
            new_company TEXT,
            new_company_url TEXT,
            city TEXT,
            signal_type TEXT,
            week_past DATE,
            week_present DATE,
            detected_at TIMESTAMP
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            id SERIAL PRIMARY KEY,
            week_present TEXT,
            week_past TEXT,
            total_rows_fetched INTEGER,
            valid_rows_processed INTEGER,
            invalid_rows_skipped INTEGER,
            skipped_reason_counts JSONB,
            signals_count INTEGER,
            company_change_count INTEGER,
            role_change_count INTEGER,
            role_and_company_change_count INTEGER,
            run_status TEXT,
            started_at TIMESTAMP,
            ended_at TIMESTAMP,
            duration_seconds INTEGER,
            error_message TEXT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()


def insert_pipeline_run(run_summary: dict):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO pipeline_runs (
            week_present,
            week_past,
            total_rows_fetched,
            valid_rows_processed,
            invalid_rows_skipped,
            skipped_reason_counts,
            signals_count,
            company_change_count,
            role_change_count,
            role_and_company_change_count,
            run_status,
            started_at,
            ended_at,
            duration_seconds,
            error_message
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        run_summary.get("week_present"),
        run_summary.get("week_past"),
        run_summary.get("total_rows_fetched", 0),
        run_summary.get("valid_rows_processed", 0),
        run_summary.get("invalid_rows_skipped", 0),
        Json(run_summary.get("skipped_reason_counts", {})),
        run_summary.get("signals_count", 0),
        run_summary.get("company_change_count", 0),
        run_summary.get("role_change_count", 0),
        run_summary.get("role_and_company_change_count", 0),
        run_summary.get("run_status"),
        run_summary.get("started_at"),
        run_summary.get("ended_at"),
        run_summary.get("duration_seconds", 0),
        run_summary.get("error_message"),
    ))

    conn.commit()
    cur.close()
    conn.close()

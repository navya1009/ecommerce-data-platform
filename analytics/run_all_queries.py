#!/usr/bin/env python3
"""
Run every analytics query in analytics/queries/ against the warehouse.

Each .sql file is split on its "-- ====" separator lines; every block that
contains SQL is one query, named by its "-- Query X.Y" header line.

Usage:
    python analytics/run_all_queries.py                  # execute all queries
    python analytics/run_all_queries.py --dry-run        # EXPLAIN only, no execution
    python analytics/run_all_queries.py --file funnel_analysis.sql

Exit code: 0 if every query passed, 1 if any failed (or the connection failed).

Connection: POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD,
POSTGRES_DB (same variables as the pipeline). Connections are read-only.
"""
import argparse
import os
import re
import sys
import time
from pathlib import Path

import psycopg

QUERIES_DIR = Path(__file__).resolve().parent / "queries"

SEPARATOR = re.compile(r"^-- =+\s*$", re.MULTILINE)
QUERY_NAME = re.compile(r"^--\s*(Query\s+[\w.]+[^\n]*)$", re.MULTILINE)


def load_queries(path: Path) -> list:
    """
    Return [(name, sql)] for every query block in a file. Comment lines are
    dropped from the SQL; blocks with no SQL (file headers) are skipped.
    """
    text = path.read_text(encoding="utf-8")
    queries = []
    current_name = None
    for index, block in enumerate(SEPARATOR.split(text)):
        match = QUERY_NAME.search(block)
        if match:
            current_name = match.group(1).strip()
        sql_lines = [
            line
            for line in block.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        sql = "\n".join(sql_lines).strip().rstrip(";").strip()
        if not sql:
            continue
        # A block without its own header belongs to the most recent header
        name = current_name or f"{path.stem} block {index}"
        queries.append((name, sql))
    return queries


def connect() -> psycopg.Connection:
    conn = psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
        connect_timeout=10,  # fail fast when the database is down
    )
    conn.autocommit = True
    conn.read_only = True  # analytics never writes
    return conn


def run_query(conn: psycopg.Connection, sql: str, dry_run: bool) -> str:
    """Execute (or EXPLAIN) one query. Returns the row-count summary text."""
    with conn.cursor() as cur:
        if dry_run:
            cur.execute(f"EXPLAIN {sql}")
            return f"EXPLAIN ok ({len(cur.fetchall())} plan lines)"
        cur.execute(sql)
        return f"{len(cur.fetchall())} rows"


def main() -> int:
    parser = argparse.ArgumentParser(description="Run analytics SQL queries")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="validate with EXPLAIN only; do not execute",
    )
    parser.add_argument(
        "--file", help="run only this file (name inside analytics/queries/)"
    )
    args = parser.parse_args()

    files = sorted(QUERIES_DIR.glob("*.sql"))
    if args.file:
        files = [f for f in files if f.name == args.file]
        if not files:
            print(f"[FAIL] no such query file: {args.file}")
            return 1

    try:
        conn = connect()
    except Exception as exc:  # connection problems are a failure, not a crash
        print(f"[FAIL] connect :: {exc}")
        return 1

    failures = 0
    passes = 0
    try:
        for path in files:
            for name, sql in load_queries(path):
                started = time.perf_counter()
                try:
                    summary = run_query(conn, sql, args.dry_run)
                except Exception as exc:
                    failures += 1
                    first_line = (
                        str(exc).strip().splitlines()[0]
                        if str(exc).strip()
                        else type(exc).__name__
                    )
                    print(f"[FAIL] {path.name} :: {name} :: {first_line}")
                    continue
                elapsed_ms = (time.perf_counter() - started) * 1000
                passes += 1
                print(
                    f"[PASS] {path.name} :: {name} :: {summary} :: {elapsed_ms:.1f} ms"
                )
    finally:
        conn.close()

    print(f"\n{passes} passed, {failures} failed")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())

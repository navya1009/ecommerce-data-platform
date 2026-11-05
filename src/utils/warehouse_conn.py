"""
PostgreSQL warehouse connection shared by the metrics and lineage modules.
Uses psycopg (psycopg3). Settings come from the same POSTGRES_* env vars the
rest of the pipeline reads; defaults point at a local dev warehouse.
"""

import os

import psycopg


def connect_warehouse():
    """Open a new autocommit-off connection to the warehouse database."""
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", 5432)),
        user=os.getenv("POSTGRES_USER", "admin"),
        password=os.getenv("POSTGRES_PASSWORD", ""),
        dbname=os.getenv("POSTGRES_DB", "postgres"),
    )


def rows_to_dicts(cursor) -> list:
    """Turn the rows of an executed cursor into a list of column-name dicts."""
    columns = [col.name for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

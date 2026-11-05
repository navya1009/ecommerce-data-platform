"""
Data lineage: which source fed which target, in which run, how many rows.

record_lineage() raises on failure so callers can decide. The ingestion and
Spark call sites wrap it so a lineage outage never fails the pipeline.
"""

from .warehouse_conn import connect_warehouse, rows_to_dicts

INSERT_SQL = """
    INSERT INTO warehouse.pipeline_lineage
        (run_id, source_system, source_table, target_table, rows_written, transformation_applied)
    VALUES (%s, %s, %s, %s, %s, %s)
"""

LINEAGE_COLUMNS = (
    "id, run_id, source_system, source_table, target_table, "
    "rows_written, transformation_applied, recorded_at"
)


def record_lineage(
    run_id: str,
    source_system: str,
    source_table: str,
    target_table: str,
    rows_written: int,
    transformation_applied: str,
) -> None:
    """Insert one lineage edge."""
    conn = connect_warehouse()
    try:
        cursor = conn.cursor()
        cursor.execute(
            INSERT_SQL,
            (
                run_id,
                source_system,
                source_table,
                target_table,
                rows_written,
                transformation_applied,
            ),
        )
        cursor.close()
        conn.commit()
    finally:
        conn.close()


def get_lineage_by_run(run_id: str) -> list:
    """Every lineage edge written by one run, in the order they were recorded."""
    conn = connect_warehouse()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT {LINEAGE_COLUMNS} FROM warehouse.pipeline_lineage "
            "WHERE run_id = %s ORDER BY recorded_at, id",
            (run_id,),
        )
        rows = rows_to_dicts(cursor)
        cursor.close()
        return rows
    finally:
        conn.close()


def table_name_variants(table_name: str) -> list:
    """
    Names a table is recorded under: 'fact_orders' and 'warehouse.fact_orders'
    refer to the same table, and jobs record it under either form.
    """
    bare = table_name.split(".")[-1]
    return sorted({table_name, bare, f"warehouse.{bare}"})


def get_lineage_for_table(table_name: str) -> list:
    """Edges into or out of a table (schema-qualified or bare name), newest first."""
    names = table_name_variants(table_name)
    conn = connect_warehouse()
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT {LINEAGE_COLUMNS} FROM warehouse.pipeline_lineage "
            "WHERE source_table = ANY(%s) OR target_table = ANY(%s) "
            "ORDER BY recorded_at DESC, id DESC",
            (names, names),
        )
        rows = rows_to_dicts(cursor)
        cursor.close()
        return rows
    finally:
        conn.close()

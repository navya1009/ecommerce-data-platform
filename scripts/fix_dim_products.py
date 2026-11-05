#!/usr/bin/env python3
"""
Fix missing products in dim_products table
Inserts all product IDs from MySQL staging orders into PostgreSQL dim_products
"""
import mysql.connector
import psycopg2
from psycopg2.extras import execute_values


def main():
    # Connect to MySQL
    print("Connecting to MySQL...")
    mysql_conn = mysql.connector.connect(
        host="mysql", user="root", password="root123", database="ecommerce_staging"
    )
    mysql_cursor = mysql_conn.cursor()
    mysql_cursor.execute("SELECT DISTINCT product_id FROM stg_raw_orders")
    product_ids = [row[0] for row in mysql_cursor.fetchall()]
    mysql_cursor.close()
    mysql_conn.close()

    print(f"Found {len(product_ids)} distinct products in MySQL")

    # Connect to PostgreSQL
    print("Connecting to PostgreSQL...")
    pg_conn = psycopg2.connect(
        host="postgres", user="admin", password="postgres123", database="postgres"
    )
    pg_cursor = pg_conn.cursor()

    # Get existing products
    pg_cursor.execute("SELECT product_id FROM warehouse.dim_products")
    existing_products = {row[0] for row in pg_cursor.fetchall()}

    print(f"Found {len(existing_products)} existing products in PostgreSQL")

    # Insert missing products
    missing_products = [pid for pid in product_ids if pid not in existing_products]

    if missing_products:
        print(f"Inserting {len(missing_products)} missing products...")

        # Insert in batches of 1000
        batch_size = 1000
        total_inserted = 0
        for i in range(0, len(missing_products), batch_size):
            batch_ids = missing_products[i : i + batch_size]  # noqa: E203
            values = [
                (
                    pid,
                    f"Product {pid}",
                    "Unknown",
                    "Unknown",
                    "Unknown",
                    0.00,
                    0.00,
                    True,
                )
                for pid in batch_ids
            ]

            execute_values(
                pg_cursor,
                """
                INSERT INTO warehouse.dim_products (product_id, name, category, subcategory, brand, cost_price, list_price, is_active)  # noqa: E501
                VALUES %s
                ON CONFLICT (product_id) DO NOTHING
                """,
                values,
            )
            total_inserted += len(batch_ids)
            print(f"  Inserted batch {i//batch_size + 1} ({len(batch_ids)} products)")

        pg_conn.commit()
        print(f"✅ Inserted {total_inserted} products into dim_products")
    else:
        print("✅ No missing products found")

    pg_cursor.close()
    pg_conn.close()


if __name__ == "__main__":
    main()

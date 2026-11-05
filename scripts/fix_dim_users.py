#!/usr/bin/env python3
"""
Fix missing users in dim_users table
Inserts all user IDs from MySQL staging orders into PostgreSQL dim_users
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
    mysql_cursor.execute("SELECT DISTINCT user_id FROM stg_raw_orders")
    user_ids = [row[0] for row in mysql_cursor.fetchall()]
    mysql_cursor.close()
    mysql_conn.close()

    print(f"Found {len(user_ids)} distinct users in MySQL")

    # Connect to PostgreSQL
    print("Connecting to PostgreSQL...")
    pg_conn = psycopg2.connect(
        host="postgres", user="admin", password="postgres123", database="postgres"
    )
    pg_cursor = pg_conn.cursor()

    # Get existing users
    pg_cursor.execute("SELECT user_id FROM warehouse.dim_users")
    existing_users = {row[0] for row in pg_cursor.fetchall()}

    print(f"Found {len(existing_users)} existing users in PostgreSQL")

    # Insert missing users
    missing_users = [uid for uid in user_ids if uid not in existing_users]

    if missing_users:
        print(f"Inserting {len(missing_users)} missing users...")

        # Insert in batches of 1000
        batch_size = 1000
        total_inserted = 0
        for i in range(0, len(missing_users), batch_size):
            batch_ids = missing_users[i : i + batch_size]  # noqa: E203
            values = [
                (
                    uid,
                    f"User {uid}",
                    f"user{uid}@example.com",
                    "Unknown",
                    "Unknown",
                    "Regular",
                )
                for uid in batch_ids
            ]

            execute_values(
                pg_cursor,
                """
                INSERT INTO warehouse.dim_users (user_id, name, email, city, country, segment)
                VALUES %s
                ON CONFLICT (user_id) DO NOTHING
                """,
                values,
            )
            total_inserted += len(batch_ids)
            print(f"  Inserted batch {i//batch_size + 1} ({len(batch_ids)} users)")

        pg_conn.commit()
        print(f"✅ Inserted {total_inserted} users into dim_users")
    else:
        print("✅ No missing users found")

    pg_cursor.close()
    pg_conn.close()


if __name__ == "__main__":
    main()

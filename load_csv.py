"""Load a CSV into a PostgreSQL table using COPY (fast, server-side CSV parsing).

Configured through environment variables (see docker-compose.yml):
    DATABASE_URL  e.g. postgresql://spark:spark@postgres:5432/spark
    CSV_FILE      path to the CSV      (default /data/BasicCompanyDataAsOneFile-2026-10-01.csv)
    TABLE_NAME    table to (re)create  (default companies)

All columns are created as TEXT; cast in SQL or Spark if you need types.
The table is dropped and rebuilt on each run, so it is safe to re-run.
"""

import csv
import os

import psycopg
from psycopg import sql

DATABASE_URL = os.environ["DATABASE_URL"]
CSV_FILE = os.environ.get("CSV_FILE", "/data/BasicCompanyDataAsOneFile-2026-10-01.csv")
TABLE_NAME = os.environ.get("TABLE_NAME", "companies")


def main():
    # Read just the header to build the table definition.
    with open(CSV_FILE, newline="", encoding="utf-8-sig") as f:
        header = [h.strip() for h in next(csv.reader(f))]

    table = sql.Identifier(TABLE_NAME)
    columns = sql.SQL(", ").join(sql.SQL("{} TEXT").format(sql.Identifier(h)) for h in header)

    with psycopg.connect(DATABASE_URL) as conn:  # commits on success
        with conn.cursor() as cur:
            cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table))
            cur.execute(sql.SQL("CREATE TABLE {} ({})").format(table, columns))

            copy_stmt = sql.SQL("COPY {} FROM STDIN WITH (FORMAT csv, HEADER true)").format(table)
            with cur.copy(copy_stmt) as copy, open(CSV_FILE, "rb") as f:
                while chunk := f.read(64 * 1024):
                    copy.write(chunk)

            cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(table))
            count = cur.fetchone()[0]

    print(f"Loaded {count} rows from {CSV_FILE} into table '{TABLE_NAME}'")


if __name__ == "__main__":
    main()

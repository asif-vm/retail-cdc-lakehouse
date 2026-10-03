from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import pandas as pd

from src.generate_events import create_events


def read_events(path: Path) -> pd.DataFrame:
    records = []
    with path.open(encoding="utf-8") as source:
        for line in source:
            event = json.loads(line)
            row = event["after"] or event["before"]
            records.append({
                "event_id": event["event_id"], "op": event["op"],
                "event_time": event["event_time"], "order_id": row["order_id"],
                "store_id": row.get("store_id", "STORE-001"),
                "customer_id": row["customer_id"], "status": row["status"],
                "amount": row["amount"], "updated_at": row["updated_at"],
            })
    df = pd.DataFrame(records)
    # Generated events use whole seconds while live UI events include
    # microseconds. Pandas 2.x otherwise infers one format for the whole column.
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, format="mixed")
    df["updated_at"] = pd.to_datetime(df["updated_at"], utc=True, format="mixed")
    return df


def run_pipeline(events_path: Path, db_path: Path) -> dict[str, int | float]:
    events = read_events(events_path)
    if events["event_id"].isna().any() or events["order_id"].isna().any():
        raise ValueError("CDC contract violation: missing event_id or order_id")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(db_path)) as con:
        con.execute("""CREATE TABLE IF NOT EXISTS bronze_events (
            event_id VARCHAR PRIMARY KEY, op VARCHAR, event_time TIMESTAMPTZ,
            order_id VARCHAR, store_id VARCHAR, customer_id VARCHAR, status VARCHAR,
            amount DOUBLE, updated_at TIMESTAMPTZ
        )""")
        con.execute("ALTER TABLE bronze_events ADD COLUMN IF NOT EXISTS store_id VARCHAR DEFAULT 'STORE-001'")
        con.register("incoming", events)
        con.execute("""INSERT OR IGNORE INTO bronze_events
            (event_id, op, event_time, order_id, store_id, customer_id, status, amount, updated_at)
            SELECT event_id, op, event_time, order_id, store_id, customer_id, status, amount, updated_at
            FROM incoming""")
        con.execute("""
            CREATE OR REPLACE TABLE silver_orders AS
            SELECT order_id, store_id, customer_id, status, amount, updated_at
            FROM bronze_events
            QUALIFY row_number() OVER (
                PARTITION BY order_id ORDER BY updated_at DESC, event_time DESC, event_id DESC
            ) = 1
        """)
        con.execute("""
            CREATE OR REPLACE TABLE gold_daily_orders AS
            SELECT CAST(updated_at AS DATE) AS order_date, store_id, status,
                   count(*) AS orders, round(sum(amount), 2) AS gross_value,
                   count(DISTINCT customer_id) AS customers
            FROM silver_orders GROUP BY 1, 2, 3 ORDER BY 1, 2, 3
        """)
        bronze = con.execute("SELECT count(*) FROM bronze_events").fetchone()[0]
        silver = con.execute("SELECT count(*) FROM silver_orders").fetchone()[0]
        duplicates = con.execute("SELECT count(*)-count(DISTINCT event_id) FROM bronze_events").fetchone()[0]
        invalid_status = con.execute("SELECT count(*) FROM silver_orders WHERE status NOT IN ('created','paid','shipped','delivered','cancelled')").fetchone()[0]
    if duplicates or invalid_status:
        raise ValueError("Data quality checks failed")
    return {"bronze_events": bronze, "current_orders": silver, "duplicate_event_ids": duplicates, "invalid_statuses": invalid_status}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, default=5000)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    events = root / "data" / "bronze" / "order_events.jsonl"
    if not events.exists():
        create_events(events, args.orders)
    print(run_pipeline(events, root / "data" / "retail.duckdb"))


if __name__ == "__main__":
    main()


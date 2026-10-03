from pathlib import Path
import json

import duckdb
from src.generate_events import create_events
from src.pipeline import run_pipeline


def test_idempotent_cdc_replay(tmp_path: Path):
    events = create_events(tmp_path / "events.jsonl", orders=250)
    db = tmp_path / "retail.duckdb"
    first = run_pipeline(events, db)
    second = run_pipeline(events, db)
    assert first == second
    assert first["current_orders"] == 250
    with duckdb.connect(str(db)) as con:
        assert con.execute("SELECT count(*) FROM gold_daily_orders").fetchone()[0] > 0


def test_mixed_iso_timestamp_precision(tmp_path: Path):
    events = create_events(tmp_path / "events.jsonl", orders=2)
    live_event = {
        "event_id": "live-ui-event",
        "op": "c",
        "event_time": "2026-10-03T14:31:29.417347+00:00",
        "before": None,
        "after": {
            "order_id": "DEMO-1",
            "store_id": "STORE-1",
            "customer_id": "CUS-1",
            "status": "created",
            "amount": 1499.0,
            "updated_at": "2026-10-03T14:31:29.417347+00:00",
        },
    }
    with events.open("a", encoding="utf-8") as out:
        out.write(json.dumps(live_event) + "\n")
    result = run_pipeline(events, tmp_path / "retail.duckdb")
    assert result["current_orders"] == 3


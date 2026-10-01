from pathlib import Path

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


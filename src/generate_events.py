from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import random
import uuid


STATUSES = ["created", "paid", "shipped", "delivered", "cancelled"]


def create_events(path: Path, orders: int = 5000, seed: int = 42) -> Path:
    rng = random.Random(seed)
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    events = []
    for order_num in range(1, orders + 1):
        order_id = f"ORD-{order_num:07d}"
        created = base + timedelta(minutes=rng.randint(0, 525_600))
        current = {
            "order_id": order_id,
            "customer_id": f"CUS-{rng.randint(1, max(50, orders // 4)):06d}",
            "status": "created",
            "amount": round(rng.uniform(100, 12000), 2),
            "updated_at": created.isoformat(),
        }
        events.append({"event_id": str(uuid.UUID(int=rng.getrandbits(128))), "op": "c", "event_time": created.isoformat(), "before": None, "after": current.copy()})
        for status in STATUSES[1:rng.randint(2, len(STATUSES))]:
            if current["status"] in {"delivered", "cancelled"}:
                break
            before = current.copy()
            current["status"] = status
            current["updated_at"] = (datetime.fromisoformat(current["updated_at"]) + timedelta(hours=rng.randint(1, 96))).isoformat()
            events.append({"event_id": str(uuid.UUID(int=rng.getrandbits(128))), "op": "u", "event_time": current["updated_at"], "before": before, "after": current.copy()})
    rng.shuffle(events)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as out:
        for event in events:
            out.write(json.dumps(event) + "\n")
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders", type=int, default=5000)
    parser.add_argument("--out", type=Path, default=Path("data/bronze/order_events.jsonl"))
    args = parser.parse_args()
    print(create_events(args.out, args.orders))


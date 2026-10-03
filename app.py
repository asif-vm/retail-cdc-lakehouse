from __future__ import annotations

import json
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from src.pipeline import run_pipeline

ROOT = Path(__file__).parent
EVENTS = ROOT / "data" / "bronze" / "order_events.jsonl"
DB = ROOT / "data" / "retail.duckdb"
LOCK = threading.Lock()
STATUSES = ["created", "paid", "shipped", "delivered", "cancelled"]
NEXT_STATUS = {
    "created": {"paid", "cancelled"},
    "paid": {"shipped", "cancelled"},
    "shipped": {"delivered", "cancelled"},
    "delivered": set(),
    "cancelled": set(),
}
app = FastAPI(title="Retail CDC Explorer")


class OrderChange(BaseModel):
    order_id: str = Field(min_length=3, max_length=40)
    store_id: str = Field(min_length=2, max_length=40)
    customer_id: str = Field(min_length=2, max_length=40)
    status: str
    amount: float = Field(gt=0, le=1_000_000)


def current_order(order_id: str) -> dict | None:
    if not DB.exists():
        return None
    with duckdb.connect(str(DB), read_only=True) as con:
        row = con.execute("SELECT order_id, store_id, customer_id, status, amount, updated_at FROM silver_orders WHERE order_id = ?", [order_id]).fetchone()
    if not row:
        return None
    return dict(zip(["order_id", "store_id", "customer_id", "status", "amount", "updated_at"], row))


@app.get("/", response_class=HTMLResponse)
def home() -> str:
    return PAGE


@app.post("/api/orders")
def record_change(change: OrderChange) -> dict:
    if change.status not in STATUSES:
        raise HTTPException(422, f"Status must be one of: {', '.join(STATUSES)}")
    with LOCK:
        before = current_order(change.order_id)
        if before and all(
            [
                before["store_id"] == change.store_id,
                before["customer_id"] == change.customer_id,
                before["status"] == change.status,
                float(before["amount"]) == change.amount,
            ]
        ):
            raise HTTPException(409, "Nothing changed. Choose a new status or edit the order details.")
        if before and change.status != before["status"] and change.status not in NEXT_STATUS[before["status"]]:
            allowed = ", ".join(sorted(NEXT_STATUS[before["status"]])) or "none (this order is complete)"
            raise HTTPException(409, f"After {before['status']}, the allowed next status is: {allowed}.")
        now = datetime.now(timezone.utc).isoformat()
        after = change.model_dump() | {"updated_at": now}
        event = {"event_id": str(uuid.uuid4()), "op": "u" if before else "c", "event_time": now, "before": before, "after": after}
        EVENTS.parent.mkdir(parents=True, exist_ok=True)
        previous_size = EVENTS.stat().st_size if EVENTS.exists() else 0
        with EVENTS.open("a", encoding="utf-8") as out:
            out.write(json.dumps(event, default=str) + "\n")
        try:
            quality = run_pipeline(EVENTS, DB)
        except Exception as exc:
            # Do not leave a half-accepted event behind when processing fails.
            with EVENTS.open("r+b") as out:
                out.truncate(previous_size)
            raise HTTPException(500, "The change was not saved because processing failed") from exc
    return {"message": "Order change captured", "operation": "updated" if before else "created", "order": after, "quality": quality}


@app.get("/api/orders")
def recent_orders(limit: int = 20) -> dict:
    if not DB.exists():
        raise HTTPException(503, "Run python -m src.pipeline first")
    safe_limit = min(max(limit, 1), 100)
    with duckdb.connect(str(DB), read_only=True) as con:
        rows = con.execute("""
            SELECT order_id, store_id, customer_id, status, amount, updated_at
            FROM silver_orders ORDER BY updated_at DESC LIMIT ?
        """, [safe_limit]).fetchall()
    columns = ["order_id", "store_id", "customer_id", "status", "amount", "updated_at"]
    return {"orders": [dict(zip(columns, row)) for row in rows]}


@app.get("/api/orders/{order_id}")
def order_story(order_id: str) -> dict:
    if not DB.exists():
        raise HTTPException(503, "Run python -m src.pipeline first")
    with duckdb.connect(str(DB), read_only=True) as con:
        rows = con.execute("""
            WITH ordered AS (
                SELECT event_time, op, status, amount, store_id,
                    lag(status) OVER timeline AS previous_status,
                    lag(amount) OVER timeline AS previous_amount,
                    lag(store_id) OVER timeline AS previous_store
                FROM bronze_events WHERE order_id = ?
                WINDOW timeline AS (ORDER BY event_time, event_id)
            )
            SELECT event_time, op, status, amount, store_id FROM ordered
            WHERE previous_status IS NULL
               OR status IS DISTINCT FROM previous_status
               OR amount IS DISTINCT FROM previous_amount
               OR store_id IS DISTINCT FROM previous_store
            ORDER BY event_time
        """, [order_id]).fetchall()
    current = current_order(order_id)
    allowed_statuses = sorted(NEXT_STATUS[current["status"]]) if current else ["created"]
    return {
        "order_id": order_id,
        "current": current,
        "allowed_statuses": allowed_statuses,
        "complete": bool(current and not allowed_statuses),
        "events": [dict(zip(["event_time", "operation", "status", "amount", "store_id"], row)) for row in rows],
    }


PAGE = r'''<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width"><title>Retail CDC Explorer</title><style>
*{box-sizing:border-box}body{margin:0;background:#0d1524;color:#eaf0f7;font:15px system-ui}header{padding:42px max(5vw,24px);background:linear-gradient(120deg,#16385f,#116466)}main{max-width:1050px;margin:auto;padding:28px;display:grid;grid-template-columns:1fr 1.2fr;gap:24px}.panel{background:#172236;border:1px solid #263754;border-radius:16px;padding:24px}h1,h2{margin-top:0}.hint{color:#9fb0c5}.fields{display:grid;grid-template-columns:1fr 1fr;gap:12px}label{display:block;color:#aebcd0;font-size:12px;margin-bottom:5px}input,select,button{width:100%;padding:11px;border-radius:8px;border:1px solid #40516b;background:#101a2b;color:white}button{background:#23a091;border:0;font-weight:700;margin-top:14px;cursor:pointer}button.secondary{background:#263754;color:#dbe8f5}.event{border-left:3px solid #23a091;padding:5px 14px;margin:16px 0}.status{display:inline-block;background:#213d50;border-radius:20px;padding:4px 10px}.ok{color:#6ee7b7}.warn{color:#ffd96c}.wide{grid-column:1/-1}.orders{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.order{background:#101a2b;border:1px solid #30435f;border-radius:10px;padding:13px;cursor:pointer}.order:hover{border-color:#23a091}.order b,.order small{display:block}.order small{color:#9fb0c5;margin-top:5px}@media(max-width:760px){main{grid-template-columns:1fr}.fields,.orders{grid-template-columns:1fr}.wide{grid-column:auto}}</style></head><body><header><h1>See change data capture in action</h1><div class="hint">Create multiple orders, change their statuses, and reopen any saved history.</div></header><main><section class="panel"><h2>Record an order change</h2><form id="form"><div class="fields"><div><label>Order ID</label><input name="order_id" required></div><div><label>Store</label><input name="store_id" value="STORE-001" required></div><div><label>Customer</label><input name="customer_id" value="CUS-100" required></div><div><label>Next status</label><select name="status"><option>created</option></select></div></div><label style="margin-top:12px">Order amount</label><input name="amount" type="number" value="1499" min="1" step="0.01"><button id="save">Save change and run pipeline</button><button class="secondary" type="button" id="new">Start a new demo order</button></form><p id="message" class="hint">Use the same order ID again with a new status to build its timeline.</p></section><section class="panel"><h2>Order history</h2><div id="story" class="hint">Submit an order to see its CDC events.</div></section><section class="panel wide"><h2>Recently saved orders</h2><p class="hint">Select an order to reopen its current state and full history.</p><div class="orders" id="orders"></div></section></main><script>
let f=document.querySelector('#form'),id=f.order_id,status=f.status,save=document.querySelector('#save'),message=document.querySelector('#message');function fresh(){id.value='DEMO-'+Date.now().toString().slice(-6);status.innerHTML='<option>created</option>';save.disabled=false;document.querySelector('#story').innerHTML='This is a new order. Save it as created to begin the timeline.';message.textContent='Ready to create a new order.'}async function recent(){let d=await fetch('/api/orders?limit=30').then(r=>r.json());document.querySelector('#orders').innerHTML=d.orders.map(o=>`<div class="order" data-id="${o.order_id}"><b>${o.order_id}</b><span class="status">${o.status}</span><small>${o.store_id} · ₹${Number(o.amount).toLocaleString()}</small></div>`).join('');document.querySelectorAll('.order').forEach(x=>x.onclick=async()=>{id.value=x.dataset.id;await story(id.value);window.scrollTo({top:0,behavior:'smooth'})})}async function story(orderId){let d=await fetch('/api/orders/'+encodeURIComponent(orderId)).then(r=>r.json());document.querySelector('#story').innerHTML=d.events.map((e,i)=>`<div class="event"><span class="status">${e.status}</span><p>${i===0?'Order created':'Order updated'} at ${new Date(e.event_time).toLocaleString()}</p><small>${e.store_id} · ₹${Number(e.amount).toLocaleString()}</small></div>`).join('')||'This is a new order.';if(d.current){f.store_id.value=d.current.store_id;f.customer_id.value=d.current.customer_id;f.amount.value=d.current.amount}status.innerHTML=d.allowed_statuses.map(s=>`<option>${s}</option>`).join('');save.disabled=d.complete;if(d.complete)message.innerHTML='<span class="warn">This order is complete. Start a new demo order to continue.</span>';else if(d.current)message.textContent=`Current status: ${d.current.status}. Choose the next valid status.`;return d}id.onchange=()=>story(id.value);document.querySelector('#new').onclick=fresh;f.onsubmit=async e=>{e.preventDefault();let o=Object.fromEntries(new FormData(f));o.amount=Number(o.amount);let r=await fetch('/api/orders',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(o)}),d=await r.json();message.innerHTML=r.ok?`<span class="ok">${d.message}. ${d.quality.bronze_events.toLocaleString()} historical events are safe.</span>`:(d.detail||'Could not save');await story(o.order_id);await recent()};fresh();recent();</script></body></html>'''

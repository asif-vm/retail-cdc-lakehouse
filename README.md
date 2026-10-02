# Retail CDC Lakehouse

[![CI](https://github.com/asif-vm/retail-cdc-lakehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/asif-vm/retail-cdc-lakehouse/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

A zero-cost, reproducible data-engineering project that captures order changes, applies them idempotently and publishes analytics-ready daily marts.

## What is actually implemented

The portable profile generates Debezium-shaped order events and proves ordering, deduplication, replay safety, current-state reconstruction and gold aggregation in DuckDB. The included Docker profile starts PostgreSQL, Redpanda (Kafka-compatible) and MinIO for the scale-up phase. Do not claim Debezium, Spark or Iceberg on a resume until those optional stages are added and run.

## Run the verified portable profile

```bash
python -m venv .venv
.venv/Scripts/activate
pip install -r requirements.txt
python -m src.pipeline
pytest -q
```

Optional infrastructure:

```bash
cp .env.example .env
# Replace the placeholder passwords in .env before starting the services.
docker compose up -d
```

## Architecture

1. Operational orders produce create/update CDC envelopes.
2. Bronze stores immutable events using `event_id` as the idempotency key.
3. Silver selects the latest event per order using deterministic window ordering.
4. Gold aggregates daily status, value and distinct-customer metrics.
5. CI replays the same events twice and proves the result is unchanged.

## Two-week scale-up path

- Days 1-3: load the free Olist data into PostgreSQL and configure Debezium.
- Days 4-6: send CDC through Redpanda and land Bronze objects in MinIO.
- Days 7-9: replace the portable transform with Spark Structured Streaming and Iceberg.
- Days 10-11: add dbt marts, schema tests and freshness checks.
- Days 12-14: benchmark throughput, document failure recovery and record the demo.

## Resume bullets

- Implemented an idempotent CDC pipeline processing **3,232 order events**, using event keys and deterministic windowing to reconstruct **1,000 current orders** with zero duplicate event IDs or invalid statuses.
- Modelled Bronze, Silver and Gold data layers in SQL, publishing daily order-status and gross-value marts with automated contract checks.
- Validated replay recovery through CI by processing the same event stream twice and asserting identical current-state and aggregate outputs.

## Interview questions

1. **Why keep immutable Bronze events?** They provide an audit trail and allow state to be rebuilt after logic changes or failures.
2. **How is replay idempotent?** `event_id` is a primary key and duplicate inserts are ignored before deterministic latest-record selection.
3. **What happens with out-of-order events?** Current state is ordered by source `updated_at`, then event time and event ID as stable tie-breakers.

# kanban-streaming-metrics

Real-time productivity metrics for a **Kanban board**, powered by
**Kafka (Redpanda) + PySpark Structured Streaming**.

Board activity (cards created, moved, completed) is published as a stream of
events; a streaming job computes live metrics — **throughput**, **cycle time**
(`todo → done`), and **work-in-progress** — over time windows.

> 🚧 Built in the open, one stage at a time. Each stage is a small, real step.

## Architecture

```
 event producer            Redpanda            PySpark
 ┌───────────────┐        ┌──────────┐        ┌────────────────────┐
 │ card_created  │  ───▶  │ board.   │  ───▶  │ structured         │
 │ card_moved    │        │ events   │        │ streaming          │
 │ card_done     │        └──────────┘        │   │                │
 └───────────────┘                            │   ▼                │
                                              │ windowed metrics   │
                                              │  (Parquet sink)    │
                                              └────────────────────┘
```

The producer is a **synthetic generator** — it emits realistic board events so
the whole pipeline runs locally with no external system.

### When is Kafka + Spark actually worth it?

A single personal board produces a trickle of events — at that volume, Kafka and
Spark are **over-engineering**, and a plain consumer (or even a periodic SQL
query) would do. This project builds the streaming architecture anyway, on
purpose, and treats *knowing when the trade-off pays off* as part of the
deliverable:

- **Justified** when events are high-volume, multi-producer, need replay, or feed
  several independent consumers.
- **Not justified** for a low-volume, single-consumer board — document it and use
  something simpler.

That judgement is the point, not the tooling.

## Quickstart (infra)

```bash
docker compose up -d
bash docker/create-topic.sh          # create the board.events topic
# Redpanda Console UI: http://localhost:8080
```

Tear down with `docker compose down -v`.

## Producer

With Redpanda up, publish synthetic board events:

```bash
pip install -r requirements.txt
PYTHONPATH=src python -m kanban_stream.producer --cards 50 --rate 5
```

Each card emits a `created → moved → done` lifecycle with realistic, varying
timestamps. Watch them arrive in the Redpanda Console (`board.events`).

## Consumer

Read the stream with PySpark Structured Streaming (downloads the Kafka connector
on first run):

```bash
PYTHONPATH=src python -m kanban_stream.consumer --bootstrap localhost:9092
```

It parses each JSON event, applies a **10-minute watermark**, and streams
**windowed throughput** (completed cards per hour). By default it prints to the
console; pass `--out` to persist to Parquet with checkpointing:

```bash
PYTHONPATH=src python -m kanban_stream.consumer \
  --bootstrap localhost:9092 --out output/throughput
```

The Parquet sink uses `outputMode("append")` so only **finalized** windows (past
the watermark) are written, and a checkpoint gives exactly-once recovery on
restart. `write_metrics_batch` writes the batch metrics (cycle time, WIP) as
Parquet snapshots.

## Metrics

`aggregations.py` holds the metric transformations, as pure `DataFrame →
DataFrame` functions:

| Metric | Function | Notes |
|---|---|---|
| **Throughput** | `throughput` | Completed cards per time window. Streaming- and batch-safe — this is what the live consumer runs, behind a watermark. |
| **Cycle time** | `cycle_times` | Per card, minutes from `created` to `done`. Analytical (a self-join), so batch-only. |
| **WIP** | `wip_timeline` | Cumulative (entered doing − completed) per window. Uses a window function, so batch-only. |

**Streaming vs analytical (the design call):** windowed counts with a watermark
work fine on the live stream. Cycle time and WIP need a join / ordered window
function, which structured streaming doesn't support in one pass — so they run as
batch queries over the persisted events (next stages), not on the hot path. Being
explicit about *which metric belongs on the stream and which belongs in batch* is
the point.

## Tests

```bash
pytest -q
```

Both the event generator and the Spark parsing (`parse_events`) are tested on
static inputs, so no broker is needed. CI runs them (Java + PySpark) on every
push and PR.

## Roadmap

- [x] **1 — Infra & architecture**: Redpanda via Docker Compose.
- [x] **2 — Event producer**: synthetic Kafka producer + unit tests + CI.
- [x] **3 — Consumer**: PySpark Structured Streaming reads and parses `board.events`.
- [x] **4 — Aggregations**: windowed throughput (streaming, watermarked), cycle time, WIP.
- [x] **5 — Metrics sink**: persist throughput (stream, checkpointed) + cycle time / WIP (batch) to Parquet.
- [ ] **6 — Query layer**: small script/notebook to read and chart the metrics.
- [ ] **7 — Integration tests & write-up**: end-to-end pipeline test and the design trade-offs.

## License

MIT — see [LICENSE](LICENSE).

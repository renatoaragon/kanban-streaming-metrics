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

## Roadmap

- [x] **1 — Infra & architecture** (this commit): Redpanda via Docker Compose.
- [ ] **2 — Event producer**: synthetic Kafka producer emitting board events.
- [ ] **3 — Consumer**: PySpark Structured Streaming reads `board.events`.
- [ ] **4 — Aggregations**: windowed throughput, cycle time, WIP (with watermarks).
- [ ] **5 — Metrics sink**: persist windowed metrics to Parquet.
- [ ] **6 — Query layer**: small script/notebook to read and chart the metrics.
- [ ] **7 — Tests, CI & write-up**: pipeline tests, CI, and the design trade-offs.

## License

MIT — see [LICENSE](LICENSE).

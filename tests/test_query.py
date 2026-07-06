from datetime import datetime

from kanban_stream.query import (
    cycle_time_summary,
    latest_wip,
    throughput_total,
    top_slowest,
)


def test_cycle_time_summary(spark):
    cycle = spark.createDataFrame(
        [("C1", 60.0), ("C2", 120.0), ("C3", 240.0)],
        ["card_id", "cycle_minutes"],
    )
    s = cycle_time_summary(cycle)
    assert s["cards"] == 3
    assert s["avg_minutes"] == 140.0
    assert s["max_minutes"] == 240.0


def test_top_slowest_orders_desc(spark):
    cycle = spark.createDataFrame(
        [("C1", 60.0), ("C2", 300.0), ("C3", 120.0)],
        ["card_id", "cycle_minutes"],
    )
    rows = top_slowest(cycle, n=2).collect()
    assert [r["card_id"] for r in rows] == ["C2", "C3"]


def test_throughput_total(spark):
    tp = spark.createDataFrame(
        [(datetime(2025, 1, 1, 9), datetime(2025, 1, 1, 10), 2),
         (datetime(2025, 1, 1, 10), datetime(2025, 1, 1, 11), 3)],
        ["window_start", "window_end", "completed"],
    )
    assert throughput_total(tp) == 5


def test_latest_wip_takes_most_recent_window(spark):
    wip = spark.createDataFrame(
        [(datetime(2025, 1, 1, 9), 2), (datetime(2025, 1, 1, 10), 1)],
        ["window_start", "wip"],
    )
    assert latest_wip(wip) == 1

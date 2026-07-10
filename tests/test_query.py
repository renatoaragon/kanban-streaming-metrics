from datetime import datetime

from kanban_stream.query import (
    cycle_time_summary,
    latest_wip,
    sla_attainment,
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


def test_summary_percentiles_expose_the_skewed_tail(spark):
    # 19 quick cards and one disaster: the average hides the tail, the
    # forecasting percentiles do not.
    rows = [(f"C{i}", 10.0) for i in range(19)] + [("C99", 1000.0)]
    cycle = spark.createDataFrame(rows, ["card_id", "cycle_minutes"])

    s = cycle_time_summary(cycle)
    assert s["median_minutes"] <= s["p85_minutes"] <= s["p95_minutes"] <= s["max_minutes"]
    assert s["p85_minutes"] == 10.0  # most cards really are fast...
    assert s["avg_minutes"] == 59.5  # ...and the average says neither fast nor slow


def test_sla_attainment_is_exact(spark):
    cycle = spark.createDataFrame(
        [("C1", 30.0), ("C2", 60.0), ("C3", 90.0), ("C4", 120.0)],
        ["card_id", "cycle_minutes"],
    )
    assert sla_attainment(cycle, target_minutes=60.0) == 50.0
    assert sla_attainment(cycle, target_minutes=120.0) == 100.0
    assert sla_attainment(cycle, target_minutes=10.0) == 0.0


def test_sla_attainment_empty_frame_is_zero(spark):
    cycle = spark.createDataFrame([], "card_id string, cycle_minutes double")
    assert sla_attainment(cycle, target_minutes=60.0) == 0.0


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

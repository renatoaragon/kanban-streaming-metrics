from datetime import datetime

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kanban_stream.aggregations import aging_wip, cycle_times, throughput, wip_timeline

SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("card_id", StringType()),
        StructField("from_status", StringType()),
        StructField("to_status", StringType()),
        StructField("ts", TimestampType()),
    ]
)


def _events(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def test_throughput_counts_completed_per_window(spark):
    rows = [
        ("a", "card_done", "C1", "doing", "done", datetime(2025, 1, 1, 9, 10)),
        ("b", "card_done", "C2", "doing", "done", datetime(2025, 1, 1, 9, 40)),
        ("c", "card_done", "C3", "doing", "done", datetime(2025, 1, 1, 11, 5)),
        ("d", "card_created", "C4", None, "todo", datetime(2025, 1, 1, 9, 0)),  # ignored
    ]
    out = {
        r["window_start"].hour: r["completed"]
        for r in throughput(_events(spark, rows), "1 hour").collect()
    }
    assert out[9] == 2
    assert out[11] == 1


def test_cycle_times_computes_minutes(spark):
    rows = [
        ("a", "card_created", "C1", None, "todo", datetime(2025, 1, 1, 9, 0)),
        ("b", "card_done", "C1", "doing", "done", datetime(2025, 1, 1, 12, 0)),
    ]
    row = cycle_times(_events(spark, rows)).collect()[0]
    assert row["card_id"] == "C1"
    assert row["cycle_minutes"] == 180.0


def test_aging_wip_lists_only_open_cards_by_age(spark):
    rows = [
        # C1: opened early, still open -> the oldest aging card
        ("a", "card_created", "C1", None, "todo", datetime(2025, 1, 1, 9, 0)),
        # C2: opened later, still open
        ("b", "card_created", "C2", None, "todo", datetime(2025, 1, 1, 11, 0)),
        # C3: opened and completed -> excluded (it has a cycle time instead)
        ("c", "card_created", "C3", None, "todo", datetime(2025, 1, 1, 9, 30)),
        ("d", "card_done", "C3", "doing", "done", datetime(2025, 1, 1, 12, 0)),  # also the latest ts
    ]
    result = aging_wip(_events(spark, rows)).collect()

    # Only the two open cards, oldest first.
    assert [r["card_id"] for r in result] == ["C1", "C2"]
    # Age is measured to the latest event in the data (12:00): C1 = 180 min.
    assert result[0]["age_minutes"] == 180.0
    assert result[1]["age_minutes"] == 60.0


def test_aging_wip_empty_when_all_cards_done(spark):
    rows = [
        ("a", "card_created", "C1", None, "todo", datetime(2025, 1, 1, 9, 0)),
        ("b", "card_done", "C1", "doing", "done", datetime(2025, 1, 1, 10, 0)),
    ]
    assert aging_wip(_events(spark, rows)).count() == 0


def test_wip_timeline_is_cumulative(spark):
    rows = [
        # hour 9: two enter doing
        ("a", "card_moved", "C1", "todo", "doing", datetime(2025, 1, 1, 9, 5)),
        ("b", "card_moved", "C2", "todo", "doing", datetime(2025, 1, 1, 9, 30)),
        # hour 10: one done -> WIP drops to 1
        ("c", "card_done", "C1", "doing", "done", datetime(2025, 1, 1, 10, 15)),
    ]
    wip = {r["window_start"].hour: r["wip"] for r in wip_timeline(_events(spark, rows), "1 hour").collect()}
    assert wip[9] == 2
    assert wip[10] == 1

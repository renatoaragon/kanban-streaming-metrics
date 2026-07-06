"""End-to-end test of the transformation chain.

Runs synthetic events through the full logic — generate -> aggregate -> query —
in one pass. This exercises everything except the two external I/O boundaries
(Kafka and Parquet), which are integration concerns, not unit logic.
"""

from dataclasses import asdict
from datetime import datetime

from pyspark.sql import functions as F

from kanban_stream.aggregations import cycle_times, throughput, wip_timeline
from kanban_stream.events import simulate
from kanban_stream.query import cycle_time_summary, latest_wip, throughput_total


def _events_df(spark, events):
    rows = [asdict(e) for e in events]
    return spark.createDataFrame(rows).withColumn("ts", F.to_timestamp("ts"))


def test_end_to_end_pipeline(spark):
    n = 30
    events = simulate(n, datetime(2025, 1, 1), seed=3)
    df = _events_df(spark, events)

    # Every card completes exactly once -> throughput totals to n.
    assert throughput_total(throughput(df)) == n

    # Cycle time is computed for every card and is positive.
    summary = cycle_time_summary(cycle_times(df))
    assert summary["cards"] == n
    assert summary["avg_minutes"] > 0
    assert summary["max_minutes"] >= summary["median_minutes"]

    # With every card eventually done, cumulative WIP nets back to zero.
    assert latest_wip(wip_timeline(df)) == 0

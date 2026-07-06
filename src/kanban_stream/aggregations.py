"""Metric transformations over parsed board events.

These are pure DataFrame -> DataFrame functions so they can be unit-tested on a
static batch. `throughput` also works on a streaming DataFrame (windowed
aggregation); `cycle_times` and `wip_timeline` are analytical (batch) — they use
joins/window functions not supported by structured streaming, and feed the
query layer in a later stage.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def throughput(events: DataFrame, window_duration: str = "1 hour") -> DataFrame:
    """Completed cards per time window (streaming- and batch-safe)."""
    return (
        events.filter(F.col("event_type") == "card_done")
        .groupBy(F.window("ts", window_duration))
        .count()
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("count").alias("completed"),
        )
    )


def cycle_times(events: DataFrame) -> DataFrame:
    """Per-card cycle time in minutes, from creation to completion."""
    created = events.filter(F.col("event_type") == "card_created").select(
        "card_id", F.col("ts").alias("created_at")
    )
    done = events.filter(F.col("event_type") == "card_done").select(
        "card_id", F.col("ts").alias("done_at")
    )
    return created.join(done, "card_id").withColumn(
        "cycle_minutes",
        (F.unix_timestamp("done_at") - F.unix_timestamp("created_at")) / 60.0,
    )


def wip_timeline(events: DataFrame, window_duration: str = "1 hour") -> DataFrame:
    """Work-in-progress per window: cumulative (entered doing - completed)."""
    entered = (
        events.filter(F.col("to_status") == "doing")
        .groupBy(F.window("ts", window_duration))
        .count()
        .select(F.col("window.start").alias("window_start"), F.col("count").alias("entered"))
    )
    completed = (
        events.filter(F.col("to_status") == "done")
        .groupBy(F.window("ts", window_duration))
        .count()
        .select(F.col("window.start").alias("window_start"), F.col("count").alias("completed"))
    )

    joined = (
        entered.join(completed, "window_start", "full_outer")
        .na.fill(0, ["entered", "completed"])
        .withColumn("net", F.col("entered") - F.col("completed"))
    )

    running = Window.orderBy("window_start").rowsBetween(
        Window.unboundedPreceding, Window.currentRow
    )
    return joined.withColumn("wip", F.sum("net").over(running)).orderBy("window_start")

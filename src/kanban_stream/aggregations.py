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


def aging_wip(events: DataFrame) -> DataFrame:
    """Cards still open and how long they have been open, oldest first.

    Cycle time only sees *completed* cards, which is survivorship bias: the
    cards stuck the longest are exactly the ones it never reports. Aging WIP is
    the mirror image -- cards created but not yet done -- so a card that is
    aging can be acted on before it turns into a bad cycle time. Age is measured
    to the latest event in the data (the "as of" now), joined in so the function
    stays a pure DataFrame transform with no driver-side clock.
    """
    as_of = events.select(F.max("ts").alias("as_of"))
    created = events.filter(F.col("event_type") == "card_created").select(
        "card_id", F.col("ts").alias("created_at")
    )
    done = events.filter(F.col("event_type") == "card_done").select("card_id").distinct()

    open_cards = created.join(done, "card_id", "left_anti")
    return (
        open_cards.crossJoin(as_of)
        .withColumn(
            "age_minutes",
            (F.unix_timestamp("as_of") - F.unix_timestamp("created_at")) / 60.0,
        )
        .select("card_id", "created_at", "as_of", "age_minutes")
        .orderBy(F.col("age_minutes").desc())
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

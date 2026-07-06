"""Persist metrics to Parquet.

Two paths, matching the streaming/analytical split from `aggregations`:
- the live windowed throughput is written from the stream (append + checkpoint);
- cycle time and WIP are written as batch snapshots over the event history.
"""

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from kanban_stream.aggregations import cycle_times, wip_timeline


def write_throughput_stream(
    metrics: DataFrame, out_path: str, checkpoint: str
) -> StreamingQuery:
    """Append finalized throughput windows to Parquet (exactly-once via checkpoint)."""
    return (
        metrics.writeStream.format("parquet")
        .option("path", out_path)
        .option("checkpointLocation", checkpoint)
        .outputMode("append")
        .start()
    )


def write_metrics_batch(events: DataFrame, out_dir: str) -> None:
    """Write cycle-time and WIP snapshots as Parquet under out_dir."""
    cycle_times(events).write.mode("overwrite").parquet(f"{out_dir}/cycle_times")
    wip_timeline(events).write.mode("overwrite").parquet(f"{out_dir}/wip")

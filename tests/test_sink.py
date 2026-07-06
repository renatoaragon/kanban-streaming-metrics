from datetime import datetime

from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from kanban_stream.sink import write_metrics_batch

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


def test_write_metrics_batch_produces_parquet(spark, tmp_path):
    rows = [
        ("a", "card_created", "C1", None, "todo", datetime(2025, 1, 1, 9, 0)),
        ("b", "card_moved", "C1", "todo", "doing", datetime(2025, 1, 1, 9, 30)),
        ("c", "card_done", "C1", "doing", "done", datetime(2025, 1, 1, 12, 0)),
    ]
    events = spark.createDataFrame(rows, SCHEMA)
    out_dir = str(tmp_path / "metrics")

    write_metrics_batch(events, out_dir)

    cycle = spark.read.parquet(f"{out_dir}/cycle_times")
    assert cycle.count() == 1
    assert cycle.collect()[0]["cycle_minutes"] == 180.0

    wip = spark.read.parquet(f"{out_dir}/wip")
    assert wip.count() >= 1

"""Query layer: read the persisted metrics and summarize them.

The summary functions are pure (`DataFrame -> value`), so they're unit-tested on
inline DataFrames with no Parquet I/O. `main` wires them to the Parquet outputs
produced by the sink.
"""

import argparse

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from kanban_stream.consumer import build_spark


def cycle_time_summary(cycle: DataFrame) -> dict:
    """Count, mean, median and max of card cycle time (minutes)."""
    row = cycle.agg(
        F.count("*").alias("cards"),
        F.round(F.avg("cycle_minutes"), 1).alias("avg_minutes"),
        F.round(F.expr("percentile_approx(cycle_minutes, 0.5)"), 1).alias("median_minutes"),
        F.round(F.max("cycle_minutes"), 1).alias("max_minutes"),
    ).collect()[0]
    return row.asDict()


def top_slowest(cycle: DataFrame, n: int = 5) -> DataFrame:
    return (
        cycle.orderBy(F.col("cycle_minutes").desc())
        .limit(n)
        .select("card_id", "cycle_minutes")
    )


def throughput_total(throughput: DataFrame) -> int:
    row = throughput.agg(F.sum("completed").alias("total")).collect()[0]
    return int(row["total"] or 0)


def latest_wip(wip: DataFrame) -> int:
    rows = wip.orderBy(F.col("window_start").desc()).limit(1).collect()
    return int(rows[0]["wip"]) if rows else 0


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize persisted metrics.")
    parser.add_argument("--metrics-dir", required=True, help="dir with cycle_times/ and wip/")
    parser.add_argument("--throughput", required=True, help="throughput Parquet path")
    args = parser.parse_args()

    spark = build_spark("kanban-query")
    spark.sparkContext.setLogLevel("WARN")

    cycle = spark.read.parquet(f"{args.metrics_dir}/cycle_times")
    wip = spark.read.parquet(f"{args.metrics_dir}/wip")
    throughput = spark.read.parquet(args.throughput)

    print("Cycle time (minutes):", cycle_time_summary(cycle))
    print(f"Total completed: {throughput_total(throughput)}")
    print(f"Current WIP: {latest_wip(wip)}")
    print("\nSlowest cards:")
    top_slowest(cycle).show(truncate=False)


if __name__ == "__main__":
    main()

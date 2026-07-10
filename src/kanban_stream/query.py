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
    """Count, mean, median, forecasting percentiles and max of cycle time (minutes).

    p85/p95 are the numbers a kanban team actually commits with: cycle-time
    distributions are right-skewed, so the average understates how long work
    really takes. "85% of cards finish within X minutes" is a forecast; the
    average is not.
    """
    row = cycle.agg(
        F.count("*").alias("cards"),
        F.round(F.avg("cycle_minutes"), 1).alias("avg_minutes"),
        F.round(F.expr("percentile_approx(cycle_minutes, 0.5)"), 1).alias("median_minutes"),
        F.round(F.expr("percentile_approx(cycle_minutes, 0.85)"), 1).alias("p85_minutes"),
        F.round(F.expr("percentile_approx(cycle_minutes, 0.95)"), 1).alias("p95_minutes"),
        F.round(F.max("cycle_minutes"), 1).alias("max_minutes"),
    ).collect()[0]
    return row.asDict()


def sla_attainment(cycle: DataFrame, target_minutes: float) -> float:
    """Share of cards (0-100) completed within a target cycle time.

    The inverse question of the percentiles: instead of "how long do 85% take",
    "how many made the promise we already gave". Returns 0.0 on an empty frame.
    """
    row = cycle.agg(
        F.count("*").alias("total"),
        F.count(F.when(F.col("cycle_minutes") <= target_minutes, 1)).alias("within"),
    ).collect()[0]
    if not row["total"]:
        return 0.0
    return round(100.0 * row["within"] / row["total"], 1)


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
    parser.add_argument(
        "--sla-minutes",
        type=float,
        default=None,
        help="optional cycle-time target; prints the share of cards within it",
    )
    args = parser.parse_args()

    spark = build_spark("kanban-query")
    spark.sparkContext.setLogLevel("WARN")

    cycle = spark.read.parquet(f"{args.metrics_dir}/cycle_times")
    wip = spark.read.parquet(f"{args.metrics_dir}/wip")
    throughput = spark.read.parquet(args.throughput)

    print("Cycle time (minutes):", cycle_time_summary(cycle))
    if args.sla_minutes is not None:
        pct = sla_attainment(cycle, args.sla_minutes)
        print(f"Within {args.sla_minutes:g} min target: {pct}%")
    print(f"Total completed: {throughput_total(throughput)}")
    print(f"Current WIP: {latest_wip(wip)}")
    print("\nSlowest cards:")
    top_slowest(cycle).show(truncate=False)


if __name__ == "__main__":
    main()

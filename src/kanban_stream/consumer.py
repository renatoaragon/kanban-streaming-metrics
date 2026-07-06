"""PySpark Structured Streaming consumer for the board.events topic.

The Kafka read is the I/O boundary; the parsing of the JSON payload is a pure
DataFrame transformation (`parse_events`) so it can be unit-tested on a static
batch without a running broker.
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

TOPIC = "board.events"
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("card_id", StringType()),
        StructField("from_status", StringType()),
        StructField("to_status", StringType()),
        StructField("ts", StringType()),
    ]
)


def build_spark(app_name: str = "kanban-consumer") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def parse_events(raw: DataFrame) -> DataFrame:
    """Parse a DataFrame with a JSON string `value` column into typed columns."""
    return (
        raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("ts", F.to_timestamp("ts"))
    )


def read_stream(spark: SparkSession, bootstrap: str, topic: str = TOPIC) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )
    return raw.selectExpr("CAST(value AS STRING) AS value")


def run(bootstrap: str, topic: str) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    parsed = parse_events(read_stream(spark, bootstrap, topic))

    query = (
        parsed.writeStream.format("console")
        .option("truncate", "false")
        .outputMode("append")
        .start()
    )
    query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume and print board events.")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default=TOPIC)
    args = parser.parse_args()
    run(args.bootstrap, args.topic)


if __name__ == "__main__":
    main()

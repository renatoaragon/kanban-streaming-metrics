"""Publish synthetic board events to the Kafka topic in real time.

Run against the local Redpanda from docker-compose:

    PYTHONPATH=src python -m kanban_stream.producer --cards 50 --rate 5
"""

import argparse
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from kanban_stream.events import BoardEvent, card_lifecycle

TOPIC = "board.events"


def build_producer(bootstrap: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda e: e.encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        linger_ms=50,
    )


def run(bootstrap: str, n_cards: int, rate: float, seed: int) -> None:
    rng = random.Random(seed)
    producer = build_producer(bootstrap)
    delay = 1.0 / rate if rate > 0 else 0.0

    sent = 0
    try:
        for i in range(1, n_cards + 1):
            now = datetime.now(timezone.utc)
            for event in card_lifecycle(f"CARD-{i:04d}", now, rng):
                producer.send(TOPIC, key=event.card_id, value=event.to_json())
                sent += 1
                if delay:
                    time.sleep(delay)
        producer.flush()
        print(f"Published {sent} events to '{TOPIC}'.")
    finally:
        producer.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish synthetic board events.")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--cards", type=int, default=50)
    parser.add_argument("--rate", type=float, default=5.0, help="events per second")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    run(args.bootstrap, args.cards, args.rate, args.seed)


if __name__ == "__main__":
    main()

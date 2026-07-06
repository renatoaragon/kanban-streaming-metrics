"""Board event model and a synthetic generator — pure, no Kafka, no wall clock.

Timestamps are injected so the generator is deterministic and unit-testable.
The producer supplies real time at runtime.
"""

import json
import random
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta

STATUSES = ["todo", "doing", "done"]
EVENT_TYPES = ["card_created", "card_moved", "card_done"]


@dataclass(frozen=True)
class BoardEvent:
    event_id: str
    event_type: str
    card_id: str
    from_status: str | None
    to_status: str
    ts: str  # ISO-8601

    def to_json(self) -> str:
        return json.dumps(asdict(self))


def card_lifecycle(
    card_id: str,
    created_at: datetime,
    rng: random.Random,
) -> list[BoardEvent]:
    """One card's journey: created (todo) -> moved (doing) -> done.

    Each step lands some random minutes after the previous one, so cycle times
    vary realistically across cards.
    """
    t_created = created_at
    t_doing = t_created + timedelta(minutes=rng.randint(1, 120))
    t_done = t_doing + timedelta(minutes=rng.randint(5, 480))

    return [
        BoardEvent(
            event_id=f"{card_id}-1",
            event_type="card_created",
            card_id=card_id,
            from_status=None,
            to_status="todo",
            ts=t_created.isoformat(),
        ),
        BoardEvent(
            event_id=f"{card_id}-2",
            event_type="card_moved",
            card_id=card_id,
            from_status="todo",
            to_status="doing",
            ts=t_doing.isoformat(),
        ),
        BoardEvent(
            event_id=f"{card_id}-3",
            event_type="card_done",
            card_id=card_id,
            from_status="doing",
            to_status="done",
            ts=t_done.isoformat(),
        ),
    ]


def simulate(n_cards: int, start_at: datetime, seed: int = 42) -> list[BoardEvent]:
    """Generate a batch of board events for n_cards, ordered by timestamp."""
    rng = random.Random(seed)
    events: list[BoardEvent] = []
    for i in range(1, n_cards + 1):
        created = start_at + timedelta(minutes=rng.randint(0, 60 * 24))
        events.extend(card_lifecycle(f"CARD-{i:04d}", created, rng))
    events.sort(key=lambda e: e.ts)
    return events

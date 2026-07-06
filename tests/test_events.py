import json
import random
from datetime import datetime

from kanban_stream.events import (
    EVENT_TYPES,
    STATUSES,
    BoardEvent,
    card_lifecycle,
    simulate,
)


def test_lifecycle_has_three_ordered_stages():
    events = card_lifecycle("CARD-1", datetime(2025, 1, 1, 9, 0), random.Random(1))
    assert [e.event_type for e in events] == [
        "card_created",
        "card_moved",
        "card_done",
    ]
    # Status transitions are coherent.
    assert (events[0].from_status, events[0].to_status) == (None, "todo")
    assert (events[1].from_status, events[1].to_status) == ("todo", "doing")
    assert (events[2].from_status, events[2].to_status) == ("doing", "done")
    # Timestamps strictly increase.
    ts = [e.ts for e in events]
    assert ts == sorted(ts)


def test_all_event_types_and_statuses_are_known():
    events = simulate(20, datetime(2025, 1, 1), seed=7)
    assert {e.event_type for e in events} <= set(EVENT_TYPES)
    assert {e.to_status for e in events} <= set(STATUSES)


def test_simulate_is_deterministic_and_sorted():
    a = simulate(10, datetime(2025, 1, 1), seed=42)
    b = simulate(10, datetime(2025, 1, 1), seed=42)
    assert [e.event_id for e in a] == [e.event_id for e in b]
    assert [e.ts for e in a] == sorted(e.ts for e in a)


def test_event_json_roundtrip():
    e = BoardEvent("id-1", "card_created", "CARD-1", None, "todo", "2025-01-01T09:00:00")
    data = json.loads(e.to_json())
    assert data["card_id"] == "CARD-1"
    assert data["to_status"] == "todo"

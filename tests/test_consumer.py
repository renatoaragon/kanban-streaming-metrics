import datetime

from kanban_stream.consumer import parse_events


def test_parse_events_extracts_typed_columns(spark):
    payload = (
        '{"event_id":"CARD-1-1","event_type":"card_created","card_id":"CARD-1",'
        '"from_status":null,"to_status":"todo","ts":"2025-01-01T09:00:00"}'
    )
    raw = spark.createDataFrame([(payload,)], ["value"])

    row = parse_events(raw).collect()[0]
    assert row["event_id"] == "CARD-1-1"
    assert row["event_type"] == "card_created"
    assert row["card_id"] == "CARD-1"
    assert row["from_status"] is None
    assert row["to_status"] == "todo"
    assert row["ts"] == datetime.datetime(2025, 1, 1, 9, 0, 0)


def test_parse_events_handles_multiple_rows(spark):
    payloads = [
        ('{"event_id":"a","event_type":"card_created","card_id":"C1",'
         '"from_status":null,"to_status":"todo","ts":"2025-01-01T09:00:00"}',),
        ('{"event_id":"b","event_type":"card_done","card_id":"C1",'
         '"from_status":"doing","to_status":"done","ts":"2025-01-01T12:00:00"}',),
    ]
    raw = spark.createDataFrame(payloads, ["value"])
    out = {r["event_id"]: r for r in parse_events(raw).collect()}
    assert out["b"]["to_status"] == "done"
    assert out["a"]["ts"].hour == 9

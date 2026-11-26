# SPDX-License-Identifier: Apache-2.0
"""A `type_definitions` answer never exceeds the frame budget
(fleetless/fleetless#1221): the overflow moves to `unresolved`."""

import contextlib
import json
import logging

from test_client_datapoints import FakeRos, _run_one_exchange

import fleetless_bridge.client as client_module
from fleetless_bridge.protocol import fit_type_definitions_message, type_definitions_message


@contextlib.contextmanager
def _client_log():
    """client.py's log lines, by a handler of its own: pytest disables
    propagation for these loggers, so `caplog` would not see them
    (`test_client_live_settings.py` says the same)."""
    records = []

    class _Handler(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = _Handler(level=logging.DEBUG)
    watched = logging.getLogger("fleetless_bridge.client")
    watched.addHandler(handler)
    try:
        yield records
    finally:
        watched.removeHandler(handler)


def _definition(name: str, fields: int) -> dict:
    return {
        "name": name,
        "kind": "msg",
        "fields": [
            {"name": f"field_{i:04d}", "type": "float64", "array": False, "fields": None}
            for i in range(fields)
        ],
    }


def test_an_answer_under_the_budget_is_unchanged():
    definitions = [_definition("pkg/msg/A", 3), _definition("pkg/msg/B", 3)]
    payload, moved = fit_type_definitions_message("r1", definitions, ["pkg/msg/Ghost"], 1_000_000)
    assert moved == []
    assert payload == type_definitions_message("r1", definitions, ["pkg/msg/Ghost"])


def test_the_overflow_moves_to_unresolved_from_the_end():
    definitions = [_definition(f"pkg/msg/T{i}", 40) for i in range(6)]
    one = len(json.dumps(definitions[0]))
    budget = len(type_definitions_message("r2", definitions[:3], [])) + 200
    assert budget < len(type_definitions_message("r2", definitions, []))
    payload, moved = fit_type_definitions_message("r2", definitions, ["pkg/msg/Ghost"], budget)
    answer = json.loads(payload)
    assert len(payload.encode("utf-8")) <= budget
    assert [d["name"] for d in answer["definitions"]] == ["pkg/msg/T0", "pkg/msg/T1", "pkg/msg/T2"]
    assert moved == ["pkg/msg/T3", "pkg/msg/T4", "pkg/msg/T5"]
    assert answer["unresolved"] == ["pkg/msg/Ghost", "pkg/msg/T3", "pkg/msg/T4", "pkg/msg/T5"]
    assert one > 200


def test_the_cut_keeps_as_many_as_fit():
    definitions = [_definition(f"pkg/msg/T{i}", 10 + i) for i in range(8)]
    for budget in range(400, len(type_definitions_message("r3", definitions, [])) + 2, 97):
        payload, moved = fit_type_definitions_message("r3", definitions, [], budget)
        answer = json.loads(payload)
        kept = len(answer["definitions"])
        if kept:
            assert len(payload.encode("utf-8")) <= budget
        # One more would not have fitted.
        if moved:
            bigger = type_definitions_message("r3", definitions[: kept + 1], moved[1:])
            assert len(bigger.encode("utf-8")) > budget
        assert [d["name"] for d in answer["definitions"]] + moved == [d["name"] for d in definitions]


def test_a_definition_that_does_not_fit_alone_is_unresolved():
    payload, moved = fit_type_definitions_message("r4", [_definition("pkg/msg/Huge", 500)], [], 2_000)
    assert moved == ["pkg/msg/Huge"]
    assert json.loads(payload) == {
        "type": "type_definitions",
        "request_id": "r4",
        "definitions": [],
        "unresolved": ["pkg/msg/Huge"],
    }


def test_the_client_answers_under_the_budget_and_says_what_it_moved(monkeypatch):
    definitions = [_definition("pkg/msg/Small", 2), _definition("pkg/msg/Huge", 400)]
    budget = len(type_definitions_message("req-9", definitions[:1], ["pkg/msg/Huge"])) + 10
    monkeypatch.setattr(client_module, "TYPE_DEFINITIONS_MAX_BYTES", budget)
    fake_ros = FakeRos(resolve=(definitions, []))

    async def send_and_recv(session):
        await session.send_type_request("req-9", ["pkg/msg/Small", "pkg/msg/Huge"])
        return await session.recv_type_definitions()

    with _client_log() as records:
        payload = _run_one_exchange(send_and_recv, ros=fake_ros)
    assert [d["name"] for d in payload["definitions"]] == ["pkg/msg/Small"]
    assert payload["unresolved"] == ["pkg/msg/Huge"]
    warnings = [
        r.getMessage()
        for r in records
        if r.levelno == logging.WARNING and "type_definitions for request" in r.getMessage()
    ]
    assert warnings == [
        f"type_definitions for request req-9: 1 definitions moved to unresolved to stay under {budget} bytes: pkg/msg/Huge"
    ]

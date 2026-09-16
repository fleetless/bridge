# SPDX-License-Identifier: Apache-2.0
"""ROS graph snapshots and type field trees, against a real spinning node."""
import time

from example_interfaces.action import Fibonacci
from conftest import wait_until
from rclpy.action import ActionServer
from sensor_msgs.msg import BatteryState
from std_srvs.srv import Trigger

import fleetless_bridge.introspection as introspection
from fleetless_bridge.introspection import graph_snapshot, resolve_types

ACTION_TYPE = "example_interfaces/action/Fibonacci"
SERVICE_TYPE = "std_srvs/srv/Trigger"


# --- graph_snapshot -------------------------------------------------------------


def test_graph_snapshot_reports_a_real_topic(spun_node):
    spun_node.create_publisher(BatteryState, "/battery", 10)
    graph = graph_snapshot(spun_node)
    topics = {t["name"]: t["types"] for t in graph["topics"]}
    assert topics["/battery"] == ["sensor_msgs/msg/BatteryState"]


def test_graph_snapshot_reports_a_real_service(spun_node):
    spun_node.create_service(Trigger, "/do_it", lambda req, resp: resp)
    graph = graph_snapshot(spun_node)
    services = {s["name"]: s["types"] for s in graph["services"]}
    assert services["/do_it"] == ["std_srvs/srv/Trigger"]


def test_graph_snapshot_reports_an_action_and_hides_its_backing_topics_and_services(
    spun_node,
):
    def execute(goal_handle):
        goal_handle.succeed()
        return Fibonacci.Result()

    ActionServer(spun_node, Fibonacci, "count", execute)
    wait_until(lambda: any(a["name"] == "/count" for a in graph_snapshot(spun_node)["actions"]))

    graph = graph_snapshot(spun_node)
    actions = {a["name"]: a["types"] for a in graph["actions"]}
    assert actions["/count"] == ["example_interfaces/action/Fibonacci"]
    # Backing topics/services also exist on the real graph but must not double
    # as a plain topic or service — the three buckets partition it, not overlap.
    assert all("/count/_action/" not in t["name"] for t in graph["topics"])
    assert all("/count/_action/" not in s["name"] for s in graph["services"])


def test_captured_at_ms_is_a_recent_wall_clock_reading(spun_node):
    before = int(time.time() * 1000)
    graph = graph_snapshot(spun_node)
    after = int(time.time() * 1000)
    assert before <= graph["captured_at_ms"] <= after


# --- resolve_types ----------------------------------------------------------------


def test_resolve_types_returns_a_field_tree_for_a_known_message_type():
    definitions, unresolved = resolve_types(["sensor_msgs/msg/BatteryState"])
    assert unresolved == []
    assert definitions[0]["name"] == "sensor_msgs/msg/BatteryState"
    assert definitions[0]["kind"] == "msg"
    percentage = next(f for f in definitions[0]["fields"] if f["name"] == "percentage")
    assert percentage == {"name": "percentage", "type": "float", "array": False, "fields": None}


def test_resolve_types_expands_nested_message_fields():
    definitions, _ = resolve_types(["geometry_msgs/msg/PoseStamped"])
    fields = {f["name"]: f for f in definitions[0]["fields"]}
    assert fields["pose"]["type"] == "geometry_msgs/msg/Pose"
    assert fields["pose"]["array"] is False
    assert fields["pose"]["fields"] is not None  # expanded, not a truncation
    nested = {f["name"]: f for f in fields["pose"]["fields"]}
    assert nested["position"]["type"] == "geometry_msgs/msg/Point"


def test_resolve_types_resolves_a_service_into_request_and_response_trees():
    definitions, unresolved = resolve_types([SERVICE_TYPE])
    assert unresolved == []
    definition = definitions[0]
    assert definition["name"] == SERVICE_TYPE
    assert definition["kind"] == "srv"
    # std_srvs/srv/Trigger.Request has no fields at all — an empty tree, not
    # a missing key or a resolution failure.
    assert definition["request"] == []
    response = {f["name"]: f for f in definition["response"]}
    assert response["success"] == {"name": "success", "type": "boolean", "array": False, "fields": None}
    assert response["message"]["type"] == "string"


def test_resolve_types_resolves_an_action_into_goal_result_feedback_trees():
    definitions, unresolved = resolve_types([ACTION_TYPE])
    assert unresolved == []
    definition = definitions[0]
    assert definition["name"] == ACTION_TYPE
    assert definition["kind"] == "action"
    goal = {f["name"]: f for f in definition["goal"]}
    assert goal["order"] == {"name": "order", "type": "int32", "array": False, "fields": None}
    result = {f["name"]: f for f in definition["result"]}
    assert result["sequence"]["array"] is True
    feedback = {f["name"]: f for f in definition["feedback"]}
    # `sequence`, not `partial_sequence`: example_interfaces' Fibonacci names the
    # feedback array `sequence`; action_tutorials_interfaces' calls it
    # `partial_sequence`. This package depends on example_interfaces because
    # `sequence` is the name that holds across all three supported distributions.
    assert feedback["sequence"]["array"] is True


def test_a_malformed_type_name_is_unresolved_not_a_crash():
    definitions, unresolved = resolve_types(["not-a-three-part-name"])
    assert definitions == []
    assert unresolved == ["not-a-three-part-name"]


def test_an_unknown_kind_segment_is_unresolved():
    definitions, unresolved = resolve_types(["some_pkg/bogus_kind/Thing"])
    assert definitions == []
    assert unresolved == ["some_pkg/bogus_kind/Thing"]


def test_an_unresolvable_type_name_is_reported_not_raised():
    definitions, unresolved = resolve_types(["nonexistent_pkg/msg/Ghost"])
    assert definitions == []
    assert unresolved == ["nonexistent_pkg/msg/Ghost"]


def test_an_unresolvable_service_and_action_are_reported_not_raised():
    definitions, unresolved = resolve_types(
        ["nonexistent_pkg/srv/Ghost", "nonexistent_pkg/action/Ghost"]
    )
    assert definitions == []
    assert set(unresolved) == {"nonexistent_pkg/srv/Ghost", "nonexistent_pkg/action/Ghost"}


def test_one_bad_name_does_not_cost_the_others_in_the_same_batch():
    definitions, unresolved = resolve_types(
        ["sensor_msgs/msg/BatteryState", "nonexistent_pkg/msg/Ghost"]
    )
    assert [d["name"] for d in definitions] == ["sensor_msgs/msg/BatteryState"]
    assert unresolved == ["nonexistent_pkg/msg/Ghost"]


def test_a_field_that_fails_to_resolve_mid_tree_puts_the_whole_definition_in_unresolved(
    monkeypatch,
):
    """A field can fail to import even when the top-level message imports
    fine — e.g. a package not sourced in this workspace. `_build_fields`
    recurses via its own unguarded `get_message()` per nested field; only
    `resolve_types`'s outer try/except catches it. The whole definition must
    land in `unresolved`, not a half-built tree or an escaped exception."""

    class Broken:
        def get_fields_and_field_types(self):
            return {"weird_field": "some_pkg/DoesNotExist"}

    class TopLevel:
        def get_fields_and_field_types(self):
            return {"nested": "some_pkg/Broken"}

    def fake_get_message(type_name):
        if type_name == "top_pkg/msg/TopLevel":
            return TopLevel()
        if type_name == "some_pkg/msg/Broken":
            return Broken()
        raise ModuleNotFoundError("no module for {!r}".format(type_name))

    monkeypatch.setattr(introspection, "get_message", fake_get_message)

    definitions, unresolved = introspection.resolve_types(["top_pkg/msg/TopLevel"])
    assert definitions == []
    assert unresolved == ["top_pkg/msg/TopLevel"]


def test_a_mid_tree_failure_does_not_cost_other_names_in_the_same_batch(monkeypatch):
    class Broken:
        def get_fields_and_field_types(self):
            return {"weird_field": "some_pkg/DoesNotExist"}

    real_get_message = introspection.get_message

    def fake_get_message(type_name):
        if type_name == "some_pkg/msg/Broken":
            return Broken()
        if type_name == "some_pkg/DoesNotExist" or type_name == "some_pkg/msg/DoesNotExist":
            raise ModuleNotFoundError(type_name)
        return real_get_message(type_name)

    monkeypatch.setattr(introspection, "get_message", fake_get_message)

    definitions, unresolved = introspection.resolve_types(
        ["some_pkg/msg/Broken", "sensor_msgs/msg/BatteryState"]
    )
    assert [d["name"] for d in definitions] == ["sensor_msgs/msg/BatteryState"]
    assert unresolved == ["some_pkg/msg/Broken"]


def test_a_field_tree_deeper_than_the_cap_is_truncated_as_a_message_not_a_primitive(
    monkeypatch,
):
    class InfiniteChain:
        """A message type that nests itself forever — real ROS types bottom
        out long before the cap, so the cap itself needs a synthetic type
        that would recurse without end if the cap did not stop it."""

        def get_fields_and_field_types(self):
            return {"next": "fake_pkg/Next"}

    monkeypatch.setattr(introspection, "get_message", lambda name: InfiniteChain())

    fields = introspection._build_fields(InfiniteChain(), depth=0)
    node = fields[0]
    depth = 1
    while node["fields"]:  # a non-empty list means "expanded, one level further"
        assert depth <= introspection.MAX_TYPE_DEPTH
        node = node["fields"][0]
        depth += 1
    # The cut is fields:[] (still a message, just unexpanded) — fields:null
    # is reserved for a genuine primitive, per the contract.
    assert node["fields"] == []
    assert depth <= introspection.MAX_TYPE_DEPTH

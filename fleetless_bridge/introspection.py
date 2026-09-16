# SPDX-License-Identifier: Apache-2.0
"""ROS graph snapshots and message type field trees.

Every function takes the `rclpy.node.Node` as a plain argument and does a
synchronous, one-shot read. Callers run it on the executor thread (see
ros_runtime.py's work queue), where graph queries are cheap — so this
module has no async machinery at all.
"""
from __future__ import annotations

import logging
import time
from typing import Dict, List, Sequence, Tuple

from rosidl_runtime_py.utilities import get_action, get_message, get_service

from fleetless_bridge.ros_types import classify, full_type_name, is_message_ref

log = logging.getLogger(__name__)

# A field tree is not expanded past this many nested-message levels; the
# bridge protocol says why (recursion depth cap 10) and how a cut is spelled:
# `fields: []` (still a message, just not expanded further) rather than
# `fields: null` (a real primitive), so a consumer renders a truncated tree
# as a struct, not a scalar.
MAX_TYPE_DEPTH = 10

NameAndTypes = Tuple[str, List[str]]


def _now_ms() -> int:
    return int(time.time() * 1000)


def _endpoints(pairs: Sequence[NameAndTypes]) -> List[dict]:
    return [{"name": name, "types": list(types)} for name, types in pairs]


def _action_service_prefixes(action_names: Sequence[str]) -> Tuple[str, ...]:
    """Actions surface as topics (`feedback`, `status`) and services
    (`send_goal`, `cancel_goal`, `get_result`) under `<action>/_action/...`,
    a stable ROS2 convention. Excluding those keeps topics/services/actions
    a partition of the graph, not double-reporting one endpoint as both a
    service and an action."""
    return tuple("{}/_action/".format(name) for name in action_names)


def _discover_actions(node) -> Tuple[List[NameAndTypes], str]:
    """Returns (actions, how). `how` says which discovery path ran, since the
    naming-convention fallback is meant to be rare — worth a log line if it
    ever fires for real."""
    try:
        import rclpy.action

        return list(rclpy.action.get_action_names_and_types(node)), "rclpy.action"
    except Exception:  # noqa: BLE001 - fall back rather than fail the snapshot
        log.warning(
            "rclpy.action.get_action_names_and_types unavailable; falling back "
            "to grouping services by the _action/ naming convention",
            exc_info=True,
        )
        return _actions_by_naming_convention(node.get_service_names_and_types()), "naming-convention fallback"


def _actions_by_naming_convention(services: Sequence[NameAndTypes]) -> List[NameAndTypes]:
    """Groups `<action>/_action/send_goal` etc. back into one action entry.
    Only used if `rclpy.action` itself is missing — the type list is taken
    from `send_goal`'s type, stripping the trailing `_SendGoal`."""
    by_action: Dict[str, List[str]] = {}
    for name, types in services:
        if "/_action/send_goal" not in name:
            continue
        action_name = name.rsplit("/_action/send_goal", 1)[0]
        action_types = [t[: -len("_SendGoal")] for t in types if t.endswith("_SendGoal")]
        by_action.setdefault(action_name, action_types)
    return sorted(by_action.items())


def graph_snapshot(node) -> dict:
    """One `ros graph` snapshot: topics, services, actions, capture time."""
    actions, how = _discover_actions(node)
    log.debug("action discovery via %s", how)
    action_names = [name for name, _ in actions]
    hidden_prefixes = _action_service_prefixes(action_names)

    def not_action_backed(pair: NameAndTypes) -> bool:
        name = pair[0]
        return not any(name.startswith(prefix) for prefix in hidden_prefixes)

    topics = [p for p in node.get_topic_names_and_types() if not_action_backed(p)]
    services = [p for p in node.get_service_names_and_types() if not_action_backed(p)]

    return {
        "topics": _endpoints(topics),
        "services": _endpoints(services),
        "actions": _endpoints(actions),
        "captured_at_ms": _now_ms(),
    }


def resolve_types(type_names: Sequence[str]) -> Tuple[List[dict], List[str]]:
    """Field trees for `type_names` — message, service or action, decided by
    the type name's own middle segment (`pkg/msg|srv|action/Name`).
    `typeDefinition` is a discriminated union on `kind`: `msg` keeps the
    `fields`, `srv` gets `request`/ `response`, `action` gets
    `goal`/`result`/`feedback` — each an independent field tree, since a Goal
    and a Result share nothing.
    `parameterFieldsOf` (contracts) names which one tree a parameter may
    target: goal for actions, request for services, fields for messages —
    the bridge does not choose that, it just resolves all the trees a kind
    has. A name that does not import — wrong package, not sourced in this
    workspace, malformed — goes into `unresolved` instead of failing the
    whole request; one bad name in a batch must not cost the others."""
    definitions: List[dict] = []
    unresolved: List[str] = []
    for type_name in type_names:
        try:
            # Wraps the top-level import and every recursive _build_fields call
            # (each its own get_message()) in one try/except: a mid-tree failure
            # reads the same as the name itself being unresolvable — the right
            # severity, since only this definition failed, not the others.
            definition = _resolve_one_type(type_name)
        except Exception:  # noqa: BLE001 - genuinely "could not resolve this one"
            unresolved.append(type_name)
            continue
        definitions.append(definition)
    return definitions, unresolved


def _resolve_one_type(type_name: str) -> dict:
    parts = type_name.split("/")
    if len(parts) != 3:
        raise ValueError("not a pkg/kind/Name type name: {!r}".format(type_name))
    _, kind, _ = parts

    if kind == "msg":
        message_class = get_message(type_name)
        return {
            "name": type_name,
            "kind": "msg",
            "fields": _build_fields(message_class, depth=0),
        }
    if kind == "srv":
        service_class = get_service(type_name)
        return {
            "name": type_name,
            "kind": "srv",
            "request": _build_fields(service_class.Request, depth=0),
            "response": _build_fields(service_class.Response, depth=0),
        }
    if kind == "action":
        action_class = get_action(type_name)
        return {
            "name": type_name,
            "kind": "action",
            "goal": _build_fields(action_class.Goal, depth=0),
            "result": _build_fields(action_class.Result, depth=0),
            "feedback": _build_fields(action_class.Feedback, depth=0),
        }
    raise ValueError("unknown type-name kind {!r} in {!r}".format(kind, type_name))


def _build_fields(message_class, depth: int) -> List[dict]:
    """Field tree for `message_class`, which itself sits at nesting `depth`
    (the requested type is depth 0)."""
    fields = []
    for name, type_str in message_class.get_fields_and_field_types().items():
        item_type, is_array = classify(type_str)
        if not is_message_ref(item_type):
            fields.append({"name": name, "type": item_type, "array": is_array, "fields": None})
            continue
        full_name = full_type_name(item_type)
        if depth + 1 >= MAX_TYPE_DEPTH:
            # Cut here: still a message (fields:[]), just not expanded.
            fields.append({"name": name, "type": full_name, "array": is_array, "fields": []})
            continue
        nested_class = get_message(full_name)
        fields.append(
            {
                "name": name,
                "type": full_name,
                "array": is_array,
                "fields": _build_fields(nested_class, depth + 1),
            }
        )
    return fields

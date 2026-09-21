# SPDX-License-Identifier: Apache-2.0
"""The wire protocol in isolation: what we send, and how we read what arrives."""
import json
import pathlib
import struct

import pytest
from jsonschema import ValidationError

from fleetless_bridge.protocol import (
    LATEST_BRIDGE_VERSION,
    PROTOCOL_VERSION,
    PROTOCOL_VERSIONS,
    APPLY_ERROR_CODE_FIELD_PATH_INVALID,
    APPLY_ERROR_CODE_UNKNOWN,
    APPLY_ERROR_CODE_WHOLE_KIND_FAILED,
    APPLY_ERROR_KIND_DATAPOINT,
    ASSET_FAILURE_KIND_REFUSED,
    ASSET_FAILURE_KIND_TOO_LARGE,
    ASSET_FAILURE_KIND_UNRESOLVABLE,
    ASSET_FAILURE_KIND_UPLOAD_FAILED,
    ActionConfig,
    ApplyError,
    CameraConfig,
    CloudCameraStart,
    CloudCameraStop,
    CloudCancel,
    CloudInvoke,
    CloudPublish,
    Config,
    DatapointConfig,
    DatapointNumeric,
    FailsafeConfig,
    HelloError,
    HelloOk,
    IntrospectRequest,
    MjpegSource,
    ParameterSpec,
    Ping,
    PublisherConfig,
    RetentionConfig,
    RosSource,
    RtspSource,
    ServiceConfig,
    TypeRequest,
    Unknown,
    V4l2Source,
    config_applied_message,
    datapoint_message,
    hello_message,
    introspect_message,
    job_lost_message,
    job_update_message,
    bridge_asset_progress_message,
    bridge_camera_state_message,
    parse_cloud_message,
    pong_message,
    snapshot_frame,
    type_definitions_message,
)
from schemas import SCHEMA_DIR, validate_frame


def _semver(text):
    return tuple(int(part) for part in text.split("."))


def test_the_protocol_version_comes_from_the_vendored_constants():
    """Not a literal any more. Until 2026-09 the `2` was typed on both sides
    of the wire and `test_contracts_sync.py` was the only thing between them
    and drift; now there is one value, read from the vendored artifact."""
    constants = json.loads(
        (pathlib.Path(__file__).resolve().parents[1] / "fleetless_bridge" / "contracts_constants.json").read_text()
    )
    assert PROTOCOL_VERSION == constants["PROTOCOL_VERSION"]
    assert PROTOCOL_VERSIONS == constants["PROTOCOL_VERSIONS"]
    assert LATEST_BRIDGE_VERSION == constants["LATEST_BRIDGE_VERSION"]


def test_this_bridge_appears_in_the_versions_table():
    """The window says which bridge version first spoke the protocol version
    this package sends. A package older than its own `bridge_from` would be
    claiming a version it predates."""
    from fleetless_bridge import __version__

    entry = next(e for e in PROTOCOL_VERSIONS if e["version"] == PROTOCOL_VERSION)
    assert _semver(entry["bridge_from"]) <= _semver(__version__)


def test_hello_carries_the_token_the_version_and_the_protocol():
    payload = json.loads(hello_message("frt_secret", "1.2.3"))
    validate_frame("bridge-hello", payload)
    assert payload == {
        "type": "hello",
        "protocol_version": PROTOCOL_VERSION,
        "token": "frt_secret",
        "bridge_version": "1.2.3",
        "active_jobs": [],
    }


def test_hello_names_every_job_the_process_still_has_in_memory_with_slug_and_state():
    job_id = "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f"
    payload = json.loads(
        hello_message("frt_secret", "1.2.3", active_jobs=[(job_id, "drive_to", "running")])
    )
    validate_frame("bridge-hello", payload)
    assert payload["active_jobs"] == [{"job_id": job_id, "slug": "drive_to", "state": "running"}]


def test_hello_names_a_terminal_job_with_its_actual_state_not_running():
    # A bridge holding a terminal result reports it as such — pure framing
    # (jobs.py's own suite covers where the triple comes from), so any
    # state string exercises the wire shape equally.
    job_id = "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f"
    payload = json.loads(
        hello_message("frt_secret", "1.2.3", active_jobs=[(job_id, "drive_to", "succeeded")])
    )
    validate_frame("bridge-hello", payload)
    assert payload["active_jobs"] == [{"job_id": job_id, "slug": "drive_to", "state": "succeeded"}]


def test_pong_echoes_the_timestamp_unchanged():
    # A realistic epoch-millisecond value: latency is only meaningful if the
    # timestamp survives the round trip exactly.
    payload = json.loads(pong_message(1754800000123))
    validate_frame("bridge-pong", payload)
    assert payload == {"type": "pong", "ts_ms": 1754800000123}


def test_hello_ok_is_read_as_a_robot_binding():
    message = parse_cloud_message('{"type":"hello_ok","robot_id":"r-42"}')
    assert message == HelloOk(robot_id="r-42")


def test_hello_ok_parses_the_window_fields_and_tolerates_their_absence():
    """Both shapes are on the wire at once: an older cloud sends neither
    object, and nothing about a missing window is an error."""
    bare = parse_cloud_message(json.dumps({"type": "hello_ok", "robot_id": "r"}))
    assert isinstance(bare, HelloOk) and bare.protocol_status is None
    full = parse_cloud_message(json.dumps({
        "type": "hello_ok", "robot_id": "r",
        "protocol": {"status": "deprecated", "sunset_at": "2026-12-20"},
        "bridge": {"latest_version": "3.9.0"},
    }))
    assert full.protocol_status == "deprecated"
    assert full.sunset_at == "2026-12-20"
    assert full.latest_bridge_version == "3.9.0"


def test_a_rejected_token_is_terminal():
    message = parse_cloud_message(
        '{"type":"hello_error","code":"invalid_token","message":"no such robot"}'
    )
    assert message == HelloError(code="invalid_token", message="no such robot")
    assert message.terminal


def test_only_an_invalid_token_is_terminal():
    """`protocol_mismatch` left the terminal set with the version window: the
    refusal is about which package is installed, and a restart that installs a
    newer one ends it without a human."""
    assert HelloError("invalid_token", "x").terminal
    assert not HelloError("protocol_mismatch", "x").terminal


def test_an_unfamiliar_rejection_is_worth_retrying():
    message = parse_cloud_message(
        '{"type":"hello_error","code":"server_busy","message":"try later"}'
    )
    assert not message.terminal


def test_ping_is_read_with_its_timestamp():
    assert parse_cloud_message('{"type":"ping","ts_ms":7}') == Ping(ts_ms=7)


def test_text_frames_may_arrive_as_bytes():
    assert parse_cloud_message(b'{"type":"ping","ts_ms":7}') == Ping(ts_ms=7)


def test_a_message_from_a_newer_cloud_is_ignored_not_fatal():
    message = parse_cloud_message('{"type":"datapoint_request","slug":"speed"}')
    assert isinstance(message, Unknown)


def test_garbage_never_raises():
    for raw in [
        "not json at all",
        "[1, 2, 3]",
        '"a bare string"',
        "",
        b"\xff\xfe",
        None,
        '{"type":"hello_ok"}',
        '{"type":"hello_error","code":"x"}',
        '{"type":"ping"}',
        '{"type":"ping","ts_ms":"soon"}',
        '{"type":"ping","ts_ms":-1}',
        # True is an int in Python; it is not a timestamp.
        '{"type":"ping","ts_ms":true}',
    ]:
        assert isinstance(parse_cloud_message(raw), Unknown), raw


# --- config -----------------------------------------------------------------

# The minimum a `doc` needs to be a document at all. Every section is
# optional; `fleetless` is the format version and is not.
_EMPTY_DOC = {"fleetless": 1}


def _config_frame(version=1, **sections):
    return {"type": "config", "version": version, "doc": dict(_EMPTY_DOC, **sections)}


def _parse_config(version=1, **sections):
    return parse_cloud_message(json.dumps(_config_frame(version, **sections)))


_RAW_DATAPOINT = {
    "topic": "/battery",
    "type": "sensor_msgs/msg/BatteryState",
    "field": "percentage",
    "rate_throttle_hz": 2,
    "numeric": {"scale": 100, "offset": 0, "unit": "%", "decimals": 1},
    "retention": {"enabled": True, "interval_seconds": 300, "max_buffer_values": 500},
}


def test_a_config_with_datapoints_is_parsed_field_by_field():
    frame = _config_frame(datapoints={"battery_percentage": _RAW_DATAPOINT})
    validate_frame("cloud-config", frame)
    message = parse_cloud_message(json.dumps(frame))
    assert message == Config(
        version=1,
        datapoints={
            "battery_percentage": DatapointConfig(
                slug="battery_percentage",
                topic="/battery",
                type="sensor_msgs/msg/BatteryState",
                field="percentage",
                rate_throttle_hz=2.0,
                # `decimals` is display-only and is not carried.
                numeric=DatapointNumeric(scale=100, offset=0, unit="%"),
                retention=RetentionConfig(
                    enabled=True, interval_seconds=300, max_buffer_values=500
                ),
            )
        },
    )


def test_the_mapping_key_is_the_entrys_slug():
    """The slug is not a field of the entry any more — it is the key the
    entry was written under, and it has to arrive on the parsed object or
    nothing downstream can address what it just read."""
    message = _parse_config(datapoints={"cabin_temperature": _RAW_DATAPOINT})
    assert list(message.datapoints) == ["cabin_temperature"]
    assert message.datapoints["cabin_temperature"].slug == "cabin_temperature"


def test_a_datapoint_without_a_retention_key_is_neither_retained_nor_buffered():
    raw = dict(_RAW_DATAPOINT)
    del raw["retention"]
    message = _parse_config(datapoints={"battery_percentage": raw})
    assert message.datapoints["battery_percentage"].retention == RetentionConfig(enabled=False)


def test_retention_without_enabled_is_off_not_an_error():
    """Contracts says absent means off, and why: stored points are
    billed, so a default that turned history on would charge for a
    value nobody asked to keep."""
    raw = dict(_RAW_DATAPOINT, retention={"max_buffer_values": 20})
    message = _parse_config(datapoints={"battery_percentage": raw})
    assert message.datapoints["battery_percentage"].retention == RetentionConfig(
        enabled=False, max_buffer_values=20
    )


def test_retentions_history_interval_is_carried_but_is_not_a_bridge_decision():
    """`interval_seconds` is how often the *cloud* writes to history — its
    own contracts comment says "not how often it is sent". Parsed only so
    the group reads whole; nothing here may act on it."""
    raw = dict(_RAW_DATAPOINT, retention={"enabled": True, "interval_seconds": 60})
    message = _parse_config(datapoints={"battery_percentage": raw})
    assert message.datapoints["battery_percentage"].retention.interval_seconds == 60
    assert message.datapoints["battery_percentage"].retention.max_buffer_values is None


def test_a_datapoint_without_a_numeric_key_gets_an_empty_group():
    raw = dict(_RAW_DATAPOINT)
    del raw["numeric"]
    message = _parse_config(datapoints={"battery_percentage": raw})
    assert message.datapoints["battery_percentage"].numeric == DatapointNumeric()


def test_version_zero_with_an_empty_document_means_nothing_published_yet():
    assert _parse_config(version=0) == Config(version=0)


def test_a_zero_rate_throttle_means_no_throttling_at_all():
    """`0` on the wire means "do not throttle" — `MaxHzPolicy` divides by
    whatever it's handed, so the number must not survive this far. The old
    `rate: {mode, hz}` union couldn't express zero and got the same
    outcome by accident."""
    raw = dict(_RAW_DATAPOINT, rate_throttle_hz=0)
    frame = _config_frame(datapoints={"joint_states": raw})
    validate_frame("cloud-config", frame)
    message = parse_cloud_message(json.dumps(frame))
    assert message.datapoints["joint_states"].rate_throttle_hz is None


def test_an_absent_rate_throttle_means_no_throttling_either():
    raw = dict(_RAW_DATAPOINT)
    del raw["rate_throttle_hz"]
    message = _parse_config(datapoints={"joint_states": raw})
    assert message.datapoints["joint_states"].rate_throttle_hz is None


def test_a_negative_rate_throttle_taints_the_whole_frame():
    raw = dict(_RAW_DATAPOINT, rate_throttle_hz=-1)
    assert isinstance(_parse_config(datapoints={"joint_states": raw}), Unknown)


def test_null_field_means_the_whole_message():
    raw = dict(_RAW_DATAPOINT, field=None)
    message = _parse_config(datapoints={"battery_raw": raw})
    assert message.datapoints["battery_raw"].field is None


def test_a_config_with_one_malformed_datapoint_taints_the_whole_frame():
    broken = dict(_RAW_DATAPOINT, retention="every now and then")
    message = _parse_config(
        datapoints={"battery_percentage": _RAW_DATAPOINT, "battery_broken": broken}
    )
    assert isinstance(message, Unknown)


def test_a_section_key_that_is_not_a_slug_taints_the_whole_frame():
    """The key is the entry's name — the thing a role grant, an invoke and a
    datapoint frame all address it by — so a name the rest of the system
    cannot express is not a document to half-apply."""
    for bad_key in ("Battery", "b", "battery percentage", "battery__level", "1st_battery",
                    "battery_", "b" * 64):
        message = _parse_config(datapoints={bad_key: _RAW_DATAPOINT})
        assert isinstance(message, Unknown), bad_key


def test_the_slug_grammar_is_underscore_separated_not_dash_separated():
    """3.0 changed the separator: legal in 2.x, illegal now, no migration —
    asserted both directions so a regex accepting both would fail here."""
    assert isinstance(_parse_config(datapoints={"battery-percentage": _RAW_DATAPOINT}), Unknown)
    assert isinstance(_parse_config(datapoints={"battery_percentage": _RAW_DATAPOINT}), Config)


def test_a_config_missing_its_version_is_unknown():
    payload = json.dumps({"type": "config", "doc": _EMPTY_DOC})
    assert isinstance(parse_cloud_message(payload), Unknown)


def test_a_doc_without_the_format_version_is_unknown():
    """`fleetless` is the one key `doc` cannot be read without: every section
    is optional, so without it a `doc` of arbitrary JSON would parse as a
    valid configuration of nothing at all."""
    payload = json.dumps({"type": "config", "version": 1, "doc": {"datapoints": {}}})
    assert isinstance(parse_cloud_message(payload), Unknown)


def test_a_doc_declaring_another_format_version_is_unknown():
    payload = json.dumps({"type": "config", "version": 1, "doc": {"fleetless": 2}})
    assert isinstance(parse_cloud_message(payload), Unknown)


# --- config: actions / services / publishers ---------------------------------

_RAW_PARAMETER = {"type": "float32", "min_value": 0, "max_value": 1.5}

_RAW_ACTION = {
    "ros_name": "/drive_to",
    "type": "example/action/DriveTo",
    "message": {"target": {"speed": "${speed}"}},
    "parameters": {"speed": _RAW_PARAMETER},
}

_RAW_SERVICE = {
    "ros_name": "/reset_odom",
    "type": "example/srv/Reset",
}

_RAW_PUBLISHER = {
    "topic": "/cmd_vel",
    "type": "geometry_msgs/msg/Twist",
    "message": {"linear": {"x": "${speed}"}, "angular": {"z": 0}},
    "parameters": {"speed": {"type": "float64", "default": 0.0}},
    "failsafe": {"timeout_ms": 300, "message": {"linear": {"x": 0}, "angular": {"z": 0}}},
    "quiet_timeout_ms": 2000,
}

_RAW_CAMERA = {
    "source": {"kind": "ros", "topic": "/image_raw", "type": "sensor_msgs/msg/Image"},
    "width": 640,
    "height": 480,
    "fps": 15,
    "bitrate_kbps": 2000,
    "snapshot_interval_seconds": 5,
}


def test_a_config_with_all_four_kinds_is_parsed_field_by_field():
    frame = _config_frame(
        actions={"drive_to": _RAW_ACTION},
        services={"reset_odom": _RAW_SERVICE},
        publishers={"drive": _RAW_PUBLISHER},
    )
    validate_frame("cloud-config", frame)
    message = parse_cloud_message(json.dumps(frame))
    assert message.actions == {
        "drive_to": ActionConfig(
            slug="drive_to",
            ros_name="/drive_to",
            type="example/action/DriveTo",
            message={"target": {"speed": "${speed}"}},
            parameters={
                "speed": ParameterSpec(type="float32", min_value=0, max_value=1.5),
            },
        )
    }
    assert message.services == {
        "reset_odom": ServiceConfig(
            slug="reset_odom",
            ros_name="/reset_odom",
            type="example/srv/Reset",
            message={},
            parameters={},
        )
    }
    assert message.publishers == {
        "drive": PublisherConfig(
            slug="drive",
            topic="/cmd_vel",
            type="geometry_msgs/msg/Twist",
            message={"linear": {"x": "${speed}"}, "angular": {"z": 0}},
            parameters={"speed": ParameterSpec(type="float64", default=0.0)},
            failsafe=FailsafeConfig(
                timeout_ms=300, message={"linear": {"x": 0}, "angular": {"z": 0}}
            ),
            quiet_timeout_ms=2000,
        )
    }


def test_an_absent_message_on_a_service_is_an_empty_request():
    """`std_srvs/srv/Trigger` has no request fields, so this is the ordinary
    case, not an edge one — not an error, not "send nothing"."""
    message = _parse_config(services={"reset_odom": _RAW_SERVICE})
    assert message.services["reset_odom"].message == {}


def test_an_absent_message_on_an_action_is_an_empty_goal():
    raw = dict(_RAW_ACTION)
    del raw["message"]
    message = _parse_config(actions={"drive_to": raw})
    assert message.actions["drive_to"].message == {}


def test_an_explicitly_null_message_taints_the_whole_frame():
    """Omission is the format's only spelling of "not set" — contracts
    enforces it with a refinement, invisible in the JSON Schema artifact,
    so validating against the vendored copy won't catch this. The parser
    must."""
    raw = dict(_RAW_ACTION, message=None)
    frame = _config_frame(actions={"drive_to": raw})
    validate_frame("cloud-config", frame)  # the artifact really does allow it
    assert isinstance(parse_cloud_message(json.dumps(frame)), Unknown)


def test_a_message_may_be_a_reference_to_a_shared_template():
    """A bare `${name}` right after `message:` names a shared message.
    What it resolves to is decided at invoke time against a real ROS
    type — the parser just carries it."""
    raw = dict(_RAW_ACTION, message="${stop_body}")
    message = _parse_config(
        messages={"stop_body": {"linear": {"x": 0}}}, actions={"drive_to": raw}
    )
    assert message.actions["drive_to"].message == "${stop_body}"
    assert message.messages == {"stop_body": {"linear": {"x": 0}}}


def test_shared_messages_are_carried_as_opaque_trees():
    frame = _config_frame(messages={"stop_body": {"linear": {"x": 0}, "angular": {"z": 0}}})
    validate_frame("cloud-config", frame)
    message = parse_cloud_message(json.dumps(frame))
    assert message.messages == {"stop_body": {"linear": {"x": 0}, "angular": {"z": 0}}}


def test_a_shared_message_that_is_null_taints_the_whole_frame():
    assert isinstance(_parse_config(messages={"stop_body": None}), Unknown)


def test_a_shared_message_keyed_by_a_non_slug_taints_the_whole_frame():
    assert isinstance(_parse_config(messages={"Stop Body": {}}), Unknown)


def test_a_parameter_keyed_by_a_non_slug_taints_the_whole_frame():
    raw = dict(_RAW_ACTION, parameters={"Speed": _RAW_PARAMETER})
    assert isinstance(_parse_config(actions={"drive_to": raw}), Unknown)


def test_a_parameter_with_no_constraints_is_legal():
    raw = dict(_RAW_ACTION, parameters={"xs": {"type": "float32"}})
    message = _parse_config(actions={"drive_to": raw})
    assert message.actions["drive_to"].parameters["xs"] == ParameterSpec(type="float32")


def test_a_parameter_is_required_exactly_when_it_has_no_default():
    """No `required` field, on the wire or here — a parameter with a
    default can be omitted, one without can't. Two spellings of one fact
    would be two things to keep in agreement."""
    raw = dict(
        _RAW_ACTION,
        parameters={"speed": {"type": "float32"}, "trim": {"type": "float32", "default": 0.5}},
    )
    message = _parse_config(actions={"drive_to": raw})
    parameters = message.actions["drive_to"].parameters
    assert parameters["speed"].default is None
    assert parameters["trim"].default == 0.5


def test_the_full_parameter_declaration_is_carried_even_though_the_cloud_enforces_it():
    """`min_value`/`max_value`/`enum`/`regex` are enforced in the cloud before
    a value ever reaches the robot. The bridge holds the whole declaration
    anyway — carrying it is not re-checking it."""
    raw = dict(
        _RAW_ACTION,
        parameters={
            "mode": {
                "type": "string",
                "enum": ["fast", "slow"],
                "regex": "^[a-z]+$",
                "description": "how briskly",
            }
        },
    )
    message = _parse_config(actions={"drive_to": raw})
    assert message.actions["drive_to"].parameters["mode"] == ParameterSpec(
        type="string",
        enum=("fast", "slow"),
        regex="^[a-z]+$",
        description="how briskly",
    )


def test_a_parameter_without_a_type_taints_the_whole_frame():
    raw = dict(_RAW_ACTION, parameters={"speed": {"min_value": 0}})
    assert isinstance(_parse_config(actions={"drive_to": raw}), Unknown)


def test_sections_default_to_empty_when_absent():
    message = _parse_config()
    assert message.datapoints == {}
    assert message.actions == {}
    assert message.services == {}
    assert message.publishers == {}
    assert message.cameras == {}
    assert message.messages == {}


def test_a_publisher_without_a_message_taints_the_whole_frame():
    """Required here, unlike on an action or a service: a publisher with
    nothing to send is not a publisher."""
    broken = dict(_RAW_PUBLISHER)
    del broken["message"]
    assert isinstance(_parse_config(publishers={"drive": broken}), Unknown)


def test_a_publisher_without_a_failsafe_taints_the_whole_frame():
    broken = dict(_RAW_PUBLISHER)
    del broken["failsafe"]
    assert isinstance(_parse_config(publishers={"drive": broken}), Unknown)


def test_a_failsafe_without_its_own_timeout_taints_the_whole_frame():
    """`timeout_ms` lives inside `failsafe` now — a frame still carrying it
    as a sibling (the 2.x shape) fails here, the intended outcome for a
    format change with no migration."""
    broken = dict(
        _RAW_PUBLISHER,
        timeout_ms=300,
        failsafe={"message": {"linear": {"x": 0}}},
    )
    assert isinstance(_parse_config(publishers={"drive": broken}), Unknown)


def test_a_failsafe_without_a_message_taints_the_whole_frame():
    broken = dict(_RAW_PUBLISHER, failsafe={"timeout_ms": 300})
    assert isinstance(_parse_config(publishers={"drive": broken}), Unknown)


def test_zero_quiet_timeout_is_legal_immediate_takeover():
    raw = dict(_RAW_PUBLISHER, quiet_timeout_ms=0)
    message = _parse_config(publishers={"drive": raw})
    assert message.publishers["drive"].quiet_timeout_ms == 0


def test_a_malformed_action_taints_the_whole_config_frame():
    broken = dict(_RAW_ACTION)
    broken.pop("ros_name")
    assert isinstance(_parse_config(actions={"drive_to": broken}), Unknown)


# --- config: cameras ---------------------------------------------------------


def test_a_config_with_a_camera_is_parsed_field_by_field():
    frame = _config_frame(cameras={"front_cam": _RAW_CAMERA})
    validate_frame("cloud-config", frame)
    message = parse_cloud_message(json.dumps(frame))
    assert message.cameras == {
        "front_cam": CameraConfig(
            slug="front_cam",
            source=RosSource(topic="/image_raw", type="sensor_msgs/msg/Image"),
            width=640,
            height=480,
            fps=15,
            bitrate_kbps=2000,
            snapshot_interval_seconds=5,
        )
    }


def test_the_snapshot_interval_arrives_in_seconds_not_milliseconds():
    """A rename *and* a unit change: the same number used to mean a
    thousand times less. `5` here is five seconds; the old field name
    isn't read at all, so a frame still sending it is missing a required
    field."""
    message = _parse_config(cameras={"front_cam": _RAW_CAMERA})
    assert message.cameras["front_cam"].snapshot_interval_seconds == 5

    old_shape = dict(_RAW_CAMERA)
    del old_shape["snapshot_interval_seconds"]
    old_shape["snapshot_interval_ms"] = 5000
    assert isinstance(_parse_config(cameras={"front_cam": old_shape}), Unknown)


def test_an_rtsp_camera_source_is_parsed_with_transport_and_inline_credentials():
    raw = dict(
        _RAW_CAMERA,
        source={
            "kind": "rtsp",
            "url": "rtsp://cam.local:8554/stream",
            "transport": "udp",
            "credentials": {"username": "admin", "password": "s3cr3t"},
        },
    )
    frame = _config_frame(cameras={"yard": raw})
    validate_frame("cloud-config", frame)
    message = parse_cloud_message(json.dumps(frame))
    assert message.cameras["yard"].source == RtspSource(
        url="rtsp://cam.local:8554/stream",
        transport="udp",
        credentials=("admin", "s3cr3t"),
    )


def test_an_rtsp_camera_source_defaults_transport_and_has_no_credentials_when_absent():
    # The contract's `.default('tcp')` used to publish as required in the
    # generated schema; the vendored copy is fixed, proven here. The parser
    # was always lenient about an omitted frame.
    raw = dict(_RAW_CAMERA, source={"kind": "rtsp", "url": "rtsp://cam.local/stream"})
    frame = _config_frame(cameras={"yard": raw})
    validate_frame("cloud-config", frame)
    message = parse_cloud_message(json.dumps(frame))
    assert message.cameras["yard"].source == RtspSource(
        url="rtsp://cam.local/stream", transport="tcp", credentials=None
    )


def test_an_rtsp_camera_source_with_an_invalid_transport_is_malformed():
    raw = dict(
        _RAW_CAMERA,
        source={"kind": "rtsp", "url": "rtsp://cam.local/stream", "transport": "quic"},
    )
    assert isinstance(_parse_config(cameras={"yard": raw}), Unknown)


def test_an_mjpeg_camera_source_is_parsed_with_inline_credentials():
    raw = dict(
        _RAW_CAMERA,
        source={
            "kind": "mjpeg",
            "url": "http://cam.local:8556/",
            "credentials": {"username": "gate", "password": "pw"},
        },
    )
    message = _parse_config(cameras={"gate": raw})
    assert message.cameras["gate"].source == MjpegSource(
        url="http://cam.local:8556/", credentials=("gate", "pw")
    )


def test_a_half_written_credential_is_carried_rather_than_refused():
    """Both halves are optional in the contract, so `username` with no
    `password` is legal. The missing half becomes empty rather than taking
    the robot off the air over a credential the adapter — with the actual
    camera in front of it — is better placed to fail on."""
    raw = dict(
        _RAW_CAMERA,
        source={
            "kind": "mjpeg",
            "url": "http://cam.local:8556/",
            "credentials": {"username": "gate"},
        },
    )
    message = _parse_config(cameras={"gate": raw})
    assert message.cameras["gate"].source.credentials == ("gate", "")


def test_a_v4l2_camera_source_is_parsed():
    raw = dict(_RAW_CAMERA, source={"kind": "v4l2", "device": "/dev/video0"})
    message = _parse_config(cameras={"dock": raw})
    assert message.cameras["dock"].source == V4l2Source(device="/dev/video0")


def test_a_camera_source_with_an_unknown_kind_taints_the_whole_config_frame():
    raw = dict(_RAW_CAMERA, source={"kind": "onvif", "url": "http://cam.local/"})
    assert isinstance(_parse_config(cameras={"gate": raw}), Unknown)


def test_a_malformed_camera_taints_the_whole_config_frame():
    broken = dict(_RAW_CAMERA)
    del broken["width"]
    assert isinstance(_parse_config(cameras={"front_cam": broken}), Unknown)


def test_a_config_has_no_credentials_member_at_all():
    """The wire-only map beside `doc` retires with the `credentials_ref`
    that named into it: a camera's password now lives inside its own
    source, with no second place to disagree. A cloud still sending the
    old sibling must not make the bridge believe it has a credential —
    there is nowhere for one to land."""
    frame = _config_frame(cameras={"front_cam": _RAW_CAMERA})
    frame["credentials"] = {"yard-nvr": {"username": "admin", "password": "s3cr3t"}}
    message = parse_cloud_message(json.dumps(frame))
    assert isinstance(message, Config)
    assert not hasattr(message, "credentials")


# --- the format change is a break, and this is what it breaks ----------------

# Pulled verbatim from the dev database (`config_versions`, robot
# `9737a133-00d2-4ddb-81bd-17a2d1c9fd20`, id `8868113f-d7c4-49e0-a2b0-
# bbeda9c72126`, version 18, published 2026-08-11T23:31:41Z). All five
# kinds populated, all four camera sources. Once proof a real document
# stayed parseable across additive changes; now proof 3.0 does not — a
# break asserted against a document someone really published, not one
# written to fail.
_REAL_PRE_FL_002_CONFIG_DOC = {
        'actions': [
            {
                'slug': 'count-up',
                'type': 'example_interfaces/action/Fibonacci',
                'ros_name': '/count',
                'parameters': [
                    {
                        'name': 'order',
                        'rule': {
                            'max': 25,
                            'min': 1,
                            'required': True
                        },
                        'type': 'int32'
                    }
                ]
            }
        ],
        'cameras': [
            {
                'fps': 5,
                'slug': 'front',
                'width': 640,
                'height': 480,
                'source': {
                    'kind': 'ros',
                    'type': 'sensor_msgs/msg/Image',
                    'topic': '/image_raw'
                },
                'bitrate_kbps': 1500,
                'snapshot_interval_ms': 3000
            },
            {
                'fps': 5,
                'slug': 'yard',
                'width': 640,
                'height': 480,
                'source': {
                    'url': 'rtsp://camera.example.test:8554/test',
                    'kind': 'rtsp',
                    'transport': 'tcp',
                    'credentials_ref': 'site-cams'
                },
                'bitrate_kbps': 1500,
                'snapshot_interval_ms': 3000
            }
        ],
        'services': [
            {
                'slug': 'add-slowly',
                'type': 'example_interfaces/srv/AddTwoInts',
                'ros_name': '/add_two_ints',
                'parameters': [
                    {'name': 'a', 'rule': {'required': True}, 'type': 'int64'},
                    {'name': 'b', 'rule': {'required': True}, 'type': 'int64'}
                ]
            }
        ],
        'datapoints': [
            {
                'rate': {'hz': 5, 'mode': 'max_hz'},
                'slug': 'tick-buffered',
                'type': 'sensor_msgs/msg/BatteryState',
                'unit': '%',
                'field': 'percentage',
                'range': {'max': 100, 'min': 0},
                'scale': 100,
                'topic': '/battery',
                'buffer': {'enabled': True, 'max_values': 500},
                'offset': None,
                'retention': True
            }
        ],
        'publishers': [
            {
                'slug': 'drive',
                'type': 'geometry_msgs/msg/Twist',
                'topic': '/cmd_vel',
                'failsafe': {'linear': {'x': 0}, 'angular': {'z': 0}},
                'parameters': [
                    {
                        'name': 'linear.x',
                        'rule': {'max': 1, 'min': -1, 'required': True},
                        'type': 'float64'
                    }
                ],
                'timeout_ms': 1000,
                'quiet_timeout_ms': 3000
            }
        ]
    }


def test_a_pre_fl_002_config_document_is_refused_by_the_vendored_schema():
    frame = {
        "type": "config",
        "version": 18,
        "doc": _REAL_PRE_FL_002_CONFIG_DOC,
        "credentials": {"site-cams": {"username": "camuser", "password": "not-a-real-secret"}},
    }
    with pytest.raises(ValidationError):
        validate_frame("cloud-config", frame)


def test_a_pre_fl_002_config_document_no_longer_parses_at_runtime():
    """The test above is `jsonschema`'s opinion, and `jsonschema` is a test
    instrument, never a wire gate — imported in exactly one place
    (`test/schemas.py`). What decides whether a robot keeps running
    is `parse_cloud_message`, so the break is asserted there too, not
    inferred from the schema agreeing."""
    payload = json.dumps(
        {"type": "config", "version": 18, "doc": _REAL_PRE_FL_002_CONFIG_DOC}
    )
    assert isinstance(parse_cloud_message(payload), Unknown)


# A hand translation of the document above into the 3.0 format. **Not**
# a recorded document, and does not claim to be one — it's good for
# covering all five kinds and all four camera sources in one frame, so a
# field that only ever appears beside others is exercised in company.
_TRANSLATED_CONFIG_DOC = {
    "fleetless": 1,
    "messages": {"stop_body": {"linear": {"x": 0.0}, "angular": {"z": 0.0}}},
    "datapoints": {
        "tick_buffered": {
            "topic": "/battery",
            "type": "sensor_msgs/msg/BatteryState",
            "field": "percentage",
            "rate_throttle_hz": 5,
            "numeric": {"scale": 100, "unit": "%"},
            "retention": {"enabled": True, "max_buffer_values": 500},
        },
        "battery_raw": {"topic": "/battery", "type": "sensor_msgs/msg/BatteryState"},
    },
    "actions": {
        "count_up": {
            "ros_name": "/count",
            "type": "example_interfaces/action/Fibonacci",
            "message": {"order": "${order}"},
            "parameters": {"order": {"type": "int32", "min_value": 1, "max_value": 25}},
        }
    },
    "services": {
        "add_slowly": {
            "ros_name": "/add_two_ints",
            "type": "example_interfaces/srv/AddTwoInts",
            "message": {"a": "${lhs}", "b": "${rhs}"},
            "parameters": {"lhs": {"type": "int64"}, "rhs": {"type": "int64"}},
        },
        "ping_it": {"ros_name": "/ping", "type": "std_srvs/srv/Trigger"},
    },
    "publishers": {
        "drive": {
            "topic": "/cmd_vel",
            "type": "geometry_msgs/msg/Twist",
            "message": {"linear": {"x": "${linear_x}"}, "angular": {"z": 0.0}},
            "parameters": {"linear_x": {"type": "float64", "min_value": -1, "max_value": 1}},
            "failsafe": {"timeout_ms": 1000, "message": "${stop_body}"},
            "quiet_timeout_ms": 3000,
        }
    },
    "cameras": {
        "front": {
            "source": {"kind": "ros", "type": "sensor_msgs/msg/Image", "topic": "/image_raw"},
            "fps": 5,
            "width": 640,
            "height": 480,
            "bitrate_kbps": 1500,
            "snapshot_interval_seconds": 3,
        },
        "yard": {
            "source": {
                "kind": "rtsp",
                "url": "rtsp://camera.example.test:8554/test",
                "transport": "tcp",
                "credentials": {"username": "camuser", "password": "not-a-real-secret"},
            },
            "fps": 5,
            "width": 640,
            "height": 480,
            "bitrate_kbps": 1500,
            "snapshot_interval_seconds": 3,
        },
        "gate": {
            "source": {"kind": "mjpeg", "url": "http://camera.example.test:8556/"},
            "fps": 5,
            "width": 640,
            "height": 480,
            "bitrate_kbps": 1500,
            "snapshot_interval_seconds": 3,
        },
        "dock": {
            "source": {"kind": "v4l2", "device": "/dev/video0"},
            "fps": 5,
            "width": 640,
            "height": 480,
            "bitrate_kbps": 1500,
            "snapshot_interval_seconds": 3,
        },
    },
}


def test_a_whole_fl_002_document_validates_and_parses():
    frame = {"type": "config", "version": 19, "doc": _TRANSLATED_CONFIG_DOC}
    validate_frame("cloud-config", frame)
    message = parse_cloud_message(json.dumps(frame))
    assert isinstance(message, Config)
    assert set(message.datapoints) == {"tick_buffered", "battery_raw"}
    assert set(message.actions) == {"count_up"}
    assert set(message.services) == {"add_slowly", "ping_it"}
    assert set(message.publishers) == {"drive"}
    assert set(message.cameras) == {"front", "yard", "gate", "dock"}
    assert set(message.messages) == {"stop_body"}
    assert {type(c.source).__name__ for c in message.cameras.values()} == {
        "RosSource", "RtspSource", "MjpegSource", "V4l2Source",
    }
    # The failsafe body is a reference; resolving it is the runtime's job.
    assert message.publishers["drive"].failsafe.message == "${stop_body}"


def test_a_config_frame_with_only_the_format_version_still_validates():
    """The guard, in the new format: every section is optional, so an
    org with nothing configured yet sends a `doc` carrying only `fleetless`,
    and it must still validate. The `.default()`-publishes-as-required trap
    has bitten this package four times — this is reading the zod source
    vs. demonstrating the artifact it produced."""
    validate_frame("cloud-config", {"type": "config", "version": 1, "doc": _EMPTY_DOC})


def test_a_hello_frame_without_active_jobs_still_validates():
    """`active_jobs` is `.default([])` on the wire; the vendored schema
    used to publish it as `required` — the same `.default()`-trap the
    test above names, now on the handshake. This bridge always sends the
    key, so the bug was latent here but real for any other
    implementation. Proves the *contract* agrees, not this bridge's own
    output — `strict=False` forces the lenient input-mode copy on purpose
    (`bridge-hello` also has a strict output-mode copy, used everywhere
    else in this file to check `hello_message()`'s real serialization,
    which never omits `active_jobs` and would fail this exact frame
    there)."""
    payload = {
        "type": "hello",
        "protocol_version": PROTOCOL_VERSION,
        "token": "frt_secret",
        "bridge_version": "1.2.3",
    }
    validate_frame("bridge-hello", payload, strict=False)


# --- invoke / cancel / publish -------------------------------------------


def test_invoke_carries_the_cloud_minted_job_id_slug_flat_params_and_patience():
    payload = json.dumps(
        {
            "type": "invoke",
            "job_id": "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
            "slug": "drive_to",
            "params": {"speed": 0.5},
            "patience_ms": 5000,
        }
    )
    assert parse_cloud_message(payload) == CloudInvoke(
        job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
        slug="drive_to",
        params={"speed": 0.5},
        patience_ms=5000,
    )


def test_invoke_without_a_patience_ms_is_unknown():
    # patience_ms is required — by the time the frame is on this
    # socket somebody has already decided; there is no default to fall
    # back to at this layer.
    payload = json.dumps(
        {
            "type": "invoke",
            "job_id": "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
            "slug": "drive_to",
            "params": {},
        }
    )
    assert isinstance(parse_cloud_message(payload), Unknown)


def test_invoke_with_a_non_positive_patience_ms_is_unknown():
    payload = json.dumps(
        {
            "type": "invoke",
            "job_id": "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
            "slug": "drive_to",
            "params": {},
            "patience_ms": 0,
        }
    )
    assert isinstance(parse_cloud_message(payload), Unknown)


def test_invoke_without_a_job_id_is_unknown():
    payload = json.dumps(
        {"type": "invoke", "slug": "drive_to", "params": {}, "patience_ms": 5000}
    )
    assert isinstance(parse_cloud_message(payload), Unknown)


def test_cancel_with_a_null_job_id_means_whatever_is_running():
    payload = json.dumps({"type": "cancel", "slug": "drive_to", "job_id": None})
    assert parse_cloud_message(payload) == CloudCancel(slug="drive_to", job_id=None)


def test_cancel_with_a_job_id_names_which_job():
    payload = json.dumps(
        {"type": "cancel", "slug": "drive_to", "job_id": "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f"}
    )
    assert parse_cloud_message(payload) == CloudCancel(
        slug="drive_to", job_id="3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f"
    )


def test_cancel_without_a_job_id_key_is_unknown():
    # job_id is required-and-nullable: the key itself must be present
    # — an old-shaped frame that omits it entirely is not the same as one
    # that explicitly says null.
    payload = json.dumps({"type": "cancel", "slug": "drive_to"})
    assert isinstance(parse_cloud_message(payload), Unknown)


def test_cancel_with_an_empty_string_job_id_is_unknown():
    payload = json.dumps({"type": "cancel", "slug": "drive_to", "job_id": ""})
    assert isinstance(parse_cloud_message(payload), Unknown)


def test_publish_carries_the_slug_and_flat_message():
    payload = json.dumps(
        {"type": "publish", "slug": "drive", "message": {"linear_x": 0.2, "angular_z": 0.0}}
    )
    assert parse_cloud_message(payload) == CloudPublish(
        slug="drive", message={"linear_x": 0.2, "angular_z": 0.0}
    )


def test_publish_without_a_message_object_is_unknown():
    payload = json.dumps({"type": "publish", "slug": "drive"})
    assert isinstance(parse_cloud_message(payload), Unknown)


# --- camera_start / camera_stop / camera_state -------------------------------


def test_camera_start_carries_slug_url_room_token_and_request_id():
    payload = json.dumps(
        {
            "type": "camera_start",
            "slug": "front_cam",
            "url": "wss://media.fleetless.dev",
            "room": "robot-1-front-cam",
            "token": "eyJhbGciOi...",
            "request_id": "req-1",
        }
    )
    assert parse_cloud_message(payload) == CloudCameraStart(
        slug="front_cam",
        url="wss://media.fleetless.dev",
        room="robot-1-front-cam",
        token="eyJhbGciOi...",
        request_id="req-1",
    )


def test_camera_start_without_a_token_is_unknown():
    payload = json.dumps(
        {
            "type": "camera_start",
            "slug": "front_cam",
            "url": "wss://media.fleetless.dev",
            "room": "robot-1-front-cam",
            "request_id": "req-1",
        }
    )
    assert isinstance(parse_cloud_message(payload), Unknown)


def test_camera_start_without_a_request_id_is_unknown():
    payload = json.dumps(
        {
            "type": "camera_start",
            "slug": "front_cam",
            "url": "wss://media.fleetless.dev",
            "room": "robot-1-front-cam",
            "token": "eyJhbGciOi...",
        }
    )
    assert isinstance(parse_cloud_message(payload), Unknown)


def test_camera_stop_carries_the_slug_and_request_id():
    payload = json.dumps(
        {"type": "camera_stop", "slug": "front_cam", "request_id": "req-2"}
    )
    assert parse_cloud_message(payload) == CloudCameraStop(
        slug="front_cam", request_id="req-2"
    )


def test_camera_stop_without_a_request_id_is_unknown():
    payload = json.dumps({"type": "camera_stop", "slug": "front_cam"})
    assert isinstance(parse_cloud_message(payload), Unknown)


def test_bridge_camera_state_reports_publishing_true_with_no_error():
    payload = json.loads(
        bridge_camera_state_message(
            "front_cam", True, cause="command", observed_at_ms=1786400000000,
            request_id="req-1",
        )
    )
    validate_frame("bridge-camera-state", payload)
    assert payload == {
        "type": "camera_state",
        "slug": "front_cam",
        "publishing": True,
        "error": None,
        "cause": "command",
        "observed_at_ms": 1786400000000,
        "request_id": "req-1",
    }


def test_bridge_camera_state_reports_a_failure_to_start():
    payload = json.loads(
        bridge_camera_state_message(
            "front_cam",
            False,
            error=("live_unavailable", "could not reach the media server"),
            cause="command",
            observed_at_ms=1786400000000,
            request_id="req-1",
        )
    )
    validate_frame("bridge-camera-state", payload)
    assert payload["publishing"] is False
    assert payload["error"] == {
        "code": "live_unavailable",
        "message": "could not reach the media server",
    }
    assert payload["cause"] == "command"
    assert payload["request_id"] == "req-1"


def test_bridge_camera_state_cause_says_why_the_frame_was_sent():
    # {publishing: False, error: None} alone used to mean three
    # different things (a camera_stop answer, a config-change stop, a
    # recovered source) — cause is what tells them apart now.
    payload = json.loads(
        bridge_camera_state_message(
            "front_cam", False, cause="config_change", observed_at_ms=1786400000000,
            request_id=None,
        )
    )
    validate_frame("bridge-camera-state", payload)
    assert payload["cause"] == "config_change"


def test_bridge_camera_state_cause_is_required():
    with pytest.raises(TypeError):
        bridge_camera_state_message(
            "front_cam", True, observed_at_ms=1786400000000, request_id=None
        )


def test_bridge_camera_state_observed_at_ms_is_required():
    # Contracts review: the cloud used to stamp its own receive time,
    # so a restatement dated an old failure to the moment of the restart —
    # this is why the sender, not the receiver, must always say when.
    with pytest.raises(TypeError):
        bridge_camera_state_message("front_cam", True, cause="command", request_id=None)


def test_bridge_camera_state_request_id_is_required_but_may_be_none():
    # required-and-nullable — the keyword itself must be supplied,
    # `None` is a real, distinct answer ("this frame answers no request"),
    # not the same as omitting it.
    with pytest.raises(TypeError):
        bridge_camera_state_message(
            "front_cam", True, cause="command", observed_at_ms=1786400000000
        )


def test_bridge_camera_state_request_id_may_be_null_for_an_unsolicited_frame():
    payload = json.loads(
        bridge_camera_state_message(
            "front_cam", False, cause="source", observed_at_ms=1786400000000, request_id=None
        )
    )
    validate_frame("bridge-camera-state", payload)
    assert payload["request_id"] is None


def test_the_schema_does_not_enforce_the_request_id_cause_pairing():
    """(contracts doc comment, `bridgeCameraState.request_id`): "non-null
    iff cause == 'command'" is a cross-field constraint that doesn't
    survive JSON Schema generation, so a frame with the pairing wrong
    still validates. Recorded decision, not a gap to file as a bug later
    — the pairing is enforced by the cloud, and any bridge-side test of
    the *rule* must check it directly (ros_runtime.py's suite does, via
    the actual construction sites), not lean on `validate_frame` to catch
    a wrong one."""
    # cause='source' (unsolicited) with a non-null request_id: wrong, and
    # the schema accepts it anyway.
    wrong_but_valid = json.loads(
        bridge_camera_state_message(
            "front_cam", False, cause="source", observed_at_ms=1786400000000,
            request_id="should-not-be-here",
        )
    )
    validate_frame("bridge-camera-state", wrong_but_valid)  # does not raise
    # cause='command' with a null request_id: also wrong, also accepted.
    also_wrong_but_valid = json.loads(
        bridge_camera_state_message(
            "front_cam", True, cause="command", observed_at_ms=1786400000000, request_id=None
        )
    )
    validate_frame("bridge-camera-state", also_wrong_but_valid)  # does not raise


# --- introspect_request / type_request ---------------------------------------


def test_an_introspect_request_carries_its_request_id():
    payload = json.dumps({"type": "introspect_request", "request_id": "req-1"})
    assert parse_cloud_message(payload) == IntrospectRequest(request_id="req-1")


def test_a_type_request_carries_its_request_id_and_type_names():
    payload = json.dumps(
        {
            "type": "type_request",
            "request_id": "req-2",
            "type_names": ["sensor_msgs/msg/BatteryState"],
        }
    )
    assert parse_cloud_message(payload) == TypeRequest(
        request_id="req-2", type_names=("sensor_msgs/msg/BatteryState",)
    )


def test_a_type_request_without_any_type_names_is_unknown():
    payload = json.dumps({"type": "type_request", "request_id": "req-3", "type_names": []})
    assert isinstance(parse_cloud_message(payload), Unknown)


# --- what the bridge builds --------------------------------------------------


def test_config_applied_reports_ok_and_per_slug_errors():
    payload = json.loads(
        config_applied_message(
            3, False,
            [ApplyError(
                slug="bad_slug", kind=APPLY_ERROR_KIND_DATAPOINT,
                code=APPLY_ERROR_CODE_UNKNOWN, message="unknown type",
            )],
        )
    )
    validate_frame("bridge-config-applied", payload)
    assert payload == {
        "type": "config_applied",
        "version": 3,
        "ok": False,
        "errors": [
            {"slug": "bad_slug", "kind": "datapoint", "code": "unknown", "message": "unknown type"}
        ],
    }


def test_config_applied_with_no_errors_needs_no_errors_list_argument():
    payload = json.loads(config_applied_message(0, True, []))
    validate_frame("bridge-config-applied", payload)
    assert payload["errors"] == []


def test_introspect_carries_the_request_id_and_graph_untouched():
    graph = {
        "topics": [{"name": "/battery", "types": ["sensor_msgs/msg/BatteryState"]}],
        "services": [],
        "actions": [],
        "captured_at_ms": 1754800000123,
    }
    payload = json.loads(introspect_message("req-1", graph))
    validate_frame("bridge-introspect", payload)
    assert payload == {"type": "introspect", "request_id": "req-1", "graph": graph}


def test_type_definitions_carries_resolved_and_unresolved_names():
    definitions = [
        {
            "name": "sensor_msgs/msg/BatteryState",
            "kind": "msg",
            "fields": [
                {"name": "percentage", "type": "float32", "array": False, "fields": None}
            ],
        }
    ]
    payload = json.loads(
        type_definitions_message("req-2", definitions, ["unknown_pkg/msg/Ghost"])
    )
    validate_frame("bridge-type-definitions", payload)
    assert payload["definitions"] == definitions
    assert payload["unresolved"] == ["unknown_pkg/msg/Ghost"]


def test_type_definitions_accepts_a_service_definition():
    definitions = [
        {
            "name": "example/srv/Reset",
            "kind": "srv",
            "request": [],
            "response": [{"name": "ok", "type": "boolean", "array": False, "fields": None}],
        }
    ]
    payload = json.loads(type_definitions_message("req-3", definitions, []))
    validate_frame("bridge-type-definitions", payload)
    assert payload["definitions"] == definitions


def test_type_definitions_accepts_an_action_definition():
    definitions = [
        {
            "name": "example/action/DriveTo",
            "kind": "action",
            "goal": [{"name": "speed", "type": "float32", "array": False, "fields": None}],
            "result": [],
            "feedback": [
                {"name": "distance", "type": "float32", "array": False, "fields": None}
            ],
        }
    ]
    payload = json.loads(type_definitions_message("req-4", definitions, []))
    validate_frame("bridge-type-definitions", payload)
    assert payload["definitions"] == definitions


def test_datapoint_carries_the_bridge_capture_timestamp():
    payload = json.loads(datapoint_message("battery_percentage", 87.5, 1754800000123))
    validate_frame("datapoint-frame", payload)
    assert payload == {
        "type": "datapoint",
        "slug": "battery_percentage",
        "value": 87.5,
        "timestamp_ms": 1754800000123,
    }


# --- job_update / job_lost ------------------------------------------------


def test_job_update_carries_state_and_the_bridge_capture_timestamp():
    payload = json.loads(
        job_update_message(
            "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
            "drive_to",
            "running",
            feedback={"distance": 2.5},
            progress=0.4,
            timestamp_ms=1786400000000,
        )
    )
    validate_frame("bridge-job-update", payload)
    assert payload == {
        "type": "job_update",
        "job_id": "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
        "slug": "drive_to",
        "state": "running",
        "feedback": {"distance": 2.5},
        "progress": 0.4,
        "result": None,
        "error": None,
        "timestamp_ms": 1786400000000,
    }


def test_job_update_with_no_feedback_progress_or_result_is_still_valid():
    payload = json.loads(
        job_update_message(
            "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
            "drive_to",
            "succeeded",
            result={"ok": True},
            timestamp_ms=1786400000000,
        )
    )
    validate_frame("bridge-job-update", payload)
    assert payload["feedback"] is None
    assert payload["progress"] is None
    assert payload["result"] == {"ok": True}
    assert payload["error"] is None


def test_job_update_carries_a_code_and_message_on_failure():
    payload = json.loads(
        job_update_message(
            "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
            "drive_to",
            "failed",
            error=("goal_rejected", "the action server rejected the goal"),
            timestamp_ms=1786400000000,
        )
    )
    validate_frame("bridge-job-update", payload)
    assert payload["error"] == {
        "code": "goal_rejected",
        "message": "the action server rejected the goal",
    }


def test_job_update_error_with_details_nests_it_inside_the_error_object():
    # a documented payload (job_queue_full's {limit, queued}) must ride inside
    # `error`, not as a sibling field and not folded into the message string.
    payload = json.loads(
        job_update_message(
            "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
            "drive_to",
            "failed",
            error=("job_queue_full", "200 jobs are already queued for report"),
            details={"limit": 200, "queued": 200},
            timestamp_ms=1786400000000,
        )
    )
    validate_frame("bridge-job-update", payload)
    assert payload["error"] == {
        "code": "job_queue_full",
        "message": "200 jobs are already queued for report",
        "details": {"limit": 200, "queued": 200},
    }


def test_job_update_error_without_details_omits_the_key_entirely():
    # `.optional()`, not `.nullable()` on the contract — most job errors
    # have nothing structured to add, and omission is the correct way to
    # say that, not `"details": null`.
    payload = json.loads(
        job_update_message(
            "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
            "drive_to",
            "failed",
            error=("busy", "slug 'drive-to' already has a running job"),
            timestamp_ms=1786400000000,
        )
    )
    validate_frame("bridge-job-update", payload)
    assert "details" not in payload["error"]


def test_job_lost_carries_the_ids_of_jobs_no_longer_accounted_for():
    payload = json.loads(
        job_lost_message(["3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f"])
    )
    validate_frame("bridge-job-lost", payload)
    assert payload == {
        "type": "job_lost",
        "job_ids": ["3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f"],
    }


def test_job_lost_with_no_ids_is_still_a_valid_frame():
    payload = json.loads(job_lost_message([]))
    validate_frame("bridge-job-lost", payload)
    assert payload["job_ids"] == []


# --- snapshot (binary frame) --------------------------------------------------


def _split_snapshot_frame(wire: bytes):
    """[4-byte BE header length][UTF-8 JSON header][image bytes] —
    unpacked the way the cloud has to, so asserting on the pieces also
    asserts on the layout consumers depend on."""
    (header_len,) = struct.unpack(">I", wire[:4])
    header_bytes = wire[4 : 4 + header_len]
    image_bytes = wire[4 + header_len :]
    return json.loads(header_bytes.decode("utf-8")), image_bytes


def test_snapshot_frame_is_a_length_prefixed_header_then_the_image_bytes():
    image_bytes = b"\xff\xd8\xff\xe0not really a jpeg but bytes are bytes here"
    wire = snapshot_frame(
        slug="front_cam",
        mime="image/jpeg",
        width=640,
        height=480,
        timestamp_ms=1786400000000,
        image_bytes=image_bytes,
    )
    header, trailing_bytes = _split_snapshot_frame(wire)
    validate_frame("snapshot-header", header)
    assert header == {
        "type": "snapshot",
        "slug": "front_cam",
        "mime": "image/jpeg",
        "width": 640,
        "height": 480,
        "timestamp_ms": 1786400000000,
    }
    assert trailing_bytes == image_bytes


def test_snapshot_frame_is_self_contained_with_no_image_bytes():
    # Not a realistic snapshot, but the layout must not assume a non-empty
    # trailer — self-contained is the point (no correlation with a
    # preceding or following frame, unlike a JSON-then-binary pair).
    wire = snapshot_frame(
        slug="front_cam", mime="image/jpeg", width=1, height=1,
        timestamp_ms=0, image_bytes=b"",
    )
    header, trailing_bytes = _split_snapshot_frame(wire)
    validate_frame("snapshot-header", header)
    assert trailing_bytes == b""


def test_asset_progress_message_carries_structured_failed_entries():
    """`failed` on the wire is `{reference, kind, details}[]`, not
    `string[]` — validated against the vendored `bridge-asset-progress`
    schema (re-vendored at contracts 764a1eb, adding `too_large`/`details`),
    not just a hand-built dict, so a producer/schema disagreement shows up
    here, not only in a real sync."""
    payload = json.loads(
        bridge_asset_progress_message(
            "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", 2, 3,
            [
                ("package://pkg/gone.stl", "unresolvable", None),
                ("package://pkg/here.stl", "upload_failed", None),
            ],
            "finished",
        )
    )
    validate_frame("bridge-asset-progress", payload)
    assert payload == {
        "type": "asset_progress",
        "sync_id": "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
        "done": 2,
        "total": 3,
        "failed": [
            {"reference": "package://pkg/gone.stl", "kind": "unresolvable", "details": None},
            {"reference": "package://pkg/here.stl", "kind": "upload_failed", "details": None},
        ],
        "state": "finished",
    }


def test_asset_progress_message_carries_too_large_details():
    """`too_large` is the one kind whose `details` is non-null —
    validated against the vendored schema, which requires the shape
    `superRefine` names in the contract (JSON Schema export can't encode
    the cross-field pairing rule itself, only the shape of a populated
    `details`)."""
    payload = json.loads(
        bridge_asset_progress_message(
            "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f", 1, 2,
            [
                (
                    "package://pkg/huge.dae", "too_large",
                    {"limit_bytes": 67108864, "size_bytes": 193886766},
                ),
            ],
            "running",
        )
    )
    validate_frame("bridge-asset-progress", payload)
    assert payload["failed"] == [
        {
            "reference": "package://pkg/huge.dae",
            "kind": "too_large",
            "details": {"limit_bytes": 67108864, "size_bytes": 193886766},
        }
    ]


def test_asset_failure_kind_constants_match_the_vendored_enum_exactly():
    """The `ASSET_KINDS` precedent: `assetKind`'s values once crossed the
    TS/Python line as hand-typed Python literals with nothing checking
    them against the contract, and it took a review to notice.
    `ASSET_KINDS` now loads straight off `contracts_constants.json` (so it
    can't drift structurally), but the four `ASSET_FAILURE_KIND_*`
    constants in `protocol.py` are still hand-typed — same shape, not yet
    the same guard.

    This is that guard: it compares the `ASSET_FAILURE_KIND_*` values this
    module exports against the vendored `bridge-asset-progress` schema's
    `enum` for `failed[].kind`, in both directions — a value in one list
    and not the other fails either way, whether contracts adds a kind
    nobody taught the bridge, or the bridge invents one contracts never
    declared."""
    with (SCHEMA_DIR / "bridge-asset-progress.schema.json").open() as handle:
        schema = json.load(handle)
    vendored_kinds = set(schema["properties"]["failed"]["items"]["properties"]["kind"]["enum"])
    bridge_kinds = {
        ASSET_FAILURE_KIND_UNRESOLVABLE,
        ASSET_FAILURE_KIND_UPLOAD_FAILED,
        ASSET_FAILURE_KIND_REFUSED,
        ASSET_FAILURE_KIND_TOO_LARGE,
    }
    assert bridge_kinds == vendored_kinds, (
        "protocol.py's ASSET_FAILURE_KIND_* constants and the vendored "
        "schema's failed[].kind enum have drifted apart: bridge has {}, "
        "contract has {}".format(
            bridge_kinds - vendored_kinds, vendored_kinds - bridge_kinds
        )
    )


def test_asset_progress_message_rejects_the_old_bare_string_shape():
    """The schema is `additionalProperties: false` on each `failed` entry
    and requires an object — proves the vendored schema enforces the
    shape rather than being re-vendored and never exercised."""
    payload = {
        "type": "asset_progress",
        "sync_id": "3f1e9a2c-6d4b-4f0a-9c8e-1b2a3c4d5e6f",
        "done": 1,
        "total": 1,
        "failed": ["package://pkg/gone.stl"],  # the old flat-string shape
        "state": "finished",
    }
    with pytest.raises(Exception):
        validate_frame("bridge-asset-progress", payload)

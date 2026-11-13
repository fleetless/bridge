# SPDX-License-Identifier: Apache-2.0
"""`live.max_packet_bytes` as the session sees it: the parameter layer read
at `hello_ok`, the `live` section of a config on top of it, and the lever
pulled only when the resolved value moved.

The layers themselves are `test_live_settings.py`'s subject and the packets
are `test_live_packet_size.py`'s. What this file covers is the wiring: which
value reaches the runtime, when, and what a section that does not resolve
costs (a log line, and nothing in the ack).
"""
import asyncio
import contextlib
import json
import logging

from fake_cloud import FakeCloud
from helpers import make_client, run_until
from test_client_datapoints import FakeRos

from fleetless_bridge.live_settings import LiveSettings


def _calls(ros):
    return [settings.max_packet_bytes for settings in ros.live_settings_calls]


@contextlib.contextmanager
def _client_log():
    """client.py's log lines. Not `caplog`: pytest disables propagation for
    these loggers, so a `caplog` assertion passes whether or not the line
    was written (`test_client_link_mode.py` says the same)."""
    records = []

    class _Handler(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = _Handler(level=logging.DEBUG)
    watched = logging.getLogger("fleetless_bridge.client")
    previous = watched.level
    watched.addHandler(handler)
    watched.setLevel(logging.DEBUG)
    try:
        yield records
    finally:
        watched.removeHandler(handler)
        watched.setLevel(previous)


def _session(ros, script):
    """Greet, run `script(session, client, box)` from the cloud's side, and
    return what it left in the box. `ros=None` builds a client with no
    runtime."""

    async def scenario():
        box = {}
        holder = {}

        async def behavior(session):
            await session.recv_hello()
            await session.accept()
            await session.recv_link_mode()
            await script(session, holder["client"], box)
            box["done"] = True
            await session.drain()

        async with FakeCloud(behavior) as cloud:
            overrides = {} if ros is None else {"ros": ros}
            holder["client"] = make_client(cloud, **overrides)
            await run_until(holder["client"], lambda: "done" in box)
        box["client"] = holder["client"]
        return box

    return asyncio.run(scenario())


async def _config(session, version, **sections):
    await session.send_config(version, {}, **sections)
    return await session.recv_config_applied()


# --- the layers, as the runtime is told ----------------------------------------


def test_with_neither_layer_the_default_holds_and_no_lever_is_pulled():
    """`live.py` applied 1200 when it was imported. A greeting that resolves
    to the same number has nothing to tell the runtime."""
    ros = FakeRos()

    async def script(session, client, box):
        box["applied"] = await _config(session, 1)

    box = _session(ros, script)
    assert box["applied"]["ok"] is True
    assert box["client"]._live_settings == LiveSettings(max_packet_bytes=1200)
    assert _calls(ros) == []


def test_the_parameter_is_read_at_hello_and_applied():
    """A launch file that sets `live.max_packet_bytes`, or a `ros2 param set`
    made before the bridge had a session."""
    ros = FakeRos()
    ros.live_params_value = {"max_packet_bytes": 1000}

    async def script(session, client, box):
        box["at_hello"] = _calls(ros)

    box = _session(ros, script)
    assert box["at_hello"] == [1000]


def test_a_yaml_key_overrides_the_parameter():
    ros = FakeRos()
    ros.live_params_value = {"max_packet_bytes": 1000}

    async def script(session, client, box):
        box["applied"] = await _config(session, 1, live={"max_packet_bytes": 900})

    box = _session(ros, script)
    assert box["applied"]["ok"] is True and box["applied"]["errors"] == []
    assert _calls(ros) == [1000, 900]


def test_an_absent_yaml_key_leaves_the_parameter_alone():
    """Neither a config without the section nor one with an empty section
    moves anything."""
    ros = FakeRos()
    ros.live_params_value = {"max_packet_bytes": 1000}

    async def script(session, client, box):
        await _config(session, 1)
        await _config(session, 2, live={})

    _session(ros, script)
    assert _calls(ros) == [1000]


def test_a_later_config_without_the_key_returns_to_the_parameter():
    ros = FakeRos()
    ros.live_params_value = {"max_packet_bytes": 1000}

    async def script(session, client, box):
        await _config(session, 1, live={"max_packet_bytes": 900})
        await _config(session, 2)

    _session(ros, script)
    assert _calls(ros) == [1000, 900, 1000]


def test_without_a_parameter_a_removed_key_returns_to_the_default():
    ros = FakeRos()

    async def script(session, client, box):
        await _config(session, 1, live={"max_packet_bytes": 900})
        await _config(session, 2)

    _session(ros, script)
    assert _calls(ros) == [900, 1200]


def test_the_lever_is_pulled_only_when_the_resolved_value_moved():
    """Every reconnect brings the config again. A stream should not be
    re-capped at the number it already has, and the log should not say so."""
    ros = FakeRos()

    async def script(session, client, box):
        await _config(session, 1, live={"max_packet_bytes": 900})
        await _config(session, 2, live={"max_packet_bytes": 900})
        await _config(session, 3, live={"max_packet_bytes": 900}, low_bandwidth={"mode": "off"})

    with _client_log() as records:
        _session(ros, script)
    assert _calls(ros) == [900]
    said = [r.getMessage() for r in records if "live.max_packet_bytes" in r.getMessage()]
    assert len(said) == 1 and "900" in said[0]


def test_an_unknown_key_in_the_section_is_ignored():
    """A console that writes a newer key must not take this bridge off the
    air, nor cost the key it does know."""
    ros = FakeRos()

    async def script(session, client, box):
        await session.send_raw(json.dumps({
            "type": "config",
            "version": 1,
            "doc": {"fleetless": 1, "live": {"max_packet_bytes": 900, "codec": "h264"}},
        }))
        box["applied"] = await session.recv_config_applied()

    box = _session(ros, script)
    assert box["applied"]["ok"] is True
    assert _calls(ros) == [900]


# --- `ros2 param set` at runtime -----------------------------------------------


def test_an_accepted_parameter_change_is_applied():
    """The runtime validates the set and calls back on the event loop with
    the whole layer; this is what the client makes of that call."""
    ros = FakeRos()

    async def script(session, client, box):
        ros.live_params_value = {"max_packet_bytes": 800}
        ros.live_callback({"max_packet_bytes": 800})
        await _config(session, 1)

    _session(ros, script)
    assert _calls(ros) == [800]


def test_a_parameter_change_under_a_yaml_key_waits_for_the_key_to_go():
    """The YAML wins while it names the key. The parameter is stored, not
    lost: it is in force the moment a config arrives without the key."""
    ros = FakeRos()

    async def script(session, client, box):
        await _config(session, 1, live={"max_packet_bytes": 900})
        ros.live_params_value = {"max_packet_bytes": 800}
        ros.live_callback({"max_packet_bytes": 800})
        box["under_yaml"] = _calls(ros)
        await _config(session, 2)

    box = _session(ros, script)
    assert box["under_yaml"] == [900]
    assert _calls(ros) == [900, 800]


def test_a_parameter_change_before_any_session_is_applied_at_once():
    """`ros2 param set` on a robot whose uplink is down. Nothing about the
    packet size needs the cloud."""
    ros = FakeRos()

    async def scenario():
        async with FakeCloud(lambda session: session.drain()) as cloud:
            make_client(cloud, ros=ros)
            ros.live_callback({"max_packet_bytes": 700})

    asyncio.run(scenario())
    assert _calls(ros) == [700]


# --- what does not resolve -----------------------------------------------------


def test_a_yaml_value_out_of_bounds_keeps_the_value_in_force_and_logs_an_error():
    """The cloud bounds the key when the configuration is saved, so this
    frame is one no conforming cloud sends. If it arrives anyway: the value
    in force stays, an error is logged, and the ack says nothing, because
    there is no apply-error kind for a section the cloud has already
    checked."""
    ros = FakeRos()

    async def script(session, client, box):
        await _config(session, 1, live={"max_packet_bytes": 900})
        for version, value in ((2, 1400), (3, 575), (4, "900"), (5, 900.5)):
            await session.send_raw(json.dumps({
                "type": "config",
                "version": version,
                "doc": {"fleetless": 1, "live": {"max_packet_bytes": value}},
            }))
            box[version] = await session.recv_config_applied()
        # And the section it had is still the one a later change builds on.
        ros.live_callback({"max_packet_bytes": 800})

    with _client_log() as records:
        box = _session(ros, script)
    for version in (2, 3, 4, 5):
        assert box[version]["ok"] is True and box[version]["errors"] == []
    assert _calls(ros) == [900]
    assert box["client"]._live_settings == LiveSettings(max_packet_bytes=900)
    errors = [r.getMessage() for r in records if r.levelno == logging.ERROR]
    assert len(errors) == 4
    for line in errors:
        assert "live.max_packet_bytes must be an integer in [576, 1350]" in line
        assert "900" in line


def test_a_parameter_no_callback_ever_judged_is_reported_and_not_applied():
    """A value given at startup is stored before the bridge's own check is
    registered, so it can be anything. It is read at `hello_ok` like any
    other: reported, not applied, and the greeting goes on."""
    ros = FakeRos()
    ros.live_params_value = {"max_packet_bytes": 5000}

    async def script(session, client, box):
        box["applied"] = await _config(session, 1)

    with _client_log() as records:
        box = _session(ros, script)
    assert box["applied"]["ok"] is True
    assert _calls(ros) == []
    assert box["client"]._live_settings == LiveSettings(max_packet_bytes=1200)
    errors = [r.getMessage() for r in records if r.levelno == logging.ERROR]
    assert len(errors) == 1
    assert "live.max_packet_bytes must be an integer in [576, 1350]" in errors[0]
    assert "1200" in errors[0]


def test_a_lever_that_raises_costs_a_log_line_and_not_the_session():
    """Unhandled, an exception here reaches the receive loop's catch-all,
    which closes the socket: a bug in a packet size would cost the whole
    session. The next change is tried again."""
    ros = FakeRos()
    ros.set_live_settings_raises = RuntimeError("no")

    async def script(session, client, box):
        box["first"] = await _config(session, 1, live={"max_packet_bytes": 900})
        ros.set_live_settings_raises = None
        box["second"] = await _config(session, 2, live={"max_packet_bytes": 900})

    with _client_log() as records:
        box = _session(ros, script)
    assert box["first"]["ok"] is True and box["second"]["ok"] is True
    # Not recorded as in force after the failure, so the same value is
    # applied again rather than skipped as "unchanged".
    assert _calls(ros) == [900, 900]
    assert any(r.levelno == logging.ERROR for r in records)


def test_a_client_with_no_runtime_resolves_and_acks():
    """`ros=None` is how most of the suite builds a client: no node, no
    aiortc, nothing to cap. The section must still parse, resolve and ack."""

    async def script(session, client, box):
        box["applied"] = await _config(session, 1, live={"max_packet_bytes": 900})

    box = _session(None, script)
    assert box["applied"]["ok"] is True
    assert box["client"]._live_settings == LiveSettings(max_packet_bytes=900)


# --- live.encoder, through the same layers -------------------------------------------


def _encoders(ros):
    return [settings.encoder for settings in ros.live_settings_calls]


def test_the_encoder_of_the_section_overrides_the_parameter_and_returns_to_it():
    ros = FakeRos()
    ros.live_params_value = {"max_packet_bytes": 1200, "encoder": "nvenc"}

    async def script(session, client, box):
        await _config(session, 1)
        await _config(session, 2, live={"encoder": "auto"})
        await _config(session, 3, live={"encoder": "auto"})  # the same again: no lever
        await _config(session, 4)

    with _client_log() as records:
        box = _session(ros, script)
    assert _encoders(ros) == ["nvenc", "auto", "nvenc"]
    assert box["client"]._live_settings == LiveSettings(max_packet_bytes=1200, encoder="nvenc")
    said = [r.getMessage() for r in records if r.levelno == logging.INFO and "live." in r.getMessage()]
    # One line per change, about the key that changed and when it holds.
    assert said == [
        "live.encoder is {}: it encodes live video from the next live start on; "
        "a running stream keeps the encoder it has".format(name)
        for name in ("nvenc", "auto", "nvenc")
    ]


def test_each_setting_that_moved_gets_its_own_line_and_the_other_gets_none():
    ros = FakeRos()

    async def script(session, client, box):
        await _config(session, 1, live={"max_packet_bytes": 900})
        await _config(session, 2, live={"max_packet_bytes": 900, "encoder": "vaapi"})
        await _config(session, 3, live={"max_packet_bytes": 1000, "encoder": "v4l2"})

    with _client_log() as records:
        _session(ros, script)
    assert [(s.max_packet_bytes, s.encoder) for s in ros.live_settings_calls] == [
        (900, "software"), (900, "vaapi"), (1000, "v4l2"),
    ]
    said = [r.getMessage() for r in records if r.levelno == logging.INFO and "live." in r.getMessage()]
    assert [line.split(":")[0] for line in said] == [
        "live.max_packet_bytes is 900",
        "live.encoder is vaapi",
        "live.max_packet_bytes is 1000",
        "live.encoder is v4l2",
    ]


def test_an_accepted_encoder_parameter_is_applied_and_waits_under_a_yaml_key():
    ros = FakeRos()

    async def script(session, client, box):
        await _config(session, 1)
        ros.live_callback({"max_packet_bytes": 1200, "encoder": "nvenc"})
        await _config(session, 2, live={"encoder": "software"})
        ros.live_callback({"max_packet_bytes": 1200, "encoder": "vaapi"})  # overridden
        box["under_the_key"] = list(_encoders(ros))
        await _config(session, 3)

    box = _session(ros, script)
    assert box["under_the_key"] == ["nvenc", "software"]
    assert _encoders(ros) == ["nvenc", "software", "vaapi"]


def test_an_encoder_this_bridge_does_not_know_keeps_the_one_in_force_and_says_so():
    """A newer cloud, or a frame no conforming cloud sends. The ack says
    nothing, the error names the rule and what stays in force."""
    ros = FakeRos()

    async def script(session, client, box):
        await _config(session, 1, live={"encoder": "nvenc", "max_packet_bytes": 900})
        for version, value in ((2, "rkmpp"), (3, "NVENC"), (4, 7)):
            await session.send_raw(json.dumps({
                "type": "config",
                "version": version,
                "doc": {"fleetless": 1, "live": {"encoder": value, "max_packet_bytes": 1000}},
            }))
            box[version] = await session.recv_config_applied()

    with _client_log() as records:
        box = _session(ros, script)
    for version in (2, 3, 4):
        assert box[version]["ok"] is True and box[version]["errors"] == []
    # The whole section is refused, the good key with it: a section is one
    # statement, and half of one is not what anybody published.
    assert [(s.max_packet_bytes, s.encoder) for s in ros.live_settings_calls] == [(900, "nvenc")]
    assert box["client"]._live_settings == LiveSettings(max_packet_bytes=900, encoder="nvenc")
    errors = [r.getMessage() for r in records if r.levelno == logging.ERROR]
    assert len(errors) == 3
    for line in errors:
        assert line == (
            "live.encoder must be one of software, auto, nvenc, vaapi, v4l2. The bridge "
            "keeps sending RTP packets of at most 900 bytes, encoded by nvenc."
        )

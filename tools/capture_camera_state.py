# SPDX-License-Identifier: Apache-2.0
"""Captures real `camera_state` frames off a real bridge process, verbatim.

Exists because "I tested my parser against a frame my test wrote" and "I
tested my parser against what the bridge sends" are different claims. The
cloud side has its own capture of the *derived* `resource_health` event;
this captures the *source* frame,
bridge -> cloud, one layer upstream of that derivation).

Stands in as "the cloud" at the wire level only — hello/hello_ok/config/
camera_start, enough to drive a real bridge through two real scenarios
against real fixtures already running on the dev host:

  A. An MJPEG camera with no credentials against camera-fixtures.sh's
     fixture (demands HTTP Basic auth) -> a real 401 -> a real,
     unsolicited `camera_state` with `cause: 'source'`.
  B. A ROS camera (the demo fake_robot.py's own `/image_raw`) started
     live, then its config retargeted while live -> a real `camera_state`
     with `cause: 'config_change'`.

Prints every frame **raw**, before any parsing on this side — a
reconstruction that has been through a schema is no longer evidence about
the wire, the same principle capture-health-frame.mjs states for itself.

Usage — three things must already be running: the MJPEG fixture
(camera-fixtures.sh start), the demo fake_robot.py on ROS_DOMAIN_ID=5
publishing /image_raw, and a real LiveKit server:

    # mint a real publisher token — livekit-api is not a bridge
    # dependency (deliberately: minting is the cloud's job) and is not an
    # apt package, so this reaches PyPI for a one-off local venv rather
    # than a system-wide pip install (see rosdep/README.md for the only
    # other place pip is used at all), which also sidesteps PEP 668's
    # externally-managed-environment refusal on jazzy/lyrical
    python3 -m venv /tmp/livekit-api-venv
    /tmp/livekit-api-venv/bin/pip install --quiet livekit-api
    /tmp/livekit-api-venv/bin/python3 -c "
from livekit import api
t = api.AccessToken('devkey', 'secret').with_identity('capture').with_grants(
    api.VideoGrants(room_join=True, room='capture-room', can_publish=True))
print(t.to_jwt())" > /tmp/livekit-token.txt

    python3 tools/capture_camera_state.py --port 8099 \
        --live-token "$(cat /tmp/livekit-token.txt)" \
        --live-room capture-room --live-url ws://127.0.0.1:7880 &

    FLEETLESS_TOKEN=frt_capture FLEETLESS_CLOUD_URL=ws://127.0.0.1:8099/bridge \
        ROS_DOMAIN_ID=5 ./run-bridge.sh
"""
import argparse
import asyncio
import json

from ws_capture_server import serve

MJPEG_URL = "http://127.0.0.1:8556/"  # camera-fixtures.sh's endpoint, no trailing path needed
ROBOT_ID = "11111111-1111-4111-8111-111111111111"

_BASE_CAMERAS = {
    "bad_mjpeg": {
        # Deliberately no credentials — the fixture demands Basic auth
        # (camera-fixtures.sh), so this is a real 401, not a simulated one.
        "source": {"kind": "mjpeg", "url": MJPEG_URL},
        "width": 320,
        "height": 240,
        "fps": 5,
        "bitrate_kbps": 500,
        "snapshot_interval_seconds": 1,
    },
    "ros_cam": {
        "source": {"kind": "ros", "topic": "/image_raw", "type": "sensor_msgs/msg/Image"},
        "width": 320,
        "height": 240,
        "fps": 5,
        "bitrate_kbps": 500,
        "snapshot_interval_seconds": 1,
    },
}


def _config(version, cameras):
    return {
        "type": "config",
        "version": version,
        "doc": {
            "fleetless": 1,
            "cameras": cameras,
        },
    }


async def _handle(ws, live_token, live_room, live_url, captured):
    print("--- bridge connected ---")

    hello_raw = await ws.recv()
    print("<<< hello (raw):", hello_raw)

    async def send(obj):
        print(">>> sending {} (raw): {}".format(obj["type"], json.dumps(obj)))
        await ws.send(json.dumps(obj))

    await send({"type": "hello_ok", "robot_id": ROBOT_ID})

    # Scenario A + the background half of B: both cameras configured.
    # bad_mjpeg's adapter starts failing in the background immediately —
    # no camera_start needed, that is the whole point.
    await send(_config(1, {"bad_mjpeg": _BASE_CAMERAS["bad_mjpeg"], "ros_cam": _BASE_CAMERAS["ros_cam"]}))

    live_started = False
    retargeted = False

    while len(captured) < 2:
        raw = await asyncio.wait_for(ws.recv(), timeout=30.0)
        if isinstance(raw, (bytes, bytearray)):
            # A binary snapshot frame (both cameras have
            # snapshot_interval_seconds set) — self-contained, not JSON, and
            # not what this script is capturing. json.loads on raw bytes can
            # misdetect the encoding
            # from a length-prefixed binary header (hit this directly: it
            # guessed UTF-32 from the 4-byte big-endian length prefix and
            # crashed the handler with 1011).
            print("<<< (binary snapshot frame, {} bytes, skipped)".format(len(raw)))
            continue
        print("<<< raw:", raw)
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue

        if msg.get("type") == "config_applied":
            print("    (config_applied: ok={})".format(msg.get("ok")))
            continue

        if msg.get("type") != "camera_state":
            continue

        cause = msg.get("cause")
        slug = msg.get("slug")

        if cause == "source" and not any(c["cause"] == "source" for c in captured):
            print("\n*** CAPTURED cause='source' (scenario A) ***\n")
            captured.append(msg)

        # Once bad_mjpeg's background failure has been seen (or after the
        # ROS adapter has had a moment to deliver its first frame), start
        # ros_cam live for real.
        if not live_started and slug in ("bad_mjpeg", "ros_cam"):
            live_started = True
            await asyncio.sleep(1.0)  # let the ROS subscription get at least one frame
            await send(
                {
                    "type": "camera_start",
                    "slug": "ros_cam",
                    "url": live_url,
                    "room": live_room,
                    "token": live_token,
                    "request_id": "capture-camera-state-req-1",  #: now required
                }
            )

        if cause == "command" and slug == "ros_cam" and msg.get("publishing") and not retargeted:
            retargeted = True
            print("    ros_cam is genuinely live — retargeting its config now")
            changed = dict(_BASE_CAMERAS["ros_cam"])
            changed["width"], changed["height"] = 640, 480  # any changed field stops the stream
            await send(_config(2, {"bad_mjpeg": _BASE_CAMERAS["bad_mjpeg"], "ros_cam": changed}))

        if cause == "config_change" and not any(c["cause"] == "config_change" for c in captured):
            print("\n*** CAPTURED cause='config_change' (scenario B) ***\n")
            captured.append(msg)

    print("\n=== both scenarios captured — closing ===")
    await ws.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8099)
    parser.add_argument("--live-token", required=True)
    parser.add_argument("--live-room", required=True)
    parser.add_argument("--live-url", required=True)
    args = parser.parse_args()

    captured = []
    done = asyncio.Event()

    async def handler(ws):
        try:
            await _handle(ws, args.live_token, args.live_room, args.live_url, captured)
        finally:
            if len(captured) >= 2:
                done.set()

    server = await serve(handler, "127.0.0.1", args.port)
    print("listening on ws://127.0.0.1:{}/bridge — point a real bridge at it".format(args.port))
    await done.wait()
    server.close()
    await server.wait_closed()

    print("\n\n========== SUMMARY ==========")
    for msg in captured:
        print("cause={}: {}".format(msg.get("cause"), json.dumps(msg)))


if __name__ == "__main__":
    asyncio.run(main())

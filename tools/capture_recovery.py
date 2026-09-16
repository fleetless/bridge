# SPDX-License-Identifier: Apache-2.0
"""One-shot capture: does a corrected credential produce a real recovery
`camera_state` frame (`cause: 'source', error: null`)?

Settles the question by asking the wire rather than by reasoning about it.
Drives a real bridge through the exact real-world sequence — wrong password
against camera-fixtures.sh's MJPEG fixture, then the *correct* one in a second
config, matching what
the cloud does on a credential rotation — and watches for the recovery
frame.

    python3 tools/capture_recovery.py --port 8100

    FLEETLESS_TOKEN=frt_capture FLEETLESS_CLOUD_URL=ws://127.0.0.1:8100/bridge \
        ROS_DOMAIN_ID=5 ./run-bridge.sh
"""
import argparse
import asyncio
import json

from ws_capture_server import serve

MJPEG_URL = "http://127.0.0.1:8556/"
ROBOT_ID = "22222222-2222-4222-8222-222222222222"
# camera-fixtures.sh's own fixture credentials.
FIXTURE_USER, FIXTURE_PASS = "camuser", "campass"


def _camera_cfg(password):
    return {
        "source": {
            "kind": "mjpeg",
            "url": MJPEG_URL,
            "credentials": {"username": FIXTURE_USER, "password": password},
        },
        "width": 320,
        "height": 240,
        "fps": 5,
        "bitrate_kbps": 500,
        "snapshot_interval_seconds": 1,
    }


def _config(version, password):
    return {
        "type": "config",
        "version": version,
        "doc": {
            "fleetless": 1,
            "cameras": {"cred_cam": _camera_cfg(password)},
        },
    }


async def _handle(ws, done):
    print("--- bridge connected ---")
    hello_raw = await ws.recv()
    print("<<< hello (raw):", hello_raw)

    async def send(obj):
        print(">>> sending {} (raw): {}".format(obj["type"], json.dumps(obj)))
        await ws.send(json.dumps(obj))

    await send({"type": "hello_ok", "robot_id": ROBOT_ID})

    # First: the wrong password — a real 401 against the real fixture.
    await send(_config(1, "wrong-password"))

    saw_error = False
    fix_sent = False
    fix_sent_at = None

    while True:
        raw = await asyncio.wait_for(ws.recv(), timeout=45.0)
        if isinstance(raw, (bytes, bytearray)):
            print("<<< (binary snapshot frame, {} bytes, skipped)".format(len(raw)))
            continue
        print("<<< raw:", raw)
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue

        if msg.get("type") == "config_applied":
            print("    (config_applied: version={} ok={})".format(msg.get("version"), msg.get("ok")))
            continue
        if msg.get("type") != "camera_state":
            continue

        cause = msg.get("cause")
        error = msg.get("error")

        if cause == "source" and error is not None and not saw_error:
            saw_error = True
            print("\n*** wrong password confirmed: real auth failure, error={} ***\n".format(error))
            # Then: the correct password, in a fresh config — exactly
            # what a credential rotation on the cloud looks like on the
            # wire.
            fix_sent = True
            fix_sent_at = asyncio.get_event_loop().time()
            await send(_config(2, FIXTURE_PASS))
            continue

        if cause == "source" and error is None and fix_sent:
            elapsed = asyncio.get_event_loop().time() - fix_sent_at
            print("\n*** RECOVERY FRAME ARRIVED, {:.1f}s after the fix was sent ***".format(elapsed))
            print("*** {} ***\n".format(json.dumps(msg)))
            done.set_result(("recovered", msg))
            return

        if cause == "source" and fix_sent and error is not None:
            print("\n*** something OTHER than a clean recovery: {} ***\n".format(json.dumps(msg)))
            done.set_result(("unexpected", msg))
            return


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument("--wait-after-fix-s", type=float, default=30.0)
    args = parser.parse_args()

    loop = asyncio.get_event_loop()
    done = loop.create_future()

    async def handler(ws):
        try:
            await asyncio.wait_for(_handle(ws, done), timeout=90.0)
        except asyncio.TimeoutError:
            if not done.done():
                done.set_result(("timeout", None))

    server = await serve(handler, "127.0.0.1", args.port)
    print("listening on ws://127.0.0.1:{}/bridge — point a real bridge at it".format(args.port))

    outcome, msg = await done
    server.close()
    await server.wait_closed()

    print("\n\n========== RESULT ==========")
    if outcome == "recovered":
        print("A recovery frame arrived: {}".format(json.dumps(msg)))
    elif outcome == "unexpected":
        print("An unexpected frame arrived instead of a clean recovery: {}".format(json.dumps(msg)))
    else:
        print("NO recovery frame arrived within the timeout.")


if __name__ == "__main__":
    asyncio.run(main())

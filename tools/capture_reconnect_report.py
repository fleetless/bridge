# SPDX-License-Identifier: Apache-2.0
"""One-shot capture: does a fresh session re-state a camera's still-broken
health unprompted, on its first config apply?

This is the second defect of the pair — a cloud restart erases its own
health store, and nothing re-establishes it, because the channel only
ever spoke about transitions. Simulates exactly that from the bridge's
side: a real MJPEG camera stays wrong-credentialed across a simulated
"cloud restart" (this capture script closes the connection and keeps
listening, standing in for the cloud coming back up with the same
published config — nothing about the camera changes), and the test is
whether the bridge volunteers the still-broken state on its own, on the
fresh hello, with no new error having occurred to trigger it.

    python3 tools/capture_reconnect_report.py --port 8101

    FLEETLESS_TOKEN=frt_capture FLEETLESS_CLOUD_URL=ws://127.0.0.1:8101/bridge \
        ROS_DOMAIN_ID=5 ./run-bridge.sh
"""
import argparse
import asyncio
import json

from ws_capture_server import serve

MJPEG_URL = "http://127.0.0.1:8556/"
ROBOT_ID = "33333333-3333-4333-8333-333333333333"


def _config(version):
    return {
        "type": "config",
        "version": version,
        "doc": {
            "fleetless": 1,
            "cameras": {
                "still_broken": {
                    "source": {"kind": "mjpeg", "url": MJPEG_URL},
                    "width": 320,
                    "height": 240,
                    "fps": 5,
                    "bitrate_kbps": 500,
                    "snapshot_interval_seconds": 1,
                }
            },
        },
    }


async def _one_session(ws, session_label):
    hello_raw = await ws.recv()
    print("<<< [{}] hello (raw): {}".format(session_label, hello_raw))

    async def send(obj):
        print(">>> [{}] sending {} (raw): {}".format(session_label, obj["type"], json.dumps(obj)))
        await ws.send(json.dumps(obj))

    await send({"type": "hello_ok", "robot_id": ROBOT_ID})
    await send(_config(1))

    saw_unsolicited_report = False
    while True:
        raw = await asyncio.wait_for(ws.recv(), timeout=20.0)
        if isinstance(raw, (bytes, bytearray)):
            continue
        print("<<< [{}] raw: {}".format(session_label, raw))
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if msg.get("type") == "config_applied":
            print("    [{}] (config_applied: ok={})".format(session_label, msg.get("ok")))
            continue
        if msg.get("type") == "camera_state" and msg.get("cause") == "source":
            saw_unsolicited_report = True
            return msg


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8101)
    args = parser.parse_args()

    result = {}

    async def handler(ws):
        if "first_msg" not in result:
            print("\n=== SESSION 1 (fresh, wrong credential) ===")
            msg = await _one_session(ws, "session-1")
            result["first_msg"] = msg
            print("\n*** session 1's error, as expected: {} ***".format(json.dumps(msg)))
            print("\n=== closing to simulate the cloud restarting ===\n")
            await ws.close(1001, "simulated cloud restart")
        else:
            print("\n=== SESSION 2 (fresh hello, SAME still-wrong config, nothing new happened) ===")
            msg = await _one_session(ws, "session-2")
            result["second_msg"] = msg
            print("\n*** session 2's UNSOLICITED report, no new error occurred: {} ***".format(
                json.dumps(msg)
            ))
            await ws.close()

    server = await serve(handler, "127.0.0.1", args.port)
    print("listening on ws://127.0.0.1:{}/bridge — point a real bridge at it".format(args.port))

    while "second_msg" not in result:
        await asyncio.sleep(0.1)

    server.close()
    await server.wait_closed()

    print("\n\n========== RESULT ==========")
    print("session 1 (the original failure):", json.dumps(result["first_msg"]))
    print("session 2 (reconnect, re-stated unprompted):", json.dumps(result["second_msg"]))
    if result["first_msg"]["error"] == result["second_msg"]["error"]:
        print("\nSAME error, re-sent on the fresh session's first config apply,")
        print("with no new failure having occurred to trigger a transition.")
        print("This is the reconnect-report fix working as intended.")


if __name__ == "__main__":
    asyncio.run(main())

# SPDX-License-Identifier: Apache-2.0
"""Manual verification against a real V4L2 device.

Not part of the pytest suite — no `/dev/video*` exists in the dev/test
container (`Dockerfile.dev`), so `camera_sources.py`'s V4L2 tests are all
against a fake `capture_factory`. This script is the manual fixture the
V4L2 half of that suite has always needed and never had: run it against
the real webcam on the dev host (a Logitech Brio 500 at `/dev/video0`,
the same device `run-fake-robot.sh` already hands to `fake_robot.py`).

    docker run --rm --device /dev/video0 \\
        -v "$PWD":/ws -w /ws fleetless-bridge-dev \\
        python3 tools/verify_v4l2.py

Answers three separate questions — kept separate in the output on
purpose, because the two are different claims:
`cap.set(...)` returning `True` is the backend *agreeing to store a
number*, not a demonstration that a read is actually bounded by it.

1. Is the backend actually V4L2 — not GStreamer or some other backend
   OpenCV's auto-detection might have chosen for a bare device path?
2. Do real frames arrive through the same adapter a robot's config would
   actually use (`V4l2SourceAdapter`), with the read-timeout property
   accepted by a real backend rather than a synthetic one?
3. Does a stalled read have an observable *bound*? Previously
   unanswered here — this script still does not fake a stall against the
   real camera below (there remains no safe, remote way to make a real
   V4L2 driver stop responding mid-read without physically unplugging the
   device), but the question itself is
   now answerable without one: the bound `V4l2SourceAdapter` gives a stalled
   read no longer depends on this property or this backend at all — the capture
   runs in its own child
   process, killed from the outside after `_V4L2_READ_TIMEOUT_S` if it
   never answers. `test_v4l2_adapter_bounds_a_genuinely_stuck_worker_and_
   kills_it` in `test/test_camera_sources.py` proves exactly that, against
   a worker that genuinely blocks forever — no real hardware needed for
   that proof, because the bound no longer lives in the hardware path.
   What this script's own Q1/Q2 answer is now defence in depth (the
   property still gets asked for, still worth knowing whether a given
   backend build honours it), not the mechanism the bound relies on.
"""
import sys
import time

import cv2

from fleetless_bridge.camera_sources import (
    _V4L2_READ_TIMEOUT_MS,
    V4l2SourceAdapter,
)

DEVICE = sys.argv[1] if len(sys.argv) > 1 else "/dev/video0"


def check_backend_and_property():
    """Question 1, and half of question 2 — driven directly against
    `cv2.VideoCapture`, not through the adapter, so the backend name and
    the raw `.set()`/`.get()` return values are visible unfiltered.
    Returns `(opened, is_v4l2, set_ok)`."""
    cap = cv2.VideoCapture(DEVICE, cv2.CAP_V4L2)
    try:
        opened = cap.isOpened()
        backend = cap.getBackendName() if opened else None
        is_v4l2 = backend == "V4L2"
        set_ok = cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, _V4L2_READ_TIMEOUT_MS) if opened else None
        get_value = cap.get(cv2.CAP_PROP_READ_TIMEOUT_MSEC) if opened else None
        print("--- Q1: is the backend actually V4L2? ---")
        print("  isOpened():", opened)
        print("  getBackendName():", backend)
        print("  -> {}".format("PASS: backend is V4L2" if is_v4l2 else "FAIL or inconclusive"))
        print()
        print("--- Q2a: does the backend ACCEPT the read timeout property? ---")
        print("  cap.set(CAP_PROP_READ_TIMEOUT_MSEC, {}) returned: {}".format(
            _V4L2_READ_TIMEOUT_MS, set_ok
        ))
        print("  cap.get(CAP_PROP_READ_TIMEOUT_MSEC) afterwards: {}".format(get_value))
        print(
            "  NOTE: this proves the backend agreed to store the number, "
            "nothing about whether a stalled read is actually bounded by it."
        )
        print()
        return opened, is_v4l2, set_ok
    finally:
        cap.release()


def capture_real_frames():
    """Question 2b — through the real adapter a robot's config would
    actually use, not a direct cv2 call."""
    print("--- Q2b: real frames through V4l2SourceAdapter ---")
    frames = []
    errors = []

    adapter = V4l2SourceAdapter(
        device=DEVICE,
        on_frame=lambda bgr, ts: frames.append((bgr.shape, bgr.dtype, ts)),
        on_error=lambda code, message: errors.append((code, message)),
    )
    adapter.start()
    try:
        time.sleep(3.0)
    finally:
        adapter.stop()

    print("  frames captured in 3s:", len(frames))
    if frames:
        shape, dtype, ts = frames[-1]
        print("  last frame: shape={} dtype={} timestamp_ms={}".format(shape, dtype, ts))
    if errors:
        print("  errors reported:", errors)
    print("  -> {}".format("PASS: real frames arrived" if frames else "FAIL: no frames"))
    print()
    return len(frames) > 0


def main():
    print("Verifying V4L2 against {}\n".format(DEVICE))
    opened, is_v4l2, set_ok = check_backend_and_property()
    if not opened:
        print("Could not open the device — nothing further to check. Is it "
              "already held by another process (run-fake-robot.sh, another "
              "container)? See camera_sources.py's own note: the webcam "
              "cannot be shared.")
        sys.exit(1)
    got_frames = capture_real_frames()

    print("--- Q3: does a stalled read have an observable BOUND? ---")
    print(
        "  NOT answered against this real camera — still no safe, remote "
        "way to make a real V4L2 driver stop responding mid-read without "
        "physically unplugging the device. But answered elsewhere since "
        "the bound no longer depends on this backend "
        "or this property at all — see test_v4l2_adapter_bounds_a_"
        "genuinely_stuck_worker_and_kills_it in test/test_camera_sources.py, "
        "which proves it against a worker that genuinely never answers."
    )
    print()
    print("=== Summary ===")
    print("Q1 (real V4L2 backend, not GStreamer/other):           {}".format(
        "verified" if is_v4l2 else "FAILED — backend was not V4L2"
    ))
    print("Q2 (real frames, property accepted by a real backend): {}".format(
        "verified" if (got_frames and set_ok) else "FAILED"
    ))
    print("Q3 (a stalled read has a bound):                       verified elsewhere (process-level) — not by this script, and no longer needs to be")


if __name__ == "__main__":
    main()

# SPDX-License-Identifier: Apache-2.0
"""The fake robot's test picture: what `/image_raw` carries on a host with no
capture device, when `FAKE_CAMERA_SOURCE=pattern` asks for it.

Off by default. The fake robot opens the real webcam unless told otherwise,
because a real frame at a real rate is what exercises encoding and timing; a
synthetic one is the fallback for a headless box, never the norm.

The picture moves: colour bars that slide one step per frame and a white block
that walks along the bottom edge. Two consecutive frames always differ, so an
encoder has motion to work on and a frozen stream is visible as one.

Only numpy here, so a test can load this without ROS or a camera.
"""
from __future__ import annotations

import numpy as np

#: The values `FAKE_CAMERA_SOURCE` takes. `device` is the default.
CAMERA_SOURCES = ("device", "pattern")

WIDTH = 640
HEIGHT = 480

#: Eight bars, BGR, as the classic test card has them, minus the nostalgia.
_BARS = np.array(
    [
        (255, 255, 255),
        (0, 255, 255),
        (255, 255, 0),
        (0, 255, 0),
        (255, 0, 255),
        (0, 0, 255),
        (255, 0, 0),
        (0, 0, 0),
    ],
    dtype=np.uint8,
)
_BLOCK = 32


def camera_source(value: str | None) -> str:
    """The camera source named by `FAKE_CAMERA_SOURCE`: `device` when unset or
    empty, `pattern` when asked for, and a `ValueError` naming both for
    anything else. A typo must not quietly open the webcam instead."""
    if value is None or value == "":
        return "device"
    if value not in CAMERA_SOURCES:
        raise ValueError(
            "FAKE_CAMERA_SOURCE={!r}: expected one of {}".format(
                value, ", ".join(CAMERA_SOURCES)
            )
        )
    return value


def pattern_frame(index: int, width: int = WIDTH, height: int = HEIGHT) -> np.ndarray:
    """Frame number `index` of the test picture: `height` x `width` x 3,
    uint8, BGR, the layout `cv_bridge` publishes as `bgr8`."""
    columns = (np.arange(width) * len(_BARS) // width + index) % len(_BARS)
    frame = np.repeat(_BARS[columns][np.newaxis, :, :], height, axis=0)
    block_x = (index * 8) % max(width - _BLOCK, 1)
    frame[height - _BLOCK:, :, :] = 32
    frame[height - _BLOCK:, block_x:block_x + _BLOCK, :] = 255
    return np.ascontiguousarray(frame)

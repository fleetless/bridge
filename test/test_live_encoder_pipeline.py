# SPDX-License-Identifier: Apache-2.0
"""The GStreamer pipeline of the bridge's own encoder, run for real over the
software stand-in chain.

Every hardware chain goes through the same `Pipeline` class: `appsrc`, the
chain, `h264parse`, `appsink`. The machines this suite runs on have no
encoder chip, so the chain here is `STAND_IN` (`x264enc`); what is proven is
everything around the encoder element, and what each chain is required to
deliver: one access unit per frame, parameter sets with every keyframe, a
keyframe on request, a bitrate that follows.

**These tests fail, they do not skip, when GStreamer is missing.** The test
image installs it (`Dockerfile.dev`); a suite that skipped without it would
be green on exactly the image that proves nothing.
"""
import pathlib
import subprocess
import sys
from fractions import Fraction

import av
import numpy as np
import pytest

from fleetless_bridge import live_encoder
from fleetless_bridge.live_encoder import (
    CHAINS,
    STAND_IN,
    Chain,
    ChainFailed,
    HardwareH264Encoder,
    Pipeline,
)

WIDTH, HEIGHT, FPS = 320, 240, 15
NAL_SLICE, NAL_IDR, NAL_SPS, NAL_PPS, NAL_AUD = 1, 5, 7, 8, 9


def moving_picture(index, width=WIDTH, height=HEIGHT):
    """A pattern that moves and does not compress to nothing: two gradients
    that shift with every frame and a bright bar that travels down. RGB."""
    picture = np.zeros((height, width, 3), dtype=np.uint8)
    picture[:, :, 0] = (np.arange(width) * 3 + index * 9) % 256
    picture[:, :, 1] = ((np.arange(height) * 2 + index * 5) % 256)[:, None]
    top = (index * 7) % (height - 32)
    picture[top:top + 32, :, 2] = 255
    return picture


def moving_frame(index, width=WIDTH, height=HEIGHT, fps=FPS):
    frame = av.VideoFrame.from_ndarray(moving_picture(index, width, height), format="rgb24")
    frame.pts = index * (90000 // fps)
    frame.time_base = Fraction(1, 90000)
    return frame


def nal_types(data):
    """The NAL unit types of an Annex-B byte string, in order."""
    return [unit[0] & 0x1F for unit in HardwareH264Encoder._split_bitstream(data) if unit]


def _pipeline(chain=STAND_IN, **kwargs):
    kwargs.setdefault("width", WIDTH)
    kwargs.setdefault("height", HEIGHT)
    kwargs.setdefault("fps", FPS)
    kwargs.setdefault("bitrate_bps", 1_000_000)
    return Pipeline(chain, **kwargs)


def _variant(**changes):
    """The stand-in with one thing about it changed: the way to get a chain
    that fails in one particular way on a machine without the hardware."""
    fields = {name: getattr(STAND_IN, name) for name in STAND_IN.__dataclass_fields__}
    fields.update(changes)
    return Chain(**fields)


# --- the image ---------------------------------------------------------------------


def test_the_test_image_has_gstreamer_and_the_stand_in():
    """If this one fails, the others here say nothing: rebuild the image
    from `Dockerfile.dev`."""
    Gst, GstVideo = live_encoder._load_gst()
    assert Gst.version()[0] == 1
    missing = [
        name for name in ("appsrc", "appsink", "videoconvert", "h264parse", "x264enc")
        if Gst.ElementFactory.find(name) is None
    ]
    assert missing == []
    assert hasattr(GstVideo, "video_event_new_upstream_force_key_unit")


# --- GStreamer beside ROS, in one process ------------------------------------------
#
# GStreamer 1.20 and 1.24 bring a second C++ unwinder into the process
# (`libunwind.so.8`), and a library loaded after it crashes on its first
# exception. `live_encoder._pin_the_unwinder` prevents that. Each case runs in
# a fresh interpreter: what is loaded in which order is the whole subject, and
# a crash must not take the suite with it.


def _fresh_interpreter(script):
    return subprocess.run(
        [sys.executable, "-X", "faulthandler", "-c", script],
        capture_output=True, text=True, timeout=120,
        cwd=str(pathlib.Path(__file__).resolve().parents[1]),
    )


def test_a_ros_node_can_be_created_after_gstreamer_was_loaded():
    """Not the bridge's order, which has its node long before the first
    frame is encoded, but the order of this suite and of anything else that
    imports the encoder first. Fast DDS throws and catches an exception while
    it starts, so this is where a wrong unwinder shows at once."""
    done = _fresh_interpreter(
        "import numpy\n"
        "from fleetless_bridge import live_encoder\n"
        "live_encoder._load_gst()\n"
        "import rclpy\n"
        "rclpy.init()\n"
        "node = rclpy.create_node('fleetless_bridge_test_after_gstreamer')\n"
        "node.destroy_node()\n"
        "rclpy.shutdown()\n"
        "print('DONE')\n"
    )
    assert done.returncode == 0 and "DONE" in done.stdout, done.stderr[-1500:]


def test_a_library_loaded_after_gstreamer_can_still_raise_an_error():
    """The bridge's order: the node first, GStreamer with the first frame of
    a hardware session, and then whatever is loaded later, a message type
    for a new datapoint or, here, an image library, raises its first C++
    exception."""
    done = _fresh_interpreter(
        "import numpy\n"
        "import rclpy\n"
        "rclpy.init()\n"
        "node = rclpy.create_node('fleetless_bridge_test_before_gstreamer')\n"
        "from fleetless_bridge import live_encoder\n"
        "from fleetless_bridge.live_encoder import Pipeline, STAND_IN\n"
        "import av, fractions\n"
        "frame = av.VideoFrame.from_ndarray(numpy.zeros((48, 64, 3), numpy.uint8), format='rgb24')\n"
        "frame.pts, frame.time_base = 0, fractions.Fraction(1, 90000)\n"
        "pipeline = Pipeline(STAND_IN, width=64, height=48, fps=15, bitrate_bps=500000)\n"
        "assert pipeline.encode(frame, False)\n"
        "pipeline.close()\n"
        "import cv2\n"
        "try:\n"
        "    cv2.cvtColor(numpy.zeros((0, 0, 3), numpy.uint8), cv2.COLOR_BGR2RGB)\n"
        "except cv2.error:\n"
        "    print('CAUGHT')\n"
        "try:\n"
        "    rclpy.create_node('not a valid name!')\n"
        "except Exception as exc:\n"
        "    print('RAISED', type(exc).__name__)\n"
        "node.destroy_node()\n"
        "rclpy.shutdown()\n"
        "print('DONE')\n"
    )
    assert done.returncode == 0, done.stderr[-1500:]
    assert done.stdout.split() == ["CAUGHT", "RAISED", "InvalidNodeNameException", "DONE"]


# --- what every chain has to deliver ---------------------------------------------


def test_one_frame_in_is_one_access_unit_out_in_annex_b():
    pipeline = _pipeline()
    decoder = av.CodecContext.create("h264", "r")
    try:
        outputs = [pipeline.encode(moving_frame(index), False) for index in range(20)]
        decoded = [len(decoder.decode(av.Packet(data))) for data in outputs]
    finally:
        pipeline.close()
    assert all(type(data) is bytes and data for data in outputs)
    # A byte stream: every unit behind a start code.
    assert all(data[:4] == b"\x00\x00\x00\x01" or data[:3] == b"\x00\x00\x01" for data in outputs)
    # One picture's worth each, and each on its own: no frame is held back
    # for a later one, and none comes out in two parts.
    assert all(NAL_IDR in nal_types(data) or NAL_SLICE in nal_types(data) for data in outputs)
    assert decoded == [1] * 20


def test_the_first_frame_and_every_forced_one_is_a_keyframe_with_its_parameter_sets():
    forced_at = {7, 8, 15}
    pipeline = _pipeline()
    try:
        types = [
            nal_types(pipeline.encode(moving_frame(index), index in forced_at))
            for index in range(20)
        ]
    finally:
        pipeline.close()
    keyframes = {index for index, units in enumerate(types) if NAL_IDR in units}
    # Exactly the first and the forced: asking is what makes one, and
    # nothing else does within these few frames.
    assert keyframes == {0} | forced_at
    for index in keyframes:
        assert {NAL_SPS, NAL_PPS} <= set(types[index]), index
        # In the order a decoder that joins here needs them.
        units = types[index]
        assert units.index(NAL_SPS) < units.index(NAL_PPS) < units.index(NAL_IDR)
    for index in set(range(20)) - keyframes:
        assert NAL_SLICE in types[index] and NAL_SPS not in types[index]


def test_what_comes_out_decodes_back_to_the_moving_pattern():
    pipeline = _pipeline(bitrate_bps=3_000_000)
    decoder = av.CodecContext.create("h264", "r")
    worst = 0.0
    try:
        for index in range(15):
            (picture,) = decoder.decode(av.Packet(pipeline.encode(moving_frame(index), False)))
            shown = picture.to_ndarray(format="rgb24").astype(np.int16)
            assert shown.shape == (HEIGHT, WIDTH, 3)
            worst = max(worst, float(np.abs(shown - moving_picture(index)).mean()))
            # It is this frame and not its neighbour: the bar has moved on.
            other = float(np.abs(shown - moving_picture(index + 8)).mean())
            assert other > 4 * np.abs(shown - moving_picture(index)).mean()
    finally:
        pipeline.close()
    assert worst < 12.0


# --- a picture of any width ------------------------------------------------------------
#
# GStreamer reads a buffer by its caps: rows padded to four bytes. A picture
# packed tightly is that layout only at widths that are a multiple of eight.


@pytest.mark.parametrize(
    "width, height",
    [(642, 480), (854, 480), (900, 600), (1022, 768), (1026, 770), (1366, 768), (426, 240)],
)
def test_a_picture_whose_width_is_not_a_multiple_of_eight_decodes_back_to_the_pattern(width, height):
    pipeline = _pipeline(width=width, height=height, bitrate_bps=3_000_000)
    decoder = av.CodecContext.create("h264", "r")
    worst = 0.0
    try:
        for index in range(6):
            data = pipeline.encode(moving_frame(index, width, height), False)
            (picture,) = decoder.decode(av.Packet(data))
            shown = picture.to_ndarray(format="rgb24").astype(np.int16)
            assert shown.shape == (height, width, 3)
            worst = max(worst, float(np.abs(shown - moving_picture(index, width, height)).mean()))
    finally:
        pipeline.close()
    # A row read from the wrong place shears the picture; this is far above it.
    assert worst < 12.0


def test_the_layout_is_the_one_gstreamer_reads_from_the_caps():
    """Against `GstVideo.VideoInfo` of this distribution's GStreamer, which
    is what an element maps a buffer without a layout of its own by. Odd
    sizes too: the configuration allows any."""
    Gst, GstVideo = live_encoder._load_gst()
    for width in (1, 2, 3, 5, 16, 62, 320, 641, 642, 854, 900, 1022, 1024, 1026, 1366, 7680):
        for height in (1, 2, 3, 48, 240, 479, 480, 600, 730, 768, 4320):
            info = GstVideo.VideoInfo.new_from_caps(
                Gst.Caps.from_string(
                    "video/x-raw,format=I420,width={},height={},framerate=15/1".format(width, height)
                )
            )
            assert live_encoder.gst_i420_layout(width, height) == (
                tuple(info.stride[:3]), tuple(info.offset[:3]), info.size,
            ), (width, height)


def test_the_planes_are_copied_row_by_row_to_where_the_layout_puts_them():
    """6x4: luma rows of 6 in a stride of 8, chroma rows of 3 in a stride of
    4. Every sample has a value of its own, so a row that lands in the wrong
    place shows."""
    frame = av.VideoFrame(6, 4, "yuv420p")
    luma = np.arange(1, 25, dtype=np.uint8).reshape(4, 6)
    blue = np.arange(101, 107, dtype=np.uint8).reshape(2, 3)
    red = np.arange(201, 207, dtype=np.uint8).reshape(2, 3)
    for plane, samples in zip(frame.planes, (luma, blue, red)):
        rows = np.frombuffer(plane, np.uint8).reshape(-1, plane.line_size)
        rows[:samples.shape[0], :samples.shape[1]] = samples

    packed = np.frombuffer(live_encoder.i420_for_gst(frame), np.uint8)
    strides, offsets, size = live_encoder.gst_i420_layout(6, 4)
    assert (strides, offsets, size) == ((8, 4, 4), (0, 32, 40), 48) and len(packed) == 48
    assert (packed[0:32].reshape(4, 8)[:, :6] == luma).all()
    assert (packed[32:40].reshape(2, 4)[:, :3] == blue).all()
    assert (packed[40:48].reshape(2, 4)[:, :3] == red).all()
    # The padding is zero, whatever libav left beside its rows.
    assert not packed[0:32].reshape(4, 8)[:, 6:].any()
    assert not packed[32:48].reshape(4, 4)[:, 3:].any()


def test_a_bitrate_change_takes_effect_while_running():
    pipeline = _pipeline(bitrate_bps=2_000_000)
    try:
        high = [len(pipeline.encode(moving_frame(index), False)) for index in range(45)]
        pipeline.set_bitrate(100_000)
        low = [len(pipeline.encode(moving_frame(index), False)) for index in range(45, 90)]
        assert pipeline.bitrate_bps == 100_000
        pipeline.set_bitrate(2_000_000)
        back = [len(pipeline.encode(moving_frame(index), False)) for index in range(90, 135)]
    finally:
        pipeline.close()
    # The last second of each stretch, once the rate control has settled.
    high, low, back = (sum(sizes[-15:]) for sizes in (high, low, back))
    assert low * 4 < high
    assert low * 4 < back


def test_the_frame_rate_of_the_camera_is_what_the_bitrate_is_divided_by():
    """The same bitrate at 2 frames a second is many more bytes a frame than
    at 30: the caps carry the camera's rate, not a constant."""

    def bytes_per_frame(fps):
        pipeline = _pipeline(fps=fps, bitrate_bps=500_000)
        try:
            sizes = [
                len(pipeline.encode(moving_frame(index, fps=fps), False)) for index in range(40)
            ]
        finally:
            pipeline.close()
        return sum(sizes[-15:]) / 15

    assert bytes_per_frame(2) > 3 * bytes_per_frame(30)


# --- the end of a pipeline ----------------------------------------------------------


def test_close_leaves_the_pipeline_in_null_and_can_be_called_again():
    pipeline = _pipeline()
    pipeline.encode(moving_frame(0), False)
    assert not pipeline.closed and pipeline.final_state is None
    pipeline.close()
    assert pipeline.closed and pipeline.final_state == "null"
    pipeline.close()
    with pytest.raises(ChainFailed) as refused:
        pipeline.encode(moving_frame(1), False)
    assert "closed" in refused.value.reason


def test_a_pipeline_that_never_saw_a_frame_closes_too():
    pipeline = _pipeline()
    pipeline.close()
    assert pipeline.final_state == "null"


# --- the ways a chain fails ----------------------------------------------------------


def test_a_chain_whose_element_is_missing_says_which():
    """The state of every machine without that chip, this one included."""
    for chain in [chain for chains in CHAINS.values() for chain in chains]:
        with pytest.raises(ChainFailed) as failed:
            _pipeline(chain)
        assert failed.value.reason.startswith("GStreamer has no element '"), chain.name
        assert failed.value.reason.split("'")[1] in chain.elements, chain.name


def test_a_chain_that_cannot_be_put_together_fails_at_construction():
    with pytest.raises(ChainFailed) as failed:
        _pipeline(_variant(caps="video/x-h264 ! nonsense-property-here=1 !"))
    assert failed.value.reason.startswith("the pipeline could not be built (")


def test_a_chain_that_does_not_start_fails_at_construction_with_what_the_element_said():
    # A branch that writes to a directory that is not there fails when the
    # pipeline is started, the way an encoder device that cannot be opened does.
    broken = _variant(
        convert="tee name=t t. ! queue ! filesink location=/nonexistent-894/out "
        "t. ! queue ! videoconvert",
        elements=("tee", "queue", "filesink", "videoconvert", "x264enc"),
    )
    with pytest.raises(ChainFailed) as failed:
        _pipeline(broken)
    assert "/nonexistent-894/out" in failed.value.reason
    assert failed.value.reason.split(":")[0].startswith("filesink")


def test_a_chain_that_reports_an_error_while_running_fails_with_what_the_element_said():
    broken = _variant(
        convert="videoconvert ! identity name=breaks error-after=3",
        elements=("videoconvert", "identity", "x264enc"),
    )
    pipeline = _pipeline(broken, frame_timeout_s=0.3)
    try:
        assert pipeline.encode(moving_frame(0), False)
        assert pipeline.encode(moving_frame(1), False)
        with pytest.raises(ChainFailed) as failed:
            pipeline.encode(moving_frame(2), False)
            pipeline.encode(moving_frame(3), False)
    finally:
        pipeline.close()
    assert failed.value.reason.startswith("breaks: ")
    assert pipeline.final_state == "null"


def test_a_chain_that_delivers_nothing_fails_after_the_time_limit():
    silent = _variant(
        convert="videoconvert ! identity drop-probability=1.0",
        elements=("videoconvert", "identity", "x264enc"),
    )
    pipeline = _pipeline(silent, frame_timeout_s=0.2)
    try:
        with pytest.raises(ChainFailed) as failed:
            pipeline.encode(moving_frame(0), False)
    finally:
        pipeline.close()
    assert failed.value.reason == "no access unit within 0.2 s of a frame"


def test_a_setting_the_installed_element_does_not_have_is_left_out():
    """The same encoder element has gained and lost properties between
    GStreamer versions. A missing tuning knob is not a missing encoder."""
    chain = _variant(settings=STAND_IN.settings + (("no-such-property-894", "1"),))
    pipeline = _pipeline(chain)
    try:
        assert pipeline.encode(moving_frame(0), False)
    finally:
        pipeline.close()


def test_the_chains_are_what_the_spec_lists_in_the_order_it_lists_them():
    assert [chain.encoder for chain in CHAINS["nvenc"]] == ["nvv4l2h264enc", "nvh264enc"]
    assert [chain.encoder for chain in CHAINS["vaapi"]] == ["vah264enc", "vaapih264enc"]
    assert [chain.encoder for chain in CHAINS["v4l2"]] == ["v4l2h264enc"]
    assert live_encoder.AUTO_ORDER == ("nvenc", "vaapi", "v4l2")
    assert [chain.name for chain in live_encoder.chains_for("auto")] == [
        "nvenc/jetson", "nvenc/desktop", "vaapi/va", "vaapi/vaapi", "v4l2",
    ]
    assert live_encoder.chains_for("nvenc") == CHAINS["nvenc"]
    assert live_encoder.chains_for("software") == ()
    names = [chain.name for chains in CHAINS.values() for chain in chains] + [STAND_IN.name]
    assert len(set(names)) == len(names)
    for backend, chains in CHAINS.items():
        assert all(chain.backend == backend and chain.encoder in chain.elements for chain in chains)
    # Jetson asks for a keyframe by the element's own signal, the rest by
    # the event; every chain names how its bitrate is set.
    assert CHAINS["nvenc"][0].keyframe == "force-IDR"
    assert all(
        chain.keyframe == "event"
        for chains in CHAINS.values() for chain in chains if chain.name != "nvenc/jetson"
    )
    assert STAND_IN.backend not in CHAINS

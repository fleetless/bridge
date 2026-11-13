# SPDX-License-Identifier: Apache-2.0
"""The bridge's own H.264 encoder for live video: the robot's encoder chip,
reached through a GStreamer pipeline, behind the interface aiortc's sender
already calls.

`live.py` has the seam: a `LivePublisher` that is handed an encoder puts it
into the sender, offers H.264 only and closes it. This module is what gets
handed over when `live.encoder` is not `software`.

**Three layers, each usable without the one above.**

- A `Chain` is one way to reach one encoder: the elements between the frames
  pushed in and the parser, and how that encoder is told its bitrate and
  asked for a keyframe. `CHAINS` lists them per backend, in the order they
  are tried.
- A `Pipeline` runs one chain: `appsrc`, the chain, `h264parse`, `appsink`.
  One frame in, one access unit out, as an Annex-B byte string. No main
  loop and no process of the bridge's own: the sample is pulled with a
  timeout and the bus is read with every frame.
- `HardwareH264Encoder` is aiortc's `H264Encoder` with `_encode_frame`
  replaced. It picks the chain, falls back to aiortc's own libx264 when no
  chain works, and remembers per process which chains did not.

**GStreamer is not a dependency of this package.** `gi` is imported inside
`_load_gst`, on the first frame of a session whose encoder is not
`software`, and nowhere else. A robot without it encodes in software and is
told which packages are missing.

**GStreamer starts one child process of its own, and writes one file.**
When its plugin registry is missing or stale, `Gst.init` runs GStreamer's
plugin scanner (`gst-plugin-scanner`) as a child and writes the registry to
the user's cache, `$XDG_CACHE_HOME/gstreamer-1.0/`. That is at the first
hardware session, after an update of a GStreamer package, and at every start
where that directory cannot be written. Nothing from a cloud message reaches
the scanner's command line. `GST_REGISTRY_FORK=no` would spare the child by
loading every installed plugin into the bridge's own process, and is not
set. `SECURITY.md` names both.

**No chain here is proven on its hardware by this package's own tests.**
Each is written to the documentation of its elements. The machines this is
developed and tested on have no encoder chip, so the suite runs the same
`Pipeline` and the same encoder class over `STAND_IN`, a software encoder
element. `tools/encoder-proof/` is how a chain is proven on the hardware
that has it, and the documentation lists the ones that have been.

No log line here carries a camera URL, a credential or a token: nothing in
this module ever holds one. What GStreamer says about a failure is logged as
it says it; that is element names, device paths and negotiation errors.
"""
from __future__ import annotations

import ctypes
import logging
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

import av
import numpy as np
from aiortc.codecs.h264 import H264Encoder

log = logging.getLogger(__name__)

#: What the bitrate target accepts, in bits per second: 1 kbps to 50 Mbps.
#: These are the bounds of `bitrate_kbps` in the configuration document (a
#: positive integer, at most 50000), not the 500 kbps to 3 Mbps aiortc clamps
#: its libx264 to. A camera configured for 8 Mbps on a chip that can do it
#: should get 8 Mbps. Contracts exports no constant for the two numbers;
#: `test_live_encoder.py` holds them against the vendored schema.
MIN_BITRATE_BPS = 1_000
MAX_BITRATE_BPS = 50_000_000

#: The target a new encoder starts at, until the publisher's budget arrives
#: a moment later. aiortc's own default for H.264.
DEFAULT_BITRATE_BPS = 1_000_000

#: A change of the target smaller than this share of what the pipeline runs
#: at is not passed on. The receiver's estimate and the publisher's budget
#: take turns on the same attribute every second; aiortc applies the same
#: tenth before it rebuilds its libx264.
BITRATE_CHANGE_SHARE = 0.1

#: How long one frame may take to come back as an access unit. A chain that
#: needs longer is not a low-latency encoder, or is not encoding at all.
FRAME_TIMEOUT_S = 1.0

#: How long a pipeline may take to start. Starting one opens the encoder
#: device, and a driver call that does not return must not keep the thread
#: that asked.
START_TIMEOUT_S = 5.0

#: How long a pipeline may take to stop. One that takes longer has a thread
#: inside an element that does not return; its chain is not built again.
STOP_TIMEOUT_S = 2.0

#: Frames between two keyframes nobody asked for. Viewers ask when they need
#: one, so this is the backstop, at libx264's own default.
KEYFRAME_INTERVAL_FRAMES = 250

#: The Debian packages every chain needs, besides the plugin of its encoder.
GST_PACKAGES = (
    "python3-gi",
    "gir1.2-gstreamer-1.0",
    "gir1.2-gst-plugins-base-1.0",
    "gstreamer1.0-plugins-base",
    "gstreamer1.0-plugins-bad",
)

#: The colours of the pictures pushed into a chain, in GStreamer's name for
#: it: BT.601, limited range. That is what libswscale makes of an RGB frame
#: when it is asked for `yuv420p` and told nothing else, at every picture
#: size, and it is what aiortc's own encoders are fed. The chain is told, so
#: that an encoder which labels its stream labels it with what is in it; a
#: viewer converts by that label. `test_live_encoder_pipeline.py` holds the
#: two against each other: the samples that arrive, and the label on them.
PICTURE_COLORIMETRY = "bt601"

_NAL_ACCESS_UNIT_DELIMITER = 9


class GstUnavailable(Exception):
    """GStreamer or its Python bindings cannot be loaded on this machine.
    `reason` is a sentence for the log and names the packages."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class ChainFailed(Exception):
    """A chain could not be built, stopped working, or returned nothing in
    time. `reason` is a short sentence for the one warning a fallback logs."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


# --- the chains ----------------------------------------------------------------


@dataclass(frozen=True)
class Chain:
    """One way to reach one H.264 encoder.

    What every chain has to deliver, whatever is behind it:

    - an Annex-B byte stream, one access unit out for one frame in: no
      B-frames, no lookahead;
    - SPS and PPS in the stream with every keyframe (`h264parse` after the
      chain repeats them, so a chain only has to produce them once);
    - a keyframe when asked: by the force-key-unit event, or by the
      element's own signal where it has one;
    - a bitrate that follows the target while running.

    `settings` are properties of the encoder, as text, the way `gst-launch`
    would take them. One that the installed element does not have is left
    out and the chain is built without it: the same element has grown and
    lost properties between GStreamer 1.20 and 1.28, and a missing tuning
    knob is not a missing encoder. `{kbps}`, `{bps}` and `{gop}` stand for
    the bitrate in either unit and the keyframe interval.
    """

    #: What the log and the proof tool call it.
    name: str
    #: The value of `live.encoder` it belongs to.
    backend: str
    #: Everything GStreamer has to have for this chain, by element name.
    elements: Tuple[str, ...]
    #: What turns the I420 frames pushed in into what the encoder takes, in
    #: `gst-launch` text.
    convert: str
    #: The encoder element.
    encoder: str
    #: Properties set on the encoder when the pipeline is built.
    settings: Tuple[Tuple[str, str], ...]
    #: The property that carries the bitrate, and its value. Set again
    #: whenever the target moves.
    bitrate: Tuple[str, str]
    #: What the encoder's output is narrowed to, as caps.
    caps: str = "video/x-h264,profile=(string){constrained-baseline,baseline}"
    #: How a keyframe is asked for: `event`, or the name of an action signal
    #: of the encoder.
    keyframe: str = "event"


#: The chains of each backend, in the order they are tried.
CHAINS: Dict[str, Tuple[Chain, ...]] = {
    "nvenc": (
        # Jetson. Written for an Orin NX on L4T 36.4, the first hardware it
        # is to be proven on.
        Chain(
            name="nvenc/jetson",
            backend="nvenc",
            elements=("nvvidconv", "nvv4l2h264enc"),
            convert="nvvidconv ! video/x-raw(memory:NVMM),format=NV12",
            encoder="nvv4l2h264enc",
            settings=(
                ("control-rate", "1"),  # constant bitrate
                ("preset-level", "1"),  # the fastest
                ("profile", "0"),  # baseline
                ("num-B-Frames", "0"),
                ("insert-sps-pps", "true"),
                # Picture order as it is sent, and the VUI that says so: a
                # browser then shows each frame as it arrives instead of
                # holding some back for a reordering that never happens.
                ("poc-type", "2"),
                ("insert-vui", "true"),
                ("iframeinterval", "{gop}"),
                ("idrinterval", "{gop}"),
            ),
            bitrate=("bitrate", "{bps}"),
            # Its caps carry no profile; the property above is what sets it.
            caps="video/x-h264",
            keyframe="force-IDR",
        ),
        # A desktop NVIDIA card. Untested.
        Chain(
            name="nvenc/desktop",
            backend="nvenc",
            elements=("videoconvert", "nvh264enc"),
            convert="videoconvert",
            encoder="nvh264enc",
            settings=(
                ("preset", "low-latency-hq"),
                ("rc-mode", "cbr"),
                ("bframes", "0"),
                ("rc-lookahead", "0"),
                ("zerolatency", "true"),
                ("gop-size", "{gop}"),
            ),
            bitrate=("bitrate", "{kbps}"),
        ),
    ),
    "vaapi": (
        # Intel and AMD, the `va` plugin of newer GStreamer. Untested.
        Chain(
            name="vaapi/va",
            backend="vaapi",
            elements=("videoconvert", "vah264enc"),
            convert="videoconvert ! video/x-raw,format=NV12",
            encoder="vah264enc",
            settings=(
                ("rate-control", "cbr"),
                ("b-frames", "0"),
                ("key-int-max", "{gop}"),
            ),
            bitrate=("bitrate", "{kbps}"),
        ),
        # The same hardware through gstreamer-vaapi, where `va` is not
        # there yet. Untested.
        Chain(
            name="vaapi/vaapi",
            backend="vaapi",
            elements=("videoconvert", "vaapih264enc"),
            convert="videoconvert ! video/x-raw,format=NV12",
            encoder="vaapih264enc",
            settings=(
                ("rate-control", "cbr"),
                ("max-bframes", "0"),
                ("keyframe-period", "{gop}"),
            ),
            bitrate=("bitrate", "{kbps}"),
        ),
    ),
    "v4l2": (
        # A V4L2 stateful encoder, as on a Raspberry Pi 4. Untested. Its
        # bitrate is a control of the device, and the controls are one
        # property that is set whole.
        Chain(
            name="v4l2",
            backend="v4l2",
            elements=("videoconvert", "v4l2h264enc"),
            convert="videoconvert",
            encoder="v4l2h264enc",
            settings=(),
            bitrate=(
                "extra-controls",
                "controls,repeat_sequence_header=1,h264_i_frame_period={gop},video_bitrate={bps}",
            ),
            # The Pi's encoder negotiates nothing above level 3.1 unless a
            # level is named.
            caps="video/x-h264,profile=(string){constrained-baseline,baseline},level=(string)4",
        ),
    ),
}

#: The order `auto` tries the backends in. Vendor-specific elements come
#: before the generic `v4l2`, which on some boards finds a device it cannot
#: drive.
AUTO_ORDER = ("nvenc", "vaapi", "v4l2")

#: The software stand-in: libx264 as a GStreamer element. Not a value of
#: `live.encoder` and never chosen on a robot; it is what the suite and the
#: tools run the real pipeline and the real encoder class over, on machines
#: without an encoder chip.
STAND_IN = Chain(
    name="stand-in/x264",
    backend="stand-in",
    elements=("videoconvert", "x264enc"),
    convert="videoconvert",
    encoder="x264enc",
    settings=(
        ("tune", "zerolatency"),
        ("speed-preset", "ultrafast"),
        ("key-int-max", "{gop}"),
    ),
    bitrate=("bitrate", "{kbps}"),
)


def chains_for(setting: str) -> Tuple[Chain, ...]:
    """The chains a value of `live.encoder` stands for, in the order they
    are tried. None for `software`."""
    if setting == "auto":
        return tuple(chain for backend in AUTO_ORDER for chain in CHAINS[backend])
    return CHAINS.get(setting, ())


# --- GStreamer, loaded when it is first needed -----------------------------------

_gst: Optional[Tuple[Any, Any]] = None
_gst_lock = threading.Lock()


def _pin_the_unwinder() -> None:
    """Keep the C++ exception unwinder of this process the one it has: gcc's.

    GStreamer 1.20 and 1.24 link `libunwind.so.8` for their own stack
    traces; 1.28 no longer does. That library also exports
    the unwinder's entry points, `_Unwind_Resume` among them, and `gi` loads
    it where every library loaded afterwards looks first. Such a library
    then unwinds its exceptions half with libunwind and half with libgcc,
    which is a segmentation fault the first time one of its exceptions
    passes a cleanup. A ROS node created after GStreamer was loaded dies of
    it at once, because Fast DDS throws and catches inside its own start; a
    message type or an image library loaded later dies of it at its first
    error.

    Putting `libgcc_s` into that same place first settles it: whoever looks
    for the unwinder finds gcc's, before GStreamer's dependency is there to
    be found. It changes nothing for libraries already loaded, and GStreamer
    uses libunwind through names of its own.

    Where GStreamer links libunwind, both are in the process long before
    this module does anything: OpenCV, which the bridge imports for its
    cameras, maps GStreamer for its own video input. That is harmless,
    because it maps them for itself alone. It is `gi` that puts them where
    everybody looks.

    `test_live_encoder_pipeline.py` loads GStreamer and then makes a ROS
    node and an image library error, each in a fresh interpreter. Without
    this function both die wherever GStreamer links that library.
    """
    try:
        ctypes.CDLL("libgcc_s.so.1", mode=ctypes.RTLD_GLOBAL)
    except OSError as exc:
        # No such library where there is no gcc runtime to clash with.
        log.debug("libgcc_s could not be loaded ahead of GStreamer: %s", type(exc).__name__)


def _load_gst() -> Tuple[Any, Any]:
    """`(Gst, GstVideo)`, imported and initialised on first use.

    The only place in the package that imports `gi`. Raises
    `GstUnavailable` with the packages to install when the bindings or a
    typelib are missing; that is a fallback like any other, not an error of
    the bridge."""
    global _gst
    with _gst_lock:
        if _gst is not None:
            return _gst
        _pin_the_unwinder()
        try:
            import gi

            gi.require_version("Gst", "1.0")
            gi.require_version("GstApp", "1.0")
            gi.require_version("GstVideo", "1.0")
            from gi.repository import Gst, GstApp, GstVideo  # noqa: F401 - GstApp registers appsrc's signals
        except ImportError:
            missing = "the Python module gi is not installed"
        except ValueError:
            # What `require_version` raises for a namespace it cannot find.
            missing = "a GStreamer typelib is not installed"
        else:
            missing = None
        if missing is not None:
            raise GstUnavailable(
                "GStreamer cannot be loaded: {}. The packages are {}, and the plugin of "
                "the encoder".format(missing, ", ".join(GST_PACKAGES))
            )
        if not Gst.is_initialized():
            Gst.init(None)
        _gst = (Gst, GstVideo)
        return _gst


# --- a picture, laid out the way GStreamer reads it ----------------------------------


def _round_up(value: int, multiple: int) -> int:
    return -(-value // multiple) * multiple


def gst_i420_layout(width: int, height: int) -> Tuple[Tuple[int, int, int], Tuple[int, int, int], int]:
    """`(strides, offsets, size)` of an I420 picture in a GStreamer buffer
    that carries no layout of its own.

    An element that is handed such a buffer reads it by the caps alone, and
    for I420 that means rows padded to a multiple of four bytes, in the
    luma plane and in each chroma plane, and an even number of luma rows.
    A picture packed tightly is that layout only where its width is a
    multiple of eight; at any other width it is too short, and every row
    after the first is read from the wrong place.

    The numbers are `gst_video_info_set_format`'s, and the pipeline's tests
    hold them against `GstVideo.VideoInfo` of each GStreamer the bridge
    runs on.
    """
    luma_stride = _round_up(width, 4)
    chroma_stride = _round_up(_round_up(width, 2) // 2, 4)
    luma_rows = _round_up(height, 2)
    chroma_rows = luma_rows // 2
    second = luma_stride * luma_rows
    third = second + chroma_stride * chroma_rows
    return (
        (luma_stride, chroma_stride, chroma_stride),
        (0, second, third),
        third + chroma_stride * chroma_rows,
    )


def i420_for_gst(picture: "av.VideoFrame") -> bytes:
    """The three planes of a `yuv420p` frame as the bytes of one GStreamer
    buffer, each row padded to where `gst_i420_layout` says the next
    begins. The padding is zero and is never shown."""
    strides, offsets, size = gst_i420_layout(picture.width, picture.height)
    packed = np.zeros(size, dtype=np.uint8)
    for plane, stride, offset in zip(picture.planes, strides, offsets):
        # libav pads its rows too, to a width of its own choosing.
        rows = np.frombuffer(plane, np.uint8).reshape(-1, abs(plane.line_size))
        packed[offset:offset + stride * plane.height].reshape(plane.height, stride)[
            :, :plane.width
        ] = rows[:plane.height, :plane.width]
    return packed.tobytes()


# --- one chain, running -----------------------------------------------------------


class Pipeline:
    """One chain as a running GStreamer pipeline: frames in, access units
    out.

    Built and used from one thread at a time; `HardwareH264Encoder` holds a
    lock around every call. Raises `ChainFailed` from the constructor when
    the chain cannot be built or does not start, and from `encode` when it
    stops delivering.
    """

    def __init__(
        self,
        chain: Chain,
        *,
        width: int,
        height: int,
        fps: int,
        bitrate_bps: int,
        frame_timeout_s: float = FRAME_TIMEOUT_S,
    ) -> None:
        Gst, GstVideo = _load_gst()
        from gi.repository import GLib  # there once `_load_gst` has returned

        self._Gst = Gst
        self._GstVideo = GstVideo
        self._GLib = GLib
        self.chain = chain
        self.size = (width, height)
        self.bitrate_bps = bitrate_bps
        self._frame_timeout_ns = int(frame_timeout_s * Gst.SECOND)
        self._frame_timeout_s = frame_timeout_s
        self._frame_ns = Gst.SECOND // max(1, fps)
        self._last_pts_ns = -1
        self._keyframes_asked = 0
        self._pipeline = None

        for element in ("appsrc", "h264parse", "appsink") + chain.elements:
            if Gst.ElementFactory.find(element) is None:
                raise ChainFailed("GStreamer has no element {!r}".format(element))

        launch = (
            "appsrc name=src ! {convert} ! {encoder} name=enc ! {caps} ! "
            "h264parse name=parse config-interval=-1 ! "
            "video/x-h264,stream-format=byte-stream,alignment=au ! appsink name=sink"
        ).format(convert=chain.convert, encoder=chain.encoder, caps=chain.caps)
        try:
            pipeline = Gst.parse_launch(launch)
        except Exception as exc:  # noqa: BLE001 - GLib.Error, by whatever name this PyGObject gives it
            raise ChainFailed("the pipeline could not be built ({})".format(_message(exc))) from None
        self._pipeline = pipeline
        try:
            self._src = pipeline.get_by_name("src")
            self._enc = pipeline.get_by_name("enc")
            self._sink = pipeline.get_by_name("sink")
            self._bus = pipeline.get_bus()

            # The frame rate is what the rate control divides the bitrate
            # by, so it is the camera's and not a constant. The colorimetry
            # is what the pictures are in (`PICTURE_COLORIMETRY`): caps that
            # leave it out are read as BT.709 above 576 lines, and the
            # encoder then writes that into the stream.
            self._src.set_property(
                "caps",
                Gst.Caps.from_string(
                    "video/x-raw,format=I420,width={},height={},framerate={}/1,"
                    "colorimetry={}".format(width, height, max(1, fps), PICTURE_COLORIMETRY)
                ),
            )
            self._src.set_property("format", Gst.Format.TIME)
            self._src.set_property("is-live", True)
            self._src.set_property("block", False)
            # Nothing waits for a clock and nothing is dropped: what comes
            # out is pulled at once, one unit for one frame.
            self._sink.set_property("sync", False)
            self._sink.set_property("async", False)
            self._sink.set_property("drop", False)
            self._sink.set_property("max-buffers", 8)

            for name, value in chain.settings:
                self._set(name, value)
            self._set(*chain.bitrate)

            if pipeline.set_state(Gst.State.PLAYING) == Gst.StateChangeReturn.FAILURE:
                raise ChainFailed(self._bus_error() or "the pipeline did not start")
            # A device that cannot be opened says so on the bus, during the
            # state change that has just returned.
            error = self._bus_error()
            if error is not None:
                raise ChainFailed(error)
        except BaseException:
            # What is raised is why it did not start; how the remains went
            # away adds nothing to that.
            try:
                self.close()
            except Exception:  # noqa: BLE001 - the first failure is the one that is reported
                pass
            raise

    # -- what the encoder is told ----------------------------------------------

    def _set(self, name: str, template: str) -> None:
        """One property of the encoder, from text. A property this element
        does not have is left out."""
        if self._enc.find_property(name) is None:
            log.debug("%s has no property %r; left out", self.chain.encoder, name)
            return
        value = template.format(
            kbps=max(1, self.bitrate_bps // 1000),
            bps=self.bitrate_bps,
            gop=KEYFRAME_INTERVAL_FRAMES,
        )
        self._Gst.util_set_object_arg(self._enc, name, value)

    def set_bitrate(self, bitrate_bps: int) -> None:
        """Re-target the running encoder."""
        self.bitrate_bps = bitrate_bps
        self._set(*self.chain.bitrate)

    def _ask_for_keyframe(self) -> None:
        if self.chain.keyframe == "event":
            self._keyframes_asked += 1
            # Upstream from the sink, so the parser sees it too and repeats
            # the parameter sets. No running time: the next frame.
            self._sink.send_event(
                self._GstVideo.video_event_new_upstream_force_key_unit(
                    self._Gst.CLOCK_TIME_NONE, True, self._keyframes_asked
                )
            )
        else:
            self._enc.emit(self.chain.keyframe)

    # -- one frame ----------------------------------------------------------------

    def encode(self, frame: "av.VideoFrame", force_keyframe: bool) -> bytes:
        """Push one frame, pull the access unit it becomes. Annex B."""
        Gst = self._Gst
        if self._pipeline is None:
            raise ChainFailed("the pipeline is closed")
        picture = frame if frame.format.name == "yuv420p" else frame.reformat(format="yuv420p")
        buffer = Gst.Buffer.new_wrapped_bytes(self._GLib.Bytes.new(i420_for_gst(picture)))
        buffer.pts = self._pts_ns(frame)
        buffer.duration = self._frame_ns
        if force_keyframe:
            self._ask_for_keyframe()
        flow = self._src.emit("push-buffer", buffer)
        if flow != Gst.FlowReturn.OK:
            raise ChainFailed(
                self._bus_error() or "the pipeline refused a frame ({})".format(flow.value_nick)
            )
        sample = self._sink.emit("try-pull-sample", self._frame_timeout_ns)
        if sample is None:
            raise ChainFailed(
                self._bus_error()
                or "no access unit within {:g} s of a frame".format(self._frame_timeout_s)
            )
        encoded = sample.get_buffer()
        data = encoded.extract_dup(0, encoded.get_size())
        # Once a frame, also when the frame went well. An element that
        # reports an error while units still come is giving up, and is
        # believed now and not at the next stall; and reading the bus is
        # what empties it.
        error = self._bus_error()
        if error is not None:
            raise ChainFailed(error)
        return data

    def _pts_ns(self, frame: "av.VideoFrame") -> int:
        """The frame's own time, in nanoseconds, and never backwards."""
        pts_ns = self._last_pts_ns + self._frame_ns
        if frame.pts is not None and frame.time_base is not None:
            pts_ns = max(self._last_pts_ns + 1, int(frame.pts * frame.time_base * self._Gst.SECOND))
        self._last_pts_ns = pts_ns
        return pts_ns

    def _bus_error(self) -> Optional[str]:
        """What an element reported, if one did: `element: message`.

        Reading it throws away everything else that was posted before:
        state changes, warnings, whatever an element has to say. Nobody
        else reads this bus, so without that a talkative encoder element
        would fill it for as long as the session lasts."""
        message = self._bus.pop_filtered(self._Gst.MessageType.ERROR)
        if message is None:
            return None
        error, _debug = message.parse_error()
        source = message.src.get_name() if message.src is not None else "pipeline"
        return "{}: {}".format(source, error.message)

    # -- the end -------------------------------------------------------------------

    @property
    def closed(self) -> bool:
        return self._pipeline is None

    def close(self) -> None:
        """To NULL, which releases the device. Safe to call twice."""
        pipeline, self._pipeline = self._pipeline, None
        if pipeline is None:
            return
        Gst = self._Gst
        pipeline.set_state(Gst.State.NULL)
        _, state, _ = pipeline.get_state(Gst.SECOND)
        self.final_state = state.value_nick
        if state != Gst.State.NULL:
            raise ChainFailed("the pipeline did not reach NULL ({})".format(state.value_nick))

    #: The state the pipeline was left in by `close()`; `null` when it went
    #: well. For the tests and the proof tool.
    final_state: Optional[str] = None


def _message(exc: BaseException) -> str:
    """What a GLib error says, without the type GLib wraps it in."""
    return str(getattr(exc, "message", None) or exc) or type(exc).__name__


def _reason(exc: BaseException) -> str:
    """Why a chain failed, for the log. A `ChainFailed` carries a sentence
    this module wrote, from what GStreamer reported about its own elements.
    Anything else is something nobody foresaw, and only its type is said:
    its message is not this module's to vouch for."""
    if isinstance(exc, ChainFailed):
        return exc.reason
    return "failed unexpectedly ({})".format(type(exc).__name__)


# --- what worked and what did not, for the process ---------------------------------


class ChainMemory:
    """Which chains are not worth building, and which one worked.

    A missing chip stays missing: probing it again at every live start costs
    a pipeline build, or a second of waiting, for an answer already known.
    So the outcome of a probe is kept until `clear()`, which the runtime
    calls when `live.encoder` changes. A restart clears it by itself.

    Three findings are kept apart, because they are about different things:

    - A chain that **cannot be built**, or does not start: an element or a
      typelib is missing, the pipeline does not parse, the device does not
      open. That is about the machine, and holds for every camera.
    - A chain whose pipeline **did not shut down** when it was closed. A
      thread of it is still inside an element, and building the chain again
      would leave another there. Kept for every camera too.
    - A chain that was built and **did not encode the first picture** it
      was given. That may be the chip and it may be the picture, and one
      failure cannot tell which. It is kept for pictures of that size: the
      same camera, or another of the same size, is not probed again, and a
      camera of another size is. So a camera whose picture a chip does not
      take encodes in software and does not switch the chip off for the
      others.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # What the log says about a chain that is bad for the process.
        self._unusable: Dict[str, str] = {}
        self._refused: Dict[Tuple[str, Tuple[int, int]], str] = {}
        self._working: Optional[str] = None

    def candidates(
        self, chains: Sequence[Chain], size: Tuple[int, int]
    ) -> Tuple[List[Chain], Dict[str, str]]:
        """`(to try, known bad)` for a picture of `size`: the chains worth
        building, the one that worked before first, and for each of the
        others what failed, in the words the log uses for it."""
        with self._lock:
            to_try: List[Chain] = []
            known_bad: Dict[str, str] = {}
            for chain in chains:
                if chain.name in self._unusable:
                    known_bad[chain.name] = self._unusable[chain.name]
                elif (chain.name, size) in self._refused:
                    known_bad[chain.name] = did_not_encode(
                        chain, size, self._refused[(chain.name, size)]
                    )
                else:
                    to_try.append(chain)
            to_try.sort(key=lambda c: c.name != self._working)  # stable: the rest keep their order
            return to_try, known_bad

    def cannot_be_built(self, chain: Chain, reason: str) -> None:
        """The chain is known bad for the process."""
        self._unusable_because(chain, cannot_be_built(chain, reason))

    def did_not_shut_down(self, chain: Chain, seconds: float) -> None:
        """The chain is known bad for the process, whatever it did before."""
        self._unusable_because(chain, did_not_shut_down(chain, seconds))

    def _unusable_because(self, chain: Chain, said: str) -> None:
        with self._lock:
            self._unusable[chain.name] = said
            if self._working == chain.name:
                self._working = None

    def did_not_encode(self, chain: Chain, size: Tuple[int, int], reason: str) -> None:
        """The chain is known bad for pictures of `size`. If it worked for
        another size it still does."""
        with self._lock:
            self._refused[(chain.name, size)] = reason

    def worked(self, chain: Chain) -> None:
        with self._lock:
            self._working = chain.name

    def stopped_working(self, chain: Chain) -> None:
        """A chain that passed its probe and failed later. It is probed
        again at the next start and not marked bad: a failure after minutes
        of video is not a missing chip."""
        with self._lock:
            if self._working == chain.name:
                self._working = None

    def clear(self) -> None:
        with self._lock:
            self._unusable.clear()
            self._refused.clear()
            self._working = None

    @property
    def working(self) -> Optional[str]:
        with self._lock:
            return self._working


def cannot_be_built(chain: Chain, reason: str) -> str:
    """What the log says about a chain that could not be built or started."""
    return "{} cannot be built: {}".format(chain.name, reason)


def did_not_shut_down(chain: Chain, seconds: float) -> str:
    """What the log says about a chain whose pipeline was closed and did not
    reach NULL in time."""
    return "{} did not shut down within {:g} s".format(chain.name, seconds)


def did_not_encode(chain: Chain, size: Tuple[int, int], reason: str) -> str:
    """What the log says about a chain that was built and did not turn the
    first picture into an access unit."""
    return "{} did not encode a {}x{} picture: {}".format(chain.name, size[0], size[1], reason)


#: The memory of this process.
MEMORY = ChainMemory()


def forget_chains() -> None:
    """Forget what was learned about the chains. Called when `live.encoder`
    changes: the next live start probes again."""
    MEMORY.clear()


# --- the encoder aiortc's sender calls ----------------------------------------------


class HardwareH264Encoder(H264Encoder):
    """aiortc's H.264 encoder with the encoding replaced: frames go through
    a `Pipeline` over the first chain that works, and through aiortc's own
    libx264 when none does.

    `encode()` is inherited and so is the packetization. aiortc calls it in
    a worker thread, one frame at a time; `target_bitrate` is written from
    the event loop, by the publisher's budget and by the receiver's
    estimate; `close()` is called by the publisher, in a thread of its own.
    One lock keeps a frame, a close and nothing else apart; the bitrate is a
    plain number read when a frame is encoded.

    **The first frame is the probe.** The chains are tried in order with
    it, each given `frame_timeout_s`. The first that returns an access unit
    carries the session. A chain that fails is remembered (`ChainMemory`):
    one that cannot be built is not built again in this process, one that
    did not encode the picture is not tried again for pictures of that
    size.

    **A pipeline is started and stopped in threads of its own.** Both are
    calls into the encoder element's driver, and one that does not return is
    the failure the time limit of a frame exists for. aiortc calls this
    class on a worker of the event loop's default executor, which the name
    resolution of every reconnect, the bridge's ROS calls and every other
    camera's encoder share: a worker lost at every live start is a bridge
    that stops answering. So a start is waited for `START_TIMEOUT_S` and a
    stop is not waited for at all where a frame is due; whatever does not
    come back is left behind in a daemon thread, and its chain is not built
    again. What failed is recorded before the pipeline is closed, never
    after.

    **The fallback is the same object.** The offer said H.264 and the
    sender holds this encoder; when no chain works, or the one in use
    stops, `super()._encode_frame` takes over for the rest of the session
    and nothing is renegotiated. The first software frame is a keyframe.
    One WARNING says which backend, which chains and why.

    **Nothing is raised out of `_encode_frame`.** aiortc ends its sending
    loop for good on an exception from `encode()`, tells nobody, and logs
    the traceback with its message. So a chain that fails in a way nobody
    foresaw is a chain that failed: software takes over, and the log gets
    the type of the exception and not its text. If software cannot encode a
    frame either, that frame is dropped, said once, and the next frame that
    can be encoded is a keyframe. A keyframe that was asked for and could
    not be made is owed: aiortc asks once.
    """

    def __init__(
        self,
        backend: str,
        chains: Sequence[Chain],
        *,
        fps: int,
        memory: Optional[ChainMemory] = None,
        frame_timeout_s: float = FRAME_TIMEOUT_S,
        pipeline_factory: Callable[..., Pipeline] = Pipeline,
    ) -> None:
        super().__init__()
        self._backend = backend
        self._chains = tuple(chains)
        self._fps = max(1, int(fps))
        self._memory = memory if memory is not None else MEMORY
        self._frame_timeout_s = frame_timeout_s
        self._pipeline_factory = pipeline_factory
        self._lock = threading.Lock()
        self._bitrate_bps = DEFAULT_BITRATE_BPS
        self._pipeline: Optional[Pipeline] = None
        self._software = False
        self._closed = False
        # A keyframe that was asked for on a frame that produced nothing.
        self._keyframe_owed = False
        # Set by a frame that could not be encoded at all, cleared by one
        # that could: a run of them is one log line.
        self._dropping = False

    # -- the bitrate ---------------------------------------------------------------

    @property
    def target_bitrate(self) -> int:
        """Target bitrate in bits per second."""
        return self._bitrate_bps

    @target_bitrate.setter
    def target_bitrate(self, bitrate: int) -> None:
        self._bitrate_bps = max(MIN_BITRATE_BPS, min(int(bitrate), MAX_BITRATE_BPS))

    # -- what a test and the proof tool may ask ---------------------------------

    @property
    def chain(self) -> Optional[Chain]:
        """The chain in use, or `None`: before the first frame, in software,
        after `close()`."""
        pipeline = self._pipeline
        return pipeline.chain if pipeline is not None else None

    @property
    def software(self) -> bool:
        """Whether this session has fallen back to libx264."""
        return self._software

    # -- one frame -------------------------------------------------------------------

    def _encode_frame(self, frame: "av.VideoFrame", force_keyframe: bool):
        # A list made under the lock and handed out after it: a generator
        # that held the lock would hold it for as long as its consumer
        # liked.
        with self._lock:
            units = self._units_or_nothing(frame, force_keyframe)
        yield from units

    def _units_or_nothing(self, frame: "av.VideoFrame", force_keyframe: bool) -> List[bytes]:
        """`_units`, and never an exception: see the class docstring."""
        force_keyframe = force_keyframe or self._keyframe_owed
        try:
            units = self._units(frame, force_keyframe)
        except Exception as exc:  # noqa: BLE001 - aiortc would end the stream on it
            units = self._units_after_a_failure(frame, exc)
        self._keyframe_owed = force_keyframe and not units and not self._closed
        if units:
            self._dropping = False
        return units

    def _units_after_a_failure(self, frame: "av.VideoFrame", exc: BaseException) -> List[bytes]:
        """Something other than a chain's own failure got out of `_units`.
        Whatever was running is given up, and software gets the frame."""
        pipeline, self._pipeline = self._pipeline, None
        was_software, self._software = self._software, True
        # A libx264 that failed inside a frame is not carried on with.
        self.codec = None
        if pipeline is not None:
            self._memory.stopped_working(pipeline.chain)
            self._shut_down(pipeline)
        if not was_software:
            log.warning(
                "live.encoder=%s: the encoder failed unexpectedly (%s). The rest of this "
                "session is encoded in software H.264.",
                self._backend, type(exc).__name__,
            )
            try:
                return self._software_units(frame, True)
            except Exception as again:  # noqa: BLE001 - same rule, one level down
                exc = again
                self.codec = None
        if not self._dropping:
            self._dropping = True
            log.error(
                "live.encoder=%s: software H.264 could not encode a frame (%s). Frames "
                "are dropped until one can be encoded.",
                self._backend, type(exc).__name__,
            )
        return []

    def _units(self, frame: "av.VideoFrame", force_keyframe: bool) -> List[bytes]:
        if self._closed:
            return []
        if self._software:
            return self._software_units(frame, force_keyframe)

        pipeline = self._pipeline
        if pipeline is not None and pipeline.size != (frame.width, frame.height):
            # A picture of another size is another stream: built anew, as
            # aiortc builds a new codec for one. The old one gets its time
            # to let go of the device before the new one opens it.
            self._pipeline = None
            self._shut_down(pipeline).wait(STOP_TIMEOUT_S)
            pipeline = None
        if pipeline is None:
            data = self._probe(frame)
            if data is None:
                return self._software_units(frame, True)
            return self._nal_units(data)

        try:
            wanted = self._bitrate_bps
            if abs(wanted - pipeline.bitrate_bps) > BITRATE_CHANGE_SHARE * pipeline.bitrate_bps:
                pipeline.set_bitrate(wanted)
            data = pipeline.encode(frame, force_keyframe)
        except Exception as exc:  # noqa: BLE001 - a chain's failure, whatever it raised
            # What happened first, the pipeline after, and not on this
            # thread: an element that did not return a frame may not return
            # from being stopped either.
            self._pipeline = None
            self._software = True
            self._memory.stopped_working(pipeline.chain)
            self._shut_down(pipeline)
            log.warning(
                "live.encoder=%s: %s stopped working (%s). The rest of this session "
                "is encoded in software H.264.",
                self._backend, pipeline.chain.name, _reason(exc),
            )
            return self._software_units(frame, True)
        return self._nal_units(data)

    def _probe(self, frame: "av.VideoFrame") -> Optional[bytes]:
        """Find a chain that encodes `frame`; its first access unit, or
        `None` with the session set to software."""
        size = (frame.width, frame.height)
        to_try, known_bad = self._memory.candidates(self._chains, size)
        failures = ["{} (known from an earlier start)".format(said) for said in known_bad.values()]
        for index, chain in enumerate(to_try):
            try:
                # On this thread and without a bound: the first time, this
                # is GStreamer scanning its plugins, which takes what it
                # takes and is no device. The bound is for the pipeline.
                _load_gst()
                pipeline = self._start(chain, frame)
            except GstUnavailable as unavailable:
                # Not this chain's failure: none of them can be built.
                for other in to_try[index:]:
                    self._memory.cannot_be_built(other, unavailable.reason)
                failures.append(unavailable.reason)
                break
            except Exception as exc:  # noqa: BLE001 - a chain's failure, whatever it raised
                self._memory.cannot_be_built(chain, _reason(exc))
                failures.append(cannot_be_built(chain, _reason(exc)))
                continue
            try:
                # The first frame of a stream is a keyframe.
                data = pipeline.encode(frame, True)
            except Exception as exc:  # noqa: BLE001 - a chain's failure, whatever it raised
                # Recorded first, closed after and elsewhere: see `_shut_down`.
                self._memory.did_not_encode(chain, size, _reason(exc))
                failures.append(did_not_encode(chain, size, _reason(exc)))
                self._shut_down(pipeline)
                continue
            self._memory.worked(chain)
            self._pipeline = pipeline
            log.info("live.encoder=%s: encoding with %s", self._backend, chain.name)
            return data

        self._software = True
        log.warning(
            "live.encoder=%s: no hardware encoder could be used (%s). This session is "
            "encoded in software H.264. Until live.encoder changes or the bridge restarts, "
            "a chain that cannot be built is not tried again, and one that did not encode "
            "a picture is not tried again for pictures of that size.",
            self._backend, "; ".join(failures) or "no chain is defined for it",
        )
        return None

    @classmethod
    def _nal_units(cls, data: bytes) -> List[bytes]:
        """The NAL units of one access unit, without the delimiter some
        encoders put in front: it carries nothing a WebRTC receiver uses."""
        return [
            unit for unit in cls._split_bitstream(data)
            if unit and unit[0] & 0x1F != _NAL_ACCESS_UNIT_DELIMITER
        ]

    def _software_units(self, frame: "av.VideoFrame", force_keyframe: bool) -> List[bytes]:
        """aiortc's own libx264, with one thing done for it.

        aiortc 1.3.0 ignores `force_keyframe` for H.264; 1.6.0 and 1.14.0 set
        the picture type themselves. With H.264 as the only codec on offer a
        viewer who joins late would otherwise wait for libx264's own next
        keyframe, minutes away at a camera's frame rate. So the picture type
        is set here, on the frame libx264 really gets: converted first,
        because a conversion inside the encoder would drop the mark.
        """
        picture = frame
        if frame.format.name != "yuv420p":
            picture = frame.reformat(format="yuv420p")
            picture.pts = frame.pts
            picture.time_base = frame.time_base
        picture.pict_type = (
            av.video.frame.PictureType.I if force_keyframe else av.video.frame.PictureType.NONE
        )
        return list(super()._encode_frame(picture, force_keyframe))

    # -- the end ---------------------------------------------------------------------

    def _start(self, chain: Chain, frame: "av.VideoFrame") -> Pipeline:
        """Build and start a pipeline for `frame` in a thread of its own,
        and wait `START_TIMEOUT_S` for it. Raises what the build raised, or
        `ChainFailed` when it did not come back in time. A pipeline that
        starts after nobody waits for it any more is shut down by the thread
        that built it."""
        started = threading.Event()
        guard = threading.Lock()
        outcome: Dict[str, Any] = {}

        def build() -> None:
            try:
                pipeline = self._pipeline_factory(
                    chain,
                    width=frame.width,
                    height=frame.height,
                    fps=self._fps,
                    bitrate_bps=self._bitrate_bps,
                    frame_timeout_s=self._frame_timeout_s,
                )
            except BaseException as exc:  # noqa: BLE001 - raised again by the thread that waits
                outcome["error"] = exc
                started.set()
                return
            with guard:
                if "given up" not in outcome:
                    outcome["pipeline"] = pipeline
                    started.set()
                    return
            self._shut_down(pipeline)

        threading.Thread(target=build, name="live-pipeline-start", daemon=True).start()
        if not started.wait(START_TIMEOUT_S):
            with guard:
                if not outcome:
                    outcome["given up"] = True
        if "given up" in outcome:
            raise ChainFailed("the pipeline did not start within {:g} s".format(START_TIMEOUT_S))
        if "error" in outcome:
            raise outcome["error"]
        return outcome["pipeline"]

    def _shut_down(self, pipeline: Pipeline) -> threading.Event:
        """Close a pipeline in a thread of its own. Returns at once; the
        event is set when the pipeline is closed.

        Stopping a pipeline waits for its streaming thread, and that thread
        can be inside an encoder element that does not return. Whoever
        closes it then does not return either, so nobody who has anything
        else to do closes one: not the thread that encodes, which has a
        frame to deliver, and not a caller that holds the lock.

        A pipeline that has not stopped after `STOP_TIMEOUT_S` is left where
        it is, in a daemon thread that holds the process up for nothing. Its
        chain is not built again in this process, also when it had passed
        its probe: every further start would leave another thread there.
        That is said once, at WARNING. What goes wrong in a close that does
        return is logged by type name and goes no further.
        """
        closed = threading.Event()
        chain, memory, backend = pipeline.chain, self._memory, self._backend

        def close() -> None:
            try:
                pipeline.close()
            except Exception as exc:  # noqa: BLE001 - it is being given up either way
                log.error("Error closing an encoder pipeline: %s", type(exc).__name__)
            finally:
                closed.set()

        def watch() -> None:
            threading.Thread(target=close, name="live-pipeline-stop", daemon=True).start()
            if closed.wait(STOP_TIMEOUT_S):
                return
            memory.did_not_shut_down(chain, STOP_TIMEOUT_S)
            log.warning(
                "live.encoder=%s: %s. A thread of its pipeline is left behind, and the "
                "chain is not built again until live.encoder changes or the bridge restarts.",
                backend, did_not_shut_down(chain, STOP_TIMEOUT_S),
            )

        threading.Thread(target=watch, name="live-pipeline-watch", daemon=True).start()
        return closed

    def close(self) -> None:
        """Release the pipeline and the codec. Called once by the publisher,
        in a thread of its own; waits for a frame that is still being
        encoded, which is at most `START_TIMEOUT_S` and `frame_timeout_s`
        for each chain a probe still has to try, and then `STOP_TIMEOUT_S`
        for the pipeline to stop. After it, `encode()` returns nothing."""
        with self._lock:
            self._closed = True
            pipeline, self._pipeline = self._pipeline, None
            self.codec = None
        if pipeline is not None:
            self._shut_down(pipeline).wait(STOP_TIMEOUT_S)


def build_encoder(setting: str, *, fps: int) -> Optional[HardwareH264Encoder]:
    """The encoder a live session gets for a value of `live.encoder`:
    `None` for `software`, which leaves aiortc to build its own, VP8 or
    H.264 as negotiated. Nothing is built or probed here; that happens with
    the session's first frame."""
    if setting == "software":
        return None
    return HardwareH264Encoder(setting, chains_for(setting), fps=fps)

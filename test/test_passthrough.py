# SPDX-License-Identifier: Apache-2.0
"""The stream between a passthrough source and the sender: start gating,
parameter sets in front of every keyframe, order and loss, the bounded queue,
pausing, the refusals and the one keyframe decode a snapshot costs.

`PassthroughStream` is fed from a source's thread and drained from the event
loop. Most tests here do both from one thread, because what they check is
what the stream decides about a unit; `test_units_cross_from_a_thread_to_the_loop`
is the one that checks the hand-over itself.

Streams are encoded with libx264 in `h264_streams.py`. The clock is injected:
"ten seconds without a keyframe" is asserted to the second without waiting
for one.
"""
import asyncio
import contextlib
import logging
import threading

import numpy as np
import pytest

import h264_streams as streams
from fleetless_bridge import passthrough
from fleetless_bridge.passthrough import PassthroughRefused, PassthroughStream

WIDTH, HEIGHT = 64, 48


class _Lines(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.DEBUG)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


@contextlib.contextmanager
def _logged():
    """What `passthrough.py` logs inside the block, as a list of messages.

    A handler of our own and not `caplog`: this suite's loggers do not
    propagate to the root logger `caplog` listens on, so a `caplog`
    assertion about a line sees nothing whether or not the line was
    written."""
    handler = _Lines()
    logger = logging.getLogger("fleetless_bridge.passthrough")
    level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    try:
        yield handler.messages
    finally:
        logger.removeHandler(handler)
        logger.setLevel(level)


class _Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def _stream(width=WIDTH, height=HEIGHT, clock=None):
    return PassthroughStream(width=width, height=height, slug="front", clock=clock or _Clock())


def _push_all(stream, units, start_ms=10_000, step_ms=33):
    for index, unit in enumerate(units):
        stream.push(unit, start_ms + index * step_ms)


async def _drain(stream):
    """Everything the stream will hand out without another push."""
    out = []
    while stream.pending():
        out.append(await stream.get())
    return out


def _attached(units_before=(), **kwargs):
    """A stream with a consumer attached, as during a live session."""
    stream = _stream(**kwargs)
    for index, unit in enumerate(units_before):
        stream.push(unit, 1_000 + index)

    async def attach():
        stream.attach(asyncio.get_running_loop())

    return stream, attach


def _types(unit):
    return [nal[0] & 0x1F for nal in unit.nals]


# --- start gating (spec test 2) ----------------------------------------------


def test_units_before_the_first_keyframe_are_dropped():
    units = streams.encode_stream(frames=8, gop=4)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        # A viewer joins in the middle of a group of pictures: the first
        # three units it could get depend on a keyframe it never saw.
        _push_all(stream, units[1:])
        return await _drain(stream)

    out = asyncio.run(scenario())
    assert len(out) == 4  # the second group, whole
    assert out[0].keyframe and 5 in _types(out[0])
    assert [unit.keyframe for unit in out[1:]] == [False, False, False]
    assert [streams.join(list(unit.nals)) for unit in out] == [
        streams.join(streams.split(unit)) for unit in units[4:]
    ]


def test_the_first_unit_sent_is_a_keyframe_behind_its_parameter_sets():
    units = streams.encode_stream(frames=4, gop=4)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, units)
        return await _drain(stream)

    first = asyncio.run(scenario())[0]
    kinds = _types(first)
    assert kinds.index(7) < kinds.index(8) < kinds.index(5)
    # In-band sets are not doubled: the source already put them there.
    assert kinds.count(7) == 1 and kinds.count(8) == 1


def test_parameter_sets_sent_once_at_the_beginning_reach_every_later_keyframe():
    """A source that puts SPS and PPS only in front of its first keyframe. A
    viewer who joins at the third keyframe needs them all the same: the
    bridge keeps the newest pair and puts it in front of each IDR it sends."""
    units = streams.encode_stream(frames=12, gop=4)
    sps, pps = streams.parameter_sets(units[0])
    once = [units[0]] + [streams.strip_parameter_sets(unit) for unit in units[1:]]
    assert 7 not in streams.nal_types(once[8])

    async def scenario():
        stream = _stream()
        _push_all(stream, once[:6])  # nobody watches yet
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, once[6:], start_ms=20_000)
        return await _drain(stream)

    out = asyncio.run(scenario())
    assert out[0].keyframe
    assert list(out[0].nals[:2]) == [sps, pps]
    assert 5 in _types(out[0])
    # And the stream still decodes from there, which it would not without them.
    pictures = streams.decode_all([streams.join(list(unit.nals)) for unit in out])
    assert len(pictures) == 4 and pictures[0].shape == (HEIGHT, WIDTH, 3)


def test_parameter_sets_given_out_of_band_reach_every_keyframe():
    """RTSP: the sets are in the session description, not in the stream."""
    units = streams.encode_stream(frames=8, gop=4)
    sps, pps = streams.parameter_sets(units[0])
    bare = [streams.strip_parameter_sets(unit) for unit in units]

    async def scenario():
        stream = _stream()
        stream.set_parameter_sets(streams.join([sps, pps]))
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, bare)
        return await _drain(stream)

    out = asyncio.run(scenario())
    keyframes = [unit for unit in out if unit.keyframe]
    assert len(keyframes) == 2
    for keyframe in keyframes:
        assert list(keyframe.nals[:2]) == [sps, pps]


def test_a_keyframe_without_any_parameter_sets_cannot_start_a_stream():
    units = [streams.strip_parameter_sets(unit) for unit in streams.encode_stream(frames=8, gop=4)]

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, units)
        return stream.pending(), stream.newest_keyframe()

    assert asyncio.run(scenario()) == (0, None)


def test_an_access_unit_delimiter_stays_in_front():
    """Some encoders open every access unit with a delimiter (NAL type 9).
    The standard wants it first, so the parameter sets go behind it."""
    units = streams.encode_stream(frames=8, gop=4)
    sps, pps = streams.parameter_sets(units[0])
    aud = b"\x09\xf0"
    with_aud = [streams.join([aud] + streams.split(streams.strip_parameter_sets(unit))) for unit in units]

    async def scenario():
        stream = _stream()
        stream.set_parameter_sets(streams.join([sps, pps]))
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, with_aud)
        return await _drain(stream)

    first = asyncio.run(scenario())[0]
    assert list(first.nals[:3]) == [aud, sps, pps]


# --- order and loss (spec test 3) --------------------------------------------


def test_every_unit_after_the_start_arrives_once_and_in_order():
    units = streams.encode_stream(frames=40, gop=10)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        out = []
        for index, unit in enumerate(units):
            stream.push(unit, 10_000 + index * 33)
            if index % 3 == 0:  # the sender keeps up, in bursts
                out.extend(await _drain(stream))
        out.extend(await _drain(stream))
        return out

    out = asyncio.run(scenario())
    assert [unit.timestamp_ms for unit in out] == [10_000 + index * 33 for index in range(40)]
    assert [streams.join(list(unit.nals)) for unit in out] == [
        streams.join(streams.split(unit)) for unit in units
    ]
    # Bit-exact: what comes out decodes to the pictures the source encoded.
    forwarded = streams.decode_all([streams.join(list(unit.nals)) for unit in out])
    original = streams.decode_all(units)
    assert len(forwarded) == len(original) == 40
    assert all(np.array_equal(a, b) for a, b in zip(forwarded, original))


def test_units_cross_from_a_thread_to_the_loop():
    """The real arrangement: a source's thread pushes, the event loop waits
    in `get()`. Nothing is lost or reordered at the hand-over, and a `get()`
    that found the queue empty wakes when the next unit lands."""
    units = streams.encode_stream(frames=30, gop=10)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())

        def source():
            for index, unit in enumerate(units):
                stream.push(unit, 10_000 + index * 33)
                if index % 7 == 0:
                    threading.Event().wait(0.01)

        thread = threading.Thread(target=source)
        thread.start()
        out = [await asyncio.wait_for(stream.get(), timeout=5.0) for _ in units]
        thread.join()
        return out

    out = asyncio.run(scenario())
    assert [unit.timestamp_ms for unit in out] == [10_000 + index * 33 for index in range(30)]


def test_an_overflow_empties_the_queue_and_resumes_at_the_next_keyframe():
    """The sender stops pulling for longer than the queue is deep. Frames of
    a stream depend on each other, so dropping the oldest would send a
    viewer pictures built on ones it never got: the queue is emptied instead
    and forwarding waits for a keyframe."""
    units = streams.encode_stream(frames=120, gop=30)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        with _logged() as lines:
            # 33 ms apart: unit 61 is the first more than two seconds
            # behind unit 0. Nobody drains.
            _push_all(stream, units[:61])
            before_overflow = stream.pending()
            _push_all(stream, units[61:75], start_ms=10_000 + 61 * 33)
            after_overflow = stream.pending()
            _push_all(stream, units[75:], start_ms=10_000 + 75 * 33)
            return before_overflow, after_overflow, await _drain(stream), lines

    before_overflow, after_overflow, out, lines = asyncio.run(scenario())
    assert before_overflow == 61
    # Emptied -- the keyframe at unit 60 with it -- and what arrived between
    # the overflow and the next keyframe was not queued.
    assert after_overflow == 0
    # The next keyframe is unit 90, and from there nothing is missing.
    assert out[0].keyframe and out[0].timestamp_ms == 10_000 + 90 * 33
    assert [unit.timestamp_ms for unit in out] == [10_000 + index * 33 for index in range(90, 120)]
    overflow_lines = [line for line in lines if "queue" in line]
    assert len(overflow_lines) == 1
    assert "front" in overflow_lines[0]


def test_one_log_line_per_overflow_episode():
    units = streams.encode_stream(frames=200, gop=100)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        with _logged() as lines:
            _push_all(stream, units[:99])  # overflows after two seconds, no keyframe until 100
            first = stream.pending()
            _push_all(stream, units[99:160], start_ms=10_000 + 99 * 33)  # restarts at 100, runs on
            await _drain(stream)
            _push_all(stream, units[160:200], start_ms=10_000 + 160 * 33)
            return first, await _drain(stream), lines

    first, tail, lines = asyncio.run(scenario())
    assert first == 0
    assert len(tail) == 40  # forwarding again, nothing lost once the sender pulls
    assert len([line for line in lines if "queue" in line]) == 1


def test_the_queue_is_bounded_in_units_as_well_as_in_time():
    """A source whose units all carry one timestamp never trips the
    two-second bound. The count bound catches it."""
    units = streams.encode_stream(frames=4, gop=4)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        stream.push(units[0], 10_000)
        for _ in range(passthrough.QUEUE_MAX_UNITS + 50):
            stream.push(units[1], 10_000)
        return stream.pending()

    assert asyncio.run(scenario()) == 0


# --- pause and resume (spec test 9), and nobody watching ---------------------


def test_nothing_is_queued_while_nobody_is_attached_but_the_keyframe_is_kept():
    units = streams.encode_stream(frames=12, gop=4)
    stream = _stream()
    _push_all(stream, units)
    assert stream.pending() == 0
    keyframe = stream.newest_keyframe()
    assert keyframe is not None
    assert keyframe.timestamp_ms == 10_000 + 8 * 33  # the newest, not the first
    assert streams.is_idr(keyframe.data)


def test_nothing_is_forwarded_while_paused_and_a_resume_starts_at_a_keyframe():
    units = streams.encode_stream(frames=16, gop=4)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, units[:6])
        before = await _drain(stream)

        stream.set_paused(True)
        _push_all(stream, units[6:10], start_ms=20_000)
        while_paused = stream.pending()
        kept = stream.newest_keyframe()

        stream.set_paused(False)
        _push_all(stream, units[10:], start_ms=30_000)
        after = await _drain(stream)
        return before, while_paused, kept, after

    before, while_paused, kept, after = asyncio.run(scenario())
    assert len(before) == 6
    assert while_paused == 0
    # The keyframe that passed while paused is still there for a snapshot.
    assert kept.timestamp_ms == 20_000 + 2 * 33
    # Units 10 and 11 depend on the keyframe nobody was sent; the resume
    # starts at unit 12.
    assert after[0].keyframe and after[0].timestamp_ms == 30_000 + 2 * 33
    assert len(after) == 4


def test_pausing_discards_what_was_queued():
    units = streams.encode_stream(frames=8, gop=4)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, units)
        stream.set_paused(True)
        return stream.pending()

    assert asyncio.run(scenario()) == 0


def test_a_resync_drops_what_is_queued_and_waits_for_a_keyframe():
    """A unit that was handed out did not reach the viewers. Everything
    queued behind it was predicted from it."""
    units = streams.encode_stream(frames=12, gop=4)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, units[:6])
        stream.resync()
        dropped = stream.pending()
        _push_all(stream, units[6:], start_ms=20_000)
        return dropped, await _drain(stream), stream.newest_keyframe()

    dropped, out, keyframe = asyncio.run(scenario())
    assert dropped == 0
    assert out[0].keyframe and out[0].timestamp_ms == 20_000 + 2 * 33
    assert len(out) == 4
    assert keyframe is not None  # the snapshot's keyframe is not the sender's business


def test_detaching_wakes_a_waiting_get_with_none():
    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        waiter = asyncio.ensure_future(stream.get())
        await asyncio.sleep(0.01)
        assert not waiter.done()
        stream.detach()
        return await asyncio.wait_for(waiter, timeout=1.0)

    assert asyncio.run(scenario()) is None


def test_a_new_attach_starts_at_a_keyframe_again():
    units = streams.encode_stream(frames=12, gop=4)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, units[:6])
        stream.detach()
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, units[6:], start_ms=20_000)
        return await _drain(stream)

    out = asyncio.run(scenario())
    assert out[0].keyframe and out[0].timestamp_ms == 20_000 + 2 * 33


# --- refusals (spec test 7) --------------------------------------------------


def test_a_size_other_than_the_configured_one_is_refused_naming_both():
    units = streams.encode_stream(width=64, height=48, frames=4, gop=4)
    stream = _stream(width=1280, height=720)
    with pytest.raises(PassthroughRefused) as refused:
        stream.push(units[0], 10_000)
    message = str(refused.value)
    assert "64x48" in message and "1280x720" in message
    assert stream.newest_keyframe() is None  # nothing is published, snapshots included
    # It stays refused: the frames after the keyframe are the same stream.
    with pytest.raises(PassthroughRefused):
        stream.push(units[1], 10_033)


def test_a_size_mismatch_in_out_of_band_parameter_sets_is_refused_at_once():
    sps, pps = streams.parameter_sets(streams.encode_stream(width=64, height=48, frames=1)[0])
    stream = _stream(width=640, height=480)
    with pytest.raises(PassthroughRefused) as refused:
        stream.set_parameter_sets(streams.join([sps, pps]))
    assert "64x48" in str(refused.value) and "640x480" in str(refused.value)


def test_a_new_sps_with_a_different_size_mid_stream_is_refused():
    small = streams.encode_stream(width=64, height=48, frames=8, gop=4)
    large = streams.encode_stream(width=128, height=96, frames=4, gop=4)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, small)
        assert stream.pending() == 8
        with pytest.raises(PassthroughRefused) as refused:
            stream.push(large[0], 20_000)
        return str(refused.value), stream.pending(), stream.newest_keyframe()

    message, pending, keyframe = asyncio.run(scenario())
    assert "128x96" in message and "64x48" in message
    assert pending == 0 and keyframe is None


def test_the_right_size_again_is_accepted_at_its_keyframe():
    small = streams.encode_stream(width=64, height=48, frames=8, gop=4)
    large = streams.encode_stream(width=128, height=96, frames=4, gop=4)
    stream = _stream()
    with pytest.raises(PassthroughRefused):
        stream.push(large[0], 10_000)
    assert stream.push(small[0], 10_100) is True
    assert stream.newest_keyframe() is not None


def test_b_frames_from_the_start_are_refused():
    units = streams.encode_stream(frames=12, gop=12, bframes=2)
    stream = _stream()
    results = []
    for index, unit in enumerate(units):
        try:
            results.append(stream.push(unit, 10_000 + index * 33))
        except PassthroughRefused as refused:
            results.append(str(refused))
    first_refusal = next(index for index, result in enumerate(results) if isinstance(result, str))
    assert "B-frames" in results[first_refusal]
    # Refused from the first B-slice on, to the end.
    assert all(isinstance(result, str) for result in results[first_refusal:])
    assert stream.newest_keyframe() is None


def test_b_frames_that_first_appear_mid_stream_are_refused_then():
    clean = streams.encode_stream(frames=8, gop=4)
    with_b = streams.encode_stream(frames=12, gop=12, bframes=2)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, clean)
        assert stream.pending() == 8
        refused_at = None
        for index, unit in enumerate(with_b):
            try:
                stream.push(unit, 20_000 + index * 33)
            except PassthroughRefused as refused:
                refused_at = (index, str(refused))
                break
        return refused_at, stream.pending()

    (index, message), pending = asyncio.run(scenario())
    assert index >= 1 and "B-frames" in message
    assert pending == 0  # the running stream's queue is gone with the refusal


def test_a_b_frame_refusal_stands_for_ten_seconds_after_the_last_b_frame():
    """A stream with B-frames has keyframes too. Accepting it again at each
    of them would report the camera broken and repaired once per keyframe
    interval; the refusal holds until the cause has been gone for ten
    seconds."""
    clock = _Clock()
    with_b = streams.encode_stream(frames=12, gop=12, bframes=2)
    clean = streams.encode_stream(frames=12, gop=4)
    stream = _stream(clock=clock)
    for index, unit in enumerate(with_b):
        try:
            stream.push(unit, 10_000 + index)
        except PassthroughRefused:
            pass
    # The source is fixed. Keyframes arrive, the refusal stands...
    clock.advance(5.0)
    with pytest.raises(PassthroughRefused) as refused:
        stream.push(clean[0], 20_000)
    assert "B-frames" in str(refused.value)
    # ...until ten seconds have passed since the last B-slice, and then a
    # keyframe starts it.
    clock.advance(5.1)
    with pytest.raises(PassthroughRefused):
        stream.push(clean[1], 30_000)  # not a keyframe: nothing to start with
    assert stream.push(clean[4], 30_100) is True
    assert stream.newest_keyframe().timestamp_ms == 30_100


def test_no_keyframe_within_ten_seconds_is_refused():
    clock = _Clock()
    units = streams.encode_stream(frames=6, gop=6)
    stream = _stream(clock=clock)
    # A viewer's nightmare: frames keep coming, none of them a keyframe.
    assert stream.push(units[1], 10_000) is False
    clock.advance(9.9)
    assert stream.push(units[2], 19_900) is False
    clock.advance(0.2)
    with pytest.raises(PassthroughRefused) as refused:
        stream.push(units[3], 20_100)
    assert "keyframe" in str(refused.value) and "10 s" in str(refused.value)
    # The keyframe that does arrive ends it.
    assert stream.push(units[0], 20_200) is True


def test_a_long_keyframe_interval_mid_stream_is_refused_too():
    clock = _Clock()
    units = streams.encode_stream(frames=6, gop=6)
    stream = _stream(clock=clock)
    assert stream.push(units[0], 10_000) is True
    for second in range(1, 10):
        clock.advance(1.0)
        assert stream.push(units[1 + second % 5], 10_000 + second * 1000) is True
    clock.advance(1.0)
    with pytest.raises(PassthroughRefused) as refused:
        stream.push(units[1], 20_000)
    assert "keyframe" in str(refused.value)


def test_a_source_that_sends_nothing_is_not_this_refusal():
    """Ten quiet seconds are a stall, and a stall is the source adapter's to
    report. The clock here runs on arriving units."""
    clock = _Clock()
    units = streams.encode_stream(frames=6, gop=3)
    stream = _stream(clock=clock)
    assert stream.push(units[0], 10_000) is True
    clock.advance(60.0)
    # A frame that is not a keyframe, a minute after the last one: late, and
    # not a stream without keyframes.
    assert stream.push(units[1], 70_000) is True
    clock.advance(60.0)
    assert stream.push(units[3], 130_000) is True  # and a keyframe after a silence


def test_ten_seconds_of_bytes_that_are_not_h264_are_refused_as_that():
    clock = _Clock()
    stream = _stream(clock=clock)
    jpeg = b"\xff\xd8\xff\xe0" + b"\x00" * 64 + b"\xff\xd9"
    for second in range(10):
        assert stream.push(jpeg, 10_000 + second * 1000) is False
        clock.advance(1.0)
    with pytest.raises(PassthroughRefused) as refused:
        stream.push(jpeg, 20_000)
    assert "H.264" in str(refused.value)
    assert "keyframe" not in str(refused.value)


# --- unparseable units -------------------------------------------------------


def test_an_unparseable_unit_is_dropped_and_forwarding_waits_for_a_keyframe():
    units = streams.encode_stream(frames=12, gop=4)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        with _logged() as lines:
            _push_all(stream, units[:2])
            assert stream.push(b"\x12\x34 not a unit", 10_100) is False
            _push_all(stream, units[2:], start_ms=10_200)
        return await _drain(stream), lines

    out, lines = asyncio.run(scenario())
    # Units 0 and 1 were queued before the hole and still go out. Units 2 and
    # 3 follow a unit that never arrived whole; the stream picks up at 4.
    assert [unit.timestamp_ms for unit in out[:2]] == [10_000, 10_033]
    assert out[2].keyframe and out[2].timestamp_ms == 10_200 + 2 * 33
    assert len(out) == 2 + 8
    assert len([line for line in lines if "not parseable" in line]) == 1


def test_unparseable_units_are_logged_at_most_once_a_minute():
    clock = _Clock()
    units = streams.encode_stream(frames=4, gop=1)
    stream = _stream(clock=clock)
    with _logged() as lines:
        for second in range(0, 125, 5):
            stream.push(units[0], 10_000 + second * 1000)  # keeps the keyframe clock happy
            stream.push(b"garbage", 10_000 + second * 1000 + 1)
            clock.advance(5.0)
    lines = [line for line in lines if "not parseable" in line]
    assert len(lines) == 3  # at 0 s, 60 s and 120 s
    assert all("front" in line for line in lines)


def test_reset_forgets_the_stream_and_keeps_the_consumer():
    """An RTSP reconnect: new session, new parameter sets. Whoever is
    watching stays attached, and the stream that comes back is judged at its
    first keyframe."""
    wrong = streams.encode_stream(width=128, height=96, frames=4, gop=4)
    right = streams.encode_stream(frames=4, gop=4)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        with pytest.raises(PassthroughRefused):
            stream.push(wrong[0], 10_000)
        stream.reset()
        _push_all(stream, right, start_ms=20_000)
        return await _drain(stream)

    out = asyncio.run(scenario())
    assert len(out) == 4 and out[0].keyframe


def test_a_reset_does_not_forget_a_refusal_that_is_being_held():
    """A camera with B-frames, an adapter that reconnects: every new session
    opens with a keyframe. If the reconnect wiped the refusal, the camera
    would read repaired for the length of one group of pictures, every time."""
    clock = _Clock()
    with_b = streams.encode_stream(frames=12, gop=12, bframes=2)
    stream = _stream(clock=clock)
    for index, unit in enumerate(with_b):
        try:
            stream.push(unit, 10_000 + index)
        except PassthroughRefused:
            pass
    clock.advance(1.0)
    stream.reset()
    with pytest.raises(PassthroughRefused) as refused:
        stream.push(with_b[0], 20_000)  # the same stream again, from its keyframe
    assert "B-frames" in str(refused.value)


def test_sets_that_are_fine_do_not_raise_a_refusal_from_before():
    wrong = streams.encode_stream(width=128, height=96, frames=4, gop=4)
    right = streams.encode_stream(frames=4, gop=4)
    stream = _stream()
    with pytest.raises(PassthroughRefused):
        stream.push(wrong[0], 10_000)
    stream.reset()
    sps, pps = streams.parameter_sets(right[0])
    stream.set_parameter_sets(streams.join([sps, pps]))  # no exception: these are right
    assert stream.push(streams.strip_parameter_sets(right[0]), 20_000) is True


def test_a_new_configured_size_is_a_new_judgement():
    units = streams.encode_stream(width=64, height=48, frames=8, gop=4)
    stream = _stream(width=1280, height=720)
    with pytest.raises(PassthroughRefused):
        stream.push(units[0], 10_000)
    # The developer corrects the configuration to what the camera sends.
    stream.set_size(64, 48)
    assert stream.refusal is None
    assert stream.push(units[1], 10_033) is False  # mid-group: nothing to start with yet
    assert stream.push(units[4], 10_133) is True

    # And the other way round: a size that no longer matches is refused at
    # once, without waiting for the source to repeat its SPS.
    stream.set_size(640, 480)
    assert stream.newest_keyframe() is None
    with pytest.raises(PassthroughRefused) as refused:
        stream.push(units[5], 10_166)
    assert "64x48" in str(refused.value) and "640x480" in str(refused.value)


# --- the one decode (spec test 8) --------------------------------------------


def test_a_keyframe_decodes_to_a_picture_of_the_streams_size():
    units = streams.encode_stream(width=100, height=50, frames=6, gop=3)
    stream = _stream(width=100, height=50)
    _push_all(stream, units)
    picture = passthrough.decode_keyframe(stream.newest_keyframe().data)
    assert picture.shape == (50, 100, 3) and picture.dtype == np.uint8
    # The newest keyframe is unit 3; it decodes to that picture, not to the
    # first one.
    expected = streams.decode_all(units)[3]
    assert np.array_equal(picture, expected)


def test_a_keyframe_whose_sets_arrived_out_of_band_decodes_alone():
    units = streams.encode_stream(frames=4, gop=4)
    sps, pps = streams.parameter_sets(units[0])
    stream = _stream()
    stream.set_parameter_sets(streams.join([sps, pps]))
    stream.push(streams.strip_parameter_sets(units[0]), 10_000)
    picture = passthrough.decode_keyframe(stream.newest_keyframe().data)
    assert picture.shape == (HEIGHT, WIDTH, 3)


def test_bytes_that_do_not_decode_raise():
    with pytest.raises(passthrough.KeyframeDecodeError):
        passthrough.decode_keyframe(b"\x00\x00\x00\x01\x65\x88\x80\x00")


def test_pushing_decodes_nothing(monkeypatch):
    """The point of passthrough. This module reaches a decoder through one
    function, and forwarding a whole stream never calls it."""
    units = streams.encode_stream(frames=12, gop=4)
    calls = []
    original = passthrough._new_decoder

    def counting():
        calls.append(1)
        return original()

    monkeypatch.setattr(passthrough, "_new_decoder", counting)

    async def scenario():
        stream = _stream()
        stream.attach(asyncio.get_running_loop())
        _push_all(stream, units)
        await _drain(stream)
        return stream.newest_keyframe()

    keyframe = asyncio.run(scenario())
    assert calls == []
    # And the counter does count: the one decode goes through it.
    passthrough.decode_keyframe(keyframe.data)
    assert calls == [1]

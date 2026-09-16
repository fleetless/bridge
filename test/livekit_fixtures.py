# SPDX-License-Identifier: Apache-2.0
"""The LiveKit signalling fixtures: what is in them, and how the made-up ones
are made.

Two frames in `test/fixtures/livekit/` are **recorded**: a real `join` and the
`refreshToken` behind it, captured off livekit-server v1.13.5 during the spike
that established both arrive as binary protobuf however the client asks. They
cannot be derived from anything and are the evidence; nothing here builds them.

The rest are **built**, by the functions below, and committed anyway. That buys
two things a generator alone does not: a reviewer can decode the bytes a test
asserts over without running it, and a hand-edited fixture goes red instead of
invisible (`test_livekit_signal.py` regenerates every built fixture and
compares).

**They are hex, not raw bytes, and that is not a preference.**
`test/test_published_prose.py` asserts that no binary file is in the published
set, and the prose and licence guards read every file in that set as text. A
raw protobuf frame either has a NUL -- forbidden by that assertion -- or lacks
one, and then crashes those guards' UTF-8 decode. Hex also diffs and reviews,
which a `.bin` does not: git calls a file with one NUL byte binary and shows
nothing about it at all.

**The interleaved fixture is the point of the walker.** `join` is read with a
tag walker rather than a generated parser so an unknown field cannot stop it
reading the ones it knows. A test that only ever feeds it today's exact frame
proves nothing about that, so `interleave_unknown_fields` puts an unknown field
of a *different wire type each time* before every field of the recorded frame,
at every level the walker descends into. Wire types 1 and 5 are in that
rotation deliberately: nothing in this protocol uses them today, which is
precisely why a walker that could not skip them would fail on the first server
that did.
"""
import pathlib

FIXTURES = pathlib.Path(__file__).resolve().parent / "fixtures" / "livekit"


def load(name):
    """One fixture, as bytes. Whitespace in the hex is insignificant."""
    return bytes.fromhex((FIXTURES / name).read_text())


def store(name, data):
    """Write one fixture. Wrapped at 64 hex digits so a diff is readable."""
    text = data.hex()
    lines = [text[i:i + 64] for i in range(0, len(text), 64)]
    (FIXTURES / name).write_text("\n".join(lines) + ("\n" if lines else ""))


# --------------------------------------------------------------------------
# A minimal protobuf encoder. Encoding is a test-only need: the bridge itself
# only ever decodes, because everything it sends is JSON.
# --------------------------------------------------------------------------


def encode_varint(value):
    out = bytearray()
    while True:
        byte = value & 0x7F
        value >>= 7
        if value:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def tag(number, wire):
    return encode_varint((number << 3) | wire)


def field_varint(number, value):
    return tag(number, 0) + encode_varint(value)


def field_bytes(number, payload):
    return tag(number, 2) + encode_varint(len(payload)) + payload


def field_fixed64(number, raw):
    assert len(raw) == 8
    return tag(number, 1) + raw


def field_fixed32(number, raw):
    assert len(raw) == 4
    return tag(number, 5) + raw


#: One unknown field of each wire type, cycled so that consecutive insertions
#: are not all the same shape. Field numbers well above anything the LiveKit
#: protocol assigns today.
UNKNOWN_FIELDS = [
    field_varint(500, 123456789),
    field_bytes(501, b"a field this bridge has never heard of"),
    field_fixed64(502, b"\x08\x07\x06\x05\x04\x03\x02\x01"),
    field_fixed32(503, b"\x04\x03\x02\x01"),
]

#: Which length-delimited fields are sub-messages the walker descends into,
#: keyed by the path that reaches them. Interleaving has to happen inside those
#: too, or the fixture only proves the top level forward-compatible -- and the
#: fields that matter (the room name, the server version, a TURN credential)
#: all live one or two levels down.
DESCEND = {
    (): {1},  # SignalResponse.join
    (1,): {1, 5, 12},  # JoinResponse.room / .ice_servers / .server_info
}


def _split(data):
    """`(number, wire, raw_value_bytes)` for one message, without decoding
    varints into integers -- the re-encoder below has to put back exactly what
    it took out."""
    index = 0
    while index < len(data):
        start = index
        key = 0
        shift = 0
        while True:
            byte = data[index]
            index += 1
            key |= (byte & 0x7F) << shift
            if not byte & 0x80:
                break
            shift += 7
        number, wire = key >> 3, key & 7
        if wire == 0:
            while data[index] & 0x80:
                index += 1
            index += 1
            yield number, wire, data[start:index]
        elif wire == 2:
            length = 0
            shift = 0
            while True:
                byte = data[index]
                index += 1
                length |= (byte & 0x7F) << shift
                if not byte & 0x80:
                    break
                shift += 7
            value = data[index:index + length]
            index += length
            yield number, wire, value
        elif wire in (1, 5):
            width = 8 if wire == 1 else 4
            index += width
            yield number, wire, data[start:index]
        else:
            raise AssertionError("wire type {} in a fixture".format(wire))


def interleave_unknown_fields(data, path=()):
    """The same message with one unknown field inserted before every field it
    has, recursively through the sub-messages the walker descends into."""
    out = bytearray()
    for position, (number, wire, value) in enumerate(_split(data)):
        out += UNKNOWN_FIELDS[position % len(UNKNOWN_FIELDS)]
        if wire == 2:
            if number in DESCEND.get(path, set()):
                value = interleave_unknown_fields(value, path + (number,))
            out += field_bytes(number, value)
        else:
            out += value
    return bytes(out)


def turn_join():
    """A `join` carrying a TURN server with a username and a credential, and
    `subscriberPrimary` set.

    The recorded frame has neither: the dev stack advertises three STUN URLs
    with no credentials and leaves `subscriberPrimary` at its proto3 default,
    so a suite built only on it would never exercise either field. Production
    LiveKit sits behind TURN -- exactly the fields a robot on a NAT depends on.
    """
    room = field_bytes(1, b"RM_fixture0000") + field_bytes(2, b"a-room-with-turn")
    stun = field_bytes(1, b"stun:stun.example.net:3478")
    turn = (
        field_bytes(1, b"turn:turn.example.net:3478?transport=udp")
        + field_bytes(1, b"turns:turn.example.net:5349?transport=tcp")
        + field_bytes(2, b"a-turn-user")
        + field_bytes(3, b"a-turn-credential")
    )
    server_info = field_bytes(2, b"1.13.5") + field_varint(3, 17)
    join = (
        field_bytes(1, room)
        + field_bytes(5, stun)
        + field_bytes(5, turn)
        + field_varint(6, 1)
        + field_varint(10, 20)
        + field_varint(11, 4)
        + field_bytes(12, server_info)
    )
    return field_bytes(1, join)


#: Every built fixture, as `name -> the bytes it must contain`. The two
#: recorded frames are deliberately absent: they are inputs here, not outputs,
#: and a recipe that could reproduce them would mean they were never evidence.
def built_fixtures():
    recorded = load("join-recorded.txt")
    return {
        "join-unknown-fields.txt": interleave_unknown_fields(recorded),
        "join-turn.txt": turn_join(),
        "empty.txt": b"",
        # Cut inside the length-delimited body of `join`, which claims 624
        # bytes: the walker must refuse, not return the fields it managed to
        # read before running out.
        "truncated-length.txt": recorded[:400],
        # Ends inside a varint: field 1, wire type 0, and then two bytes that
        # both say "more to come".
        "truncated-varint.txt": bytes([0x08, 0x80, 0x80]),
        # Eleven continuation bytes: a varint longer than any 64-bit value.
        "overlong-varint.txt": bytes([0x08]) + b"\x80" * 11 + b"\x01",
        # Field 1 with wire type 3 -- start of a deprecated group. No modern
        # encoder emits one and its length isn't knowable, so the walker
        # refuses rather than guess and desynchronise.
        "group-wire-type.txt": bytes([0x0B]),
    }


def write_all():
    """Regenerate every built fixture. `python3 -c "import sys;
    sys.path.insert(0, 'test'); import livekit_fixtures as f; f.write_all()"`
    from the repository root."""
    for name, data in built_fixtures().items():
        store(name, data)
        print("{}: {} bytes".format(name, len(data)))

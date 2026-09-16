# The live proof

A development fixture, never installed on a robot. It answers questions about
`fleetless_bridge/live.py` that no unit test can, because they are about what
a real LiveKit server, a real TURN relay, or a real browser viewer does with
what this package sends -- not about what this package's own code decides
given a payload it was handed:

- **does a real browser decode moving video from `LivePublisher`?**
  (`run-proof.sh`, `viewer-check.mjs`)
- **does this publisher actually route through a TURN relay when it has to,
  and can that be told apart from "it connected"?** (`run-relay-proof.sh`,
  `relay_probe.py`)
- **does a real server's `subscribedQualityUpdate` pause the feed when the
  only viewer leaves, and resume it when one comes back?**
  (`run-quality-proof.sh`, `quality-check.mjs`)
- **does an unattended session leak descriptors or memory?** (`run-soak.sh`,
  `soak.py`)

`test/test_live_relay.py` wraps the last three as pytest tests -- see its own
docstring for how and why it runs outside `run-tests.sh`'s own container.

Everything between the token and the pixels is the shipped code. Each harness
mints its own access token (the cloud does that on a real robot) and paints a
synthetic pattern (a camera does that); the peer connection, the track, the
signalling and the frame feed are `live.py`'s and `livekit_signal.py`'s.

## Running one case

```
./run-tests.sh --distro humble -k nothing_at_all   # builds the dev image once
tools/live-proof/run-proof.sh humble vp8
```

`run-proof.sh <distro> <vp8|h264|all> [--still]` starts the publisher inside
`fleetless-bridge-dev-<distro>` on the host network, waits for it to report
ICE connected, and then runs the browser viewer against the same room. Both
logs land in `tools/live-proof/logs/<distro>-<codec>/`, and both matter: a
viewer that saw nothing next to a publisher that never reached `connected` is
a different finding from one next to a publisher that did.

The target is the development LiveKit at `ws://localhost:7880` with the
development credentials (`LK_URL`, `LK_KEY`, `LK_SECRET` override them). The
room name carries the case and a random suffix, so a viewer cannot be looking
at another run's stream.

## What it asserts, and how each half is broken on purpose

The viewer asserts three independent things: a frame arrived (`videoWidth >
0`), it changed over one second, and it is not a flat rectangle. The first two
need separate break tests, because no single break fails both:

```
node tools/live-proof/viewer-check.mjs some-room-nobody-publishes-to --expect-none
tools/live-proof/run-proof.sh humble vp8 --still
```

The first runs the viewer with no publisher at all: no video may arrive. The
second runs a publisher painting a frozen frame: video must arrive and must
not move. A run of either that reports the ordinary verdict means the
instrument is measuring something other than what it says.

## The TURN/relay proof

```
tools/live-proof/run-relay-proof.sh humble
```

Creates its own `coturn/coturn` container (`--network host`, `-n
--lt-cred-mech`), removed by name on exit. `relay_probe.py` builds a
`connection_factory` that ignores `join`'s ICE servers, points at that coturn
instance instead, and prunes host/reflexive candidates from both what this
side offers AND what it actually tries (offering alone is not enough --
see that file's own docstring for why). It reads the SELECTED candidate pair
from aioice's own state (aiortc's public `getStats()` carries no
candidate-pair stats in the versions this package ships for) and fails unless
it is `relay`.

**This proof is unreliable on this dev host, whether or not anything else is
talking to the dev LiveKit at the same time.** Measured over roughly twenty
runs (see `test_live_relay.py`'s own docstring): the single-attempt pass
rate is under half either way, in streaks from 0-for-5 to 5-for-5 in BOTH
conditions -- a second connection makes it worse on average, but "nothing
else connected" is not a guarantee of a pass, only a better bet. The
mechanism itself -- once connected, the selected pair is `relay` -- has not
been the thing that failed in any of those runs; what fails is DTLS
completing inside LiveKit's fixed 10s `CONNECTION_TIMEOUT` at all.
`test_live_relay.py` retries that specific failure a bounded number of
times and does not retry a run that connected on something other than
`relay`; it also `warnings.warn`s on any pass that needed a retry, so a
shift from "occasionally needs one" to "always needs one" stays visible
rather than disappearing behind a later PASS.

## The quality proof

```
tools/live-proof/run-quality-proof.sh humble
```

Starts the publisher the same way `run-proof.sh` does, then runs
`quality-check.mjs`: a first viewer (the official `livekit-client`, headless)
connects and sees moving video, disconnects; a second viewer connects later
and also sees moving video. The script correlates the viewer's own
disconnect/reconnect timestamps against the publisher's own `Live publish
paused`/`resumed` log lines -- re-anchored onto one shared clock origin,
since the container and the host do not agree on timezone -- and reports the
delay in each direction.

## The soak

```
tools/live-proof/run-soak.sh humble 15 30
```

Publishes for the given number of minutes (default 15) at the given sample
interval in seconds (default 30), sampling `/proc/self/fd` and RSS inside the
container, and fits a least-squares slope over every sample after the first
two minutes of wall time. See `soak.py`'s own docstring for the flatness
thresholds and, just as importantly, for what a 15-minute run can and cannot
catch -- a flat line here is not the same claim a flat line over an hour
would be.

## What it needs on the host

- Docker, and the dev image for the distribution under test.
- The development LiveKit server, running and left alone — no script here
  starts, stops or reconfigures it.
- For the relay proof: nothing beyond Docker -- it pulls `coturn/coturn`
  itself if the image is not already local.
- For the moving-video and quality proofs: Node, a Chromium (`CHROME_PATH`,
  or one Playwright or the system installed) and `PLAYWRIGHT_CORE` pointing
  at a `playwright-core` entry point. The bridge ships no JavaScript
  dependencies and this fixture adds none; it borrows a browser driver that
  is already on the machine.
- Network access for the viewer pages, which load `livekit-client` from a CDN
  pinned by version *and* by content hash.

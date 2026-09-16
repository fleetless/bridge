#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# 3c.4's quality proof: with `add_track`'s declared layer (livekit_signal.py)
# and no latch in `live.py`'s `_on_quality_update` any more, does a real
# LiveKit server actually pause the publisher when its only viewer leaves, and
# resume it when a viewer comes back?
#
#   ./run-quality-proof.sh <distro>
#
# Same shape as run-proof.sh: the publisher runs inside the dev image for the
# distribution under test, on the host network; the viewer runs on the host
# through quality-check.mjs. Neither side sees the other directly -- this
# script correlates them by grepping the publisher's own timestamped log for
# `Live publish paused`/`resumed` against the viewer's `DISCONNECT_AT`/
# `RECONNECT_AT` lines, which is the same separation of concerns the moving-
# video proof already keeps.
set -u
cd "$(dirname "$0")"
HERE=$PWD
REPO=$(cd ../.. && pwd)

DISTRO=${1:-humble}
PUBLISH_SECONDS=${LIVE_QUALITY_PUBLISH_SECONDS:-70}
IMAGE="fleetless-bridge-dev-$DISTRO"
ROOM="fl-live-quality-$DISTRO-$RANDOM"
NAME="fl-live-quality-$DISTRO-$$"
LOGDIR="$HERE/logs/quality-$DISTRO"
mkdir -p "$LOGDIR"

DOCKER=docker
docker info >/dev/null 2>&1 || DOCKER="sudo docker"

if ! $DOCKER image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "run-quality-proof.sh: no image $IMAGE. Build it with ./run-tests.sh --distro $DISTRO first." >&2
    exit 2
fi

echo "=== quality / $DISTRO / room=$ROOM ==="
# Host-side wall clock, captured right as the container starts -- the
# container may not share the host's timezone (measured: the dev image runs
# UTC while this host runs CEST, a silent 2h skew that made an earlier
# version of this script report "no pause logged" for a publisher that had,
# in fact, paused on time). `T0_HOST` and, once the publisher's own first log
# line is available, `T0_CONTAINER` fix one shared origin for both clocks; see
# the python block below.
T0_HOST=$(date +%s.%N)
$DOCKER run --rm --name "$NAME" --network host \
    -v "$REPO:/ws:ro" -w /ws \
    "$IMAGE" python3 tools/live-proof/publish.py \
        --room "$ROOM" --codec vp8 --seconds "$PUBLISH_SECONDS" \
    > "$LOGDIR/publisher.log" 2>&1 &
PUB=$!

# "FAILED" bare is not the harness's own verdict: aioice logs one INFO line
# per losing ICE candidate pair as part of every SUCCESSFUL connection
# (`Connection(0) Check CandidatePair(...) State.FROZEN -> State.FAILED`,
# ordinary candidate-pair pruning), often before the harness's own
# "ICE/DTLS CONNECTED" line is written -- so a bare grep for "FAILED" broke
# this loop early on a publisher that was about to connect, aborting with
# exit 2 for a perfectly healthy publisher. Every line this harness
# (publish.py's `log()`) prints starts with "HARNESS"; aioice's own logger
# never does, so "HARNESS FAILED" is unambiguous.
for _ in $(seq 1 40); do
    sleep 1
    grep -q "ICE/DTLS CONNECTED" "$LOGDIR/publisher.log" && break
    grep -q "HARNESS FAILED" "$LOGDIR/publisher.log" && break
done
if ! grep -q "ICE/DTLS CONNECTED" "$LOGDIR/publisher.log"; then
    echo "  publisher did NOT connect; from its log:"
    grep -E "HARNESS FAILED|EXIT|Traceback|Error" "$LOGDIR/publisher.log" | tail -10
    wait $PUB
    exit 2
fi
echo "  publisher: ICE/DTLS CONNECTED"

node "$HERE/quality-check.mjs" "$ROOM" > "$LOGDIR/viewer.log" 2>&1
VIEWER=$?
sed -n '1,40p' "$LOGDIR/viewer.log"
echo "  viewer exit=$VIEWER"

wait $PUB
PUB_EXIT=$?
$DOCKER rm -f "$NAME" >/dev/null 2>&1 || true

if [ "$VIEWER" -ne 0 ]; then
    echo "  quality proof FAILED: the viewer script itself did not pass"
    exit 1
fi

# The viewer prints Date.now() (host epoch ms). The publisher's own
# logging.Formatter prints wall-clock HH:MM:SS.mmm with no date, **on the
# container's clock** -- which is not the host's: measured, this dev image
# runs UTC while a run on this host (CEST) showed a two-hour gap between a
# disconnect and the pause that in fact followed it within a second, from
# comparing the two clocks as if they agreed.
# `T0_HOST` (captured in the shell, above, right as the container started) and
# `T0_CONTAINER` (the publisher's own first log line, whatever its clock says)
# both mark "the moment the container's process started", to within the
# ~second it takes Python and aiortc to come up -- negligible next to the
# tens-of-seconds windows this proof measures. Every container timestamp is
# then expressed as an offset from `T0_CONTAINER` and re-anchored onto
# `T0_HOST`, which is what makes it comparable to the viewer's clock without
# this script ever needing to know *why* the two clocks disagreed.
DISCONNECT_AT=$(awk '/^DISCONNECT_AT /{print $2}' "$LOGDIR/viewer.log")
RECONNECT_AT=$(awk '/^RECONNECT_AT /{print $2}' "$LOGDIR/viewer.log")
if [ -z "$DISCONNECT_AT" ] || [ -z "$RECONNECT_AT" ]; then
    echo "  quality proof FAILED: viewer log carries no DISCONNECT_AT/RECONNECT_AT"
    exit 1
fi

python3 - "$LOGDIR/publisher.log" "$T0_HOST" "$DISCONNECT_AT" "$RECONNECT_AT" <<'PYEOF'
import re, sys

log_path, t0_host, disconnect_ms, reconnect_ms = (
    sys.argv[1], float(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]),
)
disconnect_s = disconnect_ms / 1000.0 - t0_host
reconnect_s = reconnect_ms / 1000.0 - t0_host

# Generous upper bounds, not a tight fit to the dev server's 5s pingInterval:
# wide enough that ordinary host jitter never trips them, tight enough that a
# regression to a much slower detection path (the layer declaration dropped,
# a longer pingInterval in production) still fails loudly instead of the
# delay being measured, printed, and silently accepted at any size.
PAUSE_BOUND_S = 15.0
RESUME_BOUND_S = 15.0

def to_seconds_of_day(hh, mm, ss, ms):
    return hh * 3600 + mm * 60 + ss + ms / 1000.0

TS = re.compile(r"^(\d\d):(\d\d):(\d\d)(?:\.(\d\d\d))?")
PAUSE_RESUME = re.compile(r"Live publish (paused|resumed)")

t0_container = None
paused_at = resumed_at = None
with open(log_path) as fh:
    for line in fh:
        m = TS.match(line)
        if not m:
            continue
        hh, mm, ss, ms = m.groups()
        t = to_seconds_of_day(int(hh), int(mm), int(ss), int(ms or 0))
        if t0_container is None:
            t0_container = t  # the very first timestamped line in the log
        offset = t - t0_container  # seconds since the container's process started

        pr = PAUSE_RESUME.search(line)
        if pr is None:
            continue
        which = pr.group(1)
        if which == "paused" and offset >= disconnect_s and paused_at is None:
            paused_at = offset
        if which == "resumed" and offset >= reconnect_s - 5 and resumed_at is None:
            # resumed can be logged slightly before RECONNECT_AT: RECONNECT_AT
            # is stamped when the SECOND viewer's <video> first decodes a
            # frame, which is strictly after the server's own quality update
            # that made the publisher resume encoding. -5 is the lower bound
            # on that early side; the upper bound (RESUME_BOUND_S below) is
            # what actually says "in the right order and promptly".
            resumed_at = offset

print("  viewer disconnected at t=%.3f" % disconnect_s)
if paused_at is None:
    print("  FAIL: no 'Live publish paused' logged at or after the disconnect")
    sys.exit(1)
pause_delay = paused_at - disconnect_s
print("  publisher paused %.3fs after the viewer disconnected" % pause_delay)
if pause_delay > PAUSE_BOUND_S:
    print("  FAIL: pause took %.3fs, exceeding the %.1fs bound" % (pause_delay, PAUSE_BOUND_S))
    sys.exit(1)

print("  second viewer saw video at t=%.3f" % reconnect_s)
if resumed_at is None:
    print("  FAIL: no 'Live publish resumed' logged from %.3fs before the second "
          "viewer's video onward" % 5.0)
    sys.exit(1)
resume_delay = resumed_at - reconnect_s
if resume_delay > RESUME_BOUND_S:
    print("  FAIL: resume logged %.3fs after the second viewer's video, "
          "exceeding the %.1fs bound" % (resume_delay, RESUME_BOUND_S))
    sys.exit(1)
if resume_delay >= 0:
    print("  publisher resumed %.3fs after the second viewer's video" % resume_delay)
else:
    print("  publisher had resumed by t=%.3f (%.3fs before the second viewer's video)"
          % (resumed_at, -resume_delay))

print("  PASS: pause and resume both observed, in the right order and within bound")
PYEOF
RESULT=$?
echo "  publisher exit=$PUB_EXIT"
grep -E "frames handed|EXIT|FAILED|LOST" "$LOGDIR/publisher.log" | tail -6
exit $RESULT

#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# `LivePublisher` against the dev LiveKit inside the dev image for the given
# distribution, `/proc/self/fd` and RSS sampled every 30s -- see `soak.py`'s
# own docstring for the sampling, the slope/threshold logic, and what a
# 15-minute run does and does not cover.
#
#   ./run-soak.sh <distro> [minutes] [sample-interval-s]
#
# **A viewer stays subscribed for the whole run.** Without one, LiveKit
# reports "no layer subscribed" about ten seconds after connect, `live.py`
# pauses the feed, and the per-frame path this soak exists to exercise --
# `cv2.cvtColor`, `VideoFrame.from_ndarray`, the encoder -- never runs again
# for the rest of the session: a run with no viewer samples a process that is
# asleep, not the session a robot with somebody actually watching runs. The
# viewer is `viewer-check.mjs --hold-seconds`, a real browser running the
# official `livekit-client`, started before the publisher (the order a real
# viewer-then-camera_start session runs in) and held for longer than the
# publisher's own run so it never drops out from under it.
#
# Own container, unique name, removed by that name -- never the shared dev
# LiveKit, never anything this script did not start.
set -u
cd "$(dirname "$0")"
HERE=$PWD
REPO=$(cd ../.. && pwd)

DISTRO=${1:-humble}
MINUTES=${2:-15}
SAMPLE_INTERVAL_S=${3:-30}
IMAGE="fleetless-bridge-dev-$DISTRO"
ROOM="fl-live-soak-$DISTRO-$RANDOM"
NAME="fl-live-soak-$DISTRO-$$"
LOGDIR="$HERE/logs/soak-$DISTRO"
mkdir -p "$LOGDIR"

DOCKER=docker
docker info >/dev/null 2>&1 || DOCKER="sudo docker"

if ! $DOCKER image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "run-soak.sh: no image $IMAGE. Build it with ./run-tests.sh --distro $DISTRO first." >&2
    exit 2
fi

# 60s of slack on top of the publisher's own run: the viewer joins first and
# must still be there when the publisher's very last sample is taken, not
# drop out right as the run winds down.
VIEWER_HOLD_S=$(( MINUTES * 60 + 60 ))

echo "=== soak / $DISTRO / $MINUTES minute(s) / ${SAMPLE_INTERVAL_S}s samples / room=$ROOM ===" | tee "$LOGDIR/soak.log"
echo "  viewer: hold ${VIEWER_HOLD_S}s, log at $LOGDIR/viewer.log"

node "$HERE/viewer-check.mjs" "$ROOM" --hold-seconds "$VIEWER_HOLD_S" \
    > "$LOGDIR/viewer.log" 2>&1 &
VIEWER_PID=$!

# A few seconds for the viewer to actually join the room before the publisher
# looks for a subscriber -- the order a real session runs in, and the reason
# `live.py`'s pause latch has anything to hold against from the first sample.
sleep 5

$DOCKER run --rm --name "$NAME" --network host \
    -v "$REPO:/ws:ro" -w /ws \
    "$IMAGE" python3 tools/live-proof/soak.py --room "$ROOM" --minutes "$MINUTES" \
        --sample-interval-s "$SAMPLE_INTERVAL_S" \
    2>&1 | tee -a "$LOGDIR/soak.log"
# Not `wait $!` on a `docker run` piped into `tee`: the exit code below comes
# from PIPESTATUS, since `$?` after a pipeline is `tee`'s -- the same trap as
# `pnpm test | tail` reporting `tail`'s exit code instead of the test run's.
SOAK_EXIT=${PIPESTATUS[0]}
echo "  soak exit=$SOAK_EXIT"
grep -E "SOAK (SAMPLE|PASS|FAIL)|EXIT" "$LOGDIR/soak.log" | tail -10

wait "$VIEWER_PID"
VIEWER_EXIT=$?
echo "  viewer exit=$VIEWER_EXIT"
tail -5 "$LOGDIR/viewer.log"

# The viewer dropping out mid-run is not a separate, ignorable finding: it
# means whatever the soak measured after that point is the same
# no-viewer-subscribed run this mode exists to not be. Both must pass for the
# run to mean what it claims.
if [ "$VIEWER_EXIT" -ne 0 ]; then
    echo "run-soak.sh: the viewer did not stay subscribed for the whole run --" >&2
    echo "run-soak.sh: see $LOGDIR/viewer.log. Whatever the soak measured after it" >&2
    echo "run-soak.sh: dropped is not a viewer-subscribed run any more." >&2
    exit 1
fi
exit "$SOAK_EXIT"

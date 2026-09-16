#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# One case of the live proof: start `LivePublisher` inside the dev image for
# one ROS distribution, then ask a real browser whether it decodes moving video
# from it.
#
#   ./run-proof.sh <distro> <vp8|h264|all> [--still]
#
# Both logs are kept. The viewer's verdict means nothing on its own: a viewer
# that saw no video next to a publisher that never reached `connected` is a
# different finding from one next to a publisher that did, and only the
# publisher's own log can tell them apart.
#
# The container is named after the case and removed by name at the end —
# never by "everything that is running", which on this host has repeatedly
# killed somebody else's database.
set -u
cd "$(dirname "$0")"
HERE=$PWD
REPO=$(cd ../.. && pwd)

DISTRO=${1:-humble}
CODEC=${2:-vp8}
STILL=${3:-}
SECONDS_TO_RUN=${LIVE_PROOF_SECONDS:-45}

case "$STILL" in
  ""|--still) ;;
  *) echo "run-proof.sh: third argument, if given, must be --still" >&2; exit 2 ;;
esac

LABEL="$DISTRO-$CODEC${STILL:+-still}"
ROOM="fl-live-proof-$LABEL-$RANDOM"
IMAGE="fleetless-bridge-dev-$DISTRO"
NAME="fl-live-proof-$LABEL-$$"
LOGDIR="$HERE/logs/$LABEL"
mkdir -p "$LOGDIR"

DOCKER=docker
docker info >/dev/null 2>&1 || DOCKER="sudo docker"

if ! $DOCKER image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "run-proof.sh: no image $IMAGE. Build it with ./run-tests.sh --distro $DISTRO first." >&2
    exit 2
fi

echo "=== $DISTRO / $CODEC${STILL:+ / still} / room=$ROOM ==="
$DOCKER run --rm --name "$NAME" --network host \
    -v "$REPO:/ws:ro" -w /ws \
    "$IMAGE" python3 tools/live-proof/publish.py \
        --room "$ROOM" --codec "$CODEC" --seconds "$SECONDS_TO_RUN" $STILL \
    > "$LOGDIR/publisher.log" 2>&1 &
PUB=$!

# "FAILED" bare is not the harness's own verdict: aioice logs one INFO line
# per losing ICE candidate pair as part of every SUCCESSFUL connection
# (`Connection(0) Check CandidatePair(...) State.FROZEN -> State.FAILED`,
# ordinary candidate-pair pruning), often before the harness's own
# "ICE/DTLS CONNECTED" line is written -- so a bare grep for "FAILED" broke
# the wait early on a publisher that was about to connect. Every line this
# harness (publish.py's `log()`) prints starts with "HARNESS"; aioice's own
# logger never does, so "HARNESS FAILED" is unambiguous.
for _ in $(seq 1 40); do
    sleep 1
    grep -q "ICE/DTLS CONNECTED" "$LOGDIR/publisher.log" && break
    grep -q "HARNESS FAILED" "$LOGDIR/publisher.log" && break
done
if grep -q "ICE/DTLS CONNECTED" "$LOGDIR/publisher.log"; then
    echo "  publisher: ICE/DTLS CONNECTED"
else
    echo "  publisher did NOT connect; from its log:"
    grep -E "HARNESS FAILED|EXIT|Traceback|Error" "$LOGDIR/publisher.log" | tail -10
fi

EXPECT=
[ -n "$STILL" ] && EXPECT=--expect-still
node "$HERE/viewer-check.mjs" "$ROOM" $EXPECT > "$LOGDIR/viewer.log" 2>&1
VIEWER=$?
sed -n '1,40p' "$LOGDIR/viewer.log"
echo "  viewer exit=$VIEWER"

wait $PUB
echo "  publisher exit=$?"
grep -E "frames handed|EXIT|FAILED|LOST" "$LOGDIR/publisher.log" | tail -4
$DOCKER rm -f "$NAME" >/dev/null 2>&1 || true
exit $VIEWER

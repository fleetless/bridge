#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# 3c.4's TURN/relay proof: create our own `coturn/coturn` container, point
# `relay_probe.py` at it (a `connection_factory` that ignores `join`'s ICE
# servers and forces relay -- see that file's own docstring for why filtering
# the offer alone is not enough), and check the SELECTED candidate pair on
# both ends is `relay` -- not merely "it connected".
#
#   ./run-relay-proof.sh <distro>
#
# The coturn container is created here and removed here, by the unique name
# this script gives it -- a destructive command must name its target, and
# "everything currently running" is not a target.
set -u
cd "$(dirname "$0")"
HERE=$PWD
REPO=$(cd ../.. && pwd)

DISTRO=${1:-humble}
IMAGE="fleetless-bridge-dev-$DISTRO"
ROOM="fl-relay-proof-$DISTRO-$RANDOM"
COTURN_NAME="fl-relay-coturn-$DISTRO-$$"
PROBE_NAME="fl-relay-probe-$DISTRO-$$"
COTURN_USER=relayuser
COTURN_PASS="relaypass-$RANDOM"
LOGDIR="$HERE/logs/relay-$DISTRO"
mkdir -p "$LOGDIR"

DOCKER=docker
docker info >/dev/null 2>&1 || DOCKER="sudo docker"

if ! $DOCKER image inspect "$IMAGE" >/dev/null 2>&1; then
    echo "run-relay-proof.sh: no image $IMAGE. Build it with ./run-tests.sh --distro $DISTRO first." >&2
    exit 2
fi

cleanup() {
    $DOCKER rm -f "$COTURN_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== relay / $DISTRO / room=$ROOM ==="
$DOCKER run -d --name "$COTURN_NAME" --network host coturn/coturn \
    -n --lt-cred-mech --user "$COTURN_USER:$COTURN_PASS" --realm fleetless-relay-proof.test \
    > "$LOGDIR/coturn.log" 2>&1
sleep 2

# A single attempt: this script does not retry. `relay_probe.py`'s selected
# pair, once connected, has been a `relay` pair every time it has been run
# this way -- what is unreliable on this dev host is DTLS completing inside
# LiveKit's fixed 10s `CONNECTION_TIMEOUT` over that relay path at all. A
# bounded retry cannot tell that apart from a real regression that made the
# relay path intermittently slower without breaking it outright, so this is
# named as an observed correlation with host load, not a proven cause --
# see `test_live_relay.py`'s own docstring for the honest gap.
# `test_live_relay.py` retries THIS SCRIPT a bounded number of times for
# exactly that reason, and only on that specific failure -- see its own
# docstring for the count and for why a run that connects but selects
# something other than `relay` is never retried.
$DOCKER run --rm --name "$PROBE_NAME" --network host \
    -v "$REPO:/ws:ro" -w /ws \
    "$IMAGE" python3 tools/live-proof/relay_probe.py \
        --room "$ROOM" --coturn-host 127.0.0.1 --coturn-port 3478 \
        --coturn-user "$COTURN_USER" --coturn-pass "$COTURN_PASS" --seconds 25 \
    > "$LOGDIR/probe.log" 2>&1
PROBE_EXIT=$?

grep -E "RELAY-PROBE" "$LOGDIR/probe.log"
echo "  probe exit=$PROBE_EXIT"
if [ "$PROBE_EXIT" -ne 0 ]; then
    echo "  from the probe's log:"
    grep -E "FAILED|Traceback|Error" "$LOGDIR/probe.log" | tail -10
fi
exit $PROBE_EXIT

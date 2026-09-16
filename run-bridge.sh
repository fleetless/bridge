#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Runs the bridge against a cloud, in the same container image as the tests.
# For development and for end-to-end checks; on a real robot the bridge runs as an
# installed ROS2 package instead (see README.md).
#
#   FLEETLESS_TOKEN=frt_... ./run-bridge.sh
#   FLEETLESS_TOKEN=frt_... FLEETLESS_CLOUD_URL=ws://localhost:8080/bridge ./run-bridge.sh
#   FLEETLESS_TOKEN=frt_... FLEETLESS_BRIDGE_CONTAINER_NAME=fleetless-bridge-2 ./run-bridge.sh
set -euo pipefail
cd "$(dirname "$0")"

: "${FLEETLESS_TOKEN:?is not set — create the robot in the console and pass its token}"

# Fall back to sudo when the user is not in the
# docker group, rather than failing with a permission-denied on the socket.
DOCKER=docker
docker info >/dev/null 2>&1 || DOCKER="sudo docker"

IMAGE=fleetless-bridge-dev
$DOCKER build -q -t "$IMAGE" -f Dockerfile.dev . >/dev/null

# Interactive only when there is a terminal, so this also works from a
# script or from CI, where stdin is not a tty.
tty_flags=()
if [ -t 0 ]; then
    tty_flags=(-it)
fi

# The value is substituted here rather than passed as a bare `-e
# FLEETLESS_TOKEN` (which tells docker to read it from ITS OWN process
# environment): under the sudo fallback above, sudo resets the environment by
# default, so a bare `-e FLEETLESS_TOKEN` silently hands the container an
# empty token instead of failing loudly — confirmed with a busybox repro.
#
# tools/dev-udp-only.xml forces Fast DDS onto plain UDP instead of its
# preferred shared-memory transport: this container and run-fake-robot.sh's
# have separate, unshared /dev/shm, so SHM-based delivery silently drops
# every message even though discovery (which stays on UDP multicast) works
# fine — see the file's own header for the full explanation. Development
# only; a real robot install never sets this.
# ROS_DOMAIN_ID: unset by default, same as always — this and
# run-fake-robot.sh must share one domain to see each other's topics, and
# 0 (rclpy's own default) remains that shared domain unless overridden.
# Pass one explicitly when run-tests.sh's dedicated domain (77 by default;
# see that script) would otherwise collide with a demo robot you are running
# at the same time — e.g. ROS_DOMAIN_ID=5 on both this and
# run-fake-robot.sh. See run-tests.sh's own comment for why this matters:
# an unset domain is not isolation, it is every ROS process on the host
# sharing one.
domain_flags=()
if [ -n "${ROS_DOMAIN_ID:-}" ]; then
    domain_flags=(-e "ROS_DOMAIN_ID=${ROS_DOMAIN_ID}")
fi

# FLEETLESS_DEVICES: opt-in, off by default — this script otherwise
# passes no --device at all, so a V4L2 camera is invisible to
# the bridge container even though run-fake-robot.sh's already hands it
# /dev/video0 for the fake robot to publish from. On a real robot install
# the bridge runs natively and every device is already there; this exists
# purely so the dev stack can demonstrate a V4L2 source end to end.
# Space-separated, e.g. FLEETLESS_DEVICES="/dev/video0 /dev/video2".
device_flags=()
if [ -n "${FLEETLESS_DEVICES:-}" ]; then
    for device in ${FLEETLESS_DEVICES}; do
        device_flags+=(--device "${device}")
    done
fi

# CONTAINER_NAME: this script used to pass no --name at all, so
# `docker run --rm` handed the container a random one (`bold_cray`,
# `hardcore_blackburn`, ...) and the rule — "give your own containers a
# unique --name per run and remove only that name" — was
# unfollowable by the very scripts every developer actually runs. Two such
# containers sat unnoticed on this host for six days, findable only by
# `docker inspect`ing every container's command line one at a time; a third
# was mistaken for one of them and destroyed before its owner (a live demo
# robot's bridge) was confirmed. A name is what makes "find and remove this
# one, on purpose" possible instead of "find and remove something old and
# hope." Defaulted so a plain invocation still gets one; override when
# running more than one bridge at once (matches ROS_DOMAIN_ID's pattern
# above) — a name collision fails `docker run` loudly rather than letting
# two robots share one container.
CONTAINER_NAME="${FLEETLESS_BRIDGE_CONTAINER_NAME:-fleetless-bridge}"

exec $DOCKER run --rm --name "$CONTAINER_NAME" "${tty_flags[@]}" --network host \
    -e "FLEETLESS_TOKEN=${FLEETLESS_TOKEN}" \
    -e "FLEETLESS_CLOUD_URL=${FLEETLESS_CLOUD_URL:-ws://localhost:8080/bridge}" \
    -e FASTRTPS_DEFAULT_PROFILES_FILE=/dev-udp-only.xml \
    "${domain_flags[@]}" "${device_flags[@]}" \
    -v "$PWD":/ws -v "$PWD/tools/dev-udp-only.xml":/dev-udp-only.xml \
    -w /ws "$IMAGE" \
    python3 -m fleetless_bridge.main

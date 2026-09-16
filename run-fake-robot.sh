#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Runs tools/fake_robot.py in the same container image as the bridge and its
# tests — a stand-in robot for end-to-end checks and manual development.
# Point run-bridge.sh at the same cloud and network, and it has real ROS traffic
# to introspect and subscribe to.
#
#   ./run-fake-robot.sh
#   FLEETLESS_VIDEO_DEVICE=/dev/video2 ./run-fake-robot.sh   # a different camera
#   FLEETLESS_VIDEO_DEVICE=none ./run-fake-robot.sh          # no camera at all
#   FLEETLESS_FAKEROBOT_CONTAINER_NAME=fleetless-fakerobot-2 ./run-fake-robot.sh
set -euo pipefail
cd "$(dirname "$0")"

# Same probe as run-bridge.sh: fall back to sudo when the
# user is not in the docker group, rather than failing on the socket.
DOCKER=docker
docker info >/dev/null 2>&1 || DOCKER="sudo docker"

IMAGE=fleetless-bridge-dev
$DOCKER build -q -t "$IMAGE" -f Dockerfile.dev . >/dev/null

tty_flags=()
if [ -t 0 ]; then
    tty_flags=(-it)
fi

# tools/dev-udp-only.xml forces Fast DDS onto plain UDP instead of its
# preferred shared-memory transport: this container and run-bridge.sh's have
# separate, unshared /dev/shm, so SHM-based delivery silently drops every
# message even though discovery (which stays on UDP multicast) works fine —
# see the file's own header for the full explanation. Development only; a
# real robot install never sets this.
#
# FLEETLESS_VIDEO_DEVICE hands the container a real capture device;
# fake_robot.py opens it with cv2.VideoCapture and publishes
# sensor_msgs/msg/Image on /image_raw. It defaults to /dev/video0, which is the
# webcam on the machine this was written on, and `docker run` refuses to start
# at all when the named device does not exist -- so on a headless box, or one
# whose camera is /dev/video2, the documented way to get a robot failed before
# the container existed. fake_robot.py itself has always degraded to "no
# camera" when the device will not open; the unconditional flag was what made
# that state unreachable. `none` (or an empty value) asks for no device, and is
# the same opt-in shape run-bridge.sh's FLEETLESS_DEVICES already uses.
VIDEO_DEVICE="${FLEETLESS_VIDEO_DEVICE:-/dev/video0}"
device_flags=()
case "$VIDEO_DEVICE" in
    none|"") ;;
    *)
        if [ ! -e "$VIDEO_DEVICE" ]; then
            echo "run-fake-robot.sh: $VIDEO_DEVICE does not exist." >&2
            echo "run-fake-robot.sh: set FLEETLESS_VIDEO_DEVICE=/dev/videoN to name" >&2
            echo "run-fake-robot.sh: the camera you have, or =none to run without one." >&2
            exit 1
        fi
        device_flags=(--device "$VIDEO_DEVICE")
        ;;
esac
# ROS_DOMAIN_ID: see run-bridge.sh's comment — this and run-bridge.sh
# must share one domain to see each other, unset (0) by default; pass one
# explicitly to move the demo pair off run-tests.sh's dedicated domain.
domain_flags=()
if [ -n "${ROS_DOMAIN_ID:-}" ]; then
    domain_flags=(-e "ROS_DOMAIN_ID=${ROS_DOMAIN_ID}")
fi

# CONTAINER_NAME: see run-bridge.sh's own comment on
# FLEETLESS_BRIDGE_CONTAINER_NAME for the incident this fixes — this script
# used to pass no --name either, and it is exactly this container
# (`tools/fake_robot.py`) that turned up on this host with a random Docker
# name and a six-day uptime nobody had noticed. Same default-and-overridable
# shape as ROS_DOMAIN_ID above.
CONTAINER_NAME="${FLEETLESS_FAKEROBOT_CONTAINER_NAME:-fleetless-fakerobot}"

exec $DOCKER run --rm --name "$CONTAINER_NAME" "${tty_flags[@]}" --network host \
    -e FASTRTPS_DEFAULT_PROFILES_FILE=/dev-udp-only.xml \
    "${domain_flags[@]}" \
    "${device_flags[@]}" \
    -v "$PWD":/ws -v "$PWD/tools/dev-udp-only.xml":/dev-udp-only.xml \
    -w /ws "$IMAGE" \
    python3 tools/fake_robot.py

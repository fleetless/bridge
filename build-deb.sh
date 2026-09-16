#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Builds the .deb for ONE ROS distribution, in a container of that
# distribution's own base image.
#
#   ./build-deb.sh jazzy
#
# The distribution is a variable, not three copies of debian/: the packaging
# sources are templates (debian/*.in) and everything that differs between
# distributions comes from the table in tools/distros.sh. This script renders
# the tracked tree into build/deb/<distro>/, builds there, and copies the
# artifact into dist/.
#
# **The container is named and removed by that name.** Nothing here stops,
# kills or removes a container it did not create.
set -euo pipefail
cd "$(dirname "$0")"

. tools/distros.sh

if [ "$#" -ne 1 ]; then
    echo "usage: $(basename "$0") <ros-distro>" >&2
    echo "supported: $FLEETLESS_DISTROS" >&2
    exit 2
fi

# Refused here, before anything is rendered or any image is pulled: an unknown
# name must not reach the templates, where it would produce a perfectly
# well-formed package called ros-<typo>-fleetless-bridge that only apt on a
# robot ever finds wrong. This also refuses a distribution whose table entry is
# incomplete — see fleetless_distro_require.
DISTRO=$1
fleetless_distro_require "$DISTRO"

CODENAME=$(fleetless_distro_field "$DISTRO" codename)

# Same probe as run-tests.sh: fall back to sudo when `docker info` does not
# answer, rather than failing on the socket.
DOCKER=docker
docker info >/dev/null 2>&1 || DOCKER="sudo docker"

STAGE="build/deb/$DISTRO"
CONTAINER="fleetless-bridge-build-$DISTRO"

echo "build-deb.sh: rendering the packaging for $DISTRO ($CODENAME) into $STAGE" >&2
fleetless_distro_render "$DISTRO" . "$STAGE"

# The rendered tree is what gets built, so say what it came out as. A package
# name nobody looked at is the whole failure this script exists to prevent.
SOURCE=$(sed -n 's/^Source: //p' "$STAGE/debian/control")
VERSION=$(sed -n '1s/.*(\(.*\)).*/\1/p' "$STAGE/debian/changelog")
echo "build-deb.sh: source package $SOURCE" >&2
echo "build-deb.sh: version        $VERSION" >&2

# `dpkg-buildpackage` writes the .deb into the PARENT of the source tree (see
# the comment at the end of debian/rules.in), which is build/deb/ here, and the
# artifact is copied into dist/ before the container exits.
mkdir -p dist
$DOCKER run --rm --name "$CONTAINER" \
    -v "$PWD/$STAGE":/src -v "$PWD/dist":/dist -w /src \
    -e PYTHONDONTWRITEBYTECODE=1 \
    -e "HOST_UID=$(id -u)" -e "HOST_GID=$(id -g)" \
    "ros:$DISTRO" bash -c '
        set -e
        # The container runs as root on two mounted host directories, so
        # everything it writes there lands root-owned -- including the build
        # tree dpkg leaves behind, which the NEXT render then cannot delete
        # ("rm: cannot remove build/deb/<distro>/.pybuild/...: Permission
        # denied"). Chowned back on the way out, on failure as well as on
        # success, because a failed build is exactly when somebody re-runs.
        trap "chown -R $HOST_UID:$HOST_GID /src /dist" EXIT
        apt-get update
        apt-get install -y --no-install-recommends \
            debhelper dh-python python3-all python3-setuptools fakeroot dpkg-dev
        dpkg-buildpackage -us -uc -b
        # The parent of /src is /, which is where dpkg-buildpackage puts the
        # artifact. Copied out before the container exits, and chowned back:
        # the container runs as root on a mounted host directory, so without
        # this the developer cannot delete their own build output.
        cp /ros-*-fleetless-bridge_*.deb /dist/
    '

# The one artifact this run produced, named exactly. `dist/` accumulates every
# version ever built here, so a glob would list six old packages beside the new
# one and say nothing about which is which -- and would still print something
# if this build had produced no file at all.
ARTIFACT="dist/${SOURCE}_${VERSION}_all.deb"
if [ ! -f "$ARTIFACT" ]; then
    echo "build-deb.sh: the build finished but $ARTIFACT does not exist." >&2
    exit 1
fi
echo "build-deb.sh: built $ARTIFACT" >&2

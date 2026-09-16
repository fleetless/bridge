#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Installs the package from a SERVED apt repository, the way a robot would:
# one clean container per distribution.
#
#   ./apt/check-served.sh https://apt.fleetless.dev 3.1.0
#   ./apt/check-served.sh https://apt.fleetless.dev 3.1.0 jazzy
#
# apt/check-suites.sh reads the indices; this installs from them. In each
# distribution's own `ros:<distro>` image:
#   * `apt-get install` from that suite with the served key -- the suite is
#     reachable, signed with a key the container trusts, dependency-complete;
#   * the installed version is exactly <version>-0<codename>;
#   * `import fleetless_bridge` -- the payload landed where that distribution's
#     Python looks. The first Jazzy and Lyrical packages installed cleanly and
#     then raised ModuleNotFoundError;
#   * the installed copyright declares Apache-2.0;
#   * `apt-cache show` of every OTHER distribution's package fails. An install
#     attempt alone is not this check: a Jazzy .deb filed into the humble suite
#     still fails to install on jammy, on its dependencies, and reports the
#     cross-serving as a working refusal.
#
# **Every container is named here, with the run id, and removed by that name.**
set -euo pipefail
cd "$(dirname "$0")/.."
. tools/distros.sh

if [ "$#" -lt 2 ]; then
    echo "usage: $(basename "$0") <base-url> <version> [distro...]" >&2
    exit 2
fi
BASE=${1%/}
VERSION=$2
shift 2
if ! [[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "check-served.sh: '$VERSION' is not an upstream version like 3.1.0" >&2
    exit 2
fi
DISTROS=${*:-$FLEETLESS_DISTROS}
for d in $DISTROS; do
    fleetless_distro_require "$d" || exit 2
done

DOCKER=docker
docker info >/dev/null 2>&1 || DOCKER="sudo docker"
RUN=${GITHUB_RUN_ID:-$$}
created=()
cleanup() {
    for name in ${created[@]+"${created[@]}"}; do
        $DOCKER rm -f "$name" >/dev/null 2>&1 || true
    done
}
trap cleanup EXIT

failed=0
for d in $DISTROS; do
    codename=$(fleetless_distro_field "$d" codename)
    others=""
    for o in $FLEETLESS_DISTROS; do
        [ "$o" = "$d" ] || others="$others ros-$o-fleetless-bridge"
    done
    name="fleetless-apt-check-$d-$RUN"
    created+=("$name")
    echo "check-served.sh: $d -- ros-$d-fleetless-bridge $VERSION-0$codename from $BASE" >&2
    if $DOCKER run --name "$name" \
        -e BASE="$BASE" -e D="$d" -e WANT="$VERSION-0$codename" -e OTHERS="$others" \
        "ros:$d" bash -c '
            set -euo pipefail
            export DEBIAN_FRONTEND=noninteractive
            apt-get update -qq
            apt-get install -y -qq --no-install-recommends curl ca-certificates >/dev/null
            curl -fsSL "$BASE/key.gpg" > /usr/share/keyrings/fleetless.gpg
            echo "deb [signed-by=/usr/share/keyrings/fleetless.gpg] $BASE $D main" \
                > /etc/apt/sources.list.d/fleetless.list
            apt-get update -qq
            apt-get install -y -qq "ros-$D-fleetless-bridge" >/dev/null
            got=$(dpkg-query -W -f "\${Version}" "ros-$D-fleetless-bridge")
            [ "$got" = "$WANT" ] || { echo "installed $got, expected $WANT" >&2; exit 1; }
            # As apt/verify-suites.sh does: a setup script that returns non-zero
            # is not the failure this checks, and the import below is.
            set +u; . "/opt/ros/$D/setup.sh" || true; set -u
            python3 -c "import fleetless_bridge; print(\"imported from\", fleetless_bridge.__file__)"
            grep -q "Apache-2.0" "/usr/share/doc/ros-$D-fleetless-bridge/copyright" \
                || { echo "the installed copyright does not declare Apache-2.0" >&2; exit 1; }
            for o in $OTHERS; do
                if apt-cache show "$o" >/dev/null 2>&1; then
                    echo "CROSS-SERVED: $o is reachable from the $D suite" >&2
                    exit 1
                fi
            done
        '; then
        echo "check-served.sh: $d ok" >&2
    else
        echo "check-served.sh: $d FAILED" >&2
        failed=1
    fi
done
exit "$failed"

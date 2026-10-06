# SPDX-License-Identifier: Apache-2.0
# What a test run ran on, printed once per run by run-tests.sh, which sources
# this file (it is not executed on its own).
#
# The test image is `ros:<distro>` plus `apt-get dist-upgrade` plus
# `rosdep install`, rebuilt on every CI run, and nothing in it is pinned -- on
# purpose: the bridge has to work on what a robot installs today. So two runs
# of the same commit can run different middleware, and before these lines a
# job log could not say which. With them, a change between two runs is a diff
# of two logs.
#
# Every function here reports and returns 0. A digest or a version that cannot
# be read is said in one line and the run goes on: this is evidence, not a
# gate.
#
# `$DOCKER` is the caller's (`docker` or `sudo docker`) and is word-split on
# purpose.

# The package families that decide how a sample travels between two ROS
# nodes: the RMW layer, the client libraries and the DDS implementations a
# distribution may ship. `fastdds` is Fast DDS's package name from Kilted on
# (Lyrical installs `ros-lyrical-fastdds`), `fastrtps` the name before it.
FLEETLESS_REPORTED_PACKAGES="rmw rcl fastrtps fastdds fastcdr cyclonedds"

# Pulled here rather than left to `docker build`: BuildKit resolves a FROM
# image without keeping it in the image store, so after a build there is no
# local `ros:<distro>` to read a digest from. Pulled first, the build uses this
# copy, and fleetless_report_test_image checks that it did.
fleetless_pull_base_image() {
    if ! $DOCKER pull -q "$1" >/dev/null 2>&1; then
        echo "run-tests.sh: could not pull $1; building on the local copy, if there is one" >&2
    fi
    return 0
}

# $1 base image reference (ros:<distro>), $2 test image tag, $3 distribution.
fleetless_report_test_image() {
    _fleetless_report_base_image "$1" "$2"
    _fleetless_report_packages "$2" "$3"
    return 0
}

_fleetless_report_base_image() {
    local digests base_layers image_layers
    digests=$($DOCKER image inspect --format '{{join .RepoDigests " "}}' "$1" 2>/dev/null) || digests=""
    if [ -z "$digests" ]; then
        echo "run-tests.sh: base image $1: digest unknown (no local copy with a registry digest)" >&2
        return 0
    fi
    # The test image is built on this copy exactly when its layer list starts
    # with this copy's layers.
    base_layers=$($DOCKER image inspect --format '{{join .RootFS.Layers " "}}' "$1" 2>/dev/null) || base_layers=""
    image_layers=$($DOCKER image inspect --format '{{join .RootFS.Layers " "}}' "$2" 2>/dev/null) || image_layers=""
    if [ -n "$base_layers" ] && [ "${image_layers#"$base_layers"}" != "$image_layers" ]; then
        echo "run-tests.sh: base image $1 = $digests (test image $2 is built on it)" >&2
    else
        echo "run-tests.sh: base image $1 = $digests (test image $2 is NOT built on it: the build used another copy)" >&2
    fi
    return 0
}

_fleetless_report_packages() {
    local listing installed
    # A subshell with globbing off: the patterns are dpkg-query's, and an
    # unquoted `ros-jazzy-rmw*` would otherwise expand against whatever files
    # sit in the caller's directory. dpkg-query exits 1 when one pattern
    # matches nothing (no distribution here installs cyclonedds), and still
    # prints the rest -- so the status is ignored and the output is what counts.
    listing=$(
        set -f
        patterns=""
        for family in $FLEETLESS_REPORTED_PACKAGES; do
            patterns="$patterns ros-$2-$family*"
        done
        # shellcheck disable=SC2086 -- split on purpose, globbing is off
        $DOCKER run --rm --entrypoint dpkg-query "$1" \
            -W -f '${db:Status-Abbrev} ${Package} ${Version}\n' $patterns 2>/dev/null
    ) || true
    installed=$(printf '%s\n' "$listing" | awk '$1 == "ii" && NF >= 3 { print "run-tests.sh: package " $2 " " $3 }')
    if [ -z "$installed" ]; then
        echo "run-tests.sh: ROS/DDS package versions of $1: could not be read" >&2
        return 0
    fi
    printf '%s\n' "$installed" >&2
    return 0
}

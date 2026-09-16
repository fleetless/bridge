#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Proves that the suites declared in reprepro/conf/distributions serve what
# they claim -- by installing from them, in each distribution's own container.
#
#   ./apt/verify-suites.sh
#
# **A file listing on a server is not this check.** The question is not whether
# a .deb reached `pool/`; it is whether `apt-get install
# ros-<distro>-fleetless-bridge` succeeds on a machine subscribed to that
# distribution's suite, and whether the same command for the OTHER two
# distributions fails there. Both halves are run here, for all three, against a
# real reprepro repository built from the tracked
# `reprepro/conf/distributions` beside this script, and served over HTTP.
#
# The repository is a throwaway one this script creates and removes, signed
# with a throwaway key. It cannot be run against apt.fleetless.dev and does not
# try: that repository is signed by the apt publishing workflow, the one place
# its key is decrypted. So the last mile -- that the served repository was
# published to correctly -- is apt/check-served.sh, run against the served
# repository after a publish. What this script proves is everything up to the
# moment of upload: the config, the publish path, and the .deb files themselves.
#
# **Every container is created here with a unique name and removed by that
# name.** Nothing stops, kills or removes a container or a network it did not
# create.
#
# What it does NOT prove, said out loud:
#   * nothing about the production signing key -- a throwaway key is generated
#     inside the repository container and dies with it. `SignWith:` is the one
#     line substituted, and that substitution is printed.
#   * nothing about the architectures. The package is `Architecture: all` and
#     these containers are one architecture; the arm64 half of `Architectures:`
#     is asserted by reading the served indices, not by running an arm64 robot.
set -euo pipefail
cd "$(dirname "$0")/.."

. tools/distros.sh

DOCKER=docker
docker info >/dev/null 2>&1 || DOCKER="sudo docker"

CONF=apt/reprepro/conf/distributions
WORK=build/apt-verify
NET=fleetless-apt-verify-net
SERVE=fleetless-apt-verify-serve
REPO_C=fleetless-apt-verify-repo

VERSION=$(sed -n '1s/.*(\(.*\)).*/\1/p' debian/changelog.in)   # 3.1.0-0@DEB_CODENAME@
UPSTREAM=${VERSION%%-*}

# ---------------------------------------------------------------------------
# The three artifacts, named exactly. dist/ accumulates every version ever
# built, so a glob would find a 2.0.0 and report on it happily.
# ---------------------------------------------------------------------------
DEBS=""
for d in $FLEETLESS_DISTROS; do
    codename=$(fleetless_distro_field "$d" codename)
    deb="dist/ros-$d-fleetless-bridge_${UPSTREAM}-0${codename}_all.deb"
    if [ ! -f "$deb" ]; then
        echo "verify-suites.sh: $deb does not exist." >&2
        echo "verify-suites.sh: build all three first:" >&2
        for e in $FLEETLESS_DISTROS; do echo "verify-suites.sh:   ./build-deb.sh $e" >&2; done
        exit 1
    fi
    DEBS="$DEBS $deb"
done

cleanup() {
    $DOCKER rm -f "$SERVE" >/dev/null 2>&1 || true
    for d in $FLEETLESS_DISTROS; do
        $DOCKER rm -f "fleetless-apt-verify-$d" >/dev/null 2>&1 || true
    done
    $DOCKER network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup
rm -rf "$WORK"
mkdir -p "$WORK/repo/conf"
cp "$CONF" "$WORK/repo/conf/distributions"
mkdir -p "$WORK/debs"
cp $DEBS "$WORK/debs/"

# ---------------------------------------------------------------------------
# 1. Build the repository, with reprepro, from the tracked conf beside this file.
#
# The ONLY edit to conf/distributions is SignWith:, replaced with a key
# generated inside this container and thrown away with it. Every other line --
# Codename, Architectures, Components, Origin, Label -- is the tracked bytes,
# which is the point: a wrong Architectures line produces a repository that
# looks correct and serves nothing, and it has to be THIS file that is tested.
#
# The substitution is printed so that a reader can see it is one line.
# ---------------------------------------------------------------------------
echo "=== building a throwaway reprepro repository from $CONF ===" >&2
$DOCKER run --rm --name "$REPO_C" \
    -v "$PWD/$WORK":/work -v "$PWD/apt":/apt:ro -v "$PWD/tools":/tools:ro \
    -e "HOST_UID=$(id -u)" -e "HOST_GID=$(id -g)" \
    ubuntu:24.04 bash -c '
        set -e
        trap "chown -R $HOST_UID:$HOST_GID /work" EXIT
        export DEBIAN_FRONTEND=noninteractive
        apt-get update -qq
        apt-get install -y -qq --no-install-recommends reprepro gnupg dpkg-dev >/dev/null

        gpg --batch --gen-key <<BATCH
%no-protection
Key-Type: RSA
Key-Length: 2048
Name-Real: Fleetless apt verify (throwaway)
Name-Email: verify@localhost
Expire-Date: 0
%commit
BATCH
        FPR=$(gpg --list-keys --with-colons verify@localhost | awk -F: "/^fpr:/ {print \$10; exit}")
        echo "throwaway signing key: $FPR"
        echo "--- the one substitution made to conf/distributions:"
        grep -n "^SignWith:" /work/repo/conf/distributions | sed "s/^/    was  /"
        sed -i "s/^SignWith:.*/SignWith: $FPR/" /work/repo/conf/distributions
        grep -n "^SignWith:" /work/repo/conf/distributions | sed "s/^/    now  /"
        echo "--- suites reprepro reads from it:"
        reprepro -b /work/repo dumpreferences >/dev/null || true
        grep "^Codename:" /work/repo/conf/distributions | sed "s/^/    /"

        # Published through apt/publish-deb.sh, which is the command the
        # runbook tells a human to run. Testing a different command than the
        # one the runbook names would leave the runbook unverified.
        for deb in /work/debs/*.deb; do
            /apt/publish-deb.sh /work/repo "$deb"
        done

        gpg --export "$FPR" > /work/repo/key.gpg
        echo "--- what each suite ended up carrying:"
        for s in $(grep "^Codename:" /work/repo/conf/distributions | sed "s/^Codename: *//"); do
            echo "  suite $s:"
            reprepro -b /work/repo list "$s" | sed "s/^/    /"
        done
    '

# The indices, read from the built tree rather than from reprepro's own report:
# `reprepro list` answers out of its database, and what apt fetches is
# dists/<suite>/main/binary-<arch>/Packages.
echo "=== the served Packages indices ===" >&2
for d in $FLEETLESS_DISTROS; do
    for arch in amd64 arm64; do
        idx="$WORK/repo/dists/$d/main/binary-$arch/Packages"
        if [ ! -f "$idx" ]; then
            echo "verify-suites.sh: $idx was not written." >&2
            echo "verify-suites.sh: the suite '$d' does not serve $arch, which the" >&2
            echo "verify-suites.sh: Architectures: line in $CONF says it does." >&2
            exit 1
        fi
        names=$(sed -n 's/^Package: //p' "$idx" | sort -u | tr '\n' ' ')
        echo "  $d/$arch: $names" >&2
        if [ "$names" != "ros-$d-fleetless-bridge " ]; then
            echo "verify-suites.sh: suite '$d' ($arch) carries [$names], expected exactly" >&2
            echo "verify-suites.sh: 'ros-$d-fleetless-bridge'. A suite carrying another" >&2
            echo "verify-suites.sh: distribution's package is the whole failure this file" >&2
            echo "verify-suites.sh: exists to prevent." >&2
            exit 1
        fi
    done
done

# ---------------------------------------------------------------------------
# 2. Serve it, and install from it.
# ---------------------------------------------------------------------------
$DOCKER network create "$NET" >/dev/null
$DOCKER run -d --name "$SERVE" --network "$NET" --network-alias apt-verify \
    -v "$PWD/$WORK/repo":/srv:ro -w /srv \
    python:3.12-slim python3 -m http.server 80 >/dev/null

# The server is a container that has to have finished starting before the first
# client asks. A fixed sleep cannot tell "slow" from "dead"; this loop can.
ready=no
for _ in $(seq 1 30); do
    if $DOCKER run --rm --network "$NET" python:3.12-slim \
        python3 -c 'import urllib.request,sys; urllib.request.urlopen("http://apt-verify/key.gpg").read(); ' \
        >/dev/null 2>&1; then ready=yes; break; fi
    sleep 1
done
[ "$ready" = yes ] || { echo "verify-suites.sh: the throwaway apt server never answered." >&2; exit 1; }

FAILED=""
for d in $FLEETLESS_DISTROS; do
    others=$(for o in $FLEETLESS_DISTROS; do [ "$o" = "$d" ] || printf '%s ' "$o"; done)
    echo "=== $d: installing from suite '$d', and refusing $others ===" >&2
    if $DOCKER run --rm --name "fleetless-apt-verify-$d" --network "$NET" \
        -e "DISTRO=$d" -e "OTHERS=$others" \
        "ros:$d" bash -c '
            set -e
            export DEBIAN_FRONTEND=noninteractive
            apt-get update -qq
            apt-get install -y -qq --no-install-recommends curl gnupg ca-certificates >/dev/null

            # Exactly the three lines README.md tells a user to run, with the
            # host swapped. If the README drifts from this, this check stops
            # being about the documented install.
            curl -fsSL http://apt-verify/key.gpg > /usr/share/keyrings/fleetless.gpg
            echo "deb [signed-by=/usr/share/keyrings/fleetless.gpg] http://apt-verify $DISTRO main" \
                > /etc/apt/sources.list.d/fleetless.list
            apt-get update

            echo "--- apt-cache policy, from the suite this robot subscribed to:"
            apt-cache policy "ros-$DISTRO-fleetless-bridge"

            apt-get install -y "ros-$DISTRO-fleetless-bridge"

            # Installed is not imported. The first jazzy and lyrical packages
            # installed without a murmur and then raised ModuleNotFoundError,
            # because the python site directory differs per distribution.
            # The ros:<distro> entrypoint already sourced this; sourcing it
            # again is belt and braces and its own exit code is not what is
            # being asserted here. The import on the next line is.
            source "/opt/ros/$DISTRO/setup.bash" || true
            python3 -c "import fleetless_bridge, sys; print(\"import OK:\", fleetless_bridge.__file__)"
            dpkg-query -W -f "installed: \${Package} \${Version}\n" "ros-$DISTRO-fleetless-bridge"

            # The refusal. Each of the other distributions packages must be
            # unreachable from this suite -- not merely absent from the pool,
            # but something apt cannot name.
            #
            # **The apt-cache half is the one that discriminates, and it was
            # measured that way.** With the Jazzy .deb deliberately filed into
            # the humble suite, `apt-get install ros-jazzy-fleetless-bridge` on
            # jammy still exits non-zero -- `Depends: ros-jazzy-cv-bridge but it
            # is not installable` -- so an install-only check reports the
            # refusal working while the suite is serving the wrong package.
            # `apt-cache show` is what separates "this suite does not offer it"
            # from "this suite offers it and the dependencies happen to be
            # missing", and only the second one is a repository defect.
            rc=0
            for o in $OTHERS; do
                echo "--- expecting failure: apt-get install ros-$o-fleetless-bridge"
                # NOT piped into tail: a pipeline exits with the LAST command
                # status, so `apt-get ... | tail -3` reports tail success and
                # cannot tell an install that worked from one that did not.
                out=$(apt-get install -y --no-install-recommends "ros-$o-fleetless-bridge" 2>&1) && ok=0 || ok=$?
                printf "%s\n" "$out" | tail -3
                if [ "$ok" = 0 ]; then
                    echo "REFUSAL FAILED: suite $DISTRO offered ros-$o-fleetless-bridge"
                    rc=1
                fi
                if apt-cache show "ros-$o-fleetless-bridge" >/dev/null 2>&1; then
                    echo "REFUSAL FAILED: suite $DISTRO has an index entry for ros-$o-fleetless-bridge"
                    rc=1
                fi
            done
            exit $rc
        '; then
        echo "=== $d: OK ===" >&2
    else
        echo "=== $d: FAILED ===" >&2
        FAILED="$FAILED $d"
    fi
done

if [ -n "$FAILED" ]; then
    echo "verify-suites.sh: failed for:$FAILED" >&2
    exit 1
fi
echo "verify-suites.sh: all suites install their own package and refuse the others." >&2

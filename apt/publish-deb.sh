#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Publishes ONE built .deb into ONE suite of a reprepro repository.
#
#   ./apt/publish-deb.sh /srv/apt dist/ros-jazzy-fleetless-bridge_3.1.0-0noble_all.deb
#
# **The suite is derived from the package, never given on the command line.**
# `reprepro includedeb <suite> <deb>` will happily file any .deb into any suite:
# nothing in reprepro compares the two, so `includedeb humble
# ros-jazzy-fleetless-bridge_*.deb` succeeds, and the first thing that notices
# is a Humble robot being offered a package built for Ubuntu 24.04. One
# mistyped word in a runbook is all it takes, which is why the suite is not a
# word anybody types here.
#
# It is derived from the .deb's own `Package:` field -- read out of the built
# artifact with `dpkg-deb`, not off the filename, which a copy or a rename can
# make say anything. The version's debian revision is checked against the same
# distribution's codename from the table for the same reason: those two are the
# only per-distribution strings a reader can see in `apt-cache policy`, and a
# package whose name and codename disagree was built from a half-rendered tree.
#
# Publishing one distribution therefore touches exactly one suite. reprepro
# rewrites only the `dists/<suite>/` tree it filed into; the other two suites'
# indices and signatures are not re-read, not re-signed and not changed. That
# is what makes "publish Jazzy without touching Humble" true rather than
# careful.
set -euo pipefail
cd "$(dirname "$0")/.."

. tools/distros.sh

if [ "$#" -ne 2 ]; then
    echo "usage: $(basename "$0") <reprepro-basedir> <deb>" >&2
    echo "  e.g. $(basename "$0") /srv/apt dist/ros-humble-fleetless-bridge_3.1.0-0jammy_all.deb" >&2
    exit 2
fi
BASEDIR=$1
DEB=$2

[ -f "$DEB" ] || { echo "publish-deb.sh: no such file: $DEB" >&2; exit 1; }
[ -d "$BASEDIR/conf" ] || {
    echo "publish-deb.sh: $BASEDIR/conf does not exist." >&2
    echo "publish-deb.sh: that directory is the apt repository's own conf/ -- on the server" >&2
    echo "publish-deb.sh: it is a symlink to apt/reprepro/conf in the checkout." >&2
    exit 1
}

PKG=$(dpkg-deb -f "$DEB" Package)
VERSION=$(dpkg-deb -f "$DEB" Version)
[ -n "$PKG" ] || { echo "publish-deb.sh: $DEB has no Package: field." >&2; exit 1; }

# ros-<distro>-fleetless-bridge, and nothing else. A .deb of some other package
# has no suite here at all, and guessing one from a prefix is how a stray file
# in dist/ gets published.
case "$PKG" in
    ros-*-fleetless-bridge) ;;
    *)
        echo "publish-deb.sh: '$PKG' is not a ros-<distro>-fleetless-bridge package." >&2
        echo "publish-deb.sh: the Fleetless apt repository serves that package alone." >&2
        exit 1 ;;
esac
DISTRO=${PKG#ros-}
DISTRO=${DISTRO%-fleetless-bridge}

# Refuses an unknown distribution by name and lists the supported ones. A .deb
# called ros-jazy-fleetless-bridge -- which `build-deb.sh` cannot produce, but a
# hand-run dpkg-buildpackage over a hand-edited tree can -- stops here rather
# than creating a suite-shaped hole.
fleetless_distro_require "$DISTRO"
CODENAME=$(fleetless_distro_field "$DISTRO" codename)

# The debian revision is the codename (`3.1.0-0jammy`). If it names a different
# distribution's codename than the package name does, the tree that built this
# file was rendered for one distribution and named for another.
case "$VERSION" in
    *-*"$CODENAME") ;;
    *)
        echo "publish-deb.sh: $PKG is version '$VERSION', whose debian revision does not end" >&2
        echo "publish-deb.sh: in '$CODENAME' -- the codename tools/distros.sh gives for" >&2
        echo "publish-deb.sh: '$DISTRO'. The package name and the version disagree about" >&2
        echo "publish-deb.sh: which distribution this was built for; refusing to publish it." >&2
        exit 1 ;;
esac

# Said out loud before the write, because this is the whole decision.
echo "publish-deb.sh: $DEB" >&2
echo "publish-deb.sh:   package $PKG" >&2
echo "publish-deb.sh:   version $VERSION" >&2
echo "publish-deb.sh:   -> suite $DISTRO (and no other)" >&2

reprepro -b "$BASEDIR" includedeb "$DISTRO" "$DEB"

echo "publish-deb.sh: $DISTRO now serves:" >&2
reprepro -b "$BASEDIR" list "$DISTRO" >&2

# ---------------------------------------------------------------------------
# What the suite indexes now, read back off disk.
#
# `reprepro list` above answers out of reprepro's own database. What apt fetches
# is dists/<suite>/main/binary-<arch>/Packages, which reprepro regenerates on
# every includedeb -- and the guards further up cannot see a suite that was
# already wrong before this run. A .deb filed into the wrong suite by an earlier
# hand-run `reprepro includedeb` sits there indefinitely, and this is the first
# moment somebody who can fix it is looking.
#
# `apt/check-suites.sh --local` runs the same comparison this used to carry a
# second copy of: Package/Version against the expected version, an empty or
# missing index reported as not-yet-published, anything else reported broken.
# `--local`, not over HTTP: this runs before the files have necessarily been
# served to anyone. `--require-published` is given here because a missing
# architecture at THIS point is not "nobody has published yet" -- the publish
# this script just ran is what should have written it.
#
# `--expected-version "$VERSION"` rather than letting check-suites.sh read
# `debian/changelog.in` itself: this script already extracted $VERSION from
# the .deb via dpkg-deb, above, and `apt/verify-suites.sh`'s throwaway
# container mounts only `apt/` and `tools/` -- no `debian/` at all -- so a
# changelog read here would fail on a missing file that has nothing to do
# with any suite being wrong, breaking the one rehearsal the runbook tells an
# operator to run before every publish.
# ---------------------------------------------------------------------------
if ! apt/check-suites.sh --local "$BASEDIR" --distro "$DISTRO" \
        --expected-version "$VERSION" --require-published 2>&1 \
        | sed 's/^/publish-deb.sh: /' >&2; then
    echo "publish-deb.sh: the publish succeeded and the suite is wrong." >&2
    echo "publish-deb.sh: remove a stray package with: reprepro -b $BASEDIR remove $DISTRO <package>" >&2
    exit 1
fi

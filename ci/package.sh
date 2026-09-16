#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Builds the .deb for every distribution in tools/distros.sh, proves each one
# installs from its own suite and from no other (apt/verify-suites.sh), and
# writes dist/SHA256SUMS over exactly those files.
#
#   ./ci/package.sh
#
# **dist/ is emptied first.** build-deb.sh leaves every version it ever built
# in there, and a checksum file written over a glob would vouch for a stale
# package as readily as for this run's.
set -euo pipefail
cd "$(dirname "$0")/.."
. tools/distros.sh

rm -rf dist
mkdir dist
for d in $FLEETLESS_DISTROS; do
    ./build-deb.sh "$d"
done
./apt/verify-suites.sh

VERSION=$(sed -n '1s/.*(\(.*\)).*/\1/p' debian/changelog.in)   # 3.1.0-0@DEB_CODENAME@
UPSTREAM=${VERSION%%-*}
names=()
for d in $FLEETLESS_DISTROS; do
    names+=("ros-$d-fleetless-bridge_${UPSTREAM}-0$(fleetless_distro_field "$d" codename)_all.deb")
done
(cd dist && sha256sum "${names[@]}" > SHA256SUMS)

# verify-suites.sh writes its work under build/, not dist/. Anything else in
# dist/ now is something this run did not mean to hand on.
present=$(ls dist | sort)
wanted=$(printf '%s\n' "${names[@]}" SHA256SUMS | sort)
if [ "$present" != "$wanted" ]; then
    echo "package.sh: dist/ does not hold exactly this run's packages:" >&2
    echo "$present" | sed 's/^/  /' >&2
    exit 1
fi
cat dist/SHA256SUMS

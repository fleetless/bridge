#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Refuses a release tag before anything is built.
#
#   ./ci/release-check.sh v3.1.0
#
# Each check is a way a published package would be wrong:
#   * the tag is vMAJOR.MINOR.PATCH and nothing else -- a suffix would name a
#     package version the debian changelog never declared;
#   * without its `v` it equals `__version__`, which test/test_packaging.py
#     already ties to package.xml and debian/changelog.in;
#   * its commit is reachable from MAIN_REF (default origin/main): a package
#     built from a side branch is a release nobody else's history contains.
set -euo pipefail
cd "$(dirname "$0")/.."

tag=${1:-}
MAIN_REF=${MAIN_REF:-origin/main}

if ! [[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "release-check.sh: '$tag' is not a vX.Y.Z tag" >&2
    exit 2
fi

version=$(sed -n 's/^__version__ = "\(.*\)"$/\1/p' fleetless_bridge/__init__.py)
if [ "${tag#v}" != "$version" ]; then
    echo "release-check.sh: the tag says ${tag#v}, __version__ says ${version:-nothing}" >&2
    exit 1
fi

if ! commit=$(git rev-parse -q --verify "refs/tags/$tag^{commit}"); then
    echo "release-check.sh: no tag $tag in this checkout" >&2
    exit 1
fi
if ! git merge-base --is-ancestor "$commit" "$MAIN_REF"; then
    echo "release-check.sh: $tag ($commit) is not reachable from $MAIN_REF" >&2
    exit 1
fi
echo "release-check.sh: $tag is $version at $commit, on $MAIN_REF" >&2

#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Asks a SERVED apt repository whether each declared suite is there and correct.
#
#   ./apt/check-suites.sh https://apt.fleetless.dev
#   ./apt/check-suites.sh https://apt.fleetless.dev --require-published
#
# **`--local <basedir> --distro <name>` reads a reprepro basedir on disk
# instead of asking a served URL**, for the ONE moment there is no served URL
# yet to ask: right after `apt/publish-deb.sh` runs `reprepro includedeb`,
# when the files it just wrote have not necessarily reached anything serving
# them. Skips the HTTP-availability probes (nothing is being "served" to
# probe); everything after that — the Package/Version comparison against
# `debian/changelog.in`, and what counts as NOT PUBLISHED versus BROKEN — is
# the same comparison, run once, so `publish-deb.sh` and a deployed
# `apt.fleetless.dev` are never checked against two different ideas of
# "correct".
#
# The suites come from `reprepro/conf/distributions` beside this script, never
# from a list here: adding a distribution is one stanza there, and a second copy
# of the list is a second thing to forget.
#
# **Two failures, and they are not the same failure.** Conflating them is what
# this script exists to stop:
#
#   NOT PUBLISHED  every index for the suite is 404. Nobody has run
#                  `publish-deb.sh` for that distribution yet. It is a true
#                  statement about work not done -- and it is the bridge's
#                  release step, so a cloud deploy that reported it as an error
#                  would be red at base, on every run, for something its
#                  operator cannot act on in that moment. A check that is always
#                  red is a check somebody comments out.
#
#   BROKEN         the suite is served AND wrong: it indexes another
#                  distribution's package, a version this tree never had, a
#                  version NEWER than the one this tree would publish, or it
#                  serves one architecture and not the other, or its Packages
#                  is served with no InRelease beside it. The right package at
#                  an OLDER version than this tree -- the ordinary gap between
#                  a version bump landing here and the next publish -- is NOT
#                  this; it is NOT PUBLISHED (see classify_index). What is
#                  left fires only when the served content cannot be explained
#                  by "not published yet": a bad publish, a cross-wired suite,
#                  or this tree's own declared version having moved backward
#                  past what is already served -- and the first case is a
#                  Humble robot being offered a build for a different Ubuntu
#                  release.
#
# Exit codes say which:
#   0  every declared suite is published and correct
#   2  some suite is not published; none of the published ones is broken
#   1  at least one suite is broken, or the repository could not be read
#
# `--require-published` folds 2 into 1. Use it at release time, when publishing
# IS the task; leave it off in anything that runs for another reason.
#
# **This reads indices, not a directory listing.** apt.fleetless.dev is served
# with `file_server browse`, so every path under it answers 200 with some HTML,
# and a probe of `dists/<suite>/` cannot tell a published suite from a directory
# the web server is happy to render. What a robot fetches is
# `dists/<suite>/main/binary-<arch>/Packages`.
#
# **And an index check is not an install check.** It cannot see whether the
# package's dependencies resolve on that Ubuntu release, whether the payload
# lands where that distribution's Python looks, or whether the signature
# verifies against the published key. `apt/verify-suites.sh` does all three, by
# installing, and the runbook's clean-container recipe does them against the
# real host. This script is the cheap continuous half.
#
# **It also compares the served Version against the source tree's, not only
# the Package name.** A suite that indexes exactly the right package name at
# the WRONG version is the incident this whole file was written after: an
# earlier version's copyright declared the software closed and not
# redistributable, a later one relicensed it to Apache-2.0, and nothing that
# read only `Package:` lines ever told the two versions apart. The
# expected version is derived the same way `test_the_debian_changelog_agrees…`
# does — the top entry of `debian/changelog.in`, with `@DEB_CODENAME@`
# substituted from `tools/distros.sh`'s own table for that suite — never typed
# a second time here.
set -uo pipefail
cd "$(dirname "$0")/.."

. tools/distros.sh

CONF=apt/reprepro/conf/distributions
STRICT=0
EXPECTED_VERSION_ARG=""

MODE=url
LOCAL_BASEDIR=""
DISTRO_FILTER=""

if [ "$#" -lt 1 ]; then
    echo "usage: $(basename "$0") <base-url> [--require-published]" >&2
    echo "       $(basename "$0") --local <reprepro-basedir> --distro <name> [--expected-version <version>]" >&2
    exit 1
fi
if [ "$1" = "--local" ]; then
    MODE=local
    if [ "$#" -lt 2 ]; then
        echo "$(basename "$0"): --local needs a reprepro basedir." >&2
        exit 1
    fi
    LOCAL_BASEDIR=${2%/}
    shift 2
else
    BASE=${1%/}
    shift
fi
while [ "$#" -gt 0 ]; do
    case "$1" in
        --require-published) STRICT=1; shift ;;
        --distro)
            if [ "$#" -lt 2 ]; then
                echo "$(basename "$0"): --distro needs a distribution name." >&2
                exit 1
            fi
            DISTRO_FILTER=$2; shift 2 ;;
        --expected-version)
            if [ "$#" -lt 2 ]; then
                echo "$(basename "$0"): --expected-version needs a version." >&2
                exit 1
            fi
            EXPECTED_VERSION_ARG=$2; shift 2 ;;
        *) echo "$(basename "$0"): unrecognised argument: $1" >&2; exit 1 ;;
    esac
done

# The upstream version this tree would publish, e.g. "3.1.0-0@DEB_CODENAME@" —
# read once, substituted per suite below. `--expected-version` (given only in
# --local mode, by a caller that already extracted a concrete version from
# the .deb it just filed) skips this read entirely: `publish-deb.sh` inside
# `apt/verify-suites.sh`'s throwaway container has no `debian/` at all --
# only `apt/` and `tools/` are mounted there -- so a read here would fail on
# a missing file for a reason that has nothing to do with any suite being
# wrong. Requiring `--distro` (one concrete suite, checked below) alongside
# it means there is never a placeholder left to substitute.
if [ -n "$EXPECTED_VERSION_ARG" ]; then
    CHANGELOG_VERSION=$EXPECTED_VERSION_ARG
    # No changelog to read in this mode (see above) -- classify_index's
    # older-than-tree carve-out below falls back to the numeric comparison
    # alone, exactly as it did before that carve-out could ask "was this
    # version ever real".
    CHANGELOG_VERSIONS_RAW=""
else
    CHANGELOG_VERSION=$(sed -n '1s/.*(\(.*\)).*/\1/p' debian/changelog.in)
    if [ -z "$CHANGELOG_VERSION" ]; then
        echo "check-suites.sh: could not read a version out of debian/changelog.in's" >&2
        echo "check-suites.sh: top entry. Refusing to check suites against an unknown" >&2
        echo "check-suites.sh: expected version." >&2
        exit 1
    fi
    # Every version this changelog has ever declared, raw -- @DEB_CODENAME@
    # substituted per suite in changelog_versions_for, the same way
    # expected_version_for substitutes it into just the top entry. This is
    # what tells a real past release ("the previous release is still what a
    # robot gets") from a version nobody ever built: `dpkg --compare-versions
    # … lt` alone is true of both, and conflating them turned "somebody filed
    # a stray package by hand" into a reassuring NOT PUBLISHED message.
    CHANGELOG_VERSIONS_RAW=$(sed -n \
        's/^ros-@ROS_DISTRO@-fleetless-bridge (\(.*\)) @DEB_CODENAME@;.*/\1/p' \
        debian/changelog.in)
fi

if [ -n "$EXPECTED_VERSION_ARG" ] && [ -z "$DISTRO_FILTER" ]; then
    echo "$(basename "$0"): --expected-version needs --distro too -- it names one" >&2
    echo "$(basename "$0"): concrete version for one concrete suite, never a" >&2
    echo "$(basename "$0"): @DEB_CODENAME@ placeholder to substitute across several." >&2
    exit 1
fi
if [ "$MODE" = local ] && [ -z "$DISTRO_FILTER" ]; then
    echo "$(basename "$0"): --local needs --distro too -- it reads one suite's" >&2
    echo "$(basename "$0"): freshly-written files, not a whole served repository." >&2
    exit 1
fi

[ -f "$CONF" ] || {
    echo "check-suites.sh: $CONF is missing." >&2
    echo "check-suites.sh: the suites cannot be checked against what the repository declares," >&2
    echo "check-suites.sh: and a smaller set is not an acceptable answer." >&2
    exit 1
}

SUITES=$(sed -n 's/^Codename: *//p' "$CONF")
if [ -z "$SUITES" ]; then
    echo "check-suites.sh: $CONF declares no suites." >&2
    echo "check-suites.sh: refusing to report a repository as verified against nothing." >&2
    exit 1
fi
if [ -n "$DISTRO_FILTER" ]; then
    _known=no
    for _s in $SUITES; do [ "$_s" = "$DISTRO_FILTER" ] && _known=yes; done
    if [ "$_known" != yes ]; then
        echo "check-suites.sh: '$DISTRO_FILTER' is not a suite $CONF declares." >&2
        echo "check-suites.sh: declared: $(printf '%s' "$SUITES" | tr '\n' ' ')" >&2
        exit 1
    fi
    SUITES=$DISTRO_FILTER
fi

# http_code <url> -- prints the status, or an empty line if the host never
# answered. A connection failure is retried and an HTTP status is not: the
# second is the server saying something, and hearing it five more times adds
# nothing. Same distinction deploy.sh makes, for the same reason.
#
# `-L --max-redirs 1` follows exactly one redirect (apt itself follows
# redirects; a bare http:// probe or a fronting proxy answering 308 is not a
# broken suite). More than one redirect is treated the same as no answer at
# all -- it is not a shape apt.fleetless.dev is expected to need, and guessing
# how many hops are "still fine" is worse than refusing past the first.
http_code() {
    local url=$1 code attempt
    for attempt in 1 2 3; do
        if code=$(curl -sS -o /dev/null -w '%{http_code}' -L --max-redirs 1 --max-time 20 "$url" 2>/dev/null); then
            printf '%s' "$code"
            return 0
        fi
        sleep 2
    done
    printf ''
    return 0
}

# fetch_index <url> -- prints the body, retried exactly like http_code above.
# The first phase (http_code) deliberately tells "the host did not answer"
# apart from "the host said 404"; the second phase used to throw that
# distinction away the moment it needed to READ a response rather than just
# its status -- a transient failure here (a reset connection, a reload mid
# transfer, --max-time exceeded on a slow link) produced an empty body, which
# then failed the name comparison below and was reported as BROKEN: a suite
# serving another distribution's package. That is a specific, false claim
# about a repository that was simply unreachable for a moment. Retrying the
# read the same way the probe is retried removes that false report; returning
# non-zero after three failed attempts, rather than printing an empty body,
# is what lets the caller tell "empty because retried out" from "empty
# because the suite genuinely has nothing in it" (see the loop below).
fetch_index() {
    local url=$1 attempt body
    for attempt in 1 2 3; do
        if body=$(curl -sS -L --max-redirs 1 --max-time 30 "$url" 2>/dev/null); then
            printf '%s' "$body"
            return 0
        fi
        sleep 2
    done
    return 1
}

# classify_index <suite> <arch> <payload> <expected-pkg> <expected-version> <changelog-versions>
# The one comparison both modes below feed a Packages file's content into:
# empty is NOT PUBLISHED (nothing filed in yet, whether that is a 404'd suite,
# an exported-but-empty one, or a local basedir with no such file), a
# name/version mismatch is BROKEN, anything else is correct. Mutates the
# global BROKEN/ABSENT accumulators directly -- called in the current shell,
# never through a pipe or `$(...)`, so that works.
classify_index() {
    local suite=$1 arch=$2 payload=$3 expected=$4 expected_version=$5 changelog_versions=$6
    local names versions
    names=$(printf '%s\n' "$payload" | sed -n 's/^Package: //p' | sort -u | tr '\n' ' ')
    if [ -z "$names" ]; then
        echo "  $suite ($arch): NOT PUBLISHED (no package filed into it yet)" >&2
        ABSENT="$ABSENT $suite/$arch"
        return
    fi
    versions=$(printf '%s\n' "$payload" | sed -n 's/^Version: //p' | sort -u | tr '\n' ' ')
    # The right package at an OLDER version than this tree would publish is
    # not a broken suite: it is the previous release still being served while
    # the new one has not been published yet — the normal state between a
    # version bump landing in the tree and the next publish. It was classified
    # BROKEN once, and because deploy.sh turns BROKEN into FATAL, every cloud
    # deploy aborted from the moment the tree said 3.0.1 while apt still served
    # 3.0.0 — a review caught it. Older-than-tree therefore joins the
    # NOT PUBLISHED set, but only when the served version is one this package
    # actually released: "older" is a numeric fact `dpkg --compare-versions`
    # sees on ANY string, including one debian/changelog.in never declared for
    # this suite, and a version nobody built is somebody filing by hand, not a
    # publish still pending -- so it must not read as the same reassuring
    # message. A foreign package, a version the changelog never had, or a
    # version NEWER than the tree stay BROKEN, because none of those is a
    # state a publish that has not happened yet can explain. When there is no
    # changelog to check membership against (`$changelog_versions` empty --
    # see CHANGELOG_VERSIONS_RAW above), this falls back to the numeric
    # comparison alone, same as before this distinction existed.
    if [ "$names" = "$expected " ] && [ "$versions" != "$expected_version " ] \
        && [ "$(printf '%s' "$versions" | wc -w)" -eq 1 ] \
        && dpkg --compare-versions "${versions% }" lt "$expected_version"; then
        if [ -n "$changelog_versions" ] \
            && ! printf '%s\n' "$changelog_versions" | grep -qxF "${versions% }"; then
            echo "  $suite ($arch): BROKEN -- serves [$expected] at [${versions% }], older than" >&2
            echo "     this tree's [$expected_version] but a version debian/changelog.in has" >&2
            echo "     never declared for this suite. An older-than-tree version is NOT" >&2
            echo "     PUBLISHED only when it is one of this package's own past releases;" >&2
            echo "     this one is neither that nor what this tree would publish next." >&2
            BROKEN="$BROKEN $suite/$arch"
            return
        fi
        echo "  $suite ($arch): NOT PUBLISHED at this version -- serves [$expected] at [${versions% }]," >&2
        echo "     this tree would publish [$expected_version]. The previous release is" >&2
        echo "     still what a robot gets until apt/publish-deb.sh runs for it." >&2
        ABSENT="$ABSENT $suite/$arch"
        return
    fi
    if [ "$names" != "$expected " ] || [ "$versions" != "$expected_version " ]; then
        echo "  $suite ($arch): BROKEN -- indexes [$names] at [$versions]," >&2
        echo "     expected exactly [$expected] at [$expected_version]." >&2
        echo "     A suite carrying another distribution's package, or the right" >&2
        echo "     package at the wrong version, offers a robot a build for a" >&2
        echo "     different Ubuntu release or the copyright/licence terms of a" >&2
        echo "     version it did not receive. reprepro compares nothing at publish" >&2
        echo "     time; use apt/publish-deb.sh, which derives the suite from the" >&2
        echo "     artifact." >&2
        BROKEN="$BROKEN $suite/$arch"
    else
        echo "  $suite/$arch: $names$versions(expected)" >&2
    fi
}

# expected_version_for <suite> -- the changelog's placeholder substituted from
# tools/distros.sh's own table. A suite in $CONF that the table has no
# codename for is a configuration gap -- a distribution not yet supported for
# a build at all -- refused here, before the missing entry turns into a
# silent "" that would match nothing and misreport as BROKEN.
expected_version_for() {
    local suite=$1 codename
    codename=$(fleetless_distro_field "$suite" codename) || {
        echo "check-suites.sh: '$suite' is declared in $CONF but tools/distros.sh has" >&2
        echo "check-suites.sh: no codename for it. That is a configuration gap, not" >&2
        echo "check-suites.sh: something the served repository did." >&2
        return 1
    }
    printf '%s' "${CHANGELOG_VERSION/@DEB_CODENAME@/$codename}"
}

# changelog_versions_for <suite> -- every version debian/changelog.in has
# ever declared, substituted for this suite's codename, one per line. Empty
# when CHANGELOG_VERSIONS_RAW is (no changelog was read -- see above);
# classify_index treats that as "no history to check", not as "nothing is
# ever a real release".
changelog_versions_for() {
    local suite=$1 codename
    [ -z "$CHANGELOG_VERSIONS_RAW" ] && return 0
    codename=$(fleetless_distro_field "$suite" codename) || {
        echo "check-suites.sh: '$suite' is declared in $CONF but tools/distros.sh has" >&2
        echo "check-suites.sh: no codename for it. That is a configuration gap, not" >&2
        echo "check-suites.sh: something the served repository did." >&2
        return 1
    }
    printf '%s\n' "$CHANGELOG_VERSIONS_RAW" | sed "s/@DEB_CODENAME@/$codename/g"
}

BROKEN=""
ABSENT=""
for suite in $SUITES; do
    expected="ros-$suite-fleetless-bridge"
    expected_version=$(expected_version_for "$suite") || exit 1
    changelog_versions=$(changelog_versions_for "$suite") || exit 1

    if [ "$MODE" = local ]; then
        # No serving to probe: publish-deb.sh calls this right after
        # `reprepro includedeb`, reading straight off disk. A missing file
        # here is indistinguishable from -- and reported exactly like -- a
        # suite nobody has published to over HTTP.
        for arch in amd64 arm64; do
            idx="$LOCAL_BASEDIR/dists/$suite/main/binary-$arch/Packages"
            if [ -f "$idx" ]; then
                payload=$(cat "$idx")
            else
                payload=""
            fi
            classify_index "$suite" "$arch" "$payload" "$expected" "$expected_version" "$changelog_versions"
        done
        continue
    fi

    codes=""
    bad=""
    # **All three gathered before any one of them is judged**, and that is not
    # tidiness. Whichever index is checked first decides the message the
    # operator reads, and only some of the possible messages are actionable: a
    # first check that fataled on a missing InRelease would report a signature
    # problem about a suite nobody has ever published, and a first check that
    # fataled on a missing Packages would report an unpublished suite about one
    # whose signing had failed. Neither is what happened. With all three in
    # hand, "none served" and "some served" are different branches and the
    # message says which.
    pkg_amd64=$(http_code "$BASE/dists/$suite/main/binary-amd64/Packages")
    pkg_arm64=$(http_code "$BASE/dists/$suite/main/binary-arm64/Packages")
    rel=$(http_code "$BASE/dists/$suite/InRelease")
    codes="amd64=$pkg_amd64 arm64=$pkg_arm64 InRelease=$rel"

    if [ -z "$pkg_amd64" ] || [ -z "$pkg_arm64" ] || [ -z "$rel" ]; then
        echo "check-suites.sh: $BASE did not answer for suite '$suite' ($codes)." >&2
        echo "check-suites.sh: that is the repository being unreachable, not a suite being" >&2
        echo "check-suites.sh: unpublished, and the two must not be reported as one." >&2
        exit 1
    fi

    if [ "$pkg_amd64" = 404 ] && [ "$pkg_arm64" = 404 ] && [ "$rel" = 404 ]; then
        echo "  $suite: NOT PUBLISHED (no index served)" >&2
        ABSENT="$ABSENT $suite"
        continue
    fi

    for pair in "amd64:$pkg_amd64" "arm64:$pkg_arm64" "InRelease:$rel"; do
        if [ "${pair#*:}" != 200 ]; then
            bad="$bad ${pair%%:*}(${pair#*:})"
        fi
    done
    if [ -n "$bad" ]; then
        echo "  $suite: BROKEN -- served, but incomplete:$bad" >&2
        echo "     ($codes). A suite apt can see and cannot use in full is a" >&2
        echo "     half-finished publish, not an absent one." >&2
        BROKEN="$BROKEN $suite"
        continue
    fi

    # Served and complete. Now the only question left is WHAT it serves.
    for arch in amd64 arm64; do
        if ! payload=$(fetch_index "$BASE/dists/$suite/main/binary-$arch/Packages"); then
            echo "check-suites.sh: $BASE did not answer while READING suite '$suite' ($arch)," >&2
            echo "check-suites.sh: after its status probe answered 200. That is the" >&2
            echo "check-suites.sh: repository being unreachable partway through, not a suite" >&2
            echo "check-suites.sh: being unpublished or broken, and none of the three must be" >&2
            echo "check-suites.sh: reported as one of the others." >&2
            exit 1
        fi
        classify_index "$suite" "$arch" "$payload" "$expected" "$expected_version" "$changelog_versions"
    done
done

if [ -n "$BROKEN" ]; then
    echo "check-suites.sh: broken:$BROKEN" >&2
    exit 1
fi
if [ -n "$ABSENT" ]; then
    echo "check-suites.sh: not published yet:$ABSENT" >&2
    if [ "$STRICT" = 1 ]; then
        echo "check-suites.sh: --require-published was given, so this is an error." >&2
        exit 1
    fi
    exit 2
fi
echo "check-suites.sh: every declared suite is published and serves only its own package." >&2
exit 0

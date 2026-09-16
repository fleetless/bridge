#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Creates the GitHub release for a tag from dist/, which ci/package.sh filled.
#
#   GITHUB_TOKEN=... GITHUB_REPOSITORY=owner/name ./ci/github-release.sh v3.1.0
#
# **A tag that already has a release is refused, not updated.** The apt
# publishing workflow checks these assets against SHA256SUMS and against who
# uploaded them; replacing them under the same tag is the one edit nothing
# downstream could tell from the original. A run that dies halfway leaves an
# incomplete release, which the publishing side refuses by its asset list --
# delete it by hand and re-run.
#
# curl and jq, not the gh CLI: the build runner has the first two.
set -euo pipefail
cd "$(dirname "$0")/.."

tag=${1:?usage: github-release.sh <tag>}
: "${GITHUB_TOKEN:?GITHUB_TOKEN is not set}"
: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is not set}"
API=${GITHUB_API_URL:-https://api.github.com}
auth=(-H "Authorization: Bearer $GITHUB_TOKEN" -H "Accept: application/vnd.github+json")

[ -s dist/SHA256SUMS ] || { echo "github-release.sh: dist/SHA256SUMS is missing -- run ci/package.sh first" >&2; exit 1; }

status=$(curl -s -o /dev/null -w '%{http_code}' "${auth[@]}" "$API/repos/$GITHUB_REPOSITORY/releases/tags/$tag")
if [ "$status" != 404 ]; then
    echo "github-release.sh: $tag already has a release, or the lookup failed (HTTP $status) -- refusing to touch it" >&2
    exit 1
fi

# The top debian/changelog.in entry, placeholders named rather than filled:
# one release carries every distribution's package.
body=$(awk 'NR == 1 { next } /^ -- / { exit } { print }' debian/changelog.in \
    | sed 's/@ROS_DISTRO@/<distro>/g; s/@DEB_CODENAME@/<codename>/g')

created=$(jq -n --arg tag "$tag" --arg body "$body" \
        '{tag_name: $tag, name: $tag, body: $body, draft: false, prerelease: false}' \
    | curl -fsS "${auth[@]}" -X POST --data @- "$API/repos/$GITHUB_REPOSITORY/releases")
upload=$(printf '%s' "$created" | jq -r .upload_url | sed 's/{.*}$//')

files=()
while read -r _hash name; do files+=("$name"); done < dist/SHA256SUMS
files+=(SHA256SUMS)
for f in "${files[@]}"; do
    curl -fsS "${auth[@]}" -H "Content-Type: application/octet-stream" \
        --data-binary "@dist/$f" "$upload?name=$f" \
        | jq -r '"github-release.sh: uploaded \(.name), \(.size) bytes, by \(.uploader.login)"' >&2
done
printf '%s' "$created" | jq -r .html_url

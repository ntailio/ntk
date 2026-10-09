#!/usr/bin/env bash
# Cuts release/<version> from an up-to-date trunk and writes a changelog
# template with the commits since the previous release. Never pushes.
set -euo pipefail

version=${1:?usage: scripts/release-branch.sh vX.Y.Z[-pre]}
if [[ ! $version =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: $version is not a version like v1.2.3 or v1.2.3-rc.1" >&2
  exit 1
fi
cd "$(git rev-parse --show-toplevel)"

fail() { echo "error: $*" >&2; exit 1; }
[[ $(git branch --show-current) == trunk ]] || fail "switch to trunk first"
[[ -z $(git status --porcelain) ]] || fail "trunk has uncommitted changes"
if git remote get-url origin >/dev/null 2>&1; then
  git fetch --quiet --tags origin trunk
  [[ $(git rev-parse HEAD) == $(git rev-parse FETCH_HEAD) ]] || fail "trunk isn't the same as origin/trunk; pull or push first"
  git ls-remote --exit-code --tags origin "refs/tags/$version" >/dev/null && fail "$version is already released"
fi
git rev-parse -q --verify "refs/tags/$version" >/dev/null && fail "tag $version already exists"
git rev-parse -q --verify "refs/heads/release/$version" >/dev/null && fail "branch release/$version already exists"

# A stable release is compared with the previous stable one, so its notes cover
# everything its release candidates had; a pre-release with the previous tag.
# Release tags sit on release branches, not on trunk, so count from where that
# release branched off. versionsort.suffix sorts v1.0.0-rc.1 before v1.0.0.
tags=$(git -c versionsort.suffix=- tag --list 'v*' --sort=-v:refname)
if [[ $version != *-* ]]; then
  tags=$(grep -v -- - <<<"$tags" || true)
fi
previous=$(head -n1 <<<"$tags")
skip=(--invert-grep --grep='^add changelog for v')
if [[ -n $previous ]]; then
  since="since $previous"
  commits=$(git log --reverse --format='- %s' "${skip[@]}" "$(git merge-base "$previous" HEAD)..HEAD")
else
  since="in this first release"
  [[ $version != *-* ]] && since="in this first stable release"
  commits=$(git log --reverse --format='- %s' "${skip[@]}")
fi

git switch --quiet -c "release/$version"
mkdir -p changelog
cat >"changelog/$version.md" <<EOF
# ntk $version

<!--
Write for people who use ntk. Say what changed and why it matters to them,
group related changes, and lead with anything that breaks existing use.
Delete the sections you don't need. Comments like this one are left out of
the release notes. See RELEASING.md.

Commits $since:
$commits
-->

One or two sentences on what this release is about.

## Breaking changes

- What changed, and what to do about it.

## Highlights

- **The feature**: what it lets you do.

## Fixes

- What was wrong, and what happens now.
EOF

echo "On release/$version with changelog/$version.md."
echo "Write the changelog, commit it, then push the branch to release:"
echo "  git push -u origin release/$version"

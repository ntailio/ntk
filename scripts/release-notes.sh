#!/usr/bin/env bash
# Prints the GitHub release notes for a version: changelog/<version>.md without
# its title line. Fails if the version or changelog isn't ready to release.
set -euo pipefail

version=${1:?usage: scripts/release-notes.sh vX.Y.Z[-pre]}
if [[ ! $version =~ ^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "error: $version is not a version like v1.2.3 or v1.2.3-rc.1" >&2
  exit 1
fi

file="$(dirname "$0")/../changelog/$version.md"
if [[ ! -f $file ]]; then
  echo "error: changelog/$version.md is missing" >&2
  exit 1
fi

if grep -q '<!--' "$file"; then
  echo "error: changelog/$version.md contains an HTML comment; the file is published as-is, so remove it" >&2
  exit 1
fi
notes=$(perl -0777 -pe 's/\A\s*# [^\n]*\n//; s/\A\s+//; s/\s+\z/\n/' "$file")
if [[ -z ${notes//[[:space:]]/} ]]; then
  echo "error: changelog/$version.md has no content" >&2
  exit 1
fi
if grep -qF -e "One or two sentences on what this release is about." -e "What changed, and what to do about it." \
  -e "**The feature**: what it lets you do." -e "What was wrong, and what happens now." <<<"$notes"; then
  echo "error: changelog/$version.md still contains template text" >&2
  exit 1
fi
printf '%s\n' "$notes"

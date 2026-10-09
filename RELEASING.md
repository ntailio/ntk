# Releasing ntk

Trunk is tested on every push but never released. A release is a branch named `release/<version>` with a changelog on it; pushing that branch is what releases it.

## Cut a release

```sh
git switch trunk && git pull
make release-branch VERSION=v1.2.0      # or v1.2.0-rc.1
$EDITOR changelog/v1.2.0.md             # write the changelog, see below
git commit -am "changelog for v1.2.0"
git push -u origin release/v1.2.0       # this is the release
```

`make release-branch` checks that trunk is clean and matches `origin/trunk`, and that the version doesn't exist yet. It then creates the branch and a changelog template that lists the commits since the previous release, as raw material. For a stable version, "previous release" means the previous *stable* one, so `v1.1.0` covers everything since `v1.0.x`, including what its release candidates had. A pre-release is compared with the tag right before it.

The push starts [the release workflow](.github/workflows/release.yml):

1. Checks the version, that the changelog is filled in, and that the tag doesn't exist yet.
2. Runs the full CI: lint, unit tests on macOS and Windows, and integration tests against the sandbox.
3. Builds a binary for each platform, each with a `.sha256` file, plus a build provenance attestation for the binaries.
4. Pushes `ghcr.io/ntailio/ntk:<version>` for linux/amd64 and linux/arm64, and moves `:latest` for stable versions only.
5. Creates the GitHub release, which creates the tag `<version>` on the release commit. The release notes are the changelog, without its title and comments. Versions with a suffix like `-rc.1` are marked as pre-releases.
6. Commits `changelog/<version>.md` to trunk, so trunk has every changelog.

The tag is only created once everything else has worked. If a run fails, fix the cause and push to the same branch again. Once a version is tagged it's final: to change anything, cut a new version.

## Writing the changelog

The changelog is the release notes, and people read it to decide whether to upgrade. Write it for them:

- Write a stable release for people coming from the previous stable one. They never ran the release candidates, so describe the result, not the steps in between.
- Start with one or two sentences on what the release is about.
- **Breaking changes** come first, each with what to do about it.
- **Highlights**: what you can do now that you couldn't before. One line each, led by the feature's name.
- **Fixes**: what was wrong, as a user would have noticed it.
- Group related commits into one line, and leave out internal changes nobody can see: refactors, CI, tests.
- No commit hashes, no "misc fixes", no list of every commit.
- Delete sections you don't need.

The commit list in the HTML comment is there to help you write; it never shows up in the release notes.

## Patch releases

Fixes land on trunk first. To ship a fix without everything else that's on trunk, branch from the previous release's tag instead:

```sh
git switch -c release/v1.2.1 v1.2.0
git cherry-pick <fix>
# add changelog/v1.2.1.md, commit, push
```

## Installing the result

Binaries and checksums are on the [releases page](https://github.com/ntailio/ntk/releases). Check a download with `sha256sum -c ntk-<version>-<os>-<arch>.sha256` and verify where it came from with `gh attestation verify <file> --repo ntailio/ntk`.

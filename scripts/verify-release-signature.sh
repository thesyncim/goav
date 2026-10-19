#!/bin/sh
# Verify a release tag (or, for a dry run, a commit) is signed by a key its
# author registered on GitHub. GitHub checks GPG and SSH signatures against the
# signer's account, so the runner needs no keyring or allowed-signers file.
#
#   scripts/verify-release-signature.sh tag v0.1.0
#   scripts/verify-release-signature.sh commit <sha>
#
# Needs gh with GH_TOKEN (contents: read). GITHUB_REPOSITORY defaults to the
# current gh repository. DRY_RUN=true reports a failed check as a warning
# instead of failing, so the rest of the release rehearsal still runs.
set -eu

kind="${1:?usage: verify-release-signature.sh tag|commit NAME}"
name="${2:?usage: verify-release-signature.sh tag|commit NAME}"
repo="${GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}"

refuse() {
	if [ "${DRY_RUN:-false}" = "true" ]; then
		echo "::warning::dry run: a release would refuse $kind $name: $1"
		exit 0
	fi
	echo "::error::refusing $kind $name: $1" >&2
	exit 1
}

case "$kind" in
tag)
	object="$(gh api "repos/$repo/git/ref/tags/$name" --jq '.object.type + " " + .object.sha')" ||
		refuse "the tag does not exist on GitHub; push it first"
	type="${object% *}"
	sha="${object#* }"
	[ "$type" = "tag" ] ||
		refuse "it is a lightweight tag, which carries no signature; cut it with git tag -s"
	verification="$(gh api "repos/$repo/git/tags/$sha" --jq '.verification.reason + " " + (.verification.verified | tostring)')"
	;;
commit)
	verification="$(gh api "repos/$repo/commits/$name" --jq '.commit.verification.reason + " " + (.commit.verification.verified | tostring)')" ||
		refuse "the commit is not on GitHub; push it first"
	;;
*)
	echo "usage: verify-release-signature.sh tag|commit NAME" >&2
	exit 2
	;;
esac

reason="${verification% *}"
[ "${verification#* }" = "true" ] ||
	refuse "GitHub does not verify its signature ($reason); sign it with a GPG or SSH key registered on your GitHub account (Settings, SSH and GPG keys)"
echo "$kind $name: signature verified by GitHub ($reason)"

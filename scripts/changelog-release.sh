#!/bin/sh
# Usage: scripts/changelog-release.sh VERSION
# Moves the Unreleased bullets and every changelog.d/ fragment (sorted by
# name) into a new "## VERSION" section, then deletes the fragments.
set -eu
[ $# -eq 1 ] || { echo "usage: $0 VERSION" >&2; exit 2; }
ver=$1
cd "$(dirname "$0")/.."
tmp=$(mktemp)
trap 'rm -f "$tmp"' EXIT
frags=
for f in changelog.d/*.md; do
	[ -e "$f" ] || continue
	[ "$(basename "$f")" = README.md ] && continue
	frags="$frags $f"
done
# Unreleased body: everything between "## Unreleased" and the next "## ",
# minus the fragment pointer line.
awk -v ver="$ver" -v frags="$frags" '
BEGIN { n = split(frags, a, " ") }
/^## Unreleased/ { inunrel = 1; print; next }
inunrel && /^## / {
	inunrel = 0
	while (nb > 0 && body[nb] == "") nb--
	print "## " ver
	print ""
	for (i = 1; i <= nb; i++) print body[i]
	for (i = 1; i <= n; i++) {
		while ((getline line < a[i]) > 0) print line
		close(a[i])
	}
	print ""
}
inunrel {
	if ($0 ~ /^Entries since .* live in changelog\.d\//) { print; next }
	if ($0 == "" && nb == 0) { print; next }
	body[++nb] = $0
	next
}
{ print }
' CHANGELOG.md >"$tmp"
cat "$tmp" >CHANGELOG.md
for f in $frags; do rm -f "$f"; done

#!/usr/bin/env bash
# Package an already built, CGO_ENABLED=0 goav binary without a C toolchain.
set -euo pipefail
if [[ $# != 2 ]]; then
  echo 'usage: package-macos.sh /path/to/goav /new/path/GoAV.app' >&2
  exit 2
fi
binary=$1
app=$2
if [[ ! -f "$binary" || -e "$app" ]]; then
  echo 'input binary must exist and destination must not exist' >&2
  exit 1
fi
mkdir -p "$app/Contents/MacOS"
cp "$binary" "$app/Contents/MacOS/goav"
cp "$(dirname "$0")/Info.plist" "$app/Contents/Info.plist"
codesign --force --sign - "$app"
printf 'Run: %q cameras --request-access\n' "$app/Contents/MacOS/goav"

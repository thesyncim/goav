#!/usr/bin/env bash
# go run -exec wrapper: bundles the CGO_ENABLED=0 binary into a stable, ad-hoc
# signed macOS .app with Camera + Screen Recording usage descriptions so macOS
# TCC remembers granted permissions across runs.
set -euo pipefail
if [[ $# == 0 || ! -f "$1" ]]; then
  echo 'usage: CGO_ENABLED=0 GOEXPERIMENT=simd go run -exec ./run-macos.sh . [args ...]' >&2
  exit 2
fi
if [[ $(uname -s) != Darwin ]]; then
  exec "$@"
fi
binary=$1
shift
script_dir=$(cd "$(dirname "$0")" && pwd)
plist="$script_dir/Info.plist"
key=$({ shasum -a 256 < "$binary"; shasum -a 256 < "$plist"; } | shasum -a 256 | awk '{print $1}')
cache="${GOCACHE:-$(getconf DARWIN_USER_CACHE_DIR)goav}/studio-app"
app="$cache/goav Studio.app"
stamp="$cache/.build-key"
mkdir -p "$cache"
if [[ ! -x "$app/Contents/MacOS/goav-studio" ]] || [[ "$(cat "$stamp" 2>/dev/null || true)" != "$key" ]]; then
  mkdir -p "$app/Contents/MacOS"
  cp "$binary" "$app/Contents/MacOS/goav-studio"
  chmod 0755 "$app/Contents/MacOS/goav-studio"
  cp "$plist" "$app/Contents/Info.plist"
  codesign --force --sign - "$app" >/dev/null 2>&1
  printf '%s' "$key" > "$stamp"
fi
exec "$app/Contents/MacOS/goav-studio" "$@"

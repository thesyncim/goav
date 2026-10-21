#!/usr/bin/env bash
# go run -exec wrapper: give a cgo-free executable macOS camera usage metadata.
set -euo pipefail
if [[ $# == 0 || ! -f "$1" ]]; then
  echo 'usage: go run -exec /path/to/scripts/camera/go-run-macos.sh . [args ...]' >&2
  exit 2
fi
if [[ $(uname -s) != Darwin ]]; then
  echo 'go-run-macos.sh requires macOS' >&2
  exit 2
fi
binary=$1
shift
script_dir=$(cd "$(dirname "$0")" && pwd)

# Scope the cache to the Git project, shared by its branches and worktrees.
# Hash file contents, not Go's temporary executable path, so repeated runs of
# the same build use the same signed app for permission requests and capture.
project=$(git -C "$script_dir" rev-parse --path-format=absolute --git-common-dir)
project_key=$(printf '%s' "$project" | shasum -a 256 | awk '{print $1}')
key=$({ shasum -a 256 < "$binary"; shasum -a 256 < "$script_dir/Info.plist"; } | shasum -a 256 | awk '{print $1}')
cache="${GOCACHE:-$(getconf DARWIN_USER_CACHE_DIR)goav}/camera-apps/$project_key"
app="$cache/$key/GoAV.app"
caller_umask=$(umask)
umask 077
mkdir -p "$cache"
if [[ ! -x "$app/Contents/MacOS/goav" ]]; then
  stage=$(mktemp -d "$cache/.stage.XXXXXX")
  trap 'rm -rf "$stage"' EXIT
  mkdir "$stage/$key"
  # Keep packaging output off stdout, preserving commands such as cameras --json.
  if ! output=$("$script_dir/package-macos.sh" "$binary" "$stage/$key/GoAV.app" 2>&1); then
    printf '%s\n' "$output" >&2
    exit 1
  fi
  # Publish a complete bundle. Concurrent identical builds may publish first;
  # -n preserves that bundle, and the nonempty directory cannot be replaced.
  mv -n "$stage/$key" "$cache/" || [[ -x "$app/Contents/MacOS/goav" ]]
  rm -rf "$stage"
  trap - EXIT
fi
umask "$caller_umask"
# Preserve the caller's working directory, arguments, exit status and signals.
exec "$app/Contents/MacOS/goav" "$@"

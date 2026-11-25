#!/usr/bin/env bash
# Repin the root github.com/thesyncim/goav version across every nested and
# example go.mod file in one shot.
#
# Usage:
#   scripts/repin.sh v0.1.0-alpha.8
set -euo pipefail

version="${1:?usage: scripts/repin.sh ROOT_VERSION}"
root="$(cd "$(dirname "$0")/.." && pwd)"

modules=(
  cameraav
  rtpav
  webrtcav
  player
  libcodec
  gplcodec
  goavtest/expect
  cmd/goav
  examples
  examples/gio-webrtc-showcase
  examples/webrtc-runtime-ladder
)

for mod in "${modules[@]}"; do
  gomod="$root/$mod/go.mod"
  if [ ! -f "$gomod" ]; then
    echo "skipping missing $gomod" >&2
    continue
  fi
  echo "pinning $mod -> github.com/thesyncim/goav@$version"
  go mod edit -require="github.com/thesyncim/goav@$version" "$gomod"
done

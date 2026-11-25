#!/usr/bin/env bash
# Build, vet, and test the root module, every nested module, and every example
# module against the local checkout.
#
# Usage:
#   scripts/test-all-modules.sh [--race]
set -euo pipefail

race=0
for arg in "$@"; do
  case "$arg" in
    --race) race=1 ;;
    *) echo "unknown flag: $arg (supported: --race)" >&2; exit 2 ;;
  esac
done

root="$(cd "$(dirname "$0")/.." && pwd)"
export GOEXPERIMENT="${GOEXPERIMENT:-simd}"

run_mod() {
  local dir="$1"
  echo "=== $dir ==="
  (
    cd "$root/$dir"
    CGO_ENABLED=0 go build ./...
    CGO_ENABLED=0 go vet ./...
    CGO_ENABLED=0 go test -count=1 ./...
    if [ "$race" = 1 ]; then
      CGO_ENABLED=1 go test -race -count=1 ./...
    fi
  )
}

run_mod .

for mod in cameraav rtpav webrtcav player libcodec gplcodec goavtest/expect; do
  run_mod "$mod"
done

# cmd/goav carries no replace directives so `go install ...@version` works;
# build and test it against this checkout through a temporary workspace.
echo "=== cmd/goav ==="
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
(
  export GOWORK="$scratch/go.work"
  go work init "$root" "$root/cameraav" "$root/rtpav" "$root/webrtcav" "$root/player" "$root/cmd/goav"
  cd "$root/cmd/goav"
  CGO_ENABLED=0 go build ./...
  CGO_ENABLED=0 go build -tags goav_noplay ./...
  CGO_ENABLED=0 go vet ./...
  CGO_ENABLED=0 go test -count=1 ./...
  CGO_ENABLED=0 go test -count=1 -tags goav_noplay ./...
  if [ "$race" = 1 ]; then
    CGO_ENABLED=1 go test -race -count=1 ./...
  fi
)

for gomod in "$root"/examples/go.mod "$root"/examples/*/go.mod; do
  [ -f "$gomod" ] || continue
  rel="${gomod#"$root"/}"
  run_mod "$(dirname "$rel")"
done

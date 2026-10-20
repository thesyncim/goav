#!/usr/bin/env bash
# Resolve a published candidate with no workspace or replacement directives.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
if [ "${GOAV_PROJECT_ENV:-}" != 1 ] && [ -x "${HOME}/.codex/bin/project-env" ]; then
  cd "$root"
  exec "${HOME}/.codex/bin/project-env" env GOAV_PROJECT_ENV=1 bash "$root/scripts/check-consumers.sh" "$@"
fi
revision="${1:?usage: check-consumers.sh PUBLISHED_COMMIT_OR_VERSION}"
scratch="$(mktemp -d)"
trap 'rm -rf "$scratch"' EXIT
export GOWORK=off GOFLAGS=-mod=mod CGO_ENABLED=0
for module in player cmd/goav webrtcav cameraav; do
  dir="$scratch/${module//\//-}"
  mkdir -p "$dir"
  cd "$dir"
  go mod init consumer
  go get "github.com/thesyncim/goav/${module}@${revision}"
  cp "$root/scripts/consumer/testdata/regression_test.go" .
  if [ "$module" = webrtcav ]; then
    cp "$root/scripts/consumer/testdata/rtp/record_test.go" .
  fi
  go test -count=1 -timeout=2m -v .
  go list -m all
  if [ "$module" = player ]; then
    # This is the combination shown in player/doc.go. Do not add the root
    # explicitly: its selected version must come from the player's pin.
    printf 'package consumer\nimport (_ "github.com/thesyncim/goav/player"; _ "github.com/thesyncim/goav/bundle")\n' > imports.go
    for arch in amd64 arm64; do
      GOOS=linux GOARCH="$arch" go list -deps . > dependencies.txt
      if grep -Eq '^github.com/thesyncim/(goaac|goav/adapters/goaac)(/|$)' dependencies.txt; then
        echo "player + bundle reaches the opt-in AAC implementation on $arch" >&2
        exit 1
      fi
      GOOS=linux GOARCH="$arch" go build .
    done
  elif [ "$module" = cameraav ]; then
    printf 'package consumer
import _ "github.com/thesyncim/goav/cameraav"
' > imports.go
    for target in linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64 windows/arm64; do
      GOOS="${target%/*}" GOARCH="${target#*/}" go build .
    done
  elif [ "$module" = cmd/goav ]; then
    gomod="$(go list -m -f '{{.GoMod}}' github.com/thesyncim/goav/cmd/goav)"
    if grep -q '^replace' "$gomod"; then
      echo "cmd/goav still has replace directives and cannot be installed at a version" >&2
      exit 1
    fi
    go build -tags goav_noplay -o goav github.com/thesyncim/goav/cmd/goav
    ./goav elements > elements.txt
    go version -m ./goav
  fi
done

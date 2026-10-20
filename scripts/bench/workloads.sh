#!/usr/bin/env bash
# A reproducible file ladder plus a real WebRTC mutation workload.
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
if [ "${GOAV_PROJECT_ENV:-}" != 1 ] && [ -x "${HOME}/.codex/bin/project-env" ]; then
  cd "$root"
  exec "${HOME}/.codex/bin/project-env" env GOAV_PROJECT_ENV=1 bash "$root/scripts/bench/workloads.sh" "$@"
fi
cd "$root"
out="${GOAV_WORKLOAD_OUTPUT:-$root/bench-results/workloads/$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$out"
out="$(cd "$out" && pwd)"
export GOAV_WORKLOAD_OUTPUT="$out"
export GOAV_WORKLOAD_CLIPS="${GOAV_WORKLOAD_CLIPS:-$out}"
export GOAV_WEBRTC_SOAK_DURATION="${GOAV_WEBRTC_SOAK_DURATION:-10s}"
export CGO_ENABLED=0
if [ "$GOAV_WORKLOAD_CLIPS" = "$out" ]; then
  for pair in 720p:1280x720 1080p:1920x1080; do
    ffmpeg -hide_banner -loglevel error -y \
      -f lavfi -i "testsrc2=size=${pair#*:}:rate=30" \
      -f lavfi -i "sine=frequency=440:sample_rate=48000" -t 3 \
      -c:v libx264 -preset medium -pix_fmt yuv420p -g 30 -bf 2 -crf 20 \
      -c:a aac -b:a 128k "$out/${pair%:*}.mp4"
  done
fi
{
  git rev-parse HEAD
  go version
  ffmpeg -version
  shasum -a 256 "$GOAV_WORKLOAD_CLIPS/720p.mp4" "$GOAV_WORKLOAD_CLIPS/1080p.mp4"
  printf 'webrtc_soak_duration=%s\n' "$GOAV_WEBRTC_SOAK_DURATION"
} > "$out/provenance.txt"
timing=(-v)
if [ "$(uname)" = Darwin ]; then timing=(-l); fi
build="$(mktemp -d)"
trap 'rm -rf "$build"' EXIT
(cd libcodec && go test -c -o "$build/file-ladder.test" .)
(cd webrtcav && go test -c -o "$build/webrtc-soak.test" .)
# Measure the workload processes, excluding compiler CPU and memory.
(cd libcodec && /usr/bin/time "${timing[@]}" "$build/file-ladder.test" -test.timeout=30m -test.count=1 -test.v -test.run='^TestFileLadderWorkload$') \
  2>&1 | tee "$out/file-ladder.txt"
(cd webrtcav && /usr/bin/time "${timing[@]}" "$build/webrtc-soak.test" -test.timeout=0 -test.count=1 -test.v -test.run='^(TestWebRTCLifecycleSoak|TestTaskAVLifecycleSoak)$') \
  2>&1 | tee "$out/webrtc-soak.txt"
(cd libcodec && "$build/file-ladder.test" -test.timeout=2m -test.count=1 -test.v -test.run='^TestAudioToolbox$/aac/audiotoolbox/decode/transitions$') \
  2>&1 | tee "$out/audio-transitions.txt"
for size in 720p 1080p; do
  for pair in hd:1280:720 sd:640:360; do
    name="${pair%%:*}"
    dimensions="${pair#*:}"
    ffmpeg -hide_banner -i "$GOAV_WORKLOAD_CLIPS/$size.mp4" -i "$out/$size-$name.mp4" \
      -lavfi "[0:v]scale=$dimensions:flags=area,setsar=1,setpts=PTS-STARTPTS[ref];[1:v]setsar=1,setpts=PTS-STARTPTS[got];[ref][got]psnr" \
      -an -f null - > "$out/$size-$name-quality.txt" 2>&1
    ffprobe -v error -show_streams -show_format -of json "$out/$size-$name.mp4" > "$out/$size-$name.json"
  done
done
printf 'Workload evidence: %s\n' "$out"

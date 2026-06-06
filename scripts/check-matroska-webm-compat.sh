#!/usr/bin/env sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 FILE.mkv|FILE.webm" >&2
  exit 2
fi

input=$1
if [ ! -f "$input" ]; then
  echo "missing input file: $input" >&2
  exit 2
fi

run_if_present() {
  tool=$1
  shift
  if command -v "$tool" >/dev/null 2>&1; then
    "$tool" "$@"
  else
    echo "skip: $tool not installed" >&2
  fi
}

tmpdir=${TMPDIR:-/tmp}/goav-matroska-compat.$$
mkdir -p "$tmpdir"
trap 'rm -rf "$tmpdir"' EXIT INT TERM

run_if_present ffprobe -v error -show_streams "$input"
run_if_present mkvalidator "$input"
run_if_present mkvinfo "$input"
run_if_present mkvextract tracks "$input" 0:"$tmpdir/track-0.bin"
run_if_present mkvmerge -o "$tmpdir/remux.mkv" "$input"

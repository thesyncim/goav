# Changelog

All notable changes to this project should be recorded here.

The project is pre-v1. Until the first tagged release, use commit history and
pull-request descriptions as the detailed change record. Release entries should
name user-visible API changes, behavior changes, new adapters, performance
methodology changes, and migration notes.

## Unreleased

- Added grammar-shaped live-room sync with `SyncPolicy`, `.Sync(...)`,
  `SyncTolerance`, and `SyncDropLate` for branch-local audio/video alignment;
  hold-late behavior is the default and does not add a separate public option.
- Added `AtMediaTime(...)` rebranch boundaries, per-rule `OnStream(...,
  OnRemove(...))` removal disposition, and destination commit/abort/error
  lifecycle events.
- Updated the dynamic-audio-room and Gio WebRTC showcase examples around the
  live-room runtime path with sync policy, live rebranching, and dynamic branch
  behavior.
- Added repository trust documents and CI artifact guidance for the v1
  credibility pass.
- Added checked error, operations, extension-cookbook, composability-law, and
  release-process documentation.
- Added standalone external example modules for custom sources, provider
  sources, destinations, filters, codecs, joins, transactional writers, and
  control-plane hosts.
- Added perf-lab benchmark smoke for latency quantiles, heap/RSS capture,
  SourcePush pressure, attach/detach under load, fanout sweeps,
  Matroska/WebM corpus paths, and real Opus adapter throughput.
- Added release automation with CLI checksums, module SBOM, build metadata, and
  provenance artifacts.
- Added compatibility policy and release-note template for the v0/v1 release
  decision.
- Recorded GitHub repository metadata, topic, homepage, and no-release-yet
  posture in checked repository trust documentation.
- Added PR evidence template and README trust badges for Go version and release
  notes.

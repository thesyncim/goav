### Fixed

- Native audio decoders now recreate sessions on discontinuity and codec changes, discard old delayed samples, adopt updated stream metadata, and rescale packet timestamps into the sample clock. EOS still drains valid delayed samples.
- WebRTC configuration changes reach RTP sources at a reliable packet boundary, independently of bounded observer channels. Already-started reads and buffered RTP retain their original configuration.
- Deferred node finalization failures survive reclamation and observer shutdown and remain discoverable through later graph/task Close calls with errors.Is and node context.

### Testing

- Added native AAC transition fixtures, concurrent WebRTC update coverage, and delayed cleanup failure regressions, including the external consumer gate.
- File workloads report separate per-rendition packet-input and post-decode latency. WebRTC probes label their trailing quantile window and retain whole-run maxima; a new Task soak verifies repeated attach/detach, decodable A/V, remote PLI, and recording finalization.

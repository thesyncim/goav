# Documentation Index

Read this when you need to choose the right goav document without archaeology.

## Start Here

- [`ROADMAP.md`](ROADMAP.md): reliability tiers, release gates, planned work, and non-goals.
- [`NORTH_STAR.md`](NORTH_STAR.md): design contract and evidence scoreboard.
- [`V1_SCOPE.md`](V1_SCOPE.md): what the v1 front door promises, and what remains governed pre-v1.
- [`API_SURFACE.md`](API_SURFACE.md): governed exported surface and extension boundaries.
- [`ARCHITECTURE.md`](ARCHITECTURE.md): internal layering, recipe IR, planner, runtime, and graph model.

## Usage

- [`CLI.md`](CLI.md): `goav run` schema, `goav ctl`, and when to use CLI vs library.
- [`OPERATIONS.md`](OPERATIONS.md): recipe operations, branches, joins, and runtime mutation.
- [`COMPOSABILITY_LAWS.md`](COMPOSABILITY_LAWS.md): laws and tests for composing grammar features.
- [`CONTROL_PLANE.md`](CONTROL_PLANE.md): `ctlserver` hosts and `goav ctl` control sockets.
- [`USE_CASES.md`](USE_CASES.md): workflow-shaped examples mapped to the grammar.

## Extension And Runtime

- [`ADAPTERS.md`](ADAPTERS.md): standard adapters and adapter boundaries.
- [`ADAPTER_AUTHORING.md`](ADAPTER_AUTHORING.md): how to write codecs, formats, filters, sources, destinations, and joins.
- [`EXTENSION_COOKBOOK.md`](EXTENSION_COOKBOOK.md): copyable extension patterns.
- [`COMPONENTS.md`](COMPONENTS.md): custom components and in-process hooks.
- [`FLOW_CONTROL.md`](FLOW_CONTROL.md): branch buffering, drops, and backpressure.
- [`PERFORMANCE.md`](PERFORMANCE.md): allocation pins, benchmarks, and performance posture.
- [`RTP_WEBRTC.md`](RTP_WEBRTC.md): RTP/WebRTC transport module guidance.

## Reference

- [`ERRORS.md`](ERRORS.md): structured error behavior and fix guidance.
- [`ERROR_CATALOG.md`](ERROR_CATALOG.md): current error-code catalog.
- [`ENCODER_CONFIG.md`](ENCODER_CONFIG.md): encoder settings vocabulary.
- [`COMPATIBILITY.md`](COMPATIBILITY.md): compatibility posture and exceptions.
- [`RELEASING.md`](RELEASING.md): release workflow and checks.
- [`REPOSITORY_TRUST.md`](REPOSITORY_TRUST.md): repository metadata and trust posture.
- [`GSTREAMER_ALTERNATIVE.md`](GSTREAMER_ALTERNATIVE.md): comparison with GStreamer.
- [`matroska.md`](matroska.md): Matroska/WebM container notes.

## History

Finished campaign artifacts live in [`history/`](history/). They are retained
for context, but forward-looking work should update the living docs above.

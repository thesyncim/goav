# Documentation

The [project README](../README.md) is the front door. Each topic below has one
owning doc.

## Guides

- [`USE_CASES.md`](USE_CASES.md): recipe shapes for common media workflows.
- [`OPERATIONS.md`](OPERATIONS.md): what each operation consumes, produces, inserts, or refuses.
- [`MULTI_INPUT.md`](MULTI_INPUT.md): combining streams with `Mix`, `Composite`, `Select`, and custom `Join`.
- [`FLOW_CONTROL.md`](FLOW_CONTROL.md): buffering, backpressure, playout, sync, and branch isolation.
- [`CONTROL_PLANE.md`](CONTROL_PLANE.md): inspecting and steering a running task (`ctlserver`, `goav ctl`).
- [`RTP_WEBRTC.md`](RTP_WEBRTC.md): RTP and WebRTC ingest/egress through Pion.
- [`CLI.md`](CLI.md): the `goav` CLI (`run`, `describe`, `explain`, `play`, `cameras`, `ctl`).

## Extending

- [`EXTENSION_COOKBOOK.md`](EXTENSION_COOKBOOK.md): copyable patterns for custom sources, destinations, stages, filters, codecs, and joins.
- [`ADAPTER_AUTHORING.md`](ADAPTER_AUTHORING.md): lifecycle, ownership rules, and test contracts for custom adapters.
- [`ADAPTERS.md`](ADAPTERS.md): bundled codecs, containers, filters, and licenses.

## Reference

- [`ERRORS.md`](ERRORS.md): handling `*goav.BuildError` and runtime errors.
- [`ERROR_CATALOG.md`](ERROR_CATALOG.md): generated index of every `errcode.Code`.
- [`PERFORMANCE.md`](PERFORMANCE.md): allocation pins, benchmarks, and methodology.
- [`ARCHITECTURE.md`](ARCHITECTURE.md): internal layering from recipe IR through planner to runtime graph.
- [`ROADMAP.md`](ROADMAP.md): stability status, codec decisions, and v1 checklist.
- [`RELEASING.md`](RELEASING.md): release checklist and nested-module tagging.

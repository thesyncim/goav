# Documentation

The [project README](../README.md) is the front door. These docs form a
learning path: read the first section in order, then jump to the section that
matches your work. Each topic has one owning doc.

## Start

- [`USE_CASES.md`](USE_CASES.md): first, to find the recipe shape closest to what you are building.
- [`OPERATIONS.md`](OPERATIONS.md): when you need what an operation consumes, produces, inserts, or refuses; also encoder settings and the composition laws.
- [`MULTI_INPUT.md`](MULTI_INPUT.md): when several streams must become one (mix, composite, select, custom join).

## Running live

- [`FLOW_CONTROL.md`](FLOW_CONTROL.md): when a live branch cannot keep up and you must choose what blocks, drops, or buffers.
- [`CONTROL_PLANE.md`](CONTROL_PLANE.md): when a running task must be inspected or steered from outside the process (`ctlserver`, `goav ctl`).
- [`RTP_WEBRTC.md`](RTP_WEBRTC.md): when media arrives over RTP or WebRTC through Pion.
- [`CLI.md`](CLI.md): the pipeline language and `goav run`, `describe`, `explain`, and `ctl`: recipes as text, for the command line, config files, and control sockets.

## Extending

- [`EXTENSION_COOKBOOK.md`](EXTENSION_COOKBOOK.md): when you know what to plug in (source, destination, stage, filter, codec, join, control host) and want the smallest seam to copy.
- [`ADAPTER_AUTHORING.md`](ADAPTER_AUTHORING.md): when writing a codec, container, filter, or provider adapter, for its lifecycle, ownership rules, and required tests.
- [`ADAPTERS.md`](ADAPTERS.md): to see which adapters and containers ship, what each supports, and where each stops.

## Reference

- [`ERRORS.md`](ERRORS.md): when your code handles a `*goav.BuildError`.
- [`ERROR_CATALOG.md`](ERROR_CATALOG.md): to look up one error code, its fix, and the test that pins it.
- [`API_SURFACE.md`](API_SURFACE.md): before adding an export, or to find the tier a symbol belongs to.
- [`COMPATIBILITY.md`](COMPATIBILITY.md): to learn what v1 promises, what stays governed pre-v1, and what each tag must state.
- [`PERFORMANCE.md`](PERFORMANCE.md): before trusting or making a performance claim; it separates proven, measured, and not proven.

## Project

- [`ARCHITECTURE.md`](ARCHITECTURE.md): when changing internals, for the layering from recipe IR through the planner to the runtime graph.
- [`ROADMAP.md`](ROADMAP.md): to see what is stable, experimental, planned, or a non-goal, and the gates before v1.
- [`RELEASING.md`](RELEASING.md): when cutting a tag, for the checklist, gate matrix, automation, and repository trust posture.
- [`GSTREAMER_ALTERNATIVE.md`](GSTREAMER_ALTERNATIVE.md): when choosing between goav and GStreamer.

## History

Finished campaign artifacts live in [`history/`](history/). They are kept for
context; forward-looking work updates the living docs above.

# goav

[![CI](https://github.com/thesyncim/goav/actions/workflows/ci.yml/badge.svg)](https://github.com/thesyncim/goav/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/thesyncim/goav.svg)](https://pkg.go.dev/github.com/thesyncim/goav)
[![Go Version](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](go.mod)
[![License](https://img.shields.io/github/license/thesyncim/goav)](LICENSE)
[![Status](https://img.shields.io/badge/status-pre--v1-orange)](docs/ROADMAP.md)
[![Release Notes](https://img.shields.io/badge/release_notes-CHANGELOG-blue)](CHANGELOG.md)

`goav` is a pure-Go media runtime for applications that need to describe,
validate, run, inspect, and change media work in-process.

You write one recipe: inputs, selected streams, operations, taps, branches, and
destinations. The recipe is validated before resources open, then compiled into
a task that can report its plan, expose snapshots and events, accept runtime
branches, and receive live controls.

No cgo is required for the core runtime, and no FFmpeg or GStreamer process is
started behind your back. `CGO_ENABLED=0` builds a static binary when your
selected adapters allow it.

## Install

```sh
go get github.com/thesyncim/goav
```

## 30-Second Examples

One packet-preserving recording recipe:

```go
return goav.From(goav.FileInput("input.ivf", in)).
    Copy().
    To(goav.File("recording.ivf", out)).
    Run(ctx)
```

The grammar stays small:

- `Input`: file, URI, transport provider, generated source, or application
  source.
- `Stream`: `.Audio()`, `.Video()`, or an explicit stream selector.
- `Tap`: a named attach point, optionally typed with `goav.FrameTap(...)` or
  `goav.PacketTap(...)`.
- `Branch`: one downstream operation sequence from a stream point or tap,
  declared with `goav.Branch(...)` inside `Branches(...)`.
- `Destination`: `goav.File(...)`, URI, `goav.Writer(...)`,
  `goav.Custom(...)`, shared mux, or `goav.Sink(...)`.
- `Flow`: reusable operations with no destination attached, declared with
  `goav.Flow(...)`.
- `Task`: a running graph with control, events, snapshots, attach, rebranch,
  and detach.

The public grammar stays Input, Stream, Tap, Branch, Destination, Flow, and
Task. Operations are methods on a chain: `.Decode()`, `.Copy()`, `.Resize()`,
`.Resample()`, `.Do(stage)`, `.Encode(codec)`, and `.To(destination)`.
Reuse the same destination value when branches should share one mux, sink
group, or transactional writer.

## Common Recipes

| Need | Front-door shape | Deep dive |
|---|---|---|
| Record encoded packets | `From(input).Copy().To(File(...))` | [docs/USE_CASES.md](docs/USE_CASES.md) |
| Produce several outputs from one stream | `Branches(Branch(...).To(...), ...)` | [docs/USE_CASES.md](docs/USE_CASES.md) |
| Decode, filter, and re-encode | `.Decode().Resize()/Resample().Encode(codec).To(...)` | [docs/OPERATIONS.md](docs/OPERATIONS.md) |
| Process live room tracks and mix one output | `OnStream(..., Branch("track")..., Branch("mix").To(sharedMixer))` | [docs/USE_CASES.md](docs/USE_CASES.md), [examples/dynamic-audio-room](examples/dynamic-audio-room) |
| Attach diagnostics while live | `Build`, then `Task.Attach(ctx, Branch(...).From(Tap(...)))` | [docs/CONTROL_PLANE.md](docs/CONTROL_PLANE.md) |
| Add an adapter or external component | `goav.New(goav.With...)` plus the relevant provider interface | [docs/EXTENSION_COOKBOOK.md](docs/EXTENSION_COOKBOOK.md) |

## Why goav

| If you usually reach for | goav is useful when |
|---|---|
| FFmpeg commands | The media work belongs inside your Go process, needs typed errors, or must be changed while running. |
| GStreamer | You want a smaller Go API with recipe validation, snapshots, and runtime branch attach instead of a full multimedia framework. |
| Pion-only glue | You need WebRTC/RTP ingest plus recording, decoding, transforms, sinks, and muxing without hand-wiring every path. |
| In-house pipeline code | You want extension seams for sources, destinations, codecs, filters, joins, and control-plane hosts without inventing another graph contract. |

goav is not a universal replacement for dedicated media tools. It is aimed at
applications where recipes, validation, observability, and live control are part
of the product.

## Capability Matrix

| Area | In tree today | Notes |
|---|---|---|
| Core runtime | Pure Go, in-process recipes, task lifecycle, events, watch, snapshot, stats, attach, detach, rebranch, controls | Core builds with `CGO_ENABLED=0` when selected adapters do. |
| Containers | IVF, Annex B, Matroska, WebM | Container internals are implementation packages behind the `format` extension seam. |
| Codecs | Opus, VP8, VP9, AV1 encode/decode verticals; AAC-LC and H264 receive/decode active | AAC-LC and H264 recipe encode remain descriptor-only until encoder backends are enabled. |
| Transforms | Resize, resample, custom frame stages | Shape solver inserts conversions only under explicit policy. |
| Transports | RTP and WebRTC in nested modules | Root module stays dependency-pure; transport modules carry their own third-party dependencies. |
| Extension points | Sources, destinations, codecs, formats, filters, stages, joins, control-plane hosts, goavtest fixtures and expect assertions | Registrations are per runtime; there is no global adapter table. |

## Stability Matrix

| Surface | Tier | Status |
|---|---|---|
| Recipe grammar, `Task`, structured errors, `plan`, `snapshot`, `lifecycle`, `shape`, `flow`, `av` vocabulary | Tier A | v0 stable and pinned against silent growth. |
| Provider, codec, format, filter, control-host, custom-stage, custom-join, and testing seams | Tier B | Extension points are documented and doc-pinned; new capabilities may grow them. |
| Expert graph and prebuilt low-level components | Tier C | Supported for advanced composition, but not the first-learn API. |
| Performance claims | Evidence based | Allocation pins and benchmarks are real; tail latency, RSS, soak, and comparative leadership are explicitly not proven yet. |
| Release status | Pre-v1 | The remaining v1 gate is the maintainer release decision and compatibility note; see [docs/ROADMAP.md](docs/ROADMAP.md). |

## Deep Dives

| Topic | Where to go |
|---|---|
| Use cases and branch recipes | [docs/USE_CASES.md](docs/USE_CASES.md) |
| Operation rules and errors | [docs/OPERATIONS.md](docs/OPERATIONS.md), [docs/ERRORS.md](docs/ERRORS.md), [docs/ERROR_CATALOG.md](docs/ERROR_CATALOG.md) |
| Runtime control and socket hosts | [docs/CONTROL_PLANE.md](docs/CONTROL_PLANE.md), [examples/control-plane-host](examples/control-plane-host) |
| Real-world dynamic media patterns | [examples/dynamic-audio-room](examples/dynamic-audio-room), [examples/gio-webrtc-showcase](examples/gio-webrtc-showcase), [examples/webrtc-runtime-ladder](examples/webrtc-runtime-ladder) |
| Extension cookbook and adapter authoring | [docs/EXTENSION_COOKBOOK.md](docs/EXTENSION_COOKBOOK.md), [docs/ADAPTER_AUTHORING.md](docs/ADAPTER_AUTHORING.md), [docs/ADAPTERS.md](docs/ADAPTERS.md) |
| Copyable extension modules | [examples/custom-source](examples/custom-source), [examples/provider-source](examples/provider-source), [examples/custom-destination](examples/custom-destination), [examples/custom-filter](examples/custom-filter), [examples/transactional-writer](examples/transactional-writer), [examples/custom-codec](examples/custom-codec), [examples/custom-join](examples/custom-join) |
| API surface and composability laws | [docs/API_SURFACE.md](docs/API_SURFACE.md), [docs/COMPOSABILITY_LAWS.md](docs/COMPOSABILITY_LAWS.md) |
| Reusable components | [docs/COMPONENTS.md](docs/COMPONENTS.md) |
| Performance methodology | [docs/PERFORMANCE.md](docs/PERFORMANCE.md) |
| Repository trust and release process | [docs/REPOSITORY_TRUST.md](docs/REPOSITORY_TRUST.md), [docs/RELEASING.md](docs/RELEASING.md), [docs/COMPATIBILITY.md](docs/COMPATIBILITY.md) |
| V1 credibility map | [docs/V1_CREDIBILITY_AUDIT.md](docs/V1_CREDIBILITY_AUDIT.md), [docs/ROADMAP.md](docs/ROADMAP.md) |
| FFmpeg, GStreamer, and framework comparison | [docs/GSTREAMER_ALTERNATIVE.md](docs/GSTREAMER_ALTERNATIVE.md) |

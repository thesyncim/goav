# goav

[![CI](https://github.com/thesyncim/goav/actions/workflows/ci.yml/badge.svg)](https://github.com/thesyncim/goav/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/thesyncim/goav.svg)](https://pkg.go.dev/github.com/thesyncim/goav)
[![Go Version](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](go.mod)
[![License](https://img.shields.io/github/license/thesyncim/goav)](LICENSE)
[![Status](https://img.shields.io/badge/status-pre--v1-orange)](docs/ROADMAP.md)
[![Release Notes](https://img.shields.io/badge/release_notes-CHANGELOG-blue)](CHANGELOG.md)

`goav` is a pure-Go media runtime for applications where media is part of the
product, not a subprocess bolted on from the side.

You describe the work once: where media comes from, which streams matter, what
should happen to them, where the results go, and which points should remain
available for live branches. Before anything opens, goav can explain and
validate that recipe. After it starts, the same task can be watched, inspected,
controlled, branched, rebranched, and detached.

The core runtime has no cgo requirement and does not start FFmpeg or GStreamer
behind your back. If the adapters you choose are pure Go, `CGO_ENABLED=0`
builds a static binary.

## Install

```sh
go get github.com/thesyncim/goav
```

## 30-Second Examples

Start with one packet-preserving recording:

```go
return goav.From(goav.FileInput("input.ivf", in)).
    Copy().
    To(goav.File("recording.ivf", out)).
    Run(ctx)
```

Most recipes read like that sentence:

- **Input**: a file, URI, transport provider, generated source, or application
  source.
- **Stream**: `.Audio()`, `.Video()`, or an explicit stream selector.
- **Operations**: `.Decode()`, `.Copy()`, `.Resize()`, `.Resample()`,
  `.Do(stage)`, `.Encode(codec)`, and `.Tap(...)`.
- **Tap**: a named point that later branches can attach to; use
  `goav.FrameTap(...)` or `goav.PacketTap(...)` when the domain matters.
- **Branch**: a downstream operation sequence from a stream point or tap,
  built with `goav.Branch(...)` inside `.Branches(...)`.
- **Destination**: `goav.File(...)`, `goav.Writer(...)`, `goav.Sink(...)`,
  a URI, object destination, or shared mux/sink group.
- **Flow**: reusable operations with no destination of their own, built with
  `goav.Flow(...)`.
- **Task**: the running work: events, snapshots, stats, control, attach,
  rebranch, and detach.

The public grammar stays deliberately small: Input, Stream, Tap, Branch,
Destination, Flow, Task. Reuse the same destination value when several
branches should feed one mux, sink group, or transactional writer.

## Common Recipes

| When you want to | Start with | Keep reading |
|---|---|---|
| Record encoded packets without decoding | `From(input).Copy().To(File(...))` | [Use cases](docs/USE_CASES.md) |
| Produce several outputs from one stream | `Branches(Branch(...).To(...), ...)` | [Use cases](docs/USE_CASES.md) |
| Decode, filter, and re-encode | `.Decode().Resize()/Resample().Encode(codec).To(...)` | [Operations](docs/OPERATIONS.md) |
| Process live room tracks | `Task.Attach(ctx, Branch(...).From(input.Stream(track)))` | [Dynamic audio room](examples/dynamic-audio-room) |
| Add diagnostics to a running task | `Build`, then attach a branch from a typed tap | [Control plane](docs/CONTROL_PLANE.md) |
| Plug in your own source, sink, codec, filter, or container | `goav.New(goav.With...)` plus the relevant interface | [Extension cookbook](docs/EXTENSION_COOKBOOK.md) |

## Why goav

goav is useful when media work needs to be part of your Go program's control
flow: typed errors instead of stderr parsing, validated plans before resources
open, snapshots and events while a task runs, and live branches when production
needs one more recorder, meter, preview, or debugging sink.

| If your first instinct is | goav helps when |
|---|---|
| An FFmpeg command | The job needs typed validation, in-process control, or live changes. |
| A GStreamer graph | You want a smaller Go-first recipe surface with snapshots and branch attach. |
| Pion glue code | RTP/WebRTC is only the ingress; you also need recording, decode, transforms, muxing, and sinks. |
| A custom in-house graph | You want extension seams without inventing another task, event, and lifecycle model. |

It is not trying to replace every dedicated media tool. It is for systems where
recipes, observability, runtime control, and Go-native extension points are part
of the application boundary.

## Capability Matrix

| Area | What is in tree | Human version |
|---|---|---|
| Core runtime | Recipes, task lifecycle, events, watch, snapshots, stats, attach, detach, rebranch, controls | Build one validated task, then inspect and change it while it runs. |
| Containers | IVF, Annex B, Matroska, WebM | Enough for the current packet-recording and WebRTC/RTP examples; more formats plug in through `format`. |
| Codecs | Opus, VP8, VP9, AV1 encode/decode verticals; AAC-LC and H264 receive/decode active | The common realtime paths are present; some encode paths remain descriptor-only until backends are enabled. |
| Transforms | Resize, resample, custom frame stages | Conversion happens only when the recipe grants an explicit shape policy. |
| Transports | RTP and WebRTC in nested modules | Pion stays out of the root module and lives where transport code needs it. |
| Extension points | Sources, destinations, codecs, formats, filters, stages, joins, control hosts, tests | Registrations are per runtime; there is no global adapter table. |

## Stability Matrix

goav is pre-v1, but the important surfaces are already pinned so accidental API
growth fails tests.

| Surface | Tier | What that means |
|---|---|---|
| Recipe grammar, `Task`, structured errors, `plan`, `snapshot`, `lifecycle`, `shape`, `flow`, `av` vocabulary | Tier A | Stable enough to build against; changes are deliberate and test-pinned. |
| Provider, codec, format, filter, control-host, custom-stage, custom-join, and testing seams | Tier B | Extension points are documented and may grow as capabilities grow. |
| Expert graph and low-level components | Tier C | Supported for advanced composition, but not the first API to learn. |
| Performance claims | Evidence based | Allocation pins and benchmarks are real; broad tail-latency and soak claims are not made yet. |
| Release status | Pre-v1 | The remaining gate is the maintainer release decision and compatibility note; see [Roadmap](docs/ROADMAP.md). |

## Deep Dives

| Topic | Where to go |
|---|---|
| Recipes, branches, live-room patterns | [Use cases](docs/USE_CASES.md) |
| Operation rules and structured errors | [Operations](docs/OPERATIONS.md), [Errors](docs/ERRORS.md), [Error catalog](docs/ERROR_CATALOG.md) |
| Runtime control and socket hosts | [Control plane](docs/CONTROL_PLANE.md), [control-plane host example](examples/control-plane-host) |
| Browser and dynamic-media demos | [Dynamic audio room](examples/dynamic-audio-room), [Gio WebRTC showcase](examples/gio-webrtc-showcase), [WebRTC runtime ladder](examples/webrtc-runtime-ladder) |
| Extension authoring | [Extension cookbook](docs/EXTENSION_COOKBOOK.md), [Adapter authoring](docs/ADAPTER_AUTHORING.md), [Adapters](docs/ADAPTERS.md) |
| Copyable extension modules | [custom source](examples/custom-source), [provider source](examples/provider-source), [custom destination](examples/custom-destination), [custom filter](examples/custom-filter), [transactional writer](examples/transactional-writer), [custom codec](examples/custom-codec), [custom join](examples/custom-join) |
| API governance and composition rules | [API surface](docs/API_SURFACE.md), [Composability laws](docs/COMPOSABILITY_LAWS.md) |
| Reusable components | [Components](docs/COMPONENTS.md) |
| Performance methodology | [Performance](docs/PERFORMANCE.md) |
| Repository trust and release process | [Repository trust](docs/REPOSITORY_TRUST.md), [Releasing](docs/RELEASING.md), [Compatibility](docs/COMPATIBILITY.md) |
| V1 readiness | [V1 credibility audit](docs/V1_CREDIBILITY_AUDIT.md), [Roadmap](docs/ROADMAP.md) |
| FFmpeg, GStreamer, and framework comparison | [GStreamer alternative](docs/GSTREAMER_ALTERNATIVE.md) |

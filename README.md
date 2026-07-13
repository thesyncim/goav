# goav

**Pure-Go media pipelines for Go services — describe the work, see the plan, then run it.**

[![CI](https://github.com/thesyncim/goav/actions/workflows/ci.yml/badge.svg)](https://github.com/thesyncim/goav/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/thesyncim/goav.svg)](https://pkg.go.dev/github.com/thesyncim/goav)
[![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8)](go.mod)
[![License](https://img.shields.io/github/license/thesyncim/goav)](LICENSE)
[![Status](https://img.shields.io/badge/status-pre--v1-orange)](docs/ROADMAP.md)
[![Release Notes](https://img.shields.io/badge/release_notes-CHANGELOG-blue)](CHANGELOG.md)

goav is a small, composable grammar for building media workflows inside Go
applications: pick streams, declare operations, fan out, and route to
destinations. It is **pure Go** — no cgo, no FFmpeg, no system libraries — so a
recipe compiles into your binary and ships as one artifact. Every recipe is
inspectable data: `Describe` shows the exact graph and `Explain` the typed plan
before any resource opens, and every refusal is a structured error that names
its fix.

## In 30 seconds

```go
// Decode, downscale, re-encode, and mux. The recipe is validated before it runs.
err := bundle.Run(ctx, goav.From(goav.FileInput("in.mp4", in)).
    Video().
    Decode().
    Resize(1280, 720).
    Encode(codec.VP9(codec.Bitrate(2_000_000))).
    To(goav.Write("out.webm", out)))
```

The model reads top to bottom — inputs, stream selection, ordered operations,
optional fanout, destinations, and a runnable task:

```text
From(inputs) ─▶ .Audio()/.Video() ─▶ operations ─▶ .Branches(...) ─▶ .To(dst) ─▶ Task
   input           stream select       decode/copy/     fanout          output     Run/Close
                                       resize/encode
```

## Install

```sh
go get github.com/thesyncim/goav
```

The bundled pure-Go adapters — VP8/VP9, AV1, Opus, AAC, H.264 decode, and
Matroska/WebM/IVF containers plus MP4 (read) — live in `goav/bundle`.
`bundle.Run` and `bundle.Build` give you a runtime with the standard set;
build your own with `goav.New(...)` to register exactly the adapters you
need, per service, with no global state. The codec backends are pure-Go
ports of the reference implementations (libvpx and friends), maintained as
separate `thesyncim/*` modules — no cgo anywhere in the dependency graph.

## Common recipes

| Need | Shape | Details |
| --- | --- | --- |
| Record / remux packets | `From(input).Copy().To(goav.Write(...))` | [Use cases](docs/USE_CASES.md) |
| Decode to your code | `From(input).Audio().Decode().To(goav.Sink(...))` | [Operations](docs/OPERATIONS.md) |
| Transform and encode | `.Decode().Resize(...).Encode(codec)` / `.Decode().Resample(...).Encode(codec)` | [Operations](docs/OPERATIONS.md) |
| Fan one stream out | `.Copy().Branches(...)` (packets), `.Decode().Branches(...)` (frames) | [Use cases](docs/USE_CASES.md) |
| Own a media boundary | `goav.Source(...)`, `goav.Input(provider)`, `goav.Sink(...)`, `goav.Custom(...)` | [Extension cookbook](docs/EXTENSION_COOKBOOK.md) |

Use `goav.Mux(name, destination)` to feed several branches into one mux or sink
group; repeated ungrouped destination names are rejected so sharing stays explicit.

## Why goav

- **Pure Go, one binary.** No cgo, no FFmpeg, no system codecs — `go build` and ship.
- **Plan before you run.** `Describe` returns the node-for-node graph and `Explain`
  a typed report (inputs, codecs, shapes, adapters, decisions) before any resource
  opens — both machine-consumable as stable JSON.
- **Errors you can act on.** Every build refusal is a `*goav.BuildError` with a
  stable family and code, the failing operation, and concrete fixes — not a string.
- **Live timing built in.** One shared clock per task: synchronized playout,
  pause/seek/rate controls, and QoS lateness reports — see [Flow control](docs/FLOW_CONTROL.md).
- **Explicit, app-owned extension.** Sources, sinks, destinations, codecs,
  formats, and filters plug in through exported interfaces, per runtime.

## What v1 covers

The recipes above are the v1-supported front door. Live mutation (`Attach`),
control sockets, converged streams (`Mix`/`Composite`/`Select`), and the expert
graph are **governed pre-v1** — implemented and tested, but not the beginner
path. See [V1 scope](docs/V1_SCOPE.md). goav is pre-v1: breaking changes can land
until the first tag.

## Deep dives

| Topic | Start here |
| --- | --- |
| Operation rules and shape errors | [Operations](docs/OPERATIONS.md), [Errors](docs/ERRORS.md), [Error catalog](docs/ERROR_CATALOG.md) |
| Recipes, branches, live rooms, RTP, WebRTC | [Use cases](docs/USE_CASES.md) |
| Extension authoring | [Extension cookbook](docs/EXTENSION_COOKBOOK.md), [Adapter authoring](docs/ADAPTER_AUTHORING.md), [Adapters](docs/ADAPTERS.md) |
| Copyable examples | [custom source](examples/custom-source), [provider source](examples/provider-source), [custom destination](examples/custom-destination), [custom filter](examples/custom-filter), [transactional writer](examples/transactional-writer), [custom codec](examples/custom-codec), [custom join](examples/custom-join) |
| Runtime control and observation (governed pre-v1) | [Control plane](docs/CONTROL_PLANE.md), [control-plane host](examples/control-plane-host), [Components](docs/COMPONENTS.md) |
| Where goav fits | [vs GStreamer](docs/GSTREAMER_ALTERNATIVE.md), [Architecture](docs/ARCHITECTURE.md), [Roadmap](docs/ROADMAP.md) |
| Performance and trust | [Performance](docs/PERFORMANCE.md), [Releasing](docs/RELEASING.md), [Compatibility](docs/COMPATIBILITY.md), [Repository trust](docs/REPOSITORY_TRUST.md) |

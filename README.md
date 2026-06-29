# goav

[![CI](https://github.com/thesyncim/goav/actions/workflows/ci.yml/badge.svg)](https://github.com/thesyncim/goav/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/thesyncim/goav.svg)](https://pkg.go.dev/github.com/thesyncim/goav)
[![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8)](go.mod)
[![License](https://img.shields.io/github/license/thesyncim/goav)](LICENSE)
[![Status](https://img.shields.io/badge/status-pre--v1-orange)](docs/ROADMAP.md)
[![Release Notes](https://img.shields.io/badge/release_notes-CHANGELOG-blue)](CHANGELOG.md)

Build media recipes inside Go services: describe the work, validate it before
resources open, then run it with explicit adapters.

`goav` keeps the front door small: inputs, stream selection, explicit
operations, fanout branches, destinations, and task lifecycle. Bundled adapters
live in `goav/bundle`; advanced runtime and extension topics are linked below.

The recipes below are the v1-supported workflows. Live mutation, control
sockets, converged streams, and the expert graph are governed pre-v1 features —
real and tested, but not the beginner path. See [V1 scope](docs/V1_SCOPE.md).

## Install

```sh
go get github.com/thesyncim/goav
```

## 30-Second Examples

Record packets without decoding:

```go
return bundle.Run(ctx, goav.From(goav.FileInput("input.ivf", in)).
    Copy().
    To(goav.Write("recording.ivf", out)),
)
```

The normal path is visible in the recipe: call `.Decode()` before frame work,
use `.Copy()` to stay packet-domain, and route results to `goav.Write(...)`,
`goav.Writer(...)`, `goav.URI(...)`, `goav.Custom(...)`, or `goav.Sink(...)`.
Frame-domain custom sources are already decoded and can go straight to frame
stages, sinks, or encoders without `.Decode()`.

## Common Recipes

| Need | Shape | Details |
| --- | --- | --- |
| Packet record/remux | `From(input).Copy().To(goav.Write(...))` | [Use cases](docs/USE_CASES.md) |
| Decode to app code | `From(input).Audio().Decode().To(goav.Sink(...))` | [Operations](docs/OPERATIONS.md) |
| Transform and encode | `.Decode().Resize(...).Encode(codec)` or `.Decode().Resample(...).Encode(codec)` | [Operations](docs/OPERATIONS.md) |
| Fan out one stream | `.Copy().Branches(...)` for packets, `.Decode().Branches(...)` for frame work | [Use cases](docs/USE_CASES.md) |
| Custom media boundary | `goav.Source(...)`, `goav.Input(provider)`, `goav.Sink(...)`, `goav.Custom(...)` | [Extension cookbook](docs/EXTENSION_COOKBOOK.md) |

Use `goav.Mux(name, destination)` when several branches should feed one mux,
sink group, or transactional writer. Repeated ungrouped destination names are
rejected so sharing is explicit.

## Why goav

- Recipes are Go data: `Describe` and `Explain` show the plan before resources
  open.
- Runtime adapters are explicit: use `bundle.Run` for the bundled set, or build
  a custom runtime per service.
- Build failures are structured `*goav.BuildError` values with stable families,
  codes, details, and fixes.
- App-owned sources, sinks, destinations, codecs, formats, and filters plug in
  without global process state.

## Deep Dives

| Topic | Start here |
| --- | --- |
| Operation rules and shape errors | [Operations](docs/OPERATIONS.md), [Errors](docs/ERRORS.md), [Error catalog](docs/ERROR_CATALOG.md) |
| Recipes, branches, live rooms, RTP, WebRTC | [Use cases](docs/USE_CASES.md) |
| Extension authoring | [Extension cookbook](docs/EXTENSION_COOKBOOK.md), [Adapter authoring](docs/ADAPTER_AUTHORING.md), [Adapters](docs/ADAPTERS.md) |
| Copyable examples | [custom source](examples/custom-source), [provider source](examples/provider-source), [custom destination](examples/custom-destination), [custom filter](examples/custom-filter), [transactional writer](examples/transactional-writer), [custom codec](examples/custom-codec), [custom join](examples/custom-join) |
| Runtime control and observation (governed pre-v1) | [Control plane](docs/CONTROL_PLANE.md), [control-plane host](examples/control-plane-host), [Components](docs/COMPONENTS.md) |
| Performance and release trust | [Performance](docs/PERFORMANCE.md), [Releasing](docs/RELEASING.md), [Compatibility](docs/COMPATIBILITY.md), [Repository trust](docs/REPOSITORY_TRUST.md), [V1 audit](docs/V1_CREDIBILITY_AUDIT.md) |

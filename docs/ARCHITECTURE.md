# Architecture & Roadmap

## Pipeline Lifecycle

Every recipe—whether built in Go, parsed by `launch`, or attached at runtime—passes through the same pipeline:

```text
Go API / launch DSL  ──▶  recipeir (immutable AST)  ──▶  internal/compile (shape solver + lowerer)  ──▶  WorkPlan  ──▶  pipeline.Graph
```

1. **Describe (`goav.Input` / `Stream` / `Output`):** pure values; no I/O, no goroutines, no global state.
2. **Compile (`internal/compile`):** resolves stream selectors, validates codec/container compatibility, inserts format/shape conversions, and emits either a `*goav.BuildError` (with stable `errcode.Code` and fixes) or a cold-path `WorkPlan`.
3. **Execute (`pipeline.Graph`):** binds `WorkPlan` nodes into a lock-free data plane. Direct edges hand off ownership on the producer goroutine; buffered edges use bounded lock-free rings. Steady-state media loops never touch recipe or compiler types (`TestDataPlaneTakesNoMutex`).
4. **Mutate (`task.Attach` / `Detach` / `Control`):** compiles a `WorkPatch` against a named `.Tap(...)`, `.Track(...)`, or `.Route(...)` and swaps routing atomically.

## Package Boundaries

- **Root (`goav`)**: front-door recipe grammar, `Runtime`, `Task`, `BuildError`, and runtime attach/control coordination.
- **Domain contracts**: `av` (packets, frames, events, refcounted `Buffer`), `codec`, `format`, `filter`, `pipeline`, `flow`, `shape`, `control`, `errcode`, `provider`, `runconfig`, `plan`, `source`, `component`.
- **Adapters & containers**: `adapters/*`, `container/*`, `bundle`, `playoutav`, `scheme`, `launch`, `ctlserver`, `graphrender`.
- **Nested modules** (isolated dependency trees): `rtpav`, `webrtcav` (Pion), `player` (Ebitengine/oto), `cameraav` (OS capture), `libcodec` (purego OS/shared-lib codecs), `gplcodec` (opt-in GPL), `goavtest/expect` (`go-cmp`), `cmd/goav` (CLI).

## Pre-v1 Status & Codec Decisions

`goav` is **pre-v1**: breaking API changes may land until the first `v1.0.0` tag.

- **Pure-Go core (`CGO_ENABLED=0`):** no cgo, no FFmpeg, no `unsafe`, and no assembly in the root module. `GOEXPERIMENT=simd` enables Go 1.27 portable SIMD kernels.
- **Bundled pure-Go codecs:** VP8, VP9, AV1, and Opus decode and encode.
- **H264 recipe encode** is deliberately descriptor-only until a vetted pure-Go H.264 encoder backend exists. Without an external encoder registered via `runconfig.WithEncoder`, H.264 encode returns `codec.ErrUnavailable` (use `libcodec` for VideoToolbox / OpenH264).
- **H264 decode:** available via `-tags goav_goh264` (LGPL) or `libcodec`.
- **AAC:** available via `libcodec.AudioToolbox()` (macOS) or opt-in `gplcodec.AAC()` (GPL, arm64).
- **Non-goals for core:** dynamic binary plugin loading, hardware codecs in the root module, pull-based graph scheduling.

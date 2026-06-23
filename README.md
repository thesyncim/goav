# goav

[![CI](https://github.com/thesyncim/goav/actions/workflows/ci.yml/badge.svg)](https://github.com/thesyncim/goav/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/thesyncim/goav.svg)](https://pkg.go.dev/github.com/thesyncim/goav)
[![Go Version](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](go.mod)
[![License](https://img.shields.io/github/license/thesyncim/goav)](LICENSE)
[![Status](https://img.shields.io/badge/status-pre--v1-orange)](docs/ROADMAP.md)
[![Release Notes](https://img.shields.io/badge/release_notes-CHANGELOG-blue)](CHANGELOG.md)

Build media workflows that your Go application can understand, validate, and
change while they run.

`goav` is for product code where audio/video is not just a shell command on the
side. You describe a recipe from input to destination, ask goav to validate the
shape before resources open, then run a task that can emit events, snapshots,
stats, controls, and live branch changes.

It is pure Go at the core: no cgo requirement, no FFmpeg or GStreamer process
started behind your back, and no global adapter table. If the adapters you use
are pure Go, `CGO_ENABLED=0` can build the whole thing into one binary.

Use goav when you want:

- a recording, preview, diagnostic, or live-room path inside a Go service;
- structured build errors instead of parsing stderr;
- runtime attach, detach, rebranch, pause/resume, bitrate, and keyframe control;
- custom sources, sinks, codecs, filters, joins, and control hosts that are
  registered per runtime.

Skip it when you mainly need a desktop playback stack, hardware codec discovery,
or the breadth of a mature multimedia framework.

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

That is the core idea: choose an input, choose the stream work, choose a
destination, run the task. Bigger recipes keep the same shape.

The public vocabulary is deliberately small:

- **Input** is a file, URI, transport provider, generated source, or
  application-owned source.
- **Stream** is `.Audio()`, `.Video()`, or a specific stream selector.
- **Operation** is ordered work such as `.Decode()`, `.Copy()`, `.Resize()`,
  `.Resample()`, `.Do(stage)`, `.Encode(codec)`, or `.Tap(...)`.
- **Tap** names a point for later use. `goav.FrameTap(...)` and
  `goav.PacketTap(...)` make the media domain explicit.
- **Branch** is another path from a stream or tap. Use `Branches(...)` with
  `goav.Branch(...)` when one stream needs several outcomes.
- **Destination** is where media goes: `goav.File(...)`, `goav.Writer(...)`,
  `goav.Sink(...)`, a URI, an object destination, or a shared mux/sink group.
- **Flow** is reusable operation text with no source or destination; build one
  with `goav.Flow(...)`.
- **Task** is the running work: events, watch, snapshots, stats, control,
  attach, rebranch, and detach.

Reuse the same destination value when several branches should feed one mux,
one sink group, or one transactional writer.

## Common Recipes

| I need to... | Recipe sketch | Go deeper |
|---|---|---|
| Record packets without decoding | `From(input).Copy().To(goav.File(...))` | [Use cases](docs/USE_CASES.md) |
| Record and preview from one stream | `.Branches(goav.Branch("archive").To(...), goav.Branch("preview").To(...))` | [Use cases](docs/USE_CASES.md) |
| Decode, filter, and encode again | `.Decode().Resize(...)` or `.Resample(...)`, then `.Encode(codec)` | [Operations](docs/OPERATIONS.md) |
| Attach a live room participant | `Task.Attach(ctx, goav.Branch(...).From(input.Stream(track)))` | [Dynamic audio room](examples/dynamic-audio-room) |
| Add a meter or diagnostic sink | `goav.FrameTap(...)`, then attach a branch ending in `goav.Sink(...)` | [Control plane](docs/CONTROL_PLANE.md) |
| Wrap app-owned media | `goav.Source(...)` or `goav.Input(provider)` | [Extension cookbook](docs/EXTENSION_COOKBOOK.md) |
| Add a codec, filter, muxer, or control host | `goav.New(...)` with the relevant extension option | [Adapter authoring](docs/ADAPTER_AUTHORING.md) |

## What This Unlocks

The first example is tiny on purpose. The interesting part is that the same
recipe model still holds when the task is live, streams arrive late, and one
branch needs different latency behavior than another.

### Archive the room while preview adapts

```text
participant tracks
  -> shared room timeline
  -> archive branch: align and drain
  -> preview branch: align, drop late media, keep moving
```

The recording branch can stay steady while preview sheds stale media through
normal drop stats. Start with [dynamic-audio-room](examples/dynamic-audio-room)
for the deterministic fixture, then open
[gio-webrtc-showcase](examples/gio-webrtc-showcase) for the browser-visible
version.

### Attach a meter after the task is already running

```text
running task
  -> FrameTap("levels")
  -> Branch("support-meter")
  -> Sink(levels)
  -> detach when the incident is over
```

Diagnostics are just branches from typed taps. They do not need a parallel
debug graph, and they can drain or abort with the same lifecycle rules as any
other runtime branch.

### Swap a branch at a real media boundary

```text
old branch keeps serving
replacement starts beside it
switch at media time
keep old branch if replacement fails
```

Use media-time rebranching when a preview, ladder rung, or diagnostics branch
needs to change without a visible gap. The replacement proves it can start
before the old branch is detached.

### Let transport tracks become normal recipe inputs

```text
WebRTC/RTP track appears
  -> OnStream rule matches it
  -> branch recipe attaches
  -> OnRemove chooses drain, abort, or plain detach
```

Transport-specific code stays at the edge. Once a track is in the task, it is
selected, tapped, branched, synchronized, recorded, or inspected with the same
grammar as file and generated sources.

### Expose live controls without exposing internals

```text
app command
  -> Task.Control
  -> Task.Attach
  -> Attachment.Rebranch
  -> Watch / Snapshot / Stats
```

A host can publish app-owned commands for bitrate, keyframes, branch attach,
or branch replacement while keeping callers on recipes, taps, branches, and
destinations.

## Why goav

Most media tools make the media graph powerful but distant: you build strings,
start a process, watch logs, and hope runtime state lines up with what your app
thinks is happening. goav takes the opposite bet. The recipe is ordinary Go
data, the planner can explain failures before work starts, and the running task
is still addressable by your application.

That matters when a production room needs one more recorder, a preview branch
must drop late frames without stalling archive output, or a support path needs
to attach a meter to a typed tap for ten seconds and then disappear cleanly.

| If you would normally reach for... | goav is the better fit when... |
|---|---|
| An FFmpeg command | you need typed validation, in-process events, or live controls. |
| A GStreamer graph | you want a smaller Go-first recipe language with explicit extension seams. |
| Pion glue code | RTP/WebRTC is only the ingress; you also need recording, transforms, muxing, and sinks. |
| A custom in-house media runner | you want lifecycle, errors, stats, controls, and tests without inventing them again. |

## Capability Matrix

| Area | In tree today | What that means in practice |
|---|---|---|
| Core runtime | Recipes, task lifecycle, events, watch, snapshots, stats, attach, detach, rebranch, controls | Build one validated task, then inspect and change it while it runs. |
| Live rooms | Dynamic stream rules, branch-local sync policies, detach outcomes, media-time rebranch | Record branches can stay steady while preview or diagnostics adapt. |
| Containers | IVF, Annex B, Matroska, WebM | Current packet-recording and RTP/WebRTC examples work; more formats plug in through `format`. |
| Codecs | Opus, VP8, VP9, AV1 encode/decode verticals; AAC-LC and H264 receive/decode active | Common realtime paths are present; some encode paths stay descriptor-only until backends are enabled. |
| Transforms | Resize, resample, and custom frame stages | Conversion happens only when the recipe allows it. |
| Transports | RTP and WebRTC in nested modules | Pion stays out of the root module and lives where transport code needs it. |
| Extension points | Sources, destinations, codecs, formats, filters, stages, joins, control hosts, tests | Your application can add capability without changing global process state. |

## Stability Matrix

goav is pre-v1, but the important surfaces are pinned so accidental API growth
fails tests.

| Surface | Tier | What that means |
|---|---|---|
| Recipe grammar, `Task`, structured errors, `plan`, `snapshot`, `lifecycle`, `shape`, `flow`, `av` vocabulary | Tier A | Stable enough to build against; changes are deliberate and test-pinned. |
| Provider, codec, format, filter, control-host, custom-stage, custom-join, and testing seams | Tier B | Extension points are documented and may grow as capabilities grow. |
| Expert graph and low-level components | Tier C | Supported for advanced composition, but not the first API to learn. |
| Performance claims | Evidence based | Allocation pins and benchmarks are real; broad tail-latency and soak claims are not made yet. |
| Release status | Pre-v1 | The remaining gate is the maintainer release decision and compatibility note; see [Roadmap](docs/ROADMAP.md). |

## Deep Dives

| Topic | Start here |
|---|---|
| Recipes, branches, and live-room patterns | [Use cases](docs/USE_CASES.md) |
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

# goav

[![CI](https://github.com/thesyncim/goav/actions/workflows/ci.yml/badge.svg)](https://github.com/thesyncim/goav/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/thesyncim/goav.svg)](https://pkg.go.dev/github.com/thesyncim/goav)
[![Go Version](https://img.shields.io/badge/go-1.26.4%2B-00ADD8)](go.mod)
[![License](https://img.shields.io/github/license/thesyncim/goav)](LICENSE)
[![Status](https://img.shields.io/badge/status-pre--v1-orange)](docs/ROADMAP.md)
[![Release Notes](https://img.shields.io/badge/release_notes-CHANGELOG-blue)](CHANGELOG.md)

Build media workflows that your Go application can validate, run, and change
while they are alive.

`goav` is a small recipe grammar over an explicit runtime. The root package is
for describing media work: input, stream selection, ordered operations, taps,
branches, destinations, flows, and task lifecycle. Bundled adapters live in
`goav/std`; live controls live in `goav/control`; observation helpers live in
`goav/inspect`.

Use it when media belongs inside a Go service and you need structured build
errors, in-process events, runtime branches, or app-owned sources and sinks.

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

The front-door vocabulary is intentionally small:

- **Input**: `FileInput`, `URIInput`, `Input(provider)`, or `Source`.
- **Stream**: `.Audio()`, `.Video()`, or a stream matcher.
- **Operation**: `.Decode()`, `.Copy()`, `.Resize()`, `.Resample()`, `.Do(stage)`, `.Encode(codec)`, `.Tap(...)`.
- **Tap**: `goav.FrameTap(...)` or `goav.PacketTap(...)` names a point for later branches.
- **Branch**: `Branches(goav.Branch(...))` fans one media point into several outcomes.
- **Destination**: `goav.File(...)`, `goav.Writer(...)`, `goav.Sink(...)`,
  `goav.Custom(...)`, or `goav.URI(...)`.
- **Flow**: `goav.Flow(...)` is reusable operation text with no source or destination.
- **Task**: `Run` and `Close`; richer live behavior is behind `LiveTask` and
  the opt-in capability interfaces.

Reuse the same destination value when several branches should feed one mux,
one sink group, or one transactional writer.

## Common Recipes

| I need to... | Recipe sketch | Go deeper |
|---|---|---|
| Record packets without decoding | `From(input).Copy().To(goav.File(...))` | [Use cases](docs/USE_CASES.md) |
| Record and preview from one stream | `.Branches(goav.Branch("archive").To(...), goav.Branch("preview").To(...))` | [Use cases](docs/USE_CASES.md) |
| Decode, filter, and encode again | `.Decode().Resize(...)` or `.Resample(...)`, then `.Encode(codec)` | [Operations](docs/OPERATIONS.md) |
| Attach diagnostics at runtime | `LiveTask.Attach(ctx, goav.Branch(...).From(goav.FrameTap(...)))` | [Control plane](docs/CONTROL_PLANE.md) |
| Add app-owned media | `goav.Source(...)`, `goav.Input(provider)`, `goav.Sink(...)` | [Extension cookbook](docs/EXTENSION_COOKBOOK.md) |

## Expandable Examples

<details>
<summary><strong>Live camera track: archive steadily, keep preview low-latency</strong></summary>

```go
cameraPackets := make(chan *av.Packet)
roomSync := goav.Sync("room", goav.SyncTolerance(20*time.Millisecond))
previewSync := goav.Sync("room", goav.SyncTolerance(20*time.Millisecond), goav.SyncDropLate())

roomCamera := goav.Source("room-camera",
    shape.Packet(av.MediaVideo, av.CodecVP8, shape.Realtime(true)),
    func(ctx context.Context, push goav.SourcePush) error {
        for {
            select {
            case <-ctx.Done():
                return ctx.Err()
            case packet, ok := <-cameraPackets:
                if !ok {
                    return push.EOS("room-camera")
                }
                if _, err := push.Packet(packet); err != nil {
                    return err
                }
            }
        }
    },
    goav.Codec(codec.VP8()),
)

previewTrack := goav.Sink(goav.SinkFunc("preview-track", func(context.Context, goav.Message) error {
    return nil
}))

_, err := goav.From(roomCamera).
    Video().
    Copy().
    Sync(roomSync).
    Branches(
        goav.Branch("archive").Sync(roomSync).To(goav.File("archive.ivf", out)),
        goav.Branch("preview").Sync(previewSync).To(previewTrack),
    ).
    Build(ctx)
return err
```

</details>

<details>
<summary><strong>Dynamic WebRTC/RTP tracks: attach branches as streams appear</strong></summary>

```go
roomPackets := make(chan *av.Packet)
camera := av.Stream{
    ID: "camera", Type: av.MediaVideo, TimeBase: av.RTPTimeBase(90000),
    Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo, ClockRate: 90000},
}

transport := goav.Source("webrtc-room",
    shape.Packet(av.MediaVideo, av.CodecVP8, shape.Realtime(true)),
    func(ctx context.Context, push goav.SourcePush) error {
        announced := camera
        if _, err := push.Event(av.Event{Type: av.EventStreamAdded, Stream: &announced}); err != nil {
            return err
        }
        for {
            select {
            case <-ctx.Done():
                return ctx.Err()
            case packet, ok := <-roomPackets:
                if !ok {
                    if _, err := push.Event(av.Event{Type: av.EventStreamRemoved, StreamID: camera.ID}); err != nil {
                        return err
                    }
                    return push.EOS(camera.ID)
                }
                packet.StreamID = camera.ID
                packet.Type = av.MediaVideo
                if _, err := push.Packet(packet); err != nil {
                    return err
                }
            }
        }
    },
    goav.Codec(codec.VP8()),
)

_, err := goav.From(transport).
    OnStream(
        goav.MatchStreamID("camera"),
        goav.Branch("record-camera").
            Sync(goav.Sync("room", goav.SyncTolerance(20*time.Millisecond))).
            Copy().
            To(goav.File("camera.ivf", out)),
        goav.OnRemove(goav.DrainBranch()),
    ).
    Build(ctx)
return err
```

</details>

## Why goav

goav keeps the recipe as Go data, validates it before resources open, and
returns a task your application can observe and mutate while it runs. Use the
root grammar first; import `std`, `control`, `inspect`, `provider`, `codec`,
`format`, `filter`, or `expert` only when the workflow needs that seam.

## Capability Matrix

| Area | In tree today | What that means |
|---|---|---|
| Core runtime | recipes, task lifecycle, events, watch, snapshots, stats, attach, detach, rebranch, controls | Build one validated task, then inspect or change it while it runs. |
| Extension points | sources, destinations, codecs, formats, filters, stages, joins, control hosts | Add capability per runtime, without global process state. |

## Stability Matrix

| Surface | Tier | What that means |
|---|---|---|
| Recipe grammar, `Task`, capability interfaces, `control`, `inspect`, `plan`, `snapshot`, `lifecycle`, `shape`, `flow`, `av` | Tier A | Deliberate and test-pinned, but still pre-v1. |
| Provider, codec, format, filter, control-host, custom-stage, custom-join, and testing seams | Tier B | Documented extension points that can grow with capability. |
| Expert graph and low-level components | Tier C | Supported for advanced composition, not the first API to learn. |

## Deep Dives

| Topic | Start here |
|---|---|
| Recipes, branches, and live-room patterns | [Use cases](docs/USE_CASES.md) |
| Operation rules and structured errors | [Operations](docs/OPERATIONS.md), [Errors](docs/ERRORS.md), [Error catalog](docs/ERROR_CATALOG.md) |
| Runtime control and socket hosts | [Control plane](docs/CONTROL_PLANE.md), [control-plane host example](examples/control-plane-host) |
| Extension authoring | [Extension cookbook](docs/EXTENSION_COOKBOOK.md), [Adapter authoring](docs/ADAPTER_AUTHORING.md), [Adapters](docs/ADAPTERS.md) |
| Copyable examples | [custom source](examples/custom-source), [provider source](examples/provider-source), [custom destination](examples/custom-destination), [custom filter](examples/custom-filter), [transactional writer](examples/transactional-writer), [custom codec](examples/custom-codec), [custom join](examples/custom-join) |
| Components and performance | [Components](docs/COMPONENTS.md), [Performance](docs/PERFORMANCE.md) |
| Release process | [Repository trust](docs/REPOSITORY_TRUST.md), [Releasing](docs/RELEASING.md), [Compatibility](docs/COMPATIBILITY.md), [V1 audit](docs/V1_CREDIBILITY_AUDIT.md), [Roadmap](docs/ROADMAP.md) |

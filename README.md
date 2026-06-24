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
| Add a codec, filter, muxer, or control host | `goav.MustNew(...)` with the relevant extension option | [Adapter authoring](docs/ADAPTER_AUTHORING.md) |

## Expandable Examples

Open the examples when you want to see the recipe shape. They stay on the same
grammar as the 30-second examples, but use live sources, runtime branches, taps,
and control boundaries.

<details>
<summary><strong>Live camera track: archive steadily, keep preview low-latency</strong></summary>

Feed packets from the transport edge into one task. The archive branch holds and
drains; the preview branch uses the same room timeline but can shed late packets
instead of making the recorder wait.

```go
cameraPackets := make(chan *av.Packet)
roomSync := goav.Sync("room", goav.SyncTolerance(20*time.Millisecond))
previewSync := goav.Sync("room",
    goav.SyncTolerance(20*time.Millisecond),
    goav.SyncDropLate(),
)
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
        goav.Branch("archive").
            Sync(roomSync).
            To(goav.File("archive.ivf", out)),
        goav.Branch("preview").
            Sync(previewSync).
            To(previewTrack),
    ).
    Build(ctx)
return err
```

Here `cameraPackets` is owned by your RTP/WebRTC edge and should carry
RTP-derived PTS values. goav only sees a typed live source. Run the
deterministic fixture in
[examples/dynamic-audio-room](examples/dynamic-audio-room), or the browser
demo in [examples/gio-webrtc-showcase](examples/gio-webrtc-showcase).

</details>

<details>
<summary><strong>Runtime diagnostics: attach a meter after the task starts</strong></summary>

Put a typed tap in the main recipe, then attach a support branch only when you
need it.

```go
levels := goav.Sink(goav.SinkFunc("levels", func(context.Context, goav.Message) error {
    return nil
}))

task, err := goav.From(goav.FileInput("input.ivf", in)).
    Video().
    Decode().
    Tap(goav.FrameTap("video.frames")).
    To(goav.Sink(goav.SinkFunc("archive", func(context.Context, goav.Message) error {
        return nil
    }))).
    Build(ctx)
if err != nil {
    return err
}
defer task.Close()

_, err = task.Attach(ctx, goav.Branch("support-meter").
    From(goav.FrameTap("video.frames")).
    To(levels))
return err
```

The diagnostic branch has its own lifecycle, so it can be watched, drained, or
aborted without rebuilding the archive path. The control-plane flow is covered
in [docs/CONTROL_PLANE.md](docs/CONTROL_PLANE.md).

</details>

<details>
<summary><strong>Media-time rebranch: replace work without a visible gap</strong></summary>

Open the replacement branch first, then switch at a media boundary. This example
uses a file as a repeatable source because the idea is branch rotation, not live
track discovery. If the replacement fails to attach, the old branch keeps
running.

```go
task, err := goav.From(goav.FileInput("input.ivf", in)).
    Video().
    Copy().
    Tap(goav.PacketTap("video.packets")).
    To(goav.File("live.ivf", out)).
    Build(ctx)
if err != nil {
    return err
}
defer task.Close()

rec, err := task.Attach(ctx, goav.Branch("rec").
    From(goav.PacketTap("video.packets")).
    Copy().
    To(goav.File("part-001.ivf", io.Discard)))
if err != nil {
    return err
}

_, err = rec.Rebranch(ctx,
    goav.Branch("rec").
        From(goav.PacketTap("video.packets")).
        Copy().
        To(goav.File("part-002.ivf", io.Discard)),
    goav.SwitchAt(goav.AtMediaTime(30*time.Second)),
    goav.KeepOldOnFailure(),
    goav.DrainOldBranch(),
)
return err
```

Use this for preview rungs, diagnostics, or recording rotation when a branch
needs to change shape while the task keeps running.

</details>

<details>
<summary><strong>Dynamic WebRTC/RTP tracks: attach branches as streams appear</strong></summary>

Transport code stays at the edge. It announces track lifecycle events and pushes
packets; the runtime rule attaches the same branch recipe whenever a matching
stream appears.

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
                if packet == nil {
                    continue
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

The source announces the track before media, keeps packet routing on the same
stream id, and removes the track before EOS. Late media still becomes a
validated branch of the same task. Start with
[docs/RTP_WEBRTC.md](docs/RTP_WEBRTC.md), then try
[examples/gio-webrtc-showcase](examples/gio-webrtc-showcase).

</details>

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

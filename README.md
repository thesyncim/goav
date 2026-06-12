# goav

`goav` is a realtime media runtime in pure Go. Describe the media work once —
streams, operations, branches, destinations — and compile it into an
inspectable, controllable graph. No cgo, no FFmpeg or GStreamer process:
`CGO_ENABLED=0` builds one static binary.

```go
return goav.From(input).
    Video().
    Decode().
    Resize(1280, 720).
    Encode(codec.VP9(codec.Bitrate(2_000_000))).
    To(goav.File("preview.ivf", preview)).
    Run(ctx)
```

Every job reads this way: `From(input)` starts the chain, `.Audio()`/`.Video()`
select streams, operations are chain methods, `.To(destination)` ends it. The
same grammar scales to
fanout, N-to-1 mixing, attaching to a running task, and live control — and
every chain is validated before any resource opens, so a refusal is a
structured error naming the failing operation and the exact fix.

```sh
go get github.com/thesyncim/goav
```

How goav relates to GStreamer, stated honestly, is
[docs/GSTREAMER_ALTERNATIVE.md](docs/GSTREAMER_ALTERNATIVE.md); stability
tiers and the road to v1 are [docs/ROADMAP.md](docs/ROADMAP.md).

## 30-Second Examples

Packet-preserving RTP/WebRTC record:

```go
return goav.From(goav.Input(rtpav.Receive(video, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8())))).
    Copy().
    To(goav.File("recording.ivf", file)).
    Run(ctx)
```

Packet-preserving file fanout:

```go
return goav.From(goav.FileInput("input.ivf", in)).
    Copy().
    To(
        goav.File("archive.ivf", archive),
        goav.File("preview.ivf", preview),
    ).
    Run(ctx)
```

Decode one WebRTC audio track to frames:

```go
return goav.From(goav.Input(webrtcav.Track(track))).
    Audio().
    Decode().
    To(goav.Sink(frames)).
    Run(ctx)
```

`goav.Default()` bundles the standard adapters: IVF, Annex B, Matroska, and WebM
containers; Opus, VP8, VP9, AV1, and H264 codec adapters; resize and resample
filters. Other containers need a registered adapter.

## Vocabulary

Seven nouns cover everything:

- `Input`: where media comes from — file, URI, custom source, or any transport
  provider via `goav.Input(...)` (`rtpav.Receive`, `webrtcav.Track`, or an
  external package).
- `Stream`: which media stream is selected (`.Audio()`, `.Video()`).
- `Tap`: a named attach point (`Tap`, or `FrameTap`/`PacketTap` to assert the domain).
- `Branch`: downstream operations from a stream point or tap.
- `Destination`: a file, URI, writer (including transactional object uploads),
  media sink, or shared mux/sink group.
- `Flow`: a reusable operation sequence.
- `Task`: a running graph with attach/detach, events, snapshots, and live control.

Operations are not a separate noun: they are methods on the chain —
`.Decode()`, `.Copy()`, `.Resize()`, `.Resample()`, `.Do(stage)`, `.Encode(codec)`.
Every near-miss name is a deliberate distinction — `Input` vs `Source` vs
`provider.Source`, `Events` vs `Watch` — and the full naming contract lives in
[docs/API_SURFACE.md](docs/API_SURFACE.md) alongside the pinned surface.

## Composition Patterns

### Branches And Destinations

Use branches when one selected stream should become multiple downstream
destinations. Destinations are typed values, so recipes never route by string
labels — and reusing one value is what groups branches into a shared mux or
sink group. Reuse the same destination value across branches — even across an
audio and a video chain — and they feed one container, the natural WebM shape:

```go
web := goav.File("web.webm", webFile)

return goav.From(goav.FileInput("source.webm", in)).
    Video().
    Decode().
    Branches(
        goav.Branch("v720").
            Resize(1280, 720).
            Encode(codec.VP9(codec.Bitrate(2_000_000))).
            To(web),
    ).
    Audio().
    Decode().
    Branches(
        goav.Branch("a96").
            Resample(48_000, codec.Stereo).
            Encode(codec.Opus(codec.Bitrate(96_000))).
            To(web),
    ).
    Run(ctx)
```

Distinct destination values keep branches independent — one decode fanning out
to several files, previews, or sinks. A branch ending in a sink stays in frame
domain — no encode required.

### Shape Errors

Every chain is validated operation by operation before any resource opens: an
encoder must consume frames, `Copy()` must consume packets, and resize/resample
must consume matching decoded media. File, URI, writer, and object destinations
consume packet-domain media; use `goav.Sink(...)` when a branch should end as
frames. A violation fails `Explain`, `Build`, and `Attach` alike with a
structured `BuildError` carrying the failing operation, the expected and actual
shapes, and concrete fixes — encode before a byte destination, copy from a
packet-domain point, or end in a sink.

That structure is the contract for EVERY goav error, not just shape refusals:
each `BuildError` carries a typed `errcode.Code` from the `errcode` catalog,
the failing operation and node, machine-readable details, and at least one
concrete fix when the refusal is user-fixable. Match codes with `errors.As`
and sentinels (`goav.ErrUnsupportedBuild`, ...) with `errors.Is` — see
[docs/ERRORS.md](docs/ERRORS.md).

Format mismatches a conversion would fix can be solved instead of refused:
`.Auto(...)` opts the chain into the shape solver with an explicit policy, and
`.Require(...)` asserts the resulting contract at a chosen point:

```go
return goav.From(mic).
    Audio().
    Auto(shape.AllowResample()).
    Require(shape.Frame(av.MediaAudio, shape.Audio(48_000, 2, ""))).
    Encode(codec.Opus(codec.Bitrate(96_000))).
    To(goav.File("voice.webm", out)).
    Run(ctx)
```

When a downstream operation pins format facts the current media does not
satisfy — the Opus encoder wants 48kHz but the mic is 44.1kHz — the planner
inserts the conversion as a real planned operation, visible in `Describe` and
reported by `Explain` (`inserted resample 44.1kHz→48kHz before encode-opus
(AllowResample)`). Nothing is inserted without a policy — `shape.AllowResample()`
permits sample-rate and channel conversion, `shape.AllowResize()` video
geometry, `shape.AllowConvert()` pixel/sample formats — and a needed conversion
the policy does not allow fails before any resource opens, naming the exact
policy to add. `.Require(...)` makes the contract explicit: the stream MUST
satisfy the given shape there, or the build fails with `shape_requirement_unmet`
carrying the actual and required shapes and the fix; it works on chains,
branches, and flows, and lowers to no runtime node. (`Shape(...)` merely
annotates the current media point; it is not an escape hatch around operation
contracts.)

`.Prefer(...)` is the soft twin: where the solver has a genuinely open choice,
the preference biases it — `.Prefer(shape.New(shape.Audio(0, 0, "f32")))`
steers an open sample format, `.Prefer(shape.New(shape.Realtime(true)))` picks
the realtime-capable adapter. A preference never fails a build: one that cannot
be honored is dropped, and `Explain` reports both outcomes
(`shape_preference_applied` / `shape_preference_ignored`).

### Typed Taps

Omit `From(...)` when every branch starts from the current stream point. Use a
typed tap when one branch should start from an earlier point:

```go
decoded := goav.FrameTap("video.decoded")
frames720p := goav.FrameTap("video.720p.frames")

thumbnail := goav.Sink(goav.SinkFunc("thumbnail", saveFrame))
web := goav.File("web.ivf", webFile)

return goav.From(input).
    Video().
    Decode().
    Tap(decoded).
    Resize(1280, 720).
    Tap(frames720p).
    Branches(
        goav.Branch("thumbnail").
            From(decoded).
            Resize(320, 180).
            To(thumbnail),
        goav.Branch("web").
            From(frames720p).
            Encode(codec.VP9(codec.Bitrate(2_000_000))).
            To(web),
    ).
    Run(ctx)
```

`goav.Tap` infers its domain from the chain point — frame before `.Encode(...)`
or `.Copy()`, packet after — while `goav.FrameTap`/`goav.PacketTap` assert it.
A domain mismatch is a build error that names the typed constructor to use.

### Branch Buffers And Ownership

Branch buffers are branch-local. Use blocking when a branch must preserve every
message, and dropping modes for realtime previews or diagnostics:

```go
return goav.From(input).
    Video().
    Decode().
    Branches(
        goav.Branch("archive").
            Buffer(flow.Blocking(128)).
            Encode(codec.VP9(codec.Bitrate(4_000_000))).
            To(archive),
        goav.Branch("preview").
            Buffer(flow.DropOldest(3)).
            Resize(640, 360).
            To(preview),
        goav.Branch("latest").
            Buffer(flow.Latest()).
            To(goav.Sink(goav.SinkFunc("latest", inspect))),
    ).
    Run(ctx)
```

Buffered branches own what they queue. The default copy mode,
`flow.CopyIfMutable`, copies every payload not declared immutable into
branch-owned slots, so a branch that mutates its delivered frames can never
corrupt a sibling branch or the producer. `flow.BufferCopyMode(flow.CopyAlways)`
defensively copies even immutable payloads; `flow.CopyNever` shares by reference
and refuses mutable payloads instead of silently sharing them.

## Runtime Attach

Build a task when the application needs inspection, events, or late attachment.
Place taps where future work may attach.

```go
frames720p := goav.FrameTap("video.720p.frames")
web := goav.File("web.ivf", webFile)

task, err := goav.From(input).
    Video().
    Decode().
    Branches(
        goav.Branch("720p").
            Resize(1280, 720).
            Tap(frames720p).
            Encode(codec.VP9(codec.Bitrate(2_000_000))).
            To(web),
    ).
    Build(ctx)
go func() { _ = task.Run(ctx) }()

shots, err := task.Attach(ctx,
    goav.Branch("screenshots").
        From(frames720p).
        Resize(320, 180).
        To(goav.Sink(goav.SinkFunc("screenshots", collectScreenshot))),
)
// ... later:
return task.Detach(ctx, shots)
```

Late branches can encode from frame taps, copy or decode from packet taps, and
publish their own taps for later attachments:

```go
audioEncoded := goav.PacketTap("audio.encoded")
videoEncoded := goav.PacketTap("video.encoded")
recording := goav.File("recording.webm", file)

group, err := task.Attach(ctx,
    goav.Branch("audio").From(audioEncoded).Copy().To(recording),
    goav.Branch("video").From(videoEncoded).Copy().To(recording),
)
```

`Task.Taps()` lists available attach points. One `Attach` call adds several
runtime branches atomically: later branches in the call can use taps published
by earlier ones, branches can share one destination value (one late recording
or diagnostic group), and a failure rolls the whole group back. Detaching a
parent also removes dependent branches anchored from its taps.

### Live Rebranch

`Rebranch` replaces a live branch without a delivery gap: the replacements
attach and start receiving before the old branch detaches. Switch policies gate
the handover to a stream boundary:

```go
rec, err := task.Attach(ctx,
    goav.Branch("rec").From(videoEncoded).Copy().To(goav.File("part-001.ivf", first)),
)
// ... later, rotate the recording at a clean decode point:
rec2, err := rec.Rebranch(ctx,
    goav.Branch("rec").From(videoEncoded).Copy().To(goav.File("part-002.ivf", second)),
    goav.SwitchAt(goav.NextKeyframe()),
    goav.DrainOldBranch(),
)
```

Until the next keyframe the old branch keeps delivering and the replacement
sheds media (events still pass), so the new destination starts at a decodable
sync point and the old branch detaches at that exact boundary. `DrainOldBranch`
and `AbortOldBranch` choose whether the replaced branch's destinations commit
or abort; on attach failure the old branch stays intact (`KeepOldOnFailure`
spells the default). `Pause`/`Resume` stop and restore delivery to one branch
without touching the source or its siblings.

### Dynamic Stream Discovery

A source that discovers a stream mid-run announces it with
`av.EventStreamAdded` carrying the full `av.Stream` (custom sources just push
the event; RTP/WebRTC receivers forward it). `OnStream` declares the reaction
in the recipe: when a matching stream appears, the branches attach to it at
runtime through the same planner, atomic apply, and rollback as `Task.Attach`,
with no source rebuild:

```go
task, err := goav.From(input).
    OnStream(goav.MatchMedia(av.MediaAudio),
        goav.Branch("record").Copy().To(archive),
    ).
    Audio().Copy().To(live).
    Build(ctx)
```

`MatchMedia`, `MatchCodec`, `MatchStreamID`, and `MatchStream(fn)` select
streams; several rules may match one stream and each attaches independently,
with branch names templated per matched stream (`record-<stream id>`). On
`av.EventStreamRemoved` the rule's branches detach with drain semantics — their
destinations commit — while the task keeps running. Rules appear in `Explain`
as `stream_rule` decisions before any stream exists, and `.Auto(shape.Allow...)`
policies on rule branches solve conversions at attach time. Failures are never
silent: a late branch that cannot attach rolls back fully and surfaces
`av.EventAttachError` on `Watch`/`Events`; a discovered stream matching no rule
just surfaces its event.

## Flows

When operations repeat, extract a reusable flow. A flow owns only operations; a
branch owns the destination.

```go
voice := goav.Flow("voice").Audio().
    Resample(16_000, codec.Mono).
    Encode(codec.Opus(codec.Bitrate(32_000), codec.Channels(codec.Mono)))

archive := goav.Flow("archive").Audio().
    Resample(48_000, codec.Stereo).
    Encode(codec.Opus(codec.Bitrate(128_000), codec.Channels(codec.Stereo)))

voiceOut := goav.File("voice.ogg", voiceFile)
archiveOut := goav.File("archive.ogg", archiveFile)

return goav.From(goav.Input(webrtcav.Track(audio))).
    Audio().
    Decode().
    Branches(
        goav.Branch("voice").Apply(voice).To(voiceOut),
        goav.Branch("archive").Apply(archive).To(archiveOut),
    ).
    Run(ctx)
```

Use a direct stream when one reusable flow feeds one destination
(`.Audio().Apply(voice).To(voiceOut)`); branch when the same media point needs
several downstream operation sequences. Flows also apply to runtime branches
attached from taps.

## Custom Components

Small hooks should not require implementing the full graph interfaces. Use
`PacketFunc`, `FrameFunc`, `EventFunc`, and `SinkFunc` for metering, analysis,
preview, stats, and integration points:

```go
meter := goav.FrameFunc("meter", func(ctx context.Context, frame *goav.Frame, emit goav.Emit) error {
    observe(frame)
    return emit.Frame(frame)
})

return goav.From(input).
    Audio().
    Decode().
    Do(meter).
    To(goav.Sink(levels)).
    Run(ctx)
```

Use `Source` when your application already owns media production. Declare the
shape (`shape.Packet`, `shape.Frame`, or `shape.Event`) and push through
`goav.SourcePush`:

```go
input := goav.Source("generated",
    shape.Packet(av.MediaAudio, av.CodecOpus,
        shape.Format(av.FormatRTP),
        shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
    ),
    func(ctx context.Context, push goav.SourcePush) error {
        for packet := range packets {
            if _, err := push.Packet(&packet); err != nil {
                return err
            }
        }
        return push.EOS()
    },
)

return goav.From(input).
    Audio().
    Copy().
    To(goav.Sink(packetSink)).
    Run(ctx)
```

For external adapters, `shape.Format(av.FormatID("vendor.format"))` is the
same open-string contract as custom codec ids: declare it when the source or
provider knows the packet/container framing and downstream validation should
see it.

Every push returns a `PushResult`: `Accepted` means a downstream target queued
the message, `Dropped` means a dropping buffer policy deliberately shed it —
normal realtime behavior with a nil error, so every push is accounted for; the
error keeps its meaning, flow control (`ErrBackpressure`) or fatal. Frame
sources use `shape.Frame` and `push.Frame(...)` and skip decode; event-only
sources use `shape.Event()` and `push.Event(...)`, routing directly to sinks.
Custom sources participate in the same stream, branch, destination, explain,
and runtime graph path as built-in inputs.

## Source Providers

Transports are not special-cased. `rtpav.Receive` and `webrtcav.Track` are
implementations of the one source seam, and an SRT, NDI, or proprietary ingest
package plugs in the same way — with zero goav changes. RTP and WebRTC ship
as nested modules (`github.com/thesyncim/goav/rtpav`, `.../webrtcav` — import
paths unchanged) so the core module stays free of third-party dependencies:

```go
package provider

type Source interface {
    // OpenSource opens the running source and resolves the streams it carries.
    OpenSource(ctx context.Context) (pipeline.Source, []av.Stream, error)
    // SourceShape declares the media facts the planner needs before opening.
    SourceShape() shape.Spec
}
```

The provider owns the transport vocabulary — readers, jitter buffers,
depacketizers, adaptation — and `goav.Input(provider)` turns it into a recipe
input whose declared shape lets the planner select streams and pick decoders
before the source ever opens. Optional capabilities (node name, detail,
per-stream decode bounds) are discovered by type assertion.

## Custom Destinations

One decision path covers every byte destination:

- `goav.File(name, writer)` — you already hold an open `io.Writer` (closed
  exactly once iff it implements `io.Closer`).
- `goav.URI(uri)` — a registered format adapter opens the destination.
- `goav.Writer(name, open)` — goav opens the writer on demand, after format
  and streams are selected, so uploaders see the final destination metadata
  (`provider.Info`); it closes exactly once. Return a
  `provider.TransactionalWriter` when the upload has an explicit commit
  boundary (a multipart object-store upload): it commits after successful runs
  or drained detach and aborts on failure.
- `goav.Custom(name, provider)` — a package owns a reusable destination
  provider (naming, contract, opening); the returned destination value is
  still the stable routing handle.

(`goav.Sink(...)` is the separate door for decoded frames or packets rather
than muxed bytes.)

Normal application workflows should be expressible through declarative recipes;
these constructors exist so any store or transport plugs in without leaving
them.

```go
s3 := goav.Writer("s3://bucket/call.ivf",
    func(ctx context.Context, info provider.Info) (io.WriteCloser, error) {
        // The returned writer implements provider.TransactionalWriter,
        // so the upload commits on success and aborts on failure.
        return uploader.Create(ctx, info.Name,
            uploader.ContentType(info.MIMEType),
            uploader.Metadata(info.Metadata),
        )
    },
    goav.Format(av.FormatIVF),
    goav.MIME("video/ivf"),
    goav.Metadata(av.Metadata{"kind": "call-recording"}),
)

return goav.From(input).
    Video().
    Copy().
    To(s3).
    Run(ctx)
```

Reuse one destination value when multiple branches should feed one mux or sink
group. The same destination option style works for built-in destinations —
`goav.File("", out, goav.Format(av.FormatIVF))` pins the container when there
is no name to probe.

## Multi-Input

`From` is variadic. Several inputs feed one job, each chain narrows to its
input with `goav.InputName(...)`, and reusing one destination value muxes both
encoded streams into one shared container:

```go
camera := goav.Input(webrtcav.Track(videoTrack, rtpav.WithName("camera")))
mic := goav.Input(webrtcav.Track(audioTrack, rtpav.WithName("mic")))
out := goav.File("call.webm", file)

return goav.From(camera, mic).
    Video(goav.InputName("camera")).Decode().Encode(codec.VP9(codec.Bitrate(1_000_000))).To(out).
    Audio(goav.InputName("mic")).Decode().Encode(codec.Opus(codec.Bitrate(96_000))).To(out).
    Run(ctx)
```

When a selector is ambiguous, the build error lists candidates and suggests
the narrowing to use. Two selection vocabularies, two tenses: selector options
(`goav.InputName`, `goav.StreamID`, `goav.StreamName`, `goav.StreamIndex`)
narrow streams the inputs already have at build time; match predicates
(`MatchMedia`, `MatchCodec`, `MatchStreamID`, `MatchStream(fn)`) describe
streams that may not exist yet, under
[Dynamic Stream Discovery](#dynamic-stream-discovery).

## Mix, Composite, Select

Convergence is the dual of `Branches`: N source chains join into one stream.
`Mix` sums audio arms, `Composite` paints video arms onto a canvas at
`.Region(x, y)` offsets, and `Select` forwards exactly one live arm.

```go
return goav.Mix(
    goav.From(mic1).Audio(),
    goav.From(mic2).Audio(),
).Encode(codec.Opus(codec.Bitrate(96_000))).
    To(goav.File("mix.webm", out)).
    Run(ctx)
```

Packet arms decode automatically before the join; mismatched audio arms
resample to the first arm's format with zero opt-in (the join's implicit arm
policy). The join output is a normal stream point: it takes `.Tap(...)`,
`.Branches(...)`, and `.Encode(...).To(...)` like any chain.

`Select` switches live through the control plane — no node names:

```go
task, err := goav.Select(
    goav.From(cam1).Video(),
    goav.From(cam2).Video(),
).To(preview).Build(ctx)
// ... while running:
err = task.Control(ctx, goav.SelectActive("cam2"))
```

Arm chains keep their declared `.Decode()` and `.Tap(...)` — the tap installs
on the task mid-graph, so one decode feeds the join AND any other consumer
(a runtime branch attached from the tap monitors the pre-mix media). A tap is
also a join arm: a `TapRef` converges an already-flowing point again — the
join-side dual of `Branch().From(tap)` — re-stamped under the tap name, with
no source re-opened. Paint the same decoded camera at two `Composite` canvas
regions:

```go
return goav.Composite(
    goav.From(cam).Video().Decode().Tap(goav.FrameTap("cam")).Region(0, 0),
    goav.FrameTap("cam").Region(640, 0), // the SAME decoded frames, decoded once
).To(goav.Sink(preview)).Run(ctx)
```

A tap arm must follow the arm that declares its tap; an unknown tap ref fails
the build with the declared taps listed.

Joins nest — a join is an arm like any source chain: `Mix(Mix(a, b), c)`
sub-mixes two microphones and mixes the result with a third, and
`Select(Mix(a, b), Mix(c, d))` switches between two live mixes. A nested
join's output id is its arm id (`mix`, `mix-2`), its output is converted to
the outer target like any arm, and mix clamping applies at each stage.

Arms pair by arrival order by default — right for live sources on one clock.
`Mix(...).SyncByPTS()` aligns them by timestamp instead (files starting at
different offsets, a `Seek` on one arm, drift): the earliest head frame sets
each step, arms whose head is newer sit the step out, and stale frames are
dropped to catch up. Catch-up drops are never silent: they surface on the join
node's counters — `task.Stats().Nodes["mix"].Dropped` under the `"sync"`
reason, and in `task.Snapshot()`.

## Testing Your Pipeline

`goavtest` is httptest for pipelines: pure sources and recorders with no
`*testing.T` coupling and no assertion DSL — every helper returns a real
grammar value, so test code is pipeline code.

```go
out := goavtest.NewCollector()
task, _ := goav.Mix(
    goav.From(goavtest.Audio(48000, 1, []int16{100}, []int16{200})).Audio(),
    goav.From(goavtest.Audio(48000, 1, []int16{50}, []int16{-50})).Audio(),
).To(out.Sink()).UseRuntime(goavtest.Runtime()).Build(ctx)
_ = task.Run(ctx)
// out.S16() == [[150] [150]]
```

`Audio`, `Video`, and `Packets` are deterministic PTS-stamped inputs;
`LiveAudio` never ends, for control-plane tests (`SelectActive`, `Rebranch`)
paired with the collector's `Wait(ctx, cond)`. `goavtest.Runtime()` is the
deterministic runtime: standard filters, a byte-faithful passthrough codec for
every well-known codec id, a fake container for every well-known format id,
and a fake clock (`goavtest.NewClock`) so realtime pacing records its sleeps
instead of sleeping. `Codec(id)`/`Format(id)` register the fakes individually,
and extra options are last-wins overrides.

Use `NewTestSource` when you need a provider-shaped fixture that records source
controls and can emit custom frames, packets, events, or a mixed script:

```go
packet := &av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
source := goavtest.NewTestSource("fixture",
    shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, 1, av.SampleFormatS16)),
    goavtest.TestSourceLive(),
    goavtest.TestSourceScript(
        goavtest.TestSourcePacket(packet),
        goavtest.TestSourceEvent(av.Event{Type: av.EventStats, Reason: "ready"}),
    ),
)
task, _ := goav.From(source.Input()).Audio().Copy().
    To(goavtest.NewCollector().Sink()).
    UseRuntime(goavtest.Runtime()).Build(ctx)
_ = task.Control(ctx, goav.Rate(0.5).At("fixture"))
event, _ := source.WaitControl(ctx, av.EventRate)
```

The bootstrap examples are compiled as docs, so copy-paste breakage fails in
CI: `ExampleSource_pushAccounting` shows `SourcePush` delivery accounting,
`ExampleWriter_transactionalUpload` shows a `goav.Writer` upload with
`provider.Info` and `provider.TransactionalWriter`,
`ExampleWithEncoder_customSettings` shows typed codec settings plus
`codec.Control` for native encoder options, and `ExampleTask_flowchart`
renders a live task with an attached branch through
`graphrender.RenderTaskFlowchart(task)`. `ExampleTestSourceScript` shows a
provider-shaped source fixture with mixed frame/event scripting.

## Debug And Diagnostics

Debugging is ordinary composition. Put a typed tap at the point you want to
observe, call `Explain(ctx)` before opening resources, then attach a live branch
while the task is running.

```go
decoded := goav.FrameTap("audio.decoded")

job := goav.From(goav.Input(webrtcav.Track(audio))).
    Audio().
    Decode().
    Tap(decoded).
    To(goav.Sink(playback))

report, err := job.Explain(ctx) // plan, decisions, warnings — nothing opened yet
for _, warning := range report.Warnings {
    log.Printf("goav plan warning code=%s node=%s msg=%s",
        warning.Code, warning.Node, warning.Message)
}

task, err := job.Build(ctx)
defer task.Close()
go func() { _ = task.Run(ctx) }()

levels, err := task.Attach(ctx,
    goav.Branch("levels").
        From(decoded).
        Do(goav.FrameFunc("rms", func(_ context.Context, frame *goav.Frame, emit goav.Emit) error {
            observeRMS(frame)
            return emit.Frame(frame)
        })).
        To(goav.Sink(goav.SinkFunc("levels", func(context.Context, goav.Message) error { return nil }))),
)
defer levels.Close(ctx)

state := task.Snapshot()
for _, branch := range state.Branches {
    if branch.State == lifecycle.BranchAttached && branch.Name == "levels" {
        log.Printf("goav levels frames=%d", branch.Stats.Frames)
    }
}
```

`Task.Snapshot()` returns one point-in-time view with typed lifecycle states
(`lifecycle.TaskState`, `lifecycle.BranchState`, `lifecycle.DestinationState`),
graph stats, stable taps, and active runtime branches. `Attachment.Snapshot()`
reports the branch-owned view. This works the same for video probes, screenshot
collectors, packet loss diagnostics, late recordings, and temporary previews.

`task.Events()` is the single firehose; `task.Watch(filters...)` gives each
consumer an independent, filtered subscription instead:

```go
eos := task.Watch(goav.WatchTypes(av.EventEndOfStream))
loss := task.Watch(goav.WatchTypes(av.EventPacketLoss), goav.WatchStream("video"))
```

Filters AND together. Every watcher owns a buffered channel, so a slow consumer
sheds events for itself only — the data plane and other watchers never block on
it — and watcher channels close when the task closes. Once `Watch` is in use,
subscribe every consumer through it (an unfiltered `task.Watch()` is the
`Events` equivalent).

Joins plan the same way as chains: `Describe()` of a `Mix`/`Composite`/`Select`
job shows every arm converging into the join node — including auto-inserted
per-arm decode and resample stages — and the planned spec equals the built
graph node for node. `Describe()` returns the structured graph spec; rendering
lives outside core (`graphrender.RenderURI(spec, "goav:graph")` for text, DOT,
or Mermaid). Running tasks can render the current snapshot directly:
`graphrender.RenderTaskFlowchart(task)`. Captured views use the same renderer:
`graphrender.RenderSnapshotFlowchart(task.Snapshot())` for a full task and
`graphrender.RenderBranchFlowchart(attachment.Snapshot())` for one runtime
branch.

Applications that want a CLI control surface expose a Unix socket with package
`ctl`; the bundled command then drives the same task APIs over structured JSON.
The socket can host built-in controls plus explicit app-owned commands, custom
branch-pipeline steps, runtime-registered custom codec names, and optional
custom encoder spellings for native settings:

```sh
goav ctl --control unix:///tmp/goav-live.sock control bitrate stream=video value=1200k
goav ctl --control unix:///tmp/goav-live.sock control --json '{"type":"rate","rate":0.75,"node":"fixture"}'
goav ctl --control unix:///tmp/goav-live.sock help attach
goav ctl --control unix:///tmp/goav-live.sock attach frames as archive \
  'encode codec=x_acme_audio media=audio bitrate=128k profile=voice ! filesink location=/tmp/archive.ogg format=ogg'
goav ctl --control unix:///tmp/goav-live.sock attach frames as native \
  'meter ! acmeenc bitrate=128000 quality=voice lookahead=deep ! filesink location=/tmp/archive.ogg format=ogg'
goav ctl --control unix:///tmp/goav-live.sock graph
```

`help attach` and `help rebranch` list the built-in branch grammar, encoders
discovered from the task runtime, plus custom steps and encoder spellings
registered by that host. The bootstrap guide, including
`go run ./examples/control-plane-host`, lives in
[`docs/CONTROL_PLANE.md`](docs/CONTROL_PLANE.md). That playground uses a live
`goavtest.TestSource` so `goav ctl control rate/seek/segment source=fixture`,
raw JSON control/event fallback, `goav ctl control fixture.controls`, transcode
branches, thumbnails, generic custom runtime encoders, native custom encoder
settings, graph rendering, rebranch, and detach all work from one local socket.

## Live Control

`task.Control(ctx, ...)` drives a running task without naming graph nodes.
Untargeted controls enter at the source boundary and ride the data path, so
they reach every relevant node the same way the media does:

```go
// Every live encoder for the stream produces a keyframe.
err := task.Control(ctx, goav.Keyframe("video"))

// Live encoders retarget mid-stream (libvpx, libopus apply it from the next
// frame); a backend that cannot reports an error instead of ignoring it.
err = task.Control(ctx, goav.SetBitrate("video", 900_000))

err = task.Control(ctx, goav.SelectActive("cam2")) // a running Select switches arms
err = task.Control(ctx, goav.Seek(30*time.Second)) // sources reposition
err = task.Control(ctx, goav.Rate(2.0))            // pace change (positive rates; pure pacing)

// Sources play one [start, end) window, then end naturally — trim-to-file
// segment export: the destination commits when the window completes.
err = task.Control(ctx, goav.Segment(10*time.Second, 20*time.Second))
```

The encoder-path controls (`Keyframe`, `SetBitrate`, `SelectActive`) ride
per-node queues. Realtime recipes that decode or encode build those queues by
default, as `Select` already does, so the common live-control path works without
an explicit buffer policy. Offline recipes (`WithRealtime(false)`) keep the
direct runner for transcode speed unless the runtime opts in with
`goav.WithBufferPolicy(...)`; on a direct graph these controls return
`goav.ErrControlUnsupported` instead of disappearing. Buffered recipe graphs
derive their packet/frame copy bounds from planned stream shapes, and fail
`Build` with a structured `buffer_budget_missing` error when a required shape
fact is unsupported. The time-axis controls reach sources directly and work on
every graph.

`goav.Seek`, `goav.Rate`, and `goav.Segment` are the time-axis controls; all
three broadcast to every source, and a source implementing
`pipeline.ControllableSource` honours them. A seek emits `av.EventDiscontinuity`
before the first message at the new position — the signal decoders already
reset on; a segment ends the stream exactly as at the end of the media, so
destinations finalize naturally; a source that cannot honour a control reports
a per-source error without stopping a capable sibling. File inputs honour Seek
and Segment when the container demuxer implements `format.Seeker` — the
Matroska and WebM demuxers do, repositioning through Cues to the keyframe at
or before the target; a reader that cannot seek reports `format.ErrNotSeekable`.

Realtime tasks (the default) pace file playback on a clock — each packet is
delivered when its media time is due — so Rate works on files as a live pacing
multiplier and composes with Seek/Segment; offline tasks (`WithRealtime(false)`)
pump at full speed and reject Rate with `format.ErrRateUnsupported`. The clock
is injectable per runtime (`goav.WithClock`, default monotonic), so tests never
sleep for real.

`.AtTap(name)` narrows any control to one tap's point in the graph —
`goav.Keyframe("video").AtTap("video.720p.frames")` — and `goav.Deliver(event)`
hands a verbatim event to a stage that interprets it itself. A node-targeted
form exists for expert graphs only.

## Runtimes And Custom Codecs

Registries are per-runtime — there are no global registries. `goav.Default(opts...)`
builds a runtime with the standard codecs, formats, and filters registered, then
applies your options on top; registration is last-wins, so one call can both add
new implementations and override a default. `goav.New(opts...)` starts bare.
Direct value registration covers every family: `WithDecoder`, `WithEncoder`,
`WithFilter`, `WithMuxer`, `WithDemuxer`, and `WithProber`.

Custom codecs use the same recipe grammar as built-ins: register factories, then
reference them with generic `Codec` specs. Codec descriptors drive capability
checks, so incompatible media fails before allocation or graph mutation. Adapter
authoring details live in [`docs/ADAPTERS.md`](docs/ADAPTERS.md).

Opus, VP8, and VP9 are the full encode/decode recipe verticals. H264 and AV1
receive/decode paths are active while recipe encode remains guarded as work in
progress. `Shape(...)` describes structural media compatibility only; encoder
behavior is a two-tier ladder, and the settings live in the `codec` package (not
the goav root) so the grammar stays small. Tier 1 is the common typed settings
(`codec.Bitrate`, `codec.FPS`, `codec.KeyframeInterval`, `codec.Profile`,
`codec.RateControl`, …). Tier 2 is `codec.Control`: a single raw callback handed
the adapter's concrete encoder/decoder, so you type-assert and apply anything the
library exposes — nothing is ever unreachable, and there is no separate config
blob to learn:

```go
// codec is github.com/thesyncim/goav/codec; govpx is github.com/thesyncim/govpx
vp9 := codec.VP9(
    codec.Bitrate(2_000_000),
    codec.FPS(30),
    codec.KeyframeInterval(60),
    codec.Profile("0"),
    codec.Control(func(enc any) error {      // raw escape hatch
        if e, ok := enc.(*govpx.VP9Encoder); ok {
            return e.SetCQLevel(20)          // any native libvpx control
        }
        return nil
    }),
)
```

Each adapter documents the concrete encoder/decoder type it hands `Control`; the
public grammar stays Input, Stream, Tap, Branch, Destination, Flow, and Task —
operations are methods on the chain, not a separate vocabulary.
The reusable component catalog and allocation proof map live in
[`docs/COMPONENTS.md`](docs/COMPONENTS.md).

## Status

goav is pre-v1. What is stable, experimental, deliberately deferred, and what
gates the v1 tag live in [docs/ROADMAP.md](docs/ROADMAP.md); the design spec
and evidence scoreboard are [docs/NORTH_STAR.md](docs/NORTH_STAR.md).
Performance claims follow one rule: benchmarks beat claims — what is proven by
allocation pins, what the benchmark suite measures, and what is explicitly
*not* proven live in [`docs/PERFORMANCE.md`](docs/PERFORMANCE.md).

Advanced notes live in `docs/`. An expert graph layer exists beneath the grammar
for compositions the recipe language cannot express; normal work never needs it.

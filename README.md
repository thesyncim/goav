# goav

`goav` is a pure-Go realtime media runtime. The public contract is small:

```text
describe the media work once, then compile it into an inspectable graph
```

The front door is `From(input)`. Simple jobs stay simple; complex jobs are built
from the same few concepts — and a running task stays open to inspection, late
attachment, and live control.

## Vocabulary

- `Input`: where media comes from (file, URI, custom source, or any source
  provider via `goav.Input(...)` — `rtpav.Receive` for RTP, `webrtcav.Track`
  for WebRTC, or an external transport package).
- `Stream`: which media stream is selected (`.Audio()`, `.Video()`).
- `Tap`: a named attach point (`Tap`, or `FrameTap`/`PacketTap` to assert the domain).
- `Branch`: downstream operations from a stream point or tap.
- `Destination`: a file, URI, writer, object upload, media sink, or shared mux/sink group.
- `Flow`: a reusable operation sequence.
- `Task`: a running graph with attach/detach, events, snapshots, and live control.

Operations are not a separate noun: they are methods on the chain —
`.Decode()`, `.Copy()`, `.Resize()`, `.Resample()`, `.Do(stage)`, `.Encode(codec)`.

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

Reuse the same destination value when several branches should feed one mux or
sink group.

Decode one WebRTC audio track to frames:

```go
return goav.From(goav.Input(webrtcav.Track(track))).
    Audio().
    Decode().
    To(goav.Sink(frames)).
    Run(ctx)
```

Resize and encode one video stream:

```go
return goav.From(input).
    Video().
    Decode().
    Resize(1280, 720).
    Encode(codec.VP9(codec.Bitrate(2_000_000))).
    To(goav.File("preview.ivf", preview)).
    Run(ctx)
```

`goav.Default()` bundles the standard adapters: IVF, Annex B, Matroska, and WebM
containers; Opus, VP8, VP9, AV1, and H264 codec adapters; resize and resample
filters. Other containers need a registered adapter.

## Composition Patterns

### Branches And Destinations

Use branches when one selected stream should become multiple downstream
destinations. Destinations are typed values, so normal recipes do not route by
string labels. Reusing one destination value creates a mux group or sink group.

```go
decoded := goav.Tap("video.decoded") // domain inferred; FrameTap/PacketTap assert it

archive := goav.File("archive.ivf", archiveFile)
preview := goav.File("preview.ivf", previewFile)

return goav.From(input).
    Video().
    Decode().
    Tap(decoded).
    Branches(
        goav.Branch("archive").
            Resize(1920, 1080).
            Encode(codec.VP9(codec.Bitrate(4_000_000))).
            To(archive),
        goav.Branch("preview").
            Resize(640, 360).
            Do(frameMeter).
            Encode(codec.VP8(codec.Bitrate(600_000))).
            To(preview),
    ).
    Run(ctx)
```

Several branches sharing one destination form a mux group — the natural WebM
shape:

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

A branch ending in a sink stays in frame domain — no encode required.

### Shape Errors

Every chain is validated operation by operation before any resource opens: an
encoder must consume frames, `Copy()` must consume packets, and resize/resample
must consume matching decoded media. File, URI, writer, and object destinations
consume packet-domain media; use `goav.Sink(...)` when a branch should end as
frames. A violation fails `Explain`, `Build`, and `Attach` alike with a
structured `BuildError` carrying the failing operation, the expected and actual
shapes, and concrete fixes — encode before a byte destination, copy from a
packet-domain point, or end in a sink. `Shape(...)` annotates the current media
point; it is not an escape hatch around operation contracts.

Format mismatches a conversion would fix can be solved instead of refused:
`.Auto(...)` opts the chain into the shape solver with an explicit policy. When
a downstream operation pins format facts the current media does not satisfy —
an Opus encoder wants 48kHz but the source is 44.1kHz — the planner inserts the
matching conversion from the runtime's filter registry as a real planned
operation, visible in `Describe` and reported by `Explain` as a diagnostic such
as `inserted resample 44.1kHz→48kHz before encode-opus (AllowResample)`:

```go
return goav.From(mic).
    Audio().
    Auto(shape.AllowResample()).
    Encode(codec.Opus(codec.Bitrate(96_000))).
    To(goav.File("voice.webm", out)).
    Run(ctx)
```

Nothing is inserted without a policy: `shape.AllowResample()` permits
sample-rate and channel conversion, `shape.AllowResize()` video geometry, and
`shape.AllowConvert()` pixel/sample formats. A needed conversion the policy
does not allow fails before any resource opens with the exact policy to add,
and join arms (Mix) keep their implicit always-on arm policy — mismatched arms
auto-resample with zero opt-in.

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
if err != nil {
    return err
}

go func() { _ = task.Run(ctx) }()

shots, err := task.Attach(ctx,
    goav.Branch("screenshots").
        From(frames720p).
        Resize(320, 180).
        To(goav.Sink(goav.SinkFunc("screenshots", collectScreenshot))),
)
if err != nil {
    return err
}
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
runtime branches atomically; later branches in the call can use taps published
by earlier ones, and branches can share one typed sink or mux destination value.
Reuse the same destination value inside a grouped attach to form one late
recording or diagnostic group. A grouped attach rolls back fully if any branch
fails; detaching a parent also removes dependent branches anchored from its
taps.

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
or abort on detach; when attaching the replacements fails, the old branch stays
intact (`KeepOldOnFailure` spells the default). `Pause`/`Resume` stop and
restore delivery to one branch without touching the source or its siblings.

### Dynamic Stream Discovery

A source that discovers a stream mid-run announces it with
`av.EventStreamAdded` carrying the full `av.Stream` on the event's typed
`Stream` field (custom sources just push the event; RTP/WebRTC receivers
forward it from their receiver). `OnStream` declares the reaction in the
recipe: when a matching stream appears, the branches attach to it at runtime —
anchored on the source node, routed by the discovered stream's id — through
the same planner, atomic apply, and rollback as `Task.Attach`, with no source
rebuild:

```go
task, err := goav.From(input).
    OnStream(goav.MatchMedia(av.MediaAudio),
        goav.Branch("record").Copy().To(archive),
    ).
    Audio().Copy().To(live).
    Build(ctx)
```

Branch names are templated per matched stream (`record-<stream id>`), so
repeated discoveries stay unique. `MatchMedia`, `MatchCodec`, `MatchStreamID`,
and `MatchStream(fn)` select streams; several rules may match one stream and
each attaches independently. On `av.EventStreamRemoved` the rule's branches
for that stream detach with drain semantics — their destinations commit (a
transactional destination's `Commit` runs) while the task keeps running.
Rules are visible in `Explain` as `stream_rule` decisions before any stream
appears, and `.Auto(shape.Allow...)` policies on rule branches solve
conversions at attach time exactly like planned chains. Failures are never
silent: a late branch that cannot attach rolls back fully and surfaces
`av.EventAttachError` on `Watch`/`Events`. A discovered stream matching no
rule just surfaces its event. Because the task watches its own events,
`Events()` on a rule-bearing task returns an independent subscription per
call (the documented remedy once `Watch` is in use).

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
    Apply(voice).
    To(voiceOut).
    Run(ctx)
```

Use a direct stream when one reusable flow feeds one destination. Branch when the
same media point needs several downstream operation sequences:

```go
return goav.From(goav.Input(webrtcav.Track(audio))).
    Audio().
    Decode().
    Branches(
        goav.Branch("voice").Apply(voice).To(voiceOut),
        goav.Branch("archive").Apply(archive).To(archiveOut),
    ).
    Run(ctx)
```

Flows also apply to runtime branches attached from taps.

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
`goav.SourcePush`. Every push returns a `PushResult`: `Accepted` means a
downstream target queued the message, `Dropped` means a dropping buffer policy
deliberately shed it — normal realtime behavior reported with a nil error, so
every push is accounted for. The error keeps its meaning: flow control
(`ErrBackpressure`) or fatal.

```go
input := goav.Source("generated",
    shape.Packet(av.MediaAudio, av.CodecOpus,
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

Frame sources use the same constructor with `shape.Frame` and skip decode:

```go
frames := goav.Source("pcm",
    shape.Frame(av.MediaAudio,
        shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
    ),
    func(ctx context.Context, push goav.SourcePush) error {
        for frame := range decoded {
            if _, err := push.Frame(&frame); err != nil {
                return err
            }
        }
        return push.EOS()
    },
)
```

Event-only sources use `shape.Event()` and `push.Event(...)`, routing directly
to sinks. Custom sources participate in the same stream, branch, destination,
explain, and runtime graph path as built-in inputs.

## Source Providers

Transports are not special-cased. `rtpav.Receive` and `webrtcav.Track` are
implementations of the one source seam, and an SRT, NDI, or proprietary ingest
package plugs in the same way — with zero goav changes:

```go
type SourceProvider interface {
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

Write muxed bytes anywhere that can provide an `io.WriteCloser` with
`goav.Writer(...)`. Use `goav.Object(...)` when the writer has explicit commit
and abort semantics, such as a multipart object-store upload. The destination
opens after goav has selected the format and streams, so uploaders see the final
destination metadata. Transactional writers commit after successful runs or
detach, abort on failure, and close exactly once. Normal application
workflows should be expressible through declarative recipes. Use `goav.Custom(name, provider)`
when a package owns a reusable destination provider; the returned destination
value is still the stable routing handle.

```go
s3 := goav.Object("s3://bucket/call.ivf",
    func(ctx context.Context, info goav.DestinationInfo) (goav.TransactionalDestinationWriter, error) {
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
group. The same destination option style works for built-in destinations:

```go
return goav.From(input).
    Copy().
    To(goav.File("", out, goav.Format(av.FormatIVF)))
```

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

When a selector is ambiguous, the build error lists candidates and suggests the
`InputName`, `StreamID`, `StreamName`, or `StreamIndex` narrowing to use.

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

```go
return goav.Composite(
    goav.From(cam).Video().Region(0, 0),
    goav.From(screen).Video().Region(640, 0),
).Encode(codec.VP9(codec.Bitrate(2_000_000))).
    To(goav.File("stage.webm", out)).
    Run(ctx)
```

Packet arms decode automatically before the join; mismatched audio arms
resample to the first arm's format. The join output is a normal stream point: it
takes `.Tap(...)`, `.Branches(...)`, and `.Encode(...).To(...)` like any chain.

Joins nest — a join is an arm like any source chain. Sub-mix two microphones,
then mix the result with a third; the sub-mix output is resampled to the outer
target like any arm, and clamping applies at each mix stage:

```go
return goav.Mix(
    goav.Mix(goav.From(mic1).Audio(), goav.From(mic2).Audio()),
    goav.From(mic3).Audio(),
).Encode(codec.Opus(codec.Bitrate(96_000))).
    To(goav.File("mix.webm", out)).
    Run(ctx)
```

`Select` switches between whole joins the same way — the arm ids are the
sub-joins' output ids (`mix`, `mix-2`):

```go
task, err := goav.Select(
    goav.Mix(goav.From(a).Audio(), goav.From(b).Audio()),
    goav.Mix(goav.From(c).Audio(), goav.From(d).Audio()),
).To(monitor).Build(ctx)
// ... while running:
err = task.Control(ctx, goav.SelectActive("mix-2"))
```

Arms pair by arrival order by default — right for live sources on one clock.
`.SyncByPTS()` aligns them by timestamp instead (files starting at different
offsets, a `Seek` on one arm, drift): the earliest head frame sets each step,
arms whose head is newer sit the step out, and stale frames are dropped to
catch up.

```go
goav.Mix(goav.From(songA).Audio(), goav.From(songB).Audio()).SyncByPTS().To(out)
```

`Select` switches live through the control plane — no node names:

```go
task, err := goav.Select(
    goav.From(cam1).Video(),
    goav.From(cam2).Video(),
).To(preview).Build(ctx)
// ... while running:
err = task.Control(ctx, goav.SelectActive("cam2"))
```

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

report, err := job.Explain(ctx)
if err != nil {
    return err
}
for _, warning := range report.Warnings {
    log.Printf("goav plan warning code=%s node=%s msg=%s",
        warning.Code, warning.Node, warning.Message)
}

task, err := job.Build(ctx)
if err != nil {
    return err
}
defer task.Close()

go func() {
    for event := range task.Events() {
        log.Printf("goav event type=%s stream=%s reason=%s",
            event.Type, event.StreamID, event.Reason)
    }
}()

go func() { _ = task.Run(ctx) }()

levels, err := task.Attach(ctx,
    goav.Branch("levels").
        From(decoded).
        Do(goav.FrameFunc("rms", func(_ context.Context, frame *goav.Frame, emit goav.Emit) error {
            observeRMS(frame)
            return emit.Frame(frame)
        })).
        To(goav.Sink(goav.SinkFunc("levels", func(context.Context, goav.Message) error {
            return nil
        }))),
)
if err != nil {
    return err
}
defer levels.Close(ctx)

state := task.Snapshot()
for _, branch := range state.Branches {
    if branch.State == info.BranchAttached && branch.Name == "levels" {
        log.Printf("goav levels frames=%d", branch.Stats.Frames)
    }
}
log.Printf("goav stats packets=%d frames=%d dropped=%d",
    state.Stats.Packets, state.Stats.Frames, state.Stats.Dropped)
```

`Task.Snapshot()` returns one point-in-time view with typed lifecycle states
(`info.TaskState`, `info.BranchState`, `info.DestinationState`), graph stats,
stable taps, and active runtime branches. `Attachment.Snapshot()` reports the
branch-owned view. This works the same for video probes, screenshot collectors,
packet loss diagnostics, late recording branches, and temporary preview sinks.

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

// A running Select switches arms.
err = task.Control(ctx, goav.SelectActive("cam2"))

// Sources reposition to a media position.
err = task.Control(ctx, goav.Seek(30*time.Second))

// Sources change playback pace (positive rates only; pure pacing, no
// discontinuity).
err = task.Control(ctx, goav.Rate(2.0))

// Sources play one [start, end) window, then end naturally — trim-to-file
// segment export: the destination commits when the window completes.
err = task.Control(ctx, goav.Segment(10*time.Second, 20*time.Second))
```

`goav.Seek`, `goav.Rate`, and `goav.Segment` are the time-axis controls; all
three broadcast to every source. A source implementing
`pipeline.ControllableSource` honours them: a seek (and a segment's jump to its
start) emits `av.EventDiscontinuity` before the first message at the new
position — the signal downstream decoders already reset on; a rate change is a
pure pacing change and never discontinues; a segment plays `[start, end)` and
then ends the stream exactly as at the end of the media, so destinations
finalize naturally. A source that cannot honour a control reports a clear
per-source error without stopping a capable sibling. File inputs honour Seek
and Segment out of the box when the container demuxer implements
`format.Seeker` — the Matroska and WebM demuxers do, repositioning through
Cues (with a cluster-index fallback for cue-less files) to the keyframe at or
before the target; a reader that cannot seek reports `format.ErrNotSeekable`.
Realtime tasks (the default) pace file playback on a clock — each packet is
delivered when its media time is due — so Rate works on files as a live pacing
multiplier and composes with Seek/Segment; offline tasks (`WithRealtime(false)`)
pump at full speed and reject Rate with `format.ErrRateUnsupported`. The pacing
clock is injectable per runtime (`goav.WithClock`, default monotonic), so tests
and simulations never sleep for real.

`.AtTap(name)` narrows any control to one tap's point in the graph —
`goav.Keyframe("video").AtTap("video.720p.frames")` — and `goav.Deliver(event)`
hands a verbatim event to a stage that interprets it itself. A node-targeted
form exists for expert graphs only.

## Explain And Inspect

`job.Explain(ctx)` reports the workflow before resources open: inputs, branches,
destinations, taps, stream shapes, operation output shapes, adapter requirements,
warnings, and the planned graph. Joins plan the same way: `Describe()` of a
`Mix`/`Composite`/`Select` job shows every arm converging into the join node —
including auto-inserted per-arm decode and resample stages — and the planned
spec equals the built graph node for node. `Describe()` returns the structured
graph spec; rendering lives outside core:

```go
spec, err := job.Describe()
if err != nil {
    return err
}
uri, err := graphrender.RenderURI(spec, "goav:graph")
```

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

## Current Shape

Implemented now:

- Variadic `From(inputs...)` composition with `InputName` narrowing and shared
  multi-input destinations.
- Packet-preserving `Copy().To(...)` and packet-copy `Branches(...)`.
- Stream-scoped decode, custom stages, resize/resample, Opus/VP8/VP9 encode, and
  operation-by-operation shape validation with structured build errors.
- `Mix`/`Composite`/`Select` convergence with composable join outputs, planned
  join graphs, and a live `SelectActive` switch.
- Typed taps, branches, destinations, flows, and runtime branch attachment with
  atomic grouped attach/rollback and dependent-branch detach.
- Boundary-gated `Rebranch` (`SwitchAt` next frame or keyframe, drain/abort
  dispositions) plus per-branch `Pause`/`Resume`.
- Live task control riding the data path: `Keyframe`, `SetBitrate`,
  `SelectActive`, the time-axis `Seek`/`Rate`/`Segment`, and verbatim
  `Deliver`, narrowed with `.AtTap`.
- Filtered event watching (`Watch`) with per-watcher isolation, plus snapshots
  with typed task/branch/destination lifecycle states and scoped stats.
- The branch buffer ownership contract: `CopyIfMutable` by default, with
  `CopyAlways` and safe-only `CopyNever` opt-ins.
- The `SourceProvider` transport seam, with Pion-based RTP/WebRTC providers;
  pure-Go adapters for IVF, Annex B, Matroska/WebM, Opus, VP8/VP9, AV1, H264,
  resize, and resample.
- Per-runtime registries with layered `Default(opts...)` and direct
  `WithDecoder`/`WithEncoder`/`WithFilter`/`WithMuxer`/`WithDemuxer`/`WithProber`
  registration.
- Structured `Explain(ctx)` reports and `Describe()` graph specs.

Advanced notes live in `docs/`. An expert graph layer exists beneath the grammar
for compositions the recipe language cannot express; normal work never needs it.

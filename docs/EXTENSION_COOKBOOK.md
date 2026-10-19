# Extension cookbook

This is the copy-and-adapt guide for code outside the root module. Start here
when you already know the thing you want to plug in: a source, destination,
stage, filter, codec, join, or control host. It ends with the reusable
components below the grammar, for code that wires graphs directly.

Use [`ADAPTER_AUTHORING.md`](ADAPTER_AUTHORING.md) for lifecycle details and
hot-path rules; use this file when you want the smallest public seam that
solves one extension problem.

Every recipe here stays on the front-door grammar:

```go
rt.Run(ctx, input.Audio().Decode().Do(stage).Encode(spec).To(destination))
```

External code should not need graph handles, string routing, package internals,
or a global registry. If a recipe below seems to require one of those, the
extension point is probably the wrong one.

## Choose the seam

Pick the row by ownership, not by file format. If your application already owns
media buffers, start with `goav.Source`. If another system owns opening,
streams, timing, or controls, start with `provider.Source`. If the output needs
commit/abort semantics, treat that as a destination concern, not a pipeline
stage concern.

| Need | Use | Copy from | Failure to test |
|---|---|---|---|
| Push media already owned by the app | `goav.Source(name, shape, fn)` | `examples/custom-source` | missing callback or wrong shape refuses before work starts |
| Publish dynamic app-owned tracks and optionally mix an output | `goav.Source` plus `task.Attach(ctx, task.Track(input, track)...To(sink))` | `examples/dynamic-audio-room` | inactive participant frames fail instead of silently corrupting the mix |
| Open a transport or live provider | `provider.Source` via `goav.Input(provider)` | `examples/provider-source` | nil provider or missing codec/stream facts refuse before work starts |
| Write bytes after format resolution | `goav.Writer(name, open, opts...)` | `examples/custom-destination` | nil opener or writer open error fails the task |
| Commit or abort object-store uploads | `provider.TransactionalWriter` | `examples/transactional-writer` | induced pipeline error calls `Abort`, not `Commit` |
| Run app code on each packet, frame, or event | `.Do(component.NewFrameStage(...))` or a `pipeline.Stage` | `components_test.go` | nil callback returns `component.ErrNilStageCallback` |
| Replace a frame transform | `filter.Factory` + `runconfig.WithFilter` | `examples/custom-filter` | unsupported target returns `filter.ErrUnsupportedFormat` |
| Add a codec | `codec.EncoderFactory` / `DecoderFactory` + `runconfig.WithEncoder` / `WithDecoder` | `examples/custom-codec` | descriptor-only registration reports adapter unavailable |
| Converge several arms | `goav.Join(name, pipeline.Stage, streams...)` | `examples/custom-join` | incompatible arm media refuses at build time |
| Expose app-specific control | `ctlserver.Method[T]`, `ctlserver.Elements` with `launch.Stage`/`Sink`/`Encoder` | `examples/control-plane-host` | an invalid element setting refuses the attach, pointing at the element |

## Custom Source

Use `goav.Source` when the application already has packets, frames, or events
and can push them directly. Declare the shape precisely; it is the planner's
pre-open contract and the caller's best error message.

```go
input := goav.Source("mic",
    shape.Frame(av.MediaAudio, shape.Audio(48000, 1, av.SampleFormatS16)),
    func(ctx context.Context, push source.Push) error {
        frame := av.Frame{
            StreamID: "mic",
            Type:     av.MediaAudio,
            Audio:    &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16},
            Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: pcm}}}, // never written again
        }
        if _, err := push.Frame(&frame); err != nil {
            return err
        }
        return push.EOS()
    })
```

Rules:

- Declare packet sources with `shape.Packet(...)`, frame sources with
  `shape.Frame(...)`, and event-only sources with `shape.Event()`.
- Use `push.Packet(...)`, `push.Frame(...)`, `push.Event(...)`, and
  `push.EOS()` to deliver media and lifecycle events.
- If a pushed packet or frame has an empty `StreamID`, `source.Push` fills the
  declared source stream. If you replay packets from elsewhere, restamp them to
  the source's stream ID.
- Return cleanly on `ctx` cancellation or `goav.ErrClosed`.
- Treat `goav.ErrBackpressure` as pacing feedback; the graph owns shedding and
  delivery policy.

When the source needs seek/segment control, late stream discovery, decode bounds,
or a transport-owned open phase, implement `provider.Source` and pass it with
`goav.Input(provider)`. The runnable module `examples/provider-source` verifies
the provider-owned open phase, declared shape facts, stream discovery, a
running `pipeline.Source`, and a nil-provider failure — the copyable module
for transport packages such as SRT, NDI, RTP variants, or proprietary ingest.

The runnable module `examples/custom-source` verifies the happy path and a
pre-open nil-callback failure. It is the smallest copyable module for packages
that already own media buffers and only need to push them into goav.

### Dynamic app-owned tracks

When application state owns track membership, keep the source itself simple:
emit one stream per participant, and attach downstream work with the same
stream grammar used everywhere else.

```go
input := room.Input() // goav.Source(..., shape.Frame(...), ...)
task, err := rt.Build(ctx, input.Audio().To(anchor))
if err != nil {
    return err
}

track := av.Stream{ID: "host", Type: av.MediaAudio, Codec: hostCodec}
attachments := map[string]goav.Attachment{}
host := task.Track(input, track)
attachment, err := task.Attach(ctx,
    host.As("track-host").To(perTrack),
    host.As("mix-host").To(sharedMixer),
)
if err != nil {
    return err
}
if err := room.Join(ctx, "host"); err != nil {
    _ = task.Detach(ctx, attachment, lifecycle.AbortBranch())
    return err
}
attachments["host"] = attachment

// Later, when the participant leaves, remove the source stream and then
// drain the downstream Outputs that were anchored to it.
if err := room.Leave(ctx, "host"); err != nil {
    return err
}
if err := task.Detach(ctx, attachments["host"], lifecycle.DrainBranch()); err != nil {
    return err
}
delete(attachments, "host")
return nil
```

Use `input.OnStream(match)` instead when the source discovers streams internally and the
application does not need a join-time first-frame boundary.

## Custom Destination

Use `goav.Writer` for byte destinations that should open after the output
format and stream set are known. This keeps upload clients, memory buffers, and
custom path schemes outside the media graph until the planner knows what bytes
will be written.

```go
dest := goav.Writer("mem://voice.ogg",
    func(ctx context.Context, info provider.Info) (io.WriteCloser, error) {
        // info.Format, info.Streams, info.Metadata, and info.Realtime are final.
        return upload, nil
    },
    goav.Format(av.FormatOgg),
    goav.MIME("audio/ogg"),
    goav.Metadata(av.Metadata{"kind": "voice"}),
)

err := rt.Run(ctx, input.Audio().Decode().Encode(codec.Opus()).To(dest))
```

Use `goav.Sink(component.SinkFunc(...))` instead when the destination consumes
frames or packets directly rather than muxed bytes. Use `goav.Custom(...)` or
`provider.Destination` when the destination must advertise a richer
`provider.Contract`.

When the writer is a plain file but the extension cannot infer a container from
the path, pass destination options directly:

```go
dest := goav.Write("", out, goav.Format(av.FormatIVF))
```

Pass the same destination value to several branches when they should feed one
mux, one sink, or one transactional writer. Helpers should take the
destination as a parameter rather than construct a same-name copy, which is
rejected.

The runnable module `examples/custom-destination` verifies the open-time
`provider.Info` contract and a nil-opener failure without importing internals.

## Transactional Writer

Object-store uploads need an explicit commit boundary. Return a writer that
also implements `provider.TransactionalWriter`; goav calls `Commit` after a
successful run and `Abort` after failures.

```go
type uploadWriter struct {
    io.Writer
}

func (w *uploadWriter) Close() error { return nil }
func (w *uploadWriter) Commit(context.Context) error { return nil }
func (w *uploadWriter) Abort(context.Context) error { return nil }
```

The runnable module `examples/transactional-writer` verifies both paths: the
successful recipe commits once, and an induced frame-stage error aborts without
committing.

## Custom Stage

Use `.Do(stage)` for per-message application code: meters, loggers, packet
rewriters, or a frame transform that needs no shape solving. The `component`
constructors wrap a callback as a `pipeline.Stage`:

```go
meter, err := component.NewFrameStage("meter", func(ctx context.Context, frame *av.Frame, emit component.Emit) error {
    observe(frame)
    return emit.Frame(frame)
})
if err != nil {
    return err
}
err = rt.Run(ctx, input.Audio().Decode().Do(meter).To(out))
```

`NewPacketStage`, `NewFrameStage`, `NewEventStage`, and `NewSink` return
`component.ErrNilStageCallback` or `ErrNilSinkCallback` for a nil callback
immediately. The shorter `PacketFunc`, `FrameFunc`, `EventFunc`, and
`SinkFunc` suit examples and tests; they report a nil callback later, during
validation. A packet stage runs wherever the stream carries packets, on an
undecoded stream or after the encoder as a bitstream filter
(`.Encode(codec.VP8()).Do(sei)`); a frame stage there is refused.

Implement `pipeline.Stage` directly when the stage needs explicit lifecycle,
scratch reuse, or backpressure-specific behavior. A reusable stage should:

- allocate scratch at construction or open, never in `Handle`;
- forward events it does not consume, and flush before forwarding EOS when it
  buffers (`TestComponentCustomStageForwardsEvents`,
  `TestComponentCodecStageFlushesOnEOS`);
- return deterministic errors for unsupported media or missing scratch;
- document its accepted and emitted message shapes, payload lifetime,
  executor safety, and EOS, flush, discontinuity, loss, codec-change, and
  backpressure behavior, so a caller can wire it without reading the source.

## Custom Filter

Use a filter when a frame-domain operation should participate in normal recipe
planning, validation, `Explain`, and runtime attach. If the work is just a
one-off inspection or metric, prefer `FrameFunc`/`PacketFunc`; filters are for
shape-aware transforms.

```go
desc := filter.Descriptor{
    Name:          filter.FactoryResample,
    Input:         av.MediaAudio,
    Output:        av.MediaAudio,
    SampleFormats: []string{av.SampleFormatS16},
}

rt := bundle.MustNew(
    runconfig.WithFilter(desc, myFactory{}),
)
```

The factory returns an opened `filter.FrameFilter`. `FilterInto` appends to the
caller-owned `filter.Result`; production hot paths should reuse provided
buffers and return `filter.ErrOutputBufferTooSmall` or `filter.ErrResultFull`
instead of allocating.

Copy `examples/custom-filter` for a complete module with README, `go.mod`,
test, expected output, and a build-time failure example.

## Custom Codec

Register codecs per runtime. A descriptor without a factory is useful for
capability reporting, but encode/decode recipes need factories. Keep adapter
settings in `codec.CodecSettings` so recipes, the string launcher, and control
hosts all speak the same shape.

```go
desc := codec.Descriptor{
    ID:   av.CodecID("example/pcm"),
    Name: "example PCM",
    Type: av.MediaAudio,
    Capabilities: codec.Capabilities{
        SampleFormats: []string{av.SampleFormatS16},
    },
}

rt, err := goav.New(
    runconfig.WithEncoder(desc, myCodecFactory{}),
    runconfig.WithDecoder(desc, myCodecFactory{}),
)
if err != nil {
    return err
}
```

The encoder receives decoded frames and appends packets to
`codec.EncodeResult`. The decoder receives packets and appends frames to
`codec.DecodeResult`. `examples/custom-codec` shows the full round trip,
including the packet-source detail that replayed packets must use the source's
declared stream ID.

## Custom Join

Use `goav.Join` when several arms converge into one stream and built-in
`Mix`, `Composite`, or `Select` is not the right behavior.

```go
joined := goav.Join("interleave", newInterleaveStage("interleave", "left", "right"),
    left.Audio().Decode(),
    right.Audio().Decode(),
)

err := rt.Run(ctx, joined.To(out))
```

The stage is a normal `pipeline.Stage`. Its `InputShapes` and `OutputShapes`
contract lets the planner require explicit arm decode, insert compatible
conversions, and refuse wrong media before resources open. `examples/custom-join`
demonstrates the EOS rule that matters for joins: drain while at least one arm
has pending frames; once every arm ended and all queues are empty, stop draining
and emit one joined EOS.

## Control-Plane Host

Use `ctlserver` when your application owns a running task and wants to expose
it to `goav ctl` and other clients, with methods and elements of its own.

```go
type seekParams struct {
    Position time.Duration `json:"position" ctl:"required" help:"media position: 2s"`
}
reposition := ctlserver.Method("reposition", "seek the player",
    func(ctx context.Context, task goav.LiveTask, p seekParams) (any, error) {
        return struct{}{}, task.Control(ctx, control.Seek(p.Position).At(pipeline.NodeRef("player")))
    })
meter := launch.Stage("meter", "measure each frame", func(struct{}) (pipeline.Stage, error) {
    return component.FrameFunc("meter", func(ctx context.Context, f *av.Frame, emit component.Emit) error {
        record(f)
        return emit.Frame(f)
    }), nil
})
server, err := ctlserver.New(task, reposition, ctlserver.Elements(meter))
if err != nil {
    return err
}
return server.ListenAndServe(ctx, "unix:///tmp/goav.sock")
```

A client calls `reposition position=2s` and attaches
`tap live ! meter ! encode vp8 ! as metered ! null`. `examples/control-plane-host`
is the runnable host; [`CONTROL_PLANE.md`](CONTROL_PLANE.md) has the protocol.

## Components Below The Grammar

Recipes express intent, components do the media work, and expert graphs
compose components directly. There is no separate components layer: each
reusable piece lives in the domain package that owns its contract. (A reusable
`func(goav.Stream) goav.Stream` is an ordered intent fragment, not a
component.) **Stable** contracts should hold through normal iteration;
**experimental** ones may change before a tag; **descriptor-only** adapters
are visible for discovery while the factory waits behind a build tag. Each
package pins its hot paths with allocation guards next to the contract;
[`PERFORMANCE.md`](PERFORMANCE.md) is the proven/not-proven map.

| Package | Stable | Experimental |
|---|---|---|
| `av` | `Packet`, `Frame`, `Event`, `Buffer`/`Plane` (immutable once published; `Ref`/`Retain` keep, `MakeWritable` copies before a write, `NewBuffer` pools), and reset helpers so hot paths reuse structs | - |
| `pipeline` | `Message`, `Source`, `Stage`, `Sink`, `Emitter`, `Route` (stream/event scoping), one-to-many fanout | dynamic graph mutation (closed graphs return `pipeline.ErrClosed`), buffer policy, graph stats; on a task, `task.Stats()`, `task.Snapshot()`, `task.Taps()`, and the `plan.Report` from `Explain` |
| `component` | `NewPacketStage`, `NewFrameStage`, `NewEventStage`, `NewSink`, and the `*Func` helpers | - |
| `codec` | `Registry`, `Descriptor` (identity + capability preflight), `DecoderFactory`/`EncoderFactory`, `DecoderStage` (packets -> frames; preserves realtime events, drives loss/PLC, flushes before EOS), `EncoderStage` (frames -> packets; observes control events, flushes delayed packets), caller-owned `DecodeResult`/`EncodeResult` | decode state factories for adapter scratch |
| `format` | `Registry`, `Prober`, `Demuxer`/`Muxer`, `DemuxSource` (stream-added events, packets, EOS), `MuxStage` (writes packets, emits write-result events), caller-owned `ReadResult`/`WriteResult` | `Descriptor` capability metadata |
| `filter` | `Registry`, `FrameFilter`, `SimpleRegistry` (descriptor metadata for `Explain`), `Stage` (frame transforms preserving events, flushing before EOS), caller-owned `Result` | - |
| `rtpav` | `Source` (Pion RTP reader -> packets/events with payload maps, loss/timestamp tracking, depacketizers, EOS, feedback), `StaticPayloadMap`, `SequenceDetector` (allocation-free loss detection), `Depacketizer` and the Opus/VP8/VP9/H264/AV1 depacketizers, `FeedbackWriter` | `JitterBuffer` |
| `webrtcav` | `TrackReader` (Pion `TrackRemote` as a packet reader) | `TrackSet` (track acceptance, same-stream replacement, codec updates) |
| `playoutav` | - | `Input`: scheduled packets, frames, and events through `provider.Source`, in a nested module so playout does not widen the root dependency graph |
| adapters | `adapters/gopus`, `goaac`, `ivf`, `annexb` | `adapters/govpx`, `goav1`, `goh264` (descriptor-only by default), `resample`, `resize`; see [`ADAPTERS.md`](ADAPTERS.md) |

These are the shapes recipes compile toward and that `expert.Graph(runtime)`
wires by hand. Component tests capture `pipeline.Spec` before execution and
compare it after `Run`, so the graph you inspect is the graph that ran.

| Pattern | Shape | Use it for |
|---|---|---|
| RTP Opus decode | `rtpav.Source -> rtpav.OpusDepacketizer -> codec.DecoderStage -> FrameFunc meter or pipeline.Sink` | Realtime analyzers, bots, and receive pipelines that own loss, codec-change, EOS, and feedback (`TestComponentRTPOpusDecodeGraph`). |
| File remux fanout | `format.DemuxSource -> format.MuxStage archive`, `-> format.MuxStage preview` | Packet-preserving fanout when formats already match (`TestComponentFileRemuxFanout`). |
| Analysis hook | `source -> PacketFunc or FrameFunc meter -> EventFunc logger -> sink` | Small reusable hooks; see [Custom Stage](#custom-stage) for when to implement `pipeline.Stage` (`TestComponentMuxStageEmitsWriteEvents`). |
| WebRTC receive | `webrtcav.TrackReader or TrackSet -> rtpav.Source -> depacketizer -> mux, decode, relay, or analysis` | Keeping Pion types at the WebRTC/RTP boundary (`TestComponentWebRTCTrackSetFeedsRTPSource`). |

## Compiled Bootstraps

The package examples double as executable documentation. Keep
`ExampleSource_pushAccounting`, `ExampleWriter_transactionalUpload`,
`ExampleWithEncoder_customSettings`, `ExampleTask_flowchart`, and
`ExampleTestSourceScript` compiling. Runtime diagnostics live outside core; use
`graphrender.RenderTaskFlowchart(task)` for a running task,
`graphrender.RenderSnapshotFlowchart(task.Snapshot())` for a captured task
view, and `graphrender.RenderBranchFlowchart(attachment.Snapshot())` for one
runtime branch.

## Testing Pattern

Use `goavtest` for fixtures and `goavtest/expect` for assertions:

```go
out := goavtest.NewCollector()
rt := goavtest.Runtime(runconfig.WithFilter(desc, factory))
err := rt.Run(ctx, goavtest.Audio(48000, 1, []int16{1, 2}).Audio().To(out.Sink()))

expect.NoError(t, err)
expect.S16(t, out, [][]int16{{1, 2}})
```

`goavtest/expect` uses `github.com/google/go-cmp/cmp` for structural diffs and
keeps the custom layer to goav-specific checks: collector samples, golden
output files, and `*goav.BuildError` family, code, operation, node, cause,
detail lookups, and rendered detail/fix lines. The standalone example modules
use this pattern so adapter authors can copy tests without importing internals
or writing local `errors.As` and golden-file helpers.

## Checklist

- Keep registration per runtime; never rely on package globals.
- Put unsupported shape or settings checks before resources open when possible.
- Return typed sentinels such as `codec.ErrUnsupportedFormat` or
  `filter.ErrUnsupportedFormat` from adapter open paths.
- Keep hot paths allocation-free unless the example explicitly documents that
  it is a toy implementation.
- Test one successful recipe and one refusal/failure path with
  `goavtest/expect`.
- For standalone examples, keep `go.mod`, README, `main.go`, `main_test.go`,
  expected output, and a failure example together.

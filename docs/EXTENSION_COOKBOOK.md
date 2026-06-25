# Extension cookbook

This is the copy-and-adapt guide for code outside the root module. Start here
when you already know the thing you want to plug in: a source, destination,
filter, codec, join, or control host.

Use `docs/ADAPTER_AUTHORING.md` for lifecycle details and hot-path rules; use
this file when you want the smallest public seam that solves one extension
problem.

Every recipe here stays on the front-door grammar:

```go
goav.From(input).Audio().Decode().Do(stage).Encode(codec).To(destination)
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
| Publish dynamic app-owned tracks and optionally mix an output | `goav.Source` plus `Mutable.Attach(...From(input.Stream(track)))` | `examples/dynamic-audio-room` | inactive participant frames fail instead of silently corrupting the mix |
| Open a transport or live provider | `provider.Source` via `goav.Input(provider)` | `examples/provider-source` | nil provider or missing codec/stream facts refuse before work starts |
| Write bytes after format resolution | `goav.Writer(name, open, opts...)` | `examples/custom-destination` | nil opener or writer open error fails the task |
| Commit or abort object-store uploads | `provider.TransactionalWriter` | `examples/transactional-writer` | induced pipeline error calls `Abort`, not `Commit` |
| Replace a frame transform | `filter.Factory` + `goav.WithFilter` | `examples/custom-filter` | unsupported target returns `filter.ErrUnsupportedFormat` |
| Add a codec | `codec.EncoderFactory` / `DecoderFactory` + `WithEncoder` / `WithDecoder` | `examples/custom-codec` | descriptor-only registration reports adapter unavailable |
| Converge several arms | `goav.Join(name, pipeline.Stage, arms...)` | `examples/custom-join` | incompatible arm media refuses at build time |
| Expose app-specific control | `ctl.CommandSpec`, branch steps, encoder specs | `examples/control-plane-host` | invalid settings fail before branch attach |

## Custom Source

Use `goav.Source` when the application already has packets, frames, or events
and can push them directly. Declare the shape precisely; it is the planner's
pre-open contract and the caller's best error message.

```go
input := goav.Source("mic",
    shape.Frame(av.MediaAudio, shape.Audio(48000, 1, av.SampleFormatS16)),
    func(ctx context.Context, push goav.SourcePush) error {
        frame := av.Frame{
            StreamID: "mic",
            Type:     av.MediaAudio,
            Audio:    &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16},
            Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: pcm, Ownership: av.BufferImmutable}}},
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
- If a pushed packet or frame has an empty `StreamID`, `SourcePush` fills the
  declared source stream. If you replay packets from elsewhere, restamp them to
  the source's stream ID.
- Return cleanly on `ctx` cancellation or `goav.ErrClosed`.
- Treat `goav.ErrBackpressure` as pacing feedback; the graph owns shedding and
  delivery policy.

When the source needs seek/rate control, late stream discovery, decode bounds,
or a transport-owned open phase, implement `provider.Source` and pass it with
`goav.Input(provider)`.

The runnable module `examples/custom-source` verifies the happy path and a
pre-open nil-callback failure. It is the smallest copyable module for packages
that already own media buffers and only need to push them into goav.

### Dynamic app-owned tracks

When application state owns track membership, keep the source itself simple:
emit one stream per participant, and attach downstream work with the same
runtime branch grammar used everywhere else.

```go
input := room.Input() // goav.Source(...)
task, err := goav.From(input).Audio().To(anchor).Build(ctx)
if err != nil {
    return err
}

track := av.Stream{ID: "host", Type: av.MediaAudio, Codec: hostCodec}
attachments := map[string]goav.Attachment{}
attachment, err := task.Attach(ctx,
    goav.Branch("track-host").From(input.Stream(track)).To(perTrack),
    goav.Branch("mix-host").From(input.Stream(track)).To(sharedMixer),
)
if err != nil {
    return err
}
if err := room.Join(ctx, "host"); err != nil {
    _ = task.Detach(ctx, attachment, goav.AbortBranch())
    return err
}
attachments["host"] = attachment

// Later, when the participant leaves, remove the source stream and then
// drain the downstream branches that were anchored to it.
if err := room.Leave(ctx, "host"); err != nil {
    return err
}
if err := task.Detach(ctx, attachments["host"], goav.DrainBranch()); err != nil {
    return err
}
delete(attachments, "host")
return nil
```

Use `OnStream` instead when the source discovers streams internally and the
application does not need a join-time first-frame boundary.

The runnable module `examples/provider-source` verifies the provider-owned open
phase, declared shape facts, stream discovery, a running `pipeline.Source`, and
a nil-provider failure. It is the copyable module for transport packages such
as SRT, NDI, RTP variants, or proprietary ingest.

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

err := goav.From(input).Audio().Encode(codec.Opus()).To(dest).Run(ctx)
```

Use `goav.Sink(goav.SinkFunc(...))` instead when the destination consumes
frames or packets directly rather than muxed bytes. Use `goav.Custom(...)` or
`provider.Destination` when the destination must advertise a richer
`provider.Contract`.

When the writer is a plain file but the extension cannot infer a container from
the path, pass destination options directly:

```go
dest := goav.File("", out, goav.Format(av.FormatIVF))
```

Reuse one destination value, or give matching destinations the same
`goav.Mux(name, destination)`, when several branches should feed one mux, one
sink group, or one transactional writer.

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

rt := bundle.MustNewFilters(
    goav.WithFilter(desc, myFactory{}),
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

rt := goav.MustNew(
    goav.WithEncoder(desc, myCodecFactory{}),
    goav.WithDecoder(desc, myCodecFactory{}),
)
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
    goav.From(left).Audio(),
    goav.From(right).Audio(),
)

err := joined.To(out).Run(ctx)
```

The stage is a normal `pipeline.Stage`. Its `InputShapes` and `OutputShapes`
contract lets the planner decode arms, insert compatible conversions, and
refuse wrong media before resources open. `examples/custom-join` demonstrates
the EOS rule that matters for joins: drain while at least one arm has pending
frames; once every arm ended and all queues are empty, stop draining and emit
one joined EOS.

## Control-Plane Host

Use `ctl` when your application owns a running task and wants to expose
app-specific verbs, branch steps, or encoder names through `goav ctl`.

```go
command := ctl.NewCommand[setRate](
    "vendor.rate",
    "demo playback-rate control",
    func(ctx context.Context, task goav.LiveTask, cmd setRate) (ctl.ControlResponse, error) {
        if err := task.Control(ctx, control.Rate(cmd.Value).At(pipeline.NodeRef(cmd.Source))); err != nil {
            return ctl.ControlResponse{}, err
        }
        return ctl.ControlResponse{Operation: "control vendor.rate"}, nil
    },
)

err := ctl.ServeUnixWithOptions(ctx, task, "unix:///tmp/goav.sock",
    ctl.WithCapabilities(ctl.CapabilitySet{Commands: []ctl.CommandSpec{command}}),
)
```

`examples/control-plane-host` is the runnable playground. It registers a custom
command, branch steps, and a custom encoder alias, then serves them behind a
Unix socket.

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
err := goav.From(goavtest.Audio(48000, 1, []int16{1, 2})).
    Audio().
    To(out.Sink()).
    UseRuntime(goavtest.Runtime(goav.WithFilter(desc, factory))).
    Run(ctx)

expect.NoError(t, err)
expect.S16(t, out, [][]int16{{1, 2}})
```

`goavtest/expect` uses `github.com/google/go-cmp/cmp` for structural diffs and
keeps the custom layer to goav-specific checks: collector samples, golden
output files, and `*goav.BuildError` code, operation, node, cause, typed
fields/fixes, and rendered details/suggestions. The standalone example modules
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

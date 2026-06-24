# Adapter authoring

How to add a codec, container, filter, source provider, or destination provider
without touching core. The aim is boring integration: your package implements a
small public interface, registers it on one runtime, and the normal recipe
grammar does the rest.

Every extension point is an exported interface plus a value-typed `With*`
option (or a plain value argument) on a per-runtime registry. There are no
globals; registration is last-wins, so an adapter can also override a standard
implementation under `std.MustNew(opts...)`.

The executable proof is `adapterproof/adapter_compat_test.go`: one toy
implementation of every extension point below, defined entirely in a test
package that imports only public goav packages, run end to end through the
recipe grammar. The minimal reference implementations are goavtest's fakes
(`goavtest/codec.go`, `goavtest/format.go`); `docs/ADAPTERS.md` catalogs the
real adapters. For a copy-and-adapt guide organized by use case, see
`docs/EXTENSION_COOKBOOK.md`.

The copyable, separate-module examples are:

- **`examples/custom-source`**: pushes application-owned S16 frames through
  `goav.Source` and checks `SourcePush` accepted/dropped accounting.
- **`examples/provider-source`**: implements `provider.Source`, opens a
  `pipeline.Source`, and feeds goav through `goav.Input(provider)`.
- **`examples/custom-destination`**: opens a plain byte destination with
  `goav.Writer` and checks the resolved `provider.Info`.
- **`examples/custom-filter`**: registers a frame filter with `goav.WithFilter`
  and drives it through `.Resample(...)`.
- **`examples/custom-codec`**: registers a codec with `goav.WithEncoder` and
  `goav.WithDecoder`, then round-trips packets through `.Encode(...)` and
  `.Decode()`.
- **`examples/custom-join`**: implements a `pipeline.Stage` consumed by
  `goav.Join(...)`.
- **`examples/transactional-writer`**: opens a destination with `goav.Writer`
  and demonstrates `provider.TransactionalWriter` commit/abort semantics.
- **`examples/control-plane-host`**: exposes app-owned controls, branch steps,
  sinks, and encoder spellings through a validated `ctl.CapabilitySet`.

When writing your adapter, start from the smallest shipped implementation that
matches the kind of boundary you own:

- **`adapters/resample`** is the filter template: a `Descriptor()` function, a
  stateless `Factory` whose `NewFilter` opens the instance, `Open` validation
  for stream and target, `FilterInto` that writes into the caller's
  preallocated plane with capacity checks, and a package `Register(registry)`
  hook.
- **`adapters/ivf`** is the container template: one factory implementing both
  `MuxerFactory` and `DemuxerFactory`, headers written at `Open`, one record
  per `Write`/`ReadInto` against caller-owned scratch, `io.EOF` at the clean
  end, and magic/extension probe rules.

## Rules common to every extension point

These rules are less about style and more about keeping adapters predictable
inside live pipelines. Do the expensive thinking while opening; keep the
message path simple.

- **Cold vs hot** (`docs/PERFORMANCE.md` is the contract): construction,
  `Open`, probing, and registration may allocate; per-packet/per-frame methods
  must not. Hot paths fill caller-owned result structs (`*Into` methods),
  reusing the caller's preallocated plane/payload backing (`Reset` keeps
  backing arrays) and returning a capacity sentinel instead of growing past
  `cap`. No `fmt`, map writes, closures, or error wrapping per message.
- **Serial workers**: each opened instance is driven by one stream's serial
  worker, so per-message state needs no locking. Factories may serve several
  builds; keep them stateless or guard shared state.
- **Errors**: build-time refusals reach users as `*goav.BuildError` with a
  `errcode.Code`; preflight checks descriptors and registries before anything
  opens (`errcode.DecodeAdapterMissing`, `EncodeAdapterIncompatible`,
  `TransformAdapterMissing`, `InputDemuxerMissing`, `OutputMuxerMissing`, ...).
  Return typed sentinels for unsupported config at open
  (`codec.ErrUnsupportedFormat`, ...). Any non-nil error from a hot-path
  method is fatal to the task; flow control is the graph's job, not the
  adapter's: never sleep, retry, or drop inside an adapter.
- **Buffer ownership** (`av.BufferOwnership`): mark output payloads precisely.
  `BufferBorrowed` = valid until your next call (copied before queueing);
  `BufferOwned` = receiver's to mutate (copied per branch on fanout);
  `BufferImmutable` = shareable by reference forever, never written again by
  anyone. This is the only zero-copy fanout class; it is trusted, not checked.
- **Close**: called exactly once by the owning stage/source (at task close or
  failed-build cleanup), but implement it idempotently anyway.

## Codec (`codec` package)

```go
type Decoder interface {
    Descriptor() Descriptor
    Open(context.Context, DecodeConfig) error
    DecodeInto(context.Context, *av.Packet, *DecodeResult) error
    FlushInto(context.Context, *DecodeResult) error
    HandleEvent(context.Context, *av.Event) error
    Close() error
}
// Encoder mirrors Decoder with Open(context.Context, EncodeConfig) error and
// EncodeInto(context.Context, *av.Frame, *EncodeResult) error.
type DecoderFactory interface { NewDecoder(context.Context, DecodeConfig) (Decoder, error) }
type EncoderFactory interface { NewEncoder(context.Context, EncodeConfig) (Encoder, error) }
```

Lifecycle (from `codec/stage.go` + `runtime_decode.go`/`runtime_encode.go`):

1. Build-time preflight reads your `Descriptor` (ID, Type, Modes, capability
   lists; empty list = unconstrained) and refuses incompatible plans before
   anything opens. Descriptor-only -> `errcode.*AdapterUnavailable`.
2. `New{Decoder,Encoder}(ctx, config)` is called once per stream per build
   (cold) and must return an opened instance; call your own `Open` inside the
   factory; the runtime never calls `Open` separately. Optional
   `codec.DecodeStateFactory` provisions `DecodeConfig.OpaqueState` first; if
   `NewDecoder` then fails, state with a `Close()` is closed.
3. Per message: `DecodeInto` per packet / `EncodeInto` per frame.
   `HandleEvent` sees every in-band event before it is forwarded;
   `av.EventEndOfStream` then triggers `FlushInto`; `av.EventPacketLoss` then
   triggers `DecodeInto(ctx, nil, result)`. **A nil packet is legal**; it is
   the concealment/PLC hook. An encoder must apply
   `av.EventKeyframeRequired`/`av.EventBitrateChanged` or return an error.
4. Decoders ask upstream via `DecodeResult.Requests`
   (`codec.ControlRequestKeyframe`); the stage emits them as events.
   `Close` runs once (stage-guarded).

Results: `DecodeResult`/`EncodeResult` are caller-owned scratch sized from
`DecodeBounds` (merged from stream facts and source-provider bounds). Slot
exhausted -> `codec.ErrResultFull`; preallocated plane/payload too small ->
`codec.ErrOutputBufferTooSmall`. Honor `DecodeConfig.Bounds` when sizing
internal arenas. Read `CodecSettings` at open: typed fields such as bitrate,
cadence, profile, audio shape overrides, and `Custom` adapter-owned keys all
arrive through the same settings value. The string launcher and control plane
reflect exported `CodecSettings` fields tagged with `goavctl`, `usage`, and
`help`; adding a tagged field makes it bindable and visible in generated
`goav ctl help attach` / `goav ctl capabilities` output. Invoke the raw
`Control func(any) error` escape hatch with your concrete native handle when a
setting truly belongs to the native library; a non-nil error fails the open.

Register with `goav.WithDecoder(desc, factory)`, `goav.WithEncoder(desc,
factory)`, `goav.WithCodecDescriptor(desc)` (capability-only), or a bundle
via `goav.WithCodecAdapter(func(*codec.SimpleRegistry))`; adapter packages
should export `Register(*codec.SimpleRegistry)`.

## Container (`format` package)

```go
type Demuxer interface {
    Format() av.FormatID
    Open(context.Context, Input, OpenOptions) error
    Streams() []av.Stream
    ReadInto(context.Context, *ReadResult) error
    Close() error
}
type Muxer interface {
    Format() av.FormatID
    Open(context.Context, Output, []av.Stream, OpenOptions) error
    Write(context.Context, *av.Packet, *WriteResult) error
    Close() error
}
type Prober interface { Probe(context.Context, ProbeRequest) (ProbeResult, error) }
type DemuxerFactory interface { NewDemuxer(context.Context, ProbeResult) (Demuxer, error) }
type MuxerFactory interface { NewMuxer(context.Context, av.FormatID) (Muxer, error) }
```

Lifecycle (from `format/stage.go`, `runtime_demux.go`, `mux_destination.go`):

1. Probe: every registered prober runs; highest score wins; non-matches
   return `format.ErrNotFound` fast (a prober error only surfaces if nobody
   matches). `ProbeResult.Streams` may declare streams without opening.
2. Demux: `NewDemuxer(ctx, probeResult)` (cold) -> core calls
   `Open(ctx, input, options)`. Unlike codecs, core calls `Open`
   here; on `Open` failure the core calls `Close`. `Streams()` must be
   complete immediately after `Open`. Then `ReadInto` in a loop: fill
   `out.Packet` (already reset), set `PacketReady=true` (<=1 packet per call),
   add bounded events via `AddEvent` (`ErrResultFull` past capacity). Return
   `io.EOF` for the clean end of media (the source emits EOS); any other error
   fails the task. Borrowed payloads stay valid until your next `ReadInto`
   unless you document longer.
3. Seeking is a capability: also implement `format.Seeker` and the source
   becomes controllable (`Seek` runs between reads, never racing `ReadInto`).
   Without it, time controls are refused loudly.
4. Mux: `NewMuxer(ctx, formatID)` (cold) -> core calls
   `Open(ctx, output, streams, options)` with the final stream set; on
   failure core calls `Close`. `Write` per packet in delivery order; `Close`
   finalizes exactly once; a `Write`/`Close` error marks the destination
   transaction failed (transactional writers abort instead of committing).

Register with `goav.WithDemuxer(id, factory)`,
`goav.WithMuxer(id, factory)`, `goav.WithProber(prober)`, or bundles via
`goav.WithFormatAdapter(...)`; `Register{Muxer,Demuxer}Descriptor` attaches a
capability `format.Descriptor` (media kinds, codecs, stream counts) that
destination validation reads.

## Filter (`filter` package)

```go
type FrameFilter interface {
    Descriptor() Descriptor
    Open(context.Context, Config) error
    FilterInto(context.Context, *av.Frame, *Result) error
    FlushInto(context.Context, *Result) error
    HandleEvent(context.Context, *av.Event) error
    Close() error
}
type Factory interface { NewFilter(context.Context, Config) (FrameFilter, error) }
```

Lifecycle (from `filter/stage.go` + `branch_compose_build.go`): the registry
keys on `Descriptor.Name`; `filter.FactoryResize` and
`filter.FactoryResample` are the names the grammar's
`.Resize(...)`/`.Resample(...)` resolve, so registering under those names
replaces the standard implementation. `NewFilter(ctx, config)` is called once
per stage (cold) with `Config.Stream` plus exactly one of
`Config.Video`/`Config.Audio`; return an OPENED filter (factory calls
`Open`). Then `FilterInto` per frame, `HandleEvent` before each forwarded
event, `FlushInto` on `av.EventEndOfStream`, `Close` once. Build-time
validation checks `Descriptor.Input/Output` media and the capability lists
(empty = unconstrained) -> `errcode.TransformAdapterIncompatible`. Results:
caller-owned `filter.Result`; sentinels `filter.ErrResultFull`,
`filter.ErrOutputBufferTooSmall`, `filter.ErrUnsupportedFormat`.

Register: `goav.WithFilter(desc, factory)` or `goav.WithFilterAdapter(...)`.
Use the well-known descriptor name for the conversion class you are replacing;
the last registration wins, so build from `std.MustNewFilters(...)` and pass your
adapter option after the standard filters:

```go
var passthroughResampler = filter.Descriptor{
    Name:          filter.FactoryResample,
    Input:         av.MediaAudio,
    Output:        av.MediaAudio,
    SampleFormats: []string{av.SampleFormatS16},
    Realtime:      true,
    Stateless:     true,
    Metadata:      av.Metadata{"backend": "acme-audio"},
}

type passthroughResamplerFactory struct{}

func (passthroughResamplerFactory) NewFilter(ctx context.Context, cfg filter.Config) (filter.FrameFilter, error) {
    f := &passthroughResamplerFilter{}
    if err := f.Open(ctx, cfg); err != nil {
        return nil, err
    }
    return f, nil
}

type passthroughResamplerFilter struct{}

func (*passthroughResamplerFilter) Descriptor() filter.Descriptor { return passthroughResampler }

func (*passthroughResamplerFilter) Open(_ context.Context, cfg filter.Config) error {
    if cfg.Stream.Type != av.MediaAudio || cfg.Audio == nil {
        return filter.ErrUnsupportedFormat
    }
    return nil
}

func (*passthroughResamplerFilter) FilterInto(_ context.Context, frame *av.Frame, out *filter.Result) error {
    if frame == nil {
        return nil
    }
    if len(out.Frames) == cap(out.Frames) {
        return filter.ErrResultFull
    }
    out.Frames = append(out.Frames, *frame)
    return nil
}

func (*passthroughResamplerFilter) FlushInto(context.Context, *filter.Result) error { return nil }
func (*passthroughResamplerFilter) HandleEvent(context.Context, *av.Event) error    { return nil }
func (*passthroughResamplerFilter) Close() error                                    { return nil }

runtime := std.MustNewFilters(
    goav.WithFilter(passthroughResampler, passthroughResamplerFactory{}),
)
```

`FilterInto` and `FlushInto` may append both `out.Events` and `out.Frames`;
`filter.Stage` emits result events before result frames, forwards input events
after `HandleEvent`, and flushes before forwarding `av.EventEndOfStream`.
External hosts that want a new CLI-visible branch primitive should wrap the
filter in an allowlisted `PipelineRegistry` step; see `docs/CONTROL_PLANE.md`.

## Source provider (`provider` package)

```go
package provider

type Source interface {
    OpenSource(ctx context.Context) (pipeline.Source, []av.Stream, error)
    SourceShape() shape.Spec
}
```

Optional capabilities, discovered by type assertion: `Name() string` (node
name in Describe; duplicates get "-1" suffixes), `Detail() string`, and
`DecodeBounds(av.StreamID) codec.DecodeBounds` (seeds downstream decoder
scratch). `rtpav.Receive` and `webrtcav.Track` are providers; yours plugs in
identically via `goav.From(goav.Input(provider))`.

Lifecycle (from `provider/provider.go`, `provider.go`, `source.go`):
`SourceShape()` is read before
opening. Declare domain/media/codec/format/realtime precisely, for example
`shape.Packet(media, codecID, shape.Audio(...),
shape.Format(av.FormatID("vendor.format")), shape.Realtime(true))`. The
planner selects streams and decoders from it. Use `shape.Format` when an
external source has custom framing that downstream validation should see.
`OpenSource` runs once per build; the returned streams must carry full
`av.Stream.Codec` parameters. The returned
`pipeline.Source.Start(ctx, emitter)` runs on the task: emit
`av.EventStreamAdded` announces, then media, then `av.EventEndOfStream`;
return cleanly on `ctx` cancellation or `pipeline.ErrClosed`, slow down on
`pipeline.ErrBackpressure`. Implement `pipeline.ControllableSource` to accept
seek/rate controls (record the request; apply it from the Start loop). A
single-stream push source needs no provider: `goav.Source(name, shape, fn)`.

## Destination provider (`provider` package)

```go
package provider

type Destination interface {
    Name() string
    Contract() Contract
    Open(context.Context, Info) (Writer, error)
}
type Writer interface { io.Writer; Close() error }
type TransactionalWriter interface {
    Writer
    Commit(context.Context) error
    Abort(context.Context) error
}
```

Lifecycle (from `mux_destination.go`): `Contract()` is read during planning
(keep it pure; `Formats[0]` pins the container, skipping output probing).
`Open` runs once per build after format and stream resolution.
`provider.Info` carries the final format, streams, metadata, and realtime
flag. The writer receives muxed bytes on the mux hot path. Teardown: the
muxer finalizes, then `Commit` (run succeeded or drained detach) or `Abort`
(run failed, mux write/close error, or build teardown), then `Close`; each
exactly once. `goav.Writer(name, openFn)` is the contract-free shortcut;
`goav.File` wraps an open writer (closed once iff it implements `io.Closer`).
Use via `.To(goav.Custom(name, p))`; the `Destination` value is the
routing handle branches share.

## Checklist

1. Fill the descriptor precisely. Capabilities are preflight constraints, and
   wrong ones turn into misleading `BuildError` suggestions.
2. Factory returns ready instances (codec/filter open themselves; container
   `Open` is called by core).
3. Hot paths: caller-owned results, capacity sentinels, honest
   `av.BufferOwnership`, zero allocations, no locks, no flow control.
4. Register through `With*` value options; export `Register(registry)` for
   bundles.
5. Ship three test kinds: a round-trip correctness test; a
   `testing.AllocsPerRun` guard on every hot path (see any `adapters/*`
   `*Allocs` test); an end-to-end grammar test (`goavtest.Audio/Video/Packets`
   inputs, `goavtest.NewCollector()` output, `goavtest.Runtime()` plus your
   `With*` option). Use `goavtest/expect` for assertions: it delegates
   structural diffs to `github.com/google/go-cmp/cmp` and adds
   `BuildError`, `S16`, and golden-output checks for goav tests. Use
   `goavtest.NewTestSource` when the adapter needs a
   provider-shaped, controllable source fixture; use
   `goavtest.TestSourceScript(goavtest.TestSourcePacket(...),
   goavtest.TestSourceEvent(...))` for mixed media/control scripts.
   `adapterproof/adapter_compat_test.go` is a complete worked example of all
   five extension points.

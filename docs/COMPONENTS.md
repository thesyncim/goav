# Reusable Components

`goav` is not only a recipe DSL. The rule for the project is:

```text
recipes express intent
components do the media work
expert graphs compose those components directly
```

There is no separate "components" API layer: reusable pieces live in the domain
packages that own their contracts. Reusable `Flow(name).Audio()/Video()` chains
are ordered intent fragments, not graph components; they expand into the same
stream intents that recipes, branch composers, and runtime attachments compile.

## Status

- stable: public contract is intended to hold through normal iteration.
- experimental: usable, but details may change before a release tag.
- descriptor-only: visible for discovery, but the concrete factory is behind a
  build tag or not ready in the default build.
- planned: named direction, not a current implementation contract.
- internal scaffold: temporary runtime/compiler code that should keep moving
  toward shared reusable components.

## Component Contract

Reusable components document: accepted/emitted message shape; hot-path
allocation expectations after warm-up; buffer ownership and borrowed-buffer
lifetime; event forwarding/consumption; EOS, flush, discontinuity, loss,
codec-change, and backpressure behavior; direct and bounded buffered graph
safety; and the scratch/result objects callers reuse.

## Allocation Proofs

Hot-path allocation guards live next to the domain packages that own the
contract (the proven/measured/not-proven map is `docs/PERFORMANCE.md`):

- `av`: `TestCoreResetAllocs`, `TestTimeBaseHelpersAllocs`
- `goav` (root): `TestSourcePushDeliveryAllocs`, `TestSinkFuncDeliveryAllocs`,
  `TestAudioMixStepAllocCeiling` (a pinned ceiling — the mix step allocates
  today; see `docs/PERFORMANCE.md`)
- `pipeline`: `TestMessageAndScratchResetAllocs`, `TestGraphDirectRunAllocs`,
  `TestGraphBufferedSteadyEmitAllocs`, `TestDropControllerDecideAllocs`
- `rtpav`: `TestSourceStartAllocs`, `TestSequenceDetectorAllocs`,
  `TestOpusDepacketizerAllocs`, `TestVP8DepacketizerAllocs`,
  `TestVP9DepacketizerAllocs`, `TestH264DepacketizerAllocs`,
  `TestAV1DepacketizerAllocs`, `TestJitterRingAllocs`,
  `TestFeedbackResultAllocs`
- `codec`: `TestDecoderStageAllocs`, `TestEncoderStageAllocs`
- `format`: `TestFormatResultResetAllocs`, `TestDemuxSourceAllocs`,
  `TestMuxStageAllocs`
- `filter`: `TestStageAllocs`
- default adapters: `TestDecodePacketLossConcealmentAllocs`,
  `TestMuxerWriteAllocs`, `TestDemuxerReadIntoAllocs`, `TestFilterAllocs`

## Catalog

Core media (`av`) — stable: `Packet`, `Frame`, `Event`, `Buffer`/`Plane`
(ownership flags define retain/copy/reuse), and reset helpers so hot paths
reuse structs instead of allocating.

Pipeline — stable: `pipeline.Message`, `Source`, `Stage`, `Sink`, `Emitter`,
`Route` (optional stream/event scoping), and one-to-many fanout routing.
Experimental: dynamic graph mutation (closed graphs reject additions with
`pipeline.ErrClosed`), the work-plan compile (recipe intent → one composable
branch IR with ordered-operation shape validation), `plan.Report`
(`Job.Explain(ctx)` structured explanation), runtime attach (grouped `Task.Attach` from typed taps with
preflight, rollback, branch-owned stats, and subtree detach; `Task.Taps()` lists
stable outlets), buffer policy, and graph stats (`Task.Stats()`,
`Task.Snapshot()` with spec, stats, taps, and runtime branch states).

RTP — stable: `rtpav.Source` (Pion RTP reader → packets/events with payload
maps, loss/timestamp tracking, depacketizers, EOS, feedback),
`rtpav.StaticPayloadMap`, `rtpav.SequenceDetector` (allocation-free loss
detection), `rtpav.Depacketizer` plus the Opus/VP8/VP9/H264/AV1 depacketizers,
and `rtpav.FeedbackWriter`. Experimental: `rtpav.JitterBuffer`.

Codec — stable: `codec.Registry`, `codec.Descriptor` (identity + capability
preflight), `codec.DecoderFactory`/`EncoderFactory`, `codec.DecoderStage`
(packets → frames; preserves realtime events, drives loss/PLC, flushes before
EOS), `codec.EncoderStage` (frames → packets; observes control events, flushes
delayed packets), and caller-owned `DecodeResult`/`EncodeResult`. Experimental:
decode state factories for adapter scratch.

Format — stable: `format.Registry`, `format.Prober`, `format.Demuxer`/`Muxer`,
`format.DemuxSource` (stream-added events, packets, EOS), `format.MuxStage`
(writes packets, emits write-result events), and caller-owned
`ReadResult`/`WriteResult`. Experimental: `format.Descriptor` capability
metadata.

Filters — stable: `filter.Registry`, `filter.FrameFilter`,
`filter.SimpleRegistry` (descriptor metadata for `Explain(ctx)`),
`filter.Stage` (frame transforms preserving events, flushing before EOS), and
caller-owned `filter.Result`.

WebRTC — stable: `webrtcav.TrackReader` (Pion TrackRemote as a packet reader).
Experimental: `webrtcav.TrackSet` (track acceptance, same-stream replacement,
codec updates), the codec-update boundary, and the track replacement boundary.

Adapters — see `docs/ADAPTERS.md` for the per-adapter catalog: stable
`adapters/gopus`, `adapters/ivf`, `adapters/annexb`; experimental
`adapters/govpx`, `adapters/goav1`, `adapters/goh264` (descriptor-only in
default builds), `adapters/resample`, `adapters/resize`.

## Manual Graph Patterns

These are the component shapes recipes compile toward, and the shapes advanced
users wire manually with `expert.Graph(runtime)`. Direct component tests
capture `pipeline.Spec` before execution and compare it after `Run`, so the
graph users inspect is the graph that ran.

### RTP Opus Decode

```text
rtpav.Source
  -> rtpav.OpusDepacketizer
  -> codec.DecoderStage
  -> goav.FrameFunc meter or pipeline.Sink
```

For realtime audio analyzers, bots, and receive pipelines that need direct
ownership of loss, codec-change, EOS, and feedback behavior
(`TestComponentRTPOpusDecodeGraph`).

### File Remux Fanout

```text
format.DemuxSource
  -> format.MuxStage archive
  -> format.MuxStage preview
```

When packet formats already match and the graph stays packet-preserving
(`TestComponentFileRemuxFanout`).

### Analysis Hook

```text
input source
  -> goav.PacketFunc or goav.FrameFunc meter
  -> goav.EventFunc logger
  -> sink
```

Function helpers are for small reusable hooks; implement `pipeline.Stage`
directly when a component needs explicit lifecycle, scratch reuse, or
backpressure-specific behavior (`TestComponentCodecStageFlushesOnEOS`,
`TestComponentMuxStageEmitsWriteEvents`).

### WebRTC Receive

```text
webrtcav.TrackReader or webrtcav.TrackSet
  -> rtpav.Source
  -> depacketizer
  -> mux, decode, relay, or analysis stages
```

Keeps Pion types at the WebRTC/RTP boundary
(`TestComponentWebRTCTrackSetFeedsRTPSource`).

## Custom Stage Checklist

A reusable custom stage should: implement `pipeline.Stage`; allocate scratch at
construction/open, not in `Handle`; forward events it does not consume; flush
before forwarding EOS when it buffers; return deterministic errors for
unsupported media or missing scratch; and document executor safety and borrowed
buffer ownership (`TestComponentCustomStageForwardsEvents`).

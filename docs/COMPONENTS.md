# Reusable Components

`goav` is not only a recipe DSL. Recipes are the front door, but they lower
into reusable pure-Go media components that advanced users can wire directly.

The rule for the project is:

```text
recipes express intent
components do the media work
expert graphs compose those components directly
```

There is no separate "components" API layer. Reusable pieces live in the
domain packages that own their contracts.

Recipe flows such as `AudioFlow` and `VideoFlow` are reusable ordered intent
fragments, not graph components. They can carry custom stages, taps,
transforms, and an optional terminal encoder, then expand into the same stream
intents that recipes, branch composers, and non-encoding runtime attachments
already compile.

## Status

- stable: public contract is intended to hold through normal iteration.
- experimental: usable, but details may change before a release tag.
- descriptor-only: visible for discovery, but concrete factory is behind a
  build tag or not ready in the default build.
- planned: named direction, not a current implementation contract.
- internal scaffold: temporary runtime/compiler code that should keep moving
  toward shared reusable components.

## Component Contract

Reusable components should document these points as they mature:

- accepted message shape: packet, frame, event, or mixed;
- emitted message shape;
- hot-path allocation expectations after warm-up;
- buffer ownership and borrowed-buffer lifetime;
- event forwarding and event consumption;
- EOS, flush, discontinuity, packet loss, codec-change, and backpressure
  behavior;
- direct graph and bounded buffered graph safety;
- scratch/result objects callers are expected to reuse.

## Allocation Proofs

Hot-path allocation guards live next to the domain packages that own the
contract:

- `av`: `TestCoreResetAllocs`, `TestTimeBaseHelpersAllocs`
- `pipeline`: `TestMessageAndScratchResetAllocs`, `TestGraphDirectRunAllocs`,
  `TestDropControllerDecideAllocs`
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

## Core Media

| Component | Status | Accepts/Emits | Contract Notes |
| --- | --- | --- | --- |
| `av.Packet` | stable | packet data | Carries stream ID, timestamps, codec epoch, keyframe and loss metadata, and caller-owned or borrowed payload buffers. |
| `av.Frame` | stable | frame data | Carries decoded media planes, stream identity, timestamps, and ownership metadata. |
| `av.Event` | stable | lifecycle/control | Carries stream-added, EOS, discontinuity, loss, keyframe, codec-change, and backpressure signals. |
| `av.Buffer` / `av.Plane` | stable | owned or borrowed bytes | Ownership flags define whether downstream may retain, copy, or reuse memory. |
| Reset helpers | stable | reusable structs | Hot paths reset packets, frames, events, and result structs instead of allocating new ones. |

## Pipeline

| Component | Status | Accepts/Emits | Contract Notes |
| --- | --- | --- | --- |
| `pipeline.Message` | stable | packet/frame/event | Single graph message envelope used by every component. |
| `pipeline.Source` | stable | emits messages | Owns an input boundary and fills caller-provided message/result storage. |
| `pipeline.Stage` | stable | message to messages | Transforms, filters, meters, depacketizes, decodes, encodes, or routes events. |
| `pipeline.Sink` | stable | consumes messages | Owns output, collection, or side-effect boundaries. |
| `pipeline.Emitter` | stable | emits messages | Lets stages forward packets, frames, and events without changing graph topology. |
| `pipeline.Route` | stable | graph edge policy | Describes one edge with optional stream or event scoping. |
| fanout routing | stable | one-to-many edges | One upstream can feed several downstream stages or sinks. |
| `MediaPlan` | experimental | recipe intent to composable branch IR | Captures inputs, stream selectors, branch operations, branch stream caps, target groups, taps, and decisions before graph construction. Transcode is represented as ordinary branches, not as a runtime mode. |
| `PlanReport` | experimental | recipe intent to structured explanation | `Job.Explain(ctx)` reports inputs, stream branches, branch operations with output caps, branch/tap stream caps, planner decisions, targets, adapter requirements, filter/container capability details, warnings, and the graph without embedding a text or diagram renderer in core. |
| runtime attach | experimental | running graph to new branch | `Task.Attach(ctx, goav.Branch(...).FromTap(...))` attaches downstream branches to future messages in direct graphs and bounded buffered graphs; late branches can run custom stages, resize/resample from frame taps, encode Opus/VP8/VP9 from frame taps into file/URI targets, copy packet taps into file/URI targets, inherit planned tap caps from probe/live intent and transform output, expose nested frame or packet taps, report branch-owned node counters through `Attachment.Stats()`, and detach as a subtree through `Attachment.Close(ctx)` or `Task.Detach(ctx, h)`, including dependent branches anchored from nested taps while buffered graphs keep running. `Task.Taps()` lists stable recipe and runtime outlets. |
| `pipeline.BufferPolicy` | experimental | execution policy | Controls direct, buffered, backpressure, and dropping behavior where supported. |
| graph stats | experimental | counters/events | `Task.Stats()` exposes graph packet, frame, event, drop, last-event, per-node input/output, and per-node drop counters. |

## RTP

| Component | Status | Accepts/Emits | Contract Notes |
| --- | --- | --- | --- |
| `rtpav.Source` | stable | Pion RTP reader to packets/events | Reads RTP packets, applies payload maps, loss/timestamp tracking, depacketizers, EOS, and feedback events. |
| `rtpav.StaticPayloadMap` | stable | RTP payload metadata | Maps payload type to stream and codec information without inventing new RTP primitives. |
| `rtpav.SequenceDetector` | stable | RTP sequence state | Detects packet loss and discontinuity without allocation. |
| `rtpav.JitterBuffer` | experimental | RTP packet ordering | Bounded packet ordering for realtime receive. |
| `rtpav.Depacketizer` | stable | RTP payload to `av.Packet` | Codec-specific depacketization contract for reusable RTP receive. |
| Opus/VP8/VP9/H264/AV1 depacketizers | stable | RTP payload to packets/events | Strip RTP payload headers, assemble bounded fragments, request keyframes after loss, and drop damaged video until sync. |
| `rtpav.FeedbackWriter` | stable | RTCP feedback | Writes PLI, FIR, NACK, and related feedback through Pion-backed boundaries. |

## Codec

| Component | Status | Accepts/Emits | Contract Notes |
| --- | --- | --- | --- |
| `codec.Registry` | stable | descriptors/factories | Discovers reusable decoder and encoder factories and returns cloned descriptor capability metadata for planning. |
| `codec.Descriptor` | stable | codec metadata | Owns codec identity, media type, modes, realtime status, and sample/pixel/RTP/build-tag capabilities. Decoder and encoder descriptors participate in planned and runtime branch preflight when stream metadata is known. |
| `codec.DecoderFactory` / `EncoderFactory` | stable | stage factories | Construct concrete codec components from selected stream config. |
| `codec.DecoderStage` | stable | packets to frames/events | Decodes packets, preserves realtime events, drives loss behavior, and flushes before EOS. |
| `codec.EncoderStage` | stable | frames to packets/events | Encodes frames, observes control events, preserves lifecycle events, and flushes delayed packets before EOS. |
| `codec.DecodeResult` / `EncodeResult` | stable | reusable result storage | Caller-owned result slices are reset and reused on hot paths. |
| decode state factories | experimental | adapter scratch | Factories can provision large adapter-owned or caller-owned state before decoder construction. |

## Format

| Component | Status | Accepts/Emits | Contract Notes |
| --- | --- | --- | --- |
| `format.Registry` | stable | probers/demuxers/muxers | Owns container discovery, descriptor metadata, and factory lookup. |
| `format.Descriptor` | experimental | container capability metadata | Describes container format, accepted media kinds, codecs, stream-count bounds, realtime flags, and adapter metadata for planning and `Explain(ctx)`. |
| `format.Prober` | stable | input metadata | Detects format and, when possible, stream metadata before open. |
| `format.Demuxer` / `Muxer` | stable | container I/O | Reusable file/protocol container contracts. |
| `format.DemuxSource` | stable | container to packets/events | Emits stream-added events, packets, and EOS through the pipeline. |
| `format.MuxStage` | stable | packets/events to container | Writes muxed packets, consumes upstream lifecycle, and emits muxer-produced events. |
| `format.ReadResult` / `WriteResult` | stable | reusable result storage | Caller-owned packet/write result storage avoids hot-path allocation. |

## Filters

| Component | Status | Accepts/Emits | Contract Notes |
| --- | --- | --- | --- |
| `filter.Registry` | stable | filter factories | Discovers reusable frame-transform factories. |
| `filter.FrameFilter` | stable | frames to frames/events | Concrete transform contract for resize, resample, and future filters. |
| `filter.SimpleRegistry` | stable | filter descriptors/factories | Retains `filter.Descriptor` metadata so planning and `Explain(ctx)` can report transform input/output media kind, supported pixel formats, sample formats, resize modes, realtime/stateless flags, and adapter metadata. |
| `filter.Stage` | stable | frames/events to frames/events | Adapts frame filters into pipeline stages while preserving events and flushing before EOS. |
| `filter.Result` | stable | reusable result storage | Caller-owned frame/event slices are reset and reused. |

## WebRTC

| Component | Status | Accepts/Emits | Contract Notes |
| --- | --- | --- | --- |
| `webrtcav.TrackReader` | stable | Pion TrackRemote to RTP reader | Exposes a Pion track as the same packet-reader boundary used by `rtpav.Source`. |
| `webrtcav.TrackSet` | experimental | accepted tracks to readers | Coordinates track acceptance, same-stream replacement, codec updates, reader ordering, and closure. |
| Track codec update boundary | experimental | Pion codec params to events | Emits codec-change events that RTP sources and downstream stages can react to. |
| Track replacement boundary | experimental | replacement TrackRemote | Keeps one logical reader while swapping the underlying Pion track. |

## Adapters

| Adapter | Status | Contract Notes |
| --- | --- | --- |
| `adapters/gopus` | stable | Concrete Opus decode adapter for `codec.DecoderStage`. |
| `adapters/govpx` | experimental | VP8/VP9 decode and encode behind build tags; descriptor-only in default builds. |
| `adapters/goav1` | experimental | AV1 decode behind build tags; descriptor-only in default builds. |
| `adapters/goh264` | experimental | H264 decode behind build tags; descriptor-only in default builds. |
| `adapters/ivf` | stable | Packet demux/mux for VP8, VP9, and AV1 IVF streams. |
| `adapters/annexb` | stable | Packet mux for H264 Annex B streams. |
| `adapters/resample` | experimental | S16 audio resample/channel conversion filter. |
| `adapters/resize` | experimental | I420/YUV420P exact, fit, fill, and passthrough video resize filter. |

## Manual Graph Patterns

These patterns are the component shapes recipes should compile toward. They
are also the shapes advanced users can wire manually with `Runtime.Graph()`.
The direct component tests capture `pipeline.Spec` before execution and compare
it after `Run`, so the graph users inspect is the graph that actually ran.

### RTP Opus Decode

```text
rtpav.Source
  -> rtpav.OpusDepacketizer
  -> codec.DecoderStage
  -> goav.FrameFunc meter or pipeline.Sink
```

Use this for realtime audio analyzers, bots, monitors, and receive pipelines
that need direct ownership of loss, codec-change, EOS, and feedback behavior.
`TestComponentRTPOpusDecodeGraph` covers this shape with a Pion RTP packet
reader boundary, `rtpav.Source`, the Opus depacketizer, a concrete Opus
decoder, and caller-owned frame buffers.

### File Remux Fanout

```text
format.DemuxSource
  -> format.MuxStage archive
  -> format.MuxStage preview
```

Use this when packet formats already match and the graph should stay
packet-preserving. `TestComponentFileRemuxFanout` covers this shape without
using recipe builders.

### Analysis Hook

```text
input source
  -> goav.PacketFunc or goav.FrameFunc meter
  -> goav.EventFunc logger
  -> sink
```

Function helpers are for small reusable hooks. Implement `pipeline.Stage`
directly when the component needs explicit lifecycle, scratch reuse, or
backpressure-specific behavior.

`TestComponentCodecStageFlushesOnEOS` and
`TestComponentMuxStageEmitsWriteEvents` cover codec and format stages as
directly wired graph components.

### WebRTC Receive

```text
webrtcav.TrackReader or webrtcav.TrackSet
  -> rtpav.Source
  -> depacketizer
  -> mux, decode, relay, or analysis stages
```

This keeps Pion types at the WebRTC/RTP boundary while the rest of the graph
uses `av`, `pipeline`, `codec`, `format`, and `filter` contracts.
`TestComponentWebRTCTrackSetFeedsRTPSource` covers TrackSet replacement,
long-lived reader reuse, and direct graph receive through `rtpav.Source`.

## Custom Stage Checklist

A reusable custom stage should:

- implement `pipeline.Stage`;
- allocate scratch during construction or open, not in `Handle`;
- forward events it does not consume;
- flush before forwarding EOS when it buffers data;
- return deterministic errors for unsupported media or missing scratch;
- document whether it is safe with direct and bounded buffered execution;
- document ownership expectations for borrowed packets, frames, and planes.

`TestComponentCustomStageForwardsEvents` covers the minimal event-forwarding
shape with reusable stage-owned scratch.

# Adapters

Adapters keep codec, container, and filter implementations out of the core
import graph. Use this page to see what ships today, which paths are narrow by
design, and where to start if you need to add another implementation.

Core packages (`av`, `codec`, `format`, `filter`, `pipeline`, `rtpav`,
`webrtcav`) do not import sibling codec modules; concrete integrations live
under `adapters/...` and `container/...`.

The root module keeps third-party dependencies pinned by
`TestRootModuleDependencyPurity`: only the narrow modernc runtime set required
by the built-in pure-Go AAC backend is allowed outside `github.com/thesyncim/*`
and the standard library. `rtpav` and `webrtcav` are nested modules with their
own `go.mod`; they carry the Pion dependency tree, and importing goav alone
never pulls it in. Import paths are unchanged.

To write an adapter, use `docs/ADAPTER_AUTHORING.md` for the extension
interfaces, lifecycle rules, error and ownership contracts, and required
tests. The executable proof that every extension point works from outside core
is `adapterproof/adapter_compat_test.go`.

## Rules

- Implement `codec.DecoderFactory`, `codec.EncoderFactory`,
  `format.DemuxerFactory`, `format.MuxerFactory`, or `filter.Factory`.
- Application-local factories register with `goav.WithDecoder`, `WithEncoder`,
  `WithFilter`, `WithMuxer`, `WithDemuxer`, and `WithProber`. Adapter packages
  should still expose an explicit `Register(...)` hook for runtime bundles.
- Allocate only during construction or `Open`. Hot-path methods use
  caller-owned result structs and preallocated output buffers.
- Do not register encode/decode factories until the path is real. Optional
  modules stay behind build tags or compile-safe descriptor packages.
- Descriptors own codec identity, media type, modes, realtime, and experimental
  status; capability fields list concrete sample/pixel formats, RTP payloads,
  and build tags. Discovery comes from registered descriptors, not a central
  list. Descriptor-only registrations return `codec.ErrUnavailable` from
  factory lookup until a concrete factory is registered (not `ErrNotFound`);
  once registered, descriptor capabilities become preflight constraints for
  recipe and runtime decode/encode.
- Realtime decoders that need large internal arenas should honor
  `codec.DecodeConfig.Bounds` and document any `OpaqueState` type they accept;
  factories may implement `codec.DecodeStateFactory` so high-level runtimes can
  provision adapter state.

For codecs outside the built-in Opus, VP8, VP9, H264, and AV1 specs, use
`codec.Codec(id, media, ...)` in recipes; the planner treats custom specs the
same as built-ins.

## Current Adapters

| Adapter | Status |
| --- | --- |
| `adapters/ivf` | IVF VP8/VP9/AV1 packet demux/mux; magic/extension probing; zero-alloc read/write. Single video stream, no indexing or codec conversion. |
| `adapters/annexb` | H264 Annex B packet mux (mux-only) for packet-preserving recording after RTP depacketization; start-code probing; zero-alloc write. |
| `container/matroska`, `container/webm` | Matroska/WebM mux/demux (see `docs/matroska.md`). |
| `adapters/gopus` | Opus decode/encode over `thesyncim/gopus`: depacketized packet <-> PCM frames, PLC via `EventPacketLoss`, caller-owned buffers. |
| `adapters/goaac` | AAC-LC decode over `thesyncim/goaac`: ADTS packets by default, raw AAC access units when stream `ExtraData` carries AudioSpecificConfig, interleaved S16 PCM into caller-owned buffers. Decode-only. |
| `adapters/govpx` | VP8/VP9 decode into caller-owned I420 frames and encode into caller-owned packet buffers; drop-until-keyframe on loss/corruption/discontinuity; keyframe requests via `codec.ControlRequest`; encode honors keyframe-request and codec-change events; zero-alloc hot paths; idempotent close. |
| `adapters/goav1` | AV1 low-overhead decode over `thesyncim/goav1`: `DecoderState` as documented `OpaqueState`, optional state factory with RTP decode bounds, borrowed gray8/I420/I422/I444 output (yuv* aliases normalized), loss/sync recovery from keyframe markers or parsed payloads, concrete `DecodeRTPPayloadInto` for raw RTP payload callers, runner reuse, allocation/lifecycle guards. The exact frame format matters: the backend frame pool must match the accepted sequence format. |
| `adapters/goh264` | Descriptor-only by default; the `goav_goh264` tag enables H264 decode (8-bit planar borrowed frames, keyframe requests on loss, zero-alloc mapping, idempotent close). Decode-only. |
| `adapters/resample` | Interleaved S16 PCM sample-rate (linear) and mono/stereo channel conversion; descriptor metadata for `Explain(ctx)`; caller-owned output; zero-alloc hot path. |
| `adapters/resize` | Planar 8-bit 4:2:0 (`i420`/`yuv420p`) exact/fit/fill/passthrough nearest-neighbor resize; descriptor metadata; caller-owned planes; zero-alloc hot path. |

Every "zero-alloc" entry above is test-enforced, not aspirational: each
adapter carries a `testing.AllocsPerRun` guard (`TestMuxerWriteAllocs`,
`TestDemuxerReadIntoAllocs`, `TestFilterAllocs`, decoder/encoder `*Allocs`
tests) that fails the suite if the hot path starts allocating. The repo-wide
proven/not-proven map is `docs/PERFORMANCE.md`.

All adapters are intentionally narrow; richer features (SVC controls, color
metadata, high bit depth, floating-point PCM, higher-quality scalers, H264
parsing/demux/encode) are future slices.

## Descriptor-only boundaries

Default-build `govpx`, `goav1`, and `goh264` expose descriptors without
importing concrete implementations, so applications can see planned media
compatibility and build registries without forcing tagged code paths. Concrete
factories replace descriptor-only registrations once each path has caller-owned
output buffers and allocation tests.

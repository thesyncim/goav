# Adapters

Adapters keep codec, container, and filter implementations out of the core
import graph. Use this page to see what ships today, which paths are narrow by
design, and where to start if you need to add another implementation.

Core packages (`av`, `codec`, `format`, `filter`, `pipeline`, `rtpav`,
`webrtcav`, `playoutav`) do not import sibling codec modules; concrete
integrations live under `adapters/...` and `container/...`.

The root module keeps third-party dependencies pinned by
`TestRootModuleDependencyPurity`: only the narrow modernc runtime set required
by the built-in pure-Go AAC backend is allowed outside `github.com/thesyncim/*`
and the standard library. `rtpav`, `webrtcav`, and `playoutav` are nested
modules with their own `go.mod`; RTP/WebRTC carry the Pion dependency tree,
while playout is dependency-light. Importing goav alone never pulls transport
modules in. Import paths are unchanged.

To write an adapter, use `docs/ADAPTER_AUTHORING.md` for the extension
interfaces, lifecycle rules, error and ownership contracts, and required
tests. The executable proof that every extension point works from outside core
is `adapterproof/adapter_compat_test.go`.

## Rules

- Implement `codec.DecoderFactory`, `codec.EncoderFactory`,
  `format.DemuxerFactory`, `format.MuxerFactory`, or `filter.Factory`.
- Application-local factories register with `runconfig.WithDecoder`, `WithEncoder`,
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

`playoutav` is the third source-provider proof: it adapts scheduled packets,
frames, and events through `provider.Source` as a nested module, with no core
runtime/provider changes beyond docs and CI enumeration.

<!-- BEGIN GENERATED BUNDLE CAPABILITIES -->
The tables below are generated from `bundle.Options()` and the descriptors registered by the bundled adapters. Update the descriptors, not this section by hand.

### Container Formats

| Format | Direction | Media | Codecs | Streams | Notes |
| --- | --- | --- | --- | --- | --- |
| `ivf` | demux | video | av1, vp8, vp9 | 1 | IVF destinations support one VP8, VP9, or AV1 video stream |
| `matroska` | demux | audio, subtitle, video | aac, av1, flac, h264, opus, pcm, text_utf8, vorbis, vp8, vp9 | 1+ | Matroska supports multi-track audio, video, and subtitle packets without codec conversion |
| `mp4` | demux | audio, video | aac, av1, flac, h264, opus, vp8, vp9 | 1+ | MP4 demux reads common ISO BMFF audio and video sample entries |
| `webm` | demux | audio, video | av1, opus, vorbis, vp8, vp9 | 1+ | WebM supports Opus/Vorbis audio and VP8/VP9/AV1 video packets without codec conversion |
| `annexb` | mux | video | h264 | 1 | Annex B destinations support one H264 video stream |
| `ivf` | mux | video | av1, vp8, vp9 | 1 | IVF destinations support one VP8, VP9, or AV1 video stream |
| `matroska` | mux | audio, subtitle, video | aac, av1, flac, h264, opus, pcm, text_utf8, vorbis, vp8, vp9 | 1+ | Matroska supports multi-track audio, video, and subtitle packets without codec conversion |
| `webm` | mux | audio, video | av1, opus, vorbis, vp8, vp9 | 1+ | WebM supports Opus/Vorbis audio and VP8/VP9/AV1 video packets without codec conversion |

### Codecs

| Codec | Media | Modes | Backend | Capabilities | Tags | Status |
| --- | --- | --- | --- | --- | --- | --- |
| `aac` | audio | decode | `goaac` | profiles=aac_lc; samples=s16; realtime | - | active |
| `av1` | video | decode | `goav1` | pixels=gray8, i420, i422, i444, yuv420p, yuv422p, yuv444p; rtp=video/av1; realtime; experimental | - | active |
| `av1` | video | encode | `goav1-encoder` | pixels=i420, yuv420p; rtp=video/av1; realtime; experimental | - | active |
| `h264` | video | decode | `goh264` | pixels=yuv420p; rtp=video/h264; realtime; experimental | goav_goh264 | planned-build-tagged |
| `opus` | audio | decode, encode | `gopus` | samples=s16; rtp=audio/opus; realtime | - | active |
| `vp8` | video | decode, encode | `govpx` | pixels=i420; rtp=video/vp8; realtime; experimental | - | active |
| `vp9` | video | decode, encode | `govpx` | pixels=i420; rtp=video/vp9; realtime; experimental | - | active |

### Frame Filters

| Filter | Media | Formats | Modes | Traits |
| --- | --- | --- | --- | --- |
| `resample` | audio -> audio | samples=s16 | - | backend=resample; realtime; stateless |
| `resize` | video -> video | pixels=i420, yuv420p | exact, fill, fit, passthrough | backend=resize; realtime; stateless |
<!-- END GENERATED BUNDLE CAPABILITIES -->

Every "zero-alloc" adapter claim is test-enforced, not aspirational: each
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
output buffers and allocation tests. `goh264` remains decode-only in the
default bundle for recipe encode: applications that need H.264 encoding must
register a vetted encoder explicitly with `runconfig.WithEncoder` or a codec
adapter.

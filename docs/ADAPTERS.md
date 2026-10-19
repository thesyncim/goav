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
and the standard library. `rtpav`, `webrtcav`, `playoutav`, `player`, and
`cmd/goav` are nested modules with their own `go.mod`; RTP/WebRTC carry the
Pion dependency tree, the player Ebitengine and oto, while playout is
dependency-light. Importing goav alone never pulls them in. Import paths are unchanged.

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
- H.264 packets travel as Annex B inside goav: demuxers of length-prefixed
  containers (MP4, Matroska) convert on read, their muxers convert back, and
  the Annex B muxer writes the avcC parameter sets ahead of keyframes.
- A packet's PTS is the time of its first decoded sample, so codec delay shows
  before zero: an Opus stream's first packet sits one pre-skip early. Opus
  ExtraData is always an RFC 7845 OpusHead (MP4 `dOps` converts on read);
  Matroska and WebM subtract `CodecDelay` on read and add it back on write;
  the Opus decoder drops the pre-skip, and the encoder reports its lookahead
  as pre-skip through the optional `codec.ParameterReporter`.
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

### Player

`player.New()` is a playback window: route decoded video and audio to
`w.Destination()` (or to `play` in a program, via `w.Element()`) and call
`w.Run(ctx, task)` on the main goroutine; `goav play FILE` does all of it. The
speaker is the master clock: `w.Clock()` (the device's played position) is the
task timeline's inner clock (`runconfig.WithClock`, with `WithRealtime(true)`),
so media arrives as fast as it is heard, video is shown against the heard
position (late frames dropped, early ones waited for), and space, the arrows,
and `[`/`]` are the task's own Pause, Seek, and Rate. Conversion is
limited-range BT.601/BT.709 on the CPU; at rates other than 1x the pitch moves.

| OS (amd64, arm64) | Window, audio | `CGO_ENABLED=0` | Needs at run time |
| --- | --- | --- | --- |
| macOS | Ebitengine (Metal), oto (AudioToolbox) via purego | builds, played on arm64 | nothing extra |
| Windows | Ebitengine (DirectX), oto (WASAPI) via syscall | builds; not run | nothing extra |
| Linux | Ebitengine (X11, OpenGL) via purego dlopen; oto (PulseAudio in Go, ALSA fallback) | builds; not run | libX11, libGL; PulseAudio/PipeWire or libasound |

No OS needs cgo; `CGO_ENABLED=1` builds too. On Linux the dlopen makes the
binary dynamically linked, so `cmd/goav` has a `goav_noplay` build tag for a
static CLI. Without an audio device playback continues silently.

<!-- BEGIN GENERATED BUNDLE CAPABILITIES -->
The tables below mirror `bundle.Options()` and the descriptors registered by the bundled adapters; change them together with the descriptors.

### Container Formats

| Format | Direction | Media | Codecs | Streams | Notes |
| --- | --- | --- | --- | --- | --- |
| `ivf` | demux | video | av1, vp8, vp9 | 1 | IVF destinations support one VP8, VP9, or AV1 video stream |
| `matroska` | demux | audio, subtitle, video | aac, av1, flac, h264, opus, pcm, text_utf8, vorbis, vp8, vp9 | 1+ | Matroska supports multi-track audio, video, and subtitle packets without codec conversion |
| `mp4` | demux | audio, video | aac, av1, flac, h264, opus, vp8, vp9 | 1+ | MP4 demux reads common ISO BMFF audio and video sample entries in decode-time order, honors edit lists (B-frame delay, AAC priming, start delays), with keyframe seek |
| `webm` | demux | audio, video | av1, opus, vorbis, vp8, vp9 | 1+ | WebM supports Opus/Vorbis audio and VP8/VP9/AV1 video packets without codec conversion |
| `annexb` | mux | video | h264 | 1 | Annex B destinations support one H264 video stream |
| `ivf` | mux | video | av1, vp8, vp9 | 1 | IVF destinations support one VP8, VP9, or AV1 video stream |
| `matroska` | mux | audio, subtitle, video | aac, av1, flac, h264, opus, pcm, text_utf8, vorbis, vp8, vp9 | 1+ | Matroska supports multi-track audio, video, and subtitle packets without codec conversion |
| `mp4` | mux | audio, video | aac, av1, flac, h264, opus, vp8, vp9 | 1+ | MP4 mux writes a progressive file to a seekable output and fragmented MP4 (a moof per video keyframe) to a pipe or realtime output, with edit lists for start delays, B-frame delay, and Opus pre-skip |
| `webm` | mux | audio, video | av1, opus, vorbis, vp8, vp9 | 1+ | WebM supports Opus/Vorbis audio and VP8/VP9/AV1 video packets without codec conversion |

### Codecs

| Codec | Media | Modes | Backend | Capabilities | Tags | Status |
| --- | --- | --- | --- | --- | --- | --- |
| `aac` | audio | decode | `goaac` | profiles=aac_lc; samples=s16; realtime | - | active |
| `av1` | video | decode | `goav1` | pixels=gray8, i420, i422, i444, yuv420p, yuv422p, yuv444p; rtp=video/av1; realtime; experimental | - | active |
| `av1` | video | encode | `goav1-encoder` | pixels=i420, yuv420p; rtp=video/av1; realtime; experimental | - | active |
| `h264` | video | decode | `goh264` | pixels=yuv420p; rtp=video/h264; realtime; experimental | goav_goh264 | planned-build-tagged |
| `opus` | audio | decode, encode | `gopus` | samples=s16; rates=8000,12000,16000,24000,48000; rtp=audio/opus; realtime | - | active |
| `vp8` | video | decode, encode | `govpx` | pixels=i420; rtp=video/vp8; realtime; experimental | - | active |
| `vp9` | video | decode, encode | `govpx` | pixels=i420; rtp=video/vp9; realtime; experimental | - | active |

### Frame Filters

| Filter | Media | Formats | Modes | Traits |
| --- | --- | --- | --- | --- |
| `resample` | audio -> audio | samples=s16 | - | backend=resample; realtime |
| `resize` | video -> video | pixels=i420, yuv420p | exact, fill, fit, passthrough | backend=resize; realtime; stateless |
<!-- END GENERATED BUNDLE CAPABILITIES -->

#### Platform matrix

Descriptor `Backend.Status` reports what the compiled target can do. The table
above is for the host that regenerated it.

| Codec | Backend | darwin/arm64, linux/arm64 | Other targets |
| --- | --- | --- | --- |
| `aac` decode | `goaac` | `active` | `unsupported-target`: the backend is a stub and `Open` fails with `ErrNativeUnavailable` |

`TestDescriptorStatusMatchesBackendAvailability` pins the descriptor to the
backend's own availability.

Every "zero-alloc" adapter claim is test-enforced, not aspirational: each
adapter carries a `testing.AllocsPerRun` guard (`TestMuxerWriteAllocs`,
`TestDemuxerReadIntoAllocs`, `TestFilterAllocs`,
`TestDecodePacketLossConcealmentAllocs`, decoder/encoder `*Allocs` tests) that
fails the suite if the hot path starts allocating. The repo-wide
proven/not-proven map is [`PERFORMANCE.md`](PERFORMANCE.md).

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

## Matroska and WebM notes

`container/ebml`, `container/matroska`, and `container/webm` are the pure-Go
Matroska/WebM layer. Like every container package they may use the generic
`goav` media and format vocabulary, never codec implementations or runtime
internals.

- `container/ebml`: element IDs, VINTs, read/write helpers, bounded readers,
  unknown-size masters, Void, CRC-32, seekable size patching.
- `container/matroska`: tracks, packets, mux/demux, timestamps, seeking,
  metadata, codec mapping, format registry adapters.
- `container/webm`: a WebM profile wrapper with stricter codec, document type,
  metadata, timestamp, and content-encoding validation.

```go
muxer, err := matroska.NewMuxer(w, matroska.MuxerOptions{})
trackID, err := muxer.AddTrack(matroska.Track{
    Type:  matroska.TrackVideo,
    Codec: matroska.CodecVP8,
    Video: matroska.VideoConfig{Width: 1280, Height: 720},
})
err = muxer.WritePacket(matroska.Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: frame})
err = muxer.Close()

demuxer, err := matroska.NewDemuxer(r, matroska.DemuxerOptions{})
tracks := demuxer.Tracks()
packet := matroska.Packet{Data: make([]byte, 0, maxPacket)}
err = demuxer.ReadPacket(&packet)
```

`container/webm` has the same shape but rejects tracks and metadata outside
the WebM profile.

**Supported today**, for recording, remuxing, and WebRTC-shaped packet
workflows: EBML/VINT handling; Segment, Info, Tracks, Cluster, SimpleBlock,
BlockGroup (duration, references, discard padding, codec state, block
additions); streaming output with unknown-size masters; seekable output with
patched sizes, SeekHead, and Cues; cue-assisted seeking with packet-index
fallback; Xiph/fixed/EBML lacing; track timestamp scale/offset conversion;
attachments, chapters, tags, CRC-32; block-scope AES-CTR content encryption;
caller-owned packet buffers on demux.

**Codecs.** The packages map more than the bundle registers in the table
above. Matroska carries Opus, Vorbis, FLAC, AAC, PCMU/PCMA, VP8, VP9, AV1,
H.264, H.265, and S_TEXT/UTF8; WebM carries Opus, Vorbis, VP8, VP9, and AV1,
which covers the WebRTC codec set. An omitted Opus `OpusHead` is generated;
AV1, H.264, and H.265 configuration records are validated and can be generated
from the first packet; Vorbis, FLAC, and AAC need caller-provided private data.

**Validation.** Tests cover EBML, mux/demux round trips, WebRTC-shaped codecs,
lacing, BlockGroup and cue paths, codec-private validation and generation,
profile validation, malformed metadata and encrypted blocks, and steady-state
write/read allocation guards. External checks run when `ffmpeg`, `mkvmerge`,
or `mkvalidator` is installed; field-corpus tests and benchmarks run under
`GOAV_MATROSKA_FIELD_CORPUS` and `GOAV_WEBM_FIELD_CORPUS`.

**Not yet claimed.** This is not a stable v1 container API. It still needs a
larger real-file corpus, differential tests against mature tools, a hardened
corrupt corpus, an API review (names, ownership, seek semantics, possible
extraction), and committed benchmark numbers before any performance-parity
claim. Protocol-layer repair, retransmission, FEC, jitter buffering, and
depacketization stay outside these packages.

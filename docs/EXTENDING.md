# Adapters & Extending goav

`goav` has no global registry. `bundle.New()` registers the bundled pure-Go
adapters; `goav.New(opts...)` builds a runtime with only the adapters you choose.
Root dependency purity is enforced by `TestRootModuleDependencyPurity`.

## What Ships Today

### Native & Opt-In Modules

- **`libcodec`** (purego, no cgo): `videotoolbox` (macOS H.264/HEVC 8-bit hardware), `audiotoolbox` (macOS AAC-LC), `openh264` (macOS/Linux/Windows H.264 — Cisco binary license requires attribution and `GOAV_LIBCODEC_DISABLE=openh264` toggle; see [BINARY_LICENSE.txt](../libcodec/internal/openh264/BINARY_LICENSE.txt)).
- **`gplcodec`** (opt-in copyleft): `gplcodec.AAC()`, pure-Go AAC-LC decoder (arm64 only; **GPL** — your binary becomes GPL).
- **`player`** (Ebitengine + oto via purego, `CGO_ENABLED=0`): macOS (played on arm64), Windows & Linux (cross-built in CI; Linux requires `libX11`, `libGL`, PulseAudio/PipeWire or ALSA at runtime; use `-tags goav_noplay` on `cmd/goav` for a static CLI).
- **`playoutav`** (root module, zero external deps): scheduled packet, frame, and event playout via `provider.Source` (`playoutav/playout_test.go`).

<!-- BEGIN GENERATED BUNDLE CAPABILITIES -->
The tables below mirror `bundle.All()` and the descriptors registered by the bundled adapters; change them together with the descriptors.

### Container Formats

| Format | Direction | Media | Codecs | Streams | Notes |
| --- | --- | --- | --- | --- | --- |
| `ivf` | demux | video | av1, vp8, vp9 | 1 | IVF destinations support one VP8, VP9, or AV1 video stream |
| `matroska` | demux | audio, subtitle, video | aac, av1, flac, h264, opus, pcm, text_utf8, vorbis, vp8, vp9 | 1+ | Matroska supports multi-track audio, video, and subtitle packets without codec conversion |
| `mp4` | demux | audio, video | aac, av1, flac, h264, hevc, opus, vp8, vp9 | 1+ | MP4 demux reads common ISO BMFF audio and video sample entries in decode-time order, honors edit lists (B-frame delay, AAC priming, start delays), with keyframe seek |
| `webm` | demux | audio, video | av1, opus, vorbis, vp8, vp9 | 1+ | WebM supports Opus/Vorbis audio and VP8/VP9/AV1 video packets without codec conversion |
| `annexb` | mux | video | h264 | 1 | Annex B destinations support one H264 video stream |
| `ivf` | mux | video | av1, vp8, vp9 | 1 | IVF destinations support one VP8, VP9, or AV1 video stream |
| `matroska` | mux | audio, subtitle, video | aac, av1, flac, h264, opus, pcm, text_utf8, vorbis, vp8, vp9 | 1+ | Matroska supports multi-track audio, video, and subtitle packets without codec conversion |
| `mp4` | mux | audio, video | aac, av1, flac, h264, hevc, opus, vp8, vp9 | 1+ | MP4 mux writes a progressive file to a seekable output and fragmented MP4 (a moof per video keyframe) to a pipe or realtime output, with edit lists for start delays, B-frame delay, and Opus pre-skip |
| `webm` | mux | audio, video | av1, opus, vorbis, vp8, vp9 | 1+ | WebM supports Opus/Vorbis audio and VP8/VP9/AV1 video packets without codec conversion |

### Codecs

| Codec | Media | Modes | Backend | Capabilities | Tags | Status |
| --- | --- | --- | --- | --- | --- | --- |
| `av1` | video | decode | `goav1` | pixels=gray8, i420, i422, i444, yuv420p, yuv422p, yuv444p; rtp=video/av1; realtime; experimental | - | active |
| `av1` | video | encode | `goav1` | pixels=i420, yuv420p; rtp=video/av1; realtime; experimental | - | active |
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

## Licenses

`goav` is Apache-2.0. Everything `bundle` links by default is permissive;
`-tags goav_goh264` adds LGPL H.264 decode, and importing `gplcodec` opts into
GPL. `TestRootModuleDependencyPurity` pins every root dependency's license.

## Extension Points

Every extension plugs in through a small interface and `runconfig.Option`—no
changes to core required (proven out-of-tree by `adapterproof/adapter_compat_test.go`
and `adapterproof/join_proof_test.go`):

| Extension | Interface | Registration / Entry | Runnable Example |
| --- | --- | --- | --- |
| Source / transport | `provider.Input` / `source.Func` | `goav.From(p)` / `goav.Source(...)` | [`examples/custom-source`](../examples/custom-source), [`examples/provider-source`](../examples/provider-source) |
| Codec | `codec.DecoderFactory` / `codec.EncoderFactory` | `runconfig.WithDecoder` / `WithEncoder` | [`examples/custom-codec`](../examples/custom-codec) |
| Frame filter | `filter.Factory` (`FilterInto`) | `runconfig.WithFilter` | [`examples/custom-filter`](../examples/custom-filter) |
| Container format | `format.DemuxerFactory` / `format.MuxerFactory` | `runconfig.WithDemuxer` / `WithMuxer` / `WithProber` | `container/matroska`, `container/mp4` |
| Custom stage / sink | `component.FrameFunc` / `PacketFunc` / `SinkFunc` | `stream.Do(...)` / `goav.Sink(...)` | [`examples/custom-destination`](../examples/custom-destination) |
| Custom join | `pipeline.Stage` (+ `InputShapes`/`OutputShapes`) | `goav.Join(name, stage, streams...)` | [`examples/custom-join`](../examples/custom-join) |
| Control-plane host | `ctlserver.Method` / `launch.Stage` | `ctlserver.New(task, ...)` | [`examples/control-plane-host`](../examples/control-plane-host) |

### Adapter Contracts

- **Hot-path rules:** allocate only during construction or `Open`. Hot-path `DecodeInto`/`EncodeInto`/`FilterInto`/`ReadInto`/`Write` fill caller-owned result structs, reuse buffers via `av.NewBuffer`, and return capacity errors instead of growing silently.
- **Buffer ownership:** published `av.Buffer` bytes are immutable; call `Ref`/`Retain` to keep beyond the callback and `Release` when done (`-tags goavdebug` poisons released bytes with `0xDB`).
- **Bitstream conventions:** H.264/HEVC packets travel as Annex B inside `goav` (container demuxers/muxers convert length-prefixed samples at the boundary); Opus `ExtraData` is always RFC 7845 `OpusHead` and PTS is first decoded sample time.
- **Descriptors:** register concrete factories only when backed by a real implementation. Descriptor-only registrations return `codec.ErrUnavailable` until a factory is registered.

# Adapters

Adapters keep codec, container, and filter implementations out of the core
import graph.

Core packages such as `av`, `codec`, `format`, `filter`, `pipeline`, `rtpav`,
and `webrtcav` should not import sibling codec modules directly. Concrete
integrations belong under `adapters/...`.

## Rules

- Implement `codec.DecoderFactory`, `codec.EncoderFactory`,
  `format.DemuxerFactory`, `format.MuxerFactory`, or `filter.Factory`.
- Register with an explicit registry instance; blank-import registration can be
  added later as optional convenience.
- Allocate only during construction or `Open`.
- Hot-path methods must use caller-owned result structs and preallocated output
  buffers.
- Do not register encode/decode factories until the path is real.
- Optional or unavailable modules should stay behind build tags or compile-safe
  descriptor packages.
- Descriptor-only registrations may advertise planned capabilities, but factory
  lookup must fail with `codec.ErrUnavailable` until a concrete factory is
  registered.
- Realtime decoders that need large internal arenas should honor
  `codec.DecodeConfig.Bounds` and document any adapter-specific
  `OpaqueState` type they accept.

## Current Adapters

| Adapter | Status |
| --- | --- |
| `adapters/ivf` | IVF VP8/VP9/AV1 packet demux/mux |
| `adapters/annexb` | H264 Annex B packet mux |
| `adapters/resample` | pure-Go S16 audio resample and channel conversion filter |
| `adapters/resize` | pure-Go I420/YUV420P video resize filter |
| `adapters/gopus` | Opus decode to caller-owned `s16` frames, PLC on packet-loss events |
| `adapters/govpx` | descriptor-only by default; `goav_govpx` enables VP8/VP9 decode and encode |
| `adapters/goav1` | descriptor-only AV1 boundary |
| `adapters/goh264` | descriptor-only by default; `goav_goh264` enables H264 decode |

## `ivf`

The `ivf` adapter is the first concrete format adapter. It supports one video
stream with VP8, VP9, or AV1 packet payloads.

Current surface:

- explicit registry registration through `ivf.Register`
- IVF magic and extension probing
- stream metadata from IVF headers
- demux into caller-owned `av.Packet` payload buffers
- mux from packet payloads without rewriting codec data
- zero-allocation read/write hot-path tests

It is intentionally narrow: no indexing, no frame parsing, no multi-stream
container behavior, and no codec conversion.

## `annexb`

The `annexb` adapter supports packet-preserving H264 recording after RTP
depacketization has produced Annex B access-unit payloads.

Current surface:

- explicit registry registration through `annexb.Register`
- `.h264`, `.264`, `.annexb`, and start-code probing
- mux from one H264 video packet stream
- zero-allocation write hot-path tests

It is mux-only for now. H264 parsing, demuxing, and codec decode belong in later
adapter slices.

## `gopus`

The `gopus` adapter wraps `github.com/thesyncim/gopus`.

Current surface:

- descriptor for Opus decode
- explicit registry registration
- RTP Opus depacketized packet to PCM frame path
- packet-loss concealment through `EventPacketLoss`
- caller-owned frame and plane buffer output

It does not currently claim encode support.

## `govpx`

The `govpx` adapter wraps `github.com/thesyncim/govpx` only when built with
`goav_govpx`. Normal builds keep VP8 and VP9 descriptors visible without
importing the module.

Current tagged surface:

- explicit registry registration through `govpx.Register`
- VP8 and VP9 packet decode through `DecodeIntoWithPTS`
- decoded I420 planes are written into caller-owned `av.Frame` buffers
- VP8 and VP9 encode write packet payloads into caller-owned
  `codec.EncodeResult` buffers
- startup, packet-loss, corrupt-packet, and discontinuity paths drop until the
  next VP8 or VP9 keyframe
- packet-loss paths request keyframes through `codec.ControlRequest`
- codec-change and discontinuity events reset decoder state and update stream
  identity
- encode keyframe request, codec-change, and discontinuity events force the next
  VP8 or VP9 packet to be a keyframe
- adapter-owned output preparation and keyframe request emission have
  zero-allocation hot-path tests
- close is idempotent and later decode, flush, or event calls return
  `codec.ErrClosed`

It has VP8/VP9 decode and encode for now. SVC controls, color metadata, and
format conversion remain future slices.

## `goav1`

The `goav1` adapter is still descriptor-only by default in `goav`. The
`goav_goav1` tag pins the sibling module's AV1 low-overhead and RTP payload
stream runners with caller-owned scratch, but a decoder factory should register
here only after the adapter can bind a reusable packet-by-packet runner from
`codec.DecodeConfig.Bounds`, stream metadata, and documented `OpaqueState`.

The first concrete surface should stay narrow:

- receive depacketized AV1 payload bytes and realtime events from `rtpav`
- clear retained fragments after loss/discontinuity and request a keyframe
- reset stream state on codec-change events and preserve codec epochs
- expose 8-bit 4:2:0 I420 frames first, with explicit buffer ownership
- prove lifecycle, allocation behavior, and result-capacity failures

## `resample`

The `resample` adapter is the first concrete audio filter adapter. It supports
interleaved signed 16-bit PCM audio frames.

Current surface:

- explicit registry registration through `resample.Register`
- sample-rate conversion with linear interpolation
- channel conversion for mono/stereo and simple channel count changes
- caller-owned output frame and plane buffers
- zero-allocation hot-path test

It is intentionally narrow: no floating-point PCM, no dithering, and no
streaming phase carry yet.

## `resize`

The `resize` adapter is the first concrete video filter adapter. It supports
planar 8-bit 4:2:0 frames with `i420` or `yuv420p` layout.

Current surface:

- explicit registry registration through `resize.Register`
- exact, fit, fill, and passthrough geometry modes
- deterministic nearest-neighbor scaling
- caller-owned output frame and plane buffers
- zero-allocation hot-path test

It is intentionally narrow: no RGB, NV12, high bit-depth, color conversion,
interlaced handling, or high-quality scaler yet.

## `goh264`

The `goh264` adapter wraps `github.com/thesyncim/goh264` only when built with
`goav_goh264`. Normal builds keep the descriptor visible without importing the
module.

Current tagged surface:

- explicit registry registration through `goh264.Register`
- H264 auto packet decode via the sibling module
- codec-change and discontinuity events reset decoder state
- packet-loss paths request keyframes through `codec.ControlRequest`
- decoded 8-bit Y/Cb/Cr planes are exposed as borrowed `av.Frame` planes
- adapter-owned borrowed-frame mapping and keyframe request emission have
  zero-allocation hot-path tests
- close is idempotent and later decode, flush, or event calls return
  `codec.ErrClosed`

It is decode-only and intentionally narrow for now: no high bit-depth output,
no color conversion and no encode adapter.

## Descriptor-only Boundaries

Default-build `govpx`, `goav1`, and default-build `goh264` expose descriptors
without importing concrete decoder/encoder implementations. This lets
applications see planned capabilities and build registries without breaking the
default build or forcing heavier tagged code paths. When a descriptor exists
but no factory is registered,
`DecoderFactory` or `EncoderFactory` returns `codec.ErrUnavailable`, not
`codec.ErrNotFound`.

Concrete factories should replace these descriptor-only registrations once each
codec path has caller-owned output buffers and allocation tests.

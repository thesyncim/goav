# Adapters

Adapters keep codec and container implementations out of the core import graph.

Core packages such as `av`, `codec`, `format`, `pipeline`, `rtpav`, and
`webrtcav` should not import sibling codec modules directly. Concrete
integrations belong under `adapters/...`.

## Rules

- Implement `codec.DecoderFactory`, `codec.EncoderFactory`,
  `format.DemuxerFactory`, or `format.MuxerFactory`.
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

## Current Adapters

| Adapter | Status |
| --- | --- |
| `adapters/ivf` | IVF VP8/VP9/AV1 packet demux/mux |
| `adapters/annexb` | H264 Annex B packet mux |
| `adapters/gopus` | Opus decode to caller-owned `s16` frames, PLC on packet-loss events |
| `adapters/govpx` | descriptor-only VP8/VP9 boundary |
| `adapters/goav1` | descriptor-only AV1 boundary |
| `adapters/goh264` | descriptor-only H264 boundary, concrete adapter reserved for `goav_goh264` |

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

## Descriptor-only Boundaries

`govpx`, `goav1`, and `goh264` currently expose descriptors without importing
their sibling modules. This lets applications see planned capabilities and build
registries without breaking the default build or forcing heavy dependencies.
When a descriptor exists but no factory is registered, `DecoderFactory` or
`EncoderFactory` returns `codec.ErrUnavailable`, not `codec.ErrNotFound`.

Concrete factories should replace these descriptor-only registrations once each
codec path has caller-owned output buffers and allocation tests.

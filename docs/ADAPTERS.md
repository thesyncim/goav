# Adapters

Adapters keep codec and container implementations out of the core import graph.

Core packages such as `av`, `codec`, `format`, `pipeline`, `rtpav`, and
`webrtcav` should not import sibling codec modules directly. Concrete
integrations belong under `adapters/...`.

## Rules

- Implement `codec.DecoderFactory` or `codec.EncoderFactory`.
- Register with an explicit registry instance; blank-import registration can be
  added later as optional convenience.
- Allocate only during construction or `Open`.
- Hot-path methods must use caller-owned result structs and preallocated output
  buffers.
- Do not claim encode/decode capabilities until the path is real.
- Optional or unavailable modules should stay behind build tags or compile-safe
  descriptor packages.

## Current Adapters

| Adapter | Status |
| --- | --- |
| `adapters/gopus` | Opus decode to caller-owned `s16` frames, PLC on packet-loss events |
| `adapters/govpx` | descriptor-only VP8/VP9 boundary |
| `adapters/goav1` | descriptor-only AV1 boundary |
| `adapters/goh264` | descriptor-only H264 boundary, concrete adapter reserved for `goav_goh264` |

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

Concrete factories should replace these descriptor-only registrations once each
codec path has caller-owned output buffers and allocation tests.

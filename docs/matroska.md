# Matroska and WebM

`container/matroska` and `container/webm` provide the Matroska/WebM container
layer: pure Go, codec-implementation agnostic, extractable. They may depend on
generic `goav` media/format vocabulary and EBML helpers, never on codec
implementations or runtime internals.

## Packages

- `container/ebml`: element IDs, VINTs, read/write helpers, bounded readers,
  unknown-size masters, Void, CRC-32, seekable size patching.
- `container/matroska`: tracks, packets, mux/demux, timestamps, seeking,
  metadata, codec mapping, format registry adapters.
- `container/webm`: a WebM profile wrapper with stricter codec, document type,
  metadata, timestamp, and content-encoding validation.

## Basic API

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

WebM uses the same shape through `container/webm` but rejects tracks and
metadata outside the WebM profile.

## Current Support

Usable for internal Matroska/WebM recording, remuxing, and WebRTC-shaped packet
workflows. Container features: EBML/VINT handling; Segment, Info, Tracks,
Cluster, SimpleBlock, BlockGroup (duration, references, discard padding, codec
state, block additions); streaming output with unknown-size masters; seekable
output with patched sizes, SeekHead, and Cues; cue-assisted seeking with
packet-index fallback; Xiph/fixed/EBML lacing; track timestamp scale/offset
conversion; attachments, chapters, tags, CRC-32; block-scope AES-CTR content
encryption; caller-owned packet buffers on demux; format registry adapters for
`av.Stream`/`av.Packet`.

Codecs — Matroska: Opus, Vorbis, FLAC, AAC, PCMU/PCMA, VP8, VP9, AV1, H.264,
H.265, S_TEXT/UTF8. WebM: Opus, Vorbis, VP8, VP9, AV1. The WebRTC codec set is
fully covered for Matroska and the valid WebM subset for WebM.

Codec-private handling: Opus `OpusHead` can be generated when omitted; AV1,
H.264, and H.265 configuration records are validated and can be generated from
first-packet payloads (Annex B input converted to stored length-prefixed
samples); Vorbis/FLAC/AAC require caller-provided private data.

## Validation

Tests cover EBML handling, mux/demux round trips, WebRTC-shaped codec coverage,
lacing/BlockGroup/cue paths, codec-private validation/generation, profile
validation, malformed metadata and encrypted blocks, and allocation guards for
steady-state packet write/read. Optional external compatibility checks run when
tools like `ffmpeg`/`mkvmerge`/`mkvalidator` are installed; optional field-corpus
tests and benchmarks are driven by `GOAV_MATROSKA_FIELD_CORPUS` /
`GOAV_WEBM_FIELD_CORPUS` (see the test files for invocations).

## Future Work

Not yet a stable v1 container API: run a larger real-file corpus, capture
comparative benchmarks, add differential tests against mature tools, harden a
corrupt corpus, decide additional codec families, review the public API
(names, ownership, seek semantics, possible extraction), and commit benchmark
numbers before claiming performance parity. Protocol-layer repair,
retransmission, FEC, jitter buffering, and depacketization remain outside this
package.

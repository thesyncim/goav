# Matroska and WebM

This is the container layer for Matroska and WebM inside `goav`. It is pure Go,
codec-implementation agnostic, and intentionally separate from RTP, WebRTC,
repair, jitter buffering, and depacketization.

## Architecture

The implementation is split into three packages:

- `container/ebml` provides low-level EBML framing: element IDs, variable-size
  integers, known and unknown sizes, bounded readers, skipping, Void elements,
  and seekable size patching.
- `container/matroska` provides the high-level Matroska API around tracks,
  packets, timestamps, muxing, demuxing, and format registry adapters.
- `container/webm` wraps Matroska with WebM profile restrictions, shares the
  same streaming/seekable output behavior, and registers a distinct
  `av.FormatWebM` format ID when explicitly enabled.

The Matroska and WebM packages depend only on generic `goav` vocabulary (`av`
and `format`) plus EBML. They do not depend on codec implementations, RTP,
RTMP, WebRTC, or pipeline internals.

## Public API

The high-level API is intentionally small:

```go
muxer, err := matroska.NewMuxer(w, matroska.MuxerOptions{})
trackID, err := muxer.AddTrack(matroska.Track{Type: matroska.TrackVideo, Codec: matroska.CodecVP8})
err = muxer.WritePacket(matroska.Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: frame})
err = muxer.Close()

demuxer, err := matroska.NewDemuxer(r, matroska.DemuxerOptions{})
tracks := demuxer.Tracks()
packet := matroska.Packet{Data: make([]byte, 0, maxFrame)}
err = demuxer.ReadPacket(&packet)
```

WebM uses the same shape through `container/webm`, but rejects non-WebM codecs.

## Supported Features

Current milestone:

- EBML VINT encode/decode, element header read/write, bounded parsing, unknown
  sizes, skipping, Void writing, and seekable size patching.
- Matroska EBML header, Segment, Info, Tracks, Cluster, and SimpleBlock.
- Unknown-size Segment and Cluster mode for streaming-style output.
- Seekable Segment and Cluster size patching when the writer implements
  `io.Seeker`.
- Seekable Info Duration patching when packet timestamps/durations are known.
- SeekHead writing and parsing for seekable files.
- Cues writing and parsing for keyframe packets in seekable files.
- Cue-based `SeekToTime` for seekable demuxers.
- BlockGroup reading and writing for single-frame blocks with BlockDuration;
  non-keyframe BlockGroups use `ReferenceBlock=0` when exact dependency
  information is not available.
- Xiph, fixed-size, and EBML laced block demuxing with bounded scratch buffers.
- Matroska mux/demux for Opus, PCMU, PCMA, VP8, VP9, AV1, H.264, and H.265
  track declarations, with WebM enforcing Opus, VP8, VP9, and AV1 only.
- WebM-compatible muxing for VP8/VP9/AV1 plus Opus track metadata, with
  WebM demuxers requiring the `webm` EBML document type.
- Format registry adapters for `av.Stream` and `av.Packet`.
- Caller-owned packet data for demuxing.

## Deferred Features

These are intentionally not in the first milestone:

- Frame-exact random seeking and index-assisted extraction APIs.
- Multiple-reference BlockGroup writing and laced block muxing.
- Chapters, tags, attachments, language variants, default/forced flags beyond
  basic defaults, and unknown-element preservation.
- Full codec-private parsers for every codec family.
- RTP, RTX, RED, ULPFEC, FlexFEC, jitter buffering, or codec depacketization.

Those belong either in future Matroska phases or in separate media pipeline
layers before packets reach the container.

## Timestamp Model

Public packets use nanoseconds:

```go
type Packet struct {
    TrackID    uint32
    TimeNS     int64
    DurationNS int64
    Data       []byte
}
```

The muxer converts `TimeNS` into Segment timecodes using `TimecodeScaleNS`.
The default scale is 1 ms, matching common Matroska/WebM practice. Tests use
timestamps that round-trip exactly at that scale. Callers that need nanosecond
precision can set `TimecodeScaleNS` to `1`.

SimpleBlock and Block store a signed 16-bit timestamp relative to the active
Cluster. The muxer starts a new Cluster when the relative timestamp would
overflow or when `ClusterMaxDurationNS` is reached. When `DurationNS` is set,
the muxer writes a single-frame BlockGroup with BlockDuration in timestamp-scale
ticks. In seekable mode, `Info.Duration` is patched on close to the maximum
observed packet end time expressed in those same timestamp-scale ticks.
Seekable mode also writes Cues for keyframe packets using Segment-relative
Cluster positions, and a SeekHead that points to Info, Tracks, and Cues.
`SeekToTime` uses those Cues to jump to the nearest preceding cue cluster;
callers should continue reading until they reach the exact target packet.

## Codec Mapping

Current mappings:

- Opus: `A_OPUS`
- PCMU/PCMA: `A_MS/ACM` with generated WAVEFORMATEX codec private data
- VP8: `V_VP8`
- VP9: `V_VP9`
- AV1: `V_AV1`
- H.264: `V_MPEG4/ISO/AVC`
- H.265: `V_MPEGH/ISO/HEVC`

WebM accepts only Opus, VP8, VP9, and AV1. It rejects H.264, H.265, PCM
variants, repair streams, retransmission streams, FEC streams, and non-WebM
Matroska document types.

## Zero-Allocation Strategy

The steady-state packet paths avoid allocations:

- The muxer writes SimpleBlock headers from fixed scratch buffers.
- Header and track metadata may allocate before packet writing starts.
- The demuxer reuses embedded `io.LimitedReader` and fixed scratch buffers for
  block parsing.
- `ReadPacket` writes frame bytes into caller-owned `Packet.Data` capacity.
  When a non-laced frame is too large, the demuxer returns
  `ErrPayloadTooSmall` after skipping that packet so the next read stays
  aligned; pending laced frames can be retried with a larger buffer.
- Format adapters keep stream-to-track mappings in slices built during `Open`.

Unit tests assert 0 alloc/op for steady-state `WritePacket` and `ReadPacket`.

## Correctness Oracles

Primary references:

- RFC 8794 for EBML framing and variable-size integers.
- RFC 9559 and the official Matroska element and codec specifications.
- WebM container guidelines for WebM profile restrictions.

Compatibility tools are optional in CI and run when installed:

- `ffprobe`
- `mkvalidator`
- `mkvinfo`
- `mkvextract`
- `mkvmerge`

## Benchmark Plan

Committed benchmarks cover:

- EBML size VINT encode/decode.
- EBML element scanning.
- SimpleBlock write throughput and allocations.
- SimpleBlock read throughput and allocations.

Future benchmarks should add large-file scan speed, mux/demux bytes per second
on real WebRTC recordings, more lacing variants, and comparisons against
`ffmpeg`, `mkvmerge`, and other Go EBML/Matroska libraries on the same corpus.

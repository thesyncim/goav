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
err = muxer.WriteLacedPacket(matroska.LacedPacket{
    TrackID: trackID, TimeNS: 20_000_000, Keyframe: true,
    Lacing: matroska.LacingAuto, Frames: frames,
})
err = muxer.Close()

demuxer, err := matroska.NewDemuxer(r, matroska.DemuxerOptions{})
tracks := demuxer.Tracks()
packet := matroska.Packet{Data: make([]byte, 0, maxFrame)}
err = demuxer.ReadPacket(&packet)
err = demuxer.ReadPacketAtTime(1_000_000_000, &packet)
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
- Cues writing and parsing for keyframe packets in seekable files, including
  `CueRelativePosition` for block-level precision inside clusters.
- Cue-based `SeekToTime` for seekable demuxers, using `CueRelativePosition`
  when present to position directly on the cued block.
- Cue-assisted `ReadPacketAtTime` extraction for the first packet at or after
  a requested timestamp.
- BlockGroup reading and writing for single-frame blocks with BlockDuration;
  packets may carry one or more `ReferenceBlock` offsets, and non-keyframe
  duration BlockGroups use `ReferenceBlock=0` when exact dependency
  information is not available.
- Xiph, fixed-size, and EBML laced block muxing and demuxing with bounded
  scratch buffers.
- Matroska mux/demux for Opus, PCMU, PCMA, VP8, VP9, AV1, H.264, and H.265
  track declarations, with WebM enforcing Opus, VP8, VP9, and AV1 only.
- The WebRTC codec surface exposed by `av` is covered by Matroska:
  Opus, AV1, H.264, VP9, and VP8. WebM covers the valid WebM subset:
  Opus, AV1, VP9, and VP8.
- Opus tracks generate mono/stereo `OpusHead` codec-private data when callers
  do not provide their own, and demuxers validate `OpusHead` before exposing
  Opus track metadata.
- H.264 tracks validate caller-provided AVCDecoderConfigurationRecord
  codec-private data on mux and demux before exposing or writing track
  metadata.
- H.264 muxing can generate AVCDecoderConfigurationRecord codec-private data
  from SPS/PPS NAL units in the first Annex B packet or laced packet before
  the header is written.
- H.264 packet muxing converts public Annex B access units, including laced
  frames, into length-prefixed AVC samples when AVC codec-private data is
  available; demuxing converts those AVC samples back into Annex B packets for
  the public API.
- AV1 tracks validate caller-provided AV1CodecConfigurationRecord codec-private
  data on mux and demux before exposing or writing track metadata; muxing can
  generate the record from a first-packet sequence header OBU before the header
  is written.
- WebM-compatible muxing for VP8/VP9/AV1 plus Opus track metadata, with
  WebM demuxers requiring the `webm` EBML document type.
- Matroska attachment mux/demux for `AttachedFile` entries, including
  filename, media type, description, UID, binary data, defensive cloning, and
  SeekHead entries in seekable output.
- EBML CRC-32 validation for finite metadata masters, including top-level
  SeekHead, Info, Tracks, Cues, Attachments and nested track, video, audio,
  colour, projection, cue, seek, and attachment masters.
- Format registry adapters for `av.Stream` and `av.Packet`.
- Caller-owned packet data for demuxing.
- Demux validation rejects on-disk track, cue, and block track numbers that
  cannot fit the public `uint32` track ID surface, and blocks that reference
  undeclared tracks.
- Mux and demux validation rejects negative or overflowing track timing,
  audio, and video metadata before it can wrap into EBML unsigned integers or
  public `int` fields.

## Deferred Features

These are intentionally not in the first milestone:

- Dense indexing and frame-exact random access beyond cue-assisted
  `ReadPacketAtTime`.
- Chapters, tags, language variants, default/forced flags beyond basic
  defaults, and unknown-element preservation.
- Full codec-private generation and parsers for every codec family.
- RTP, RTX, RED, ULPFEC, FlexFEC, jitter buffering, or codec depacketization.

Those belong either in future Matroska phases or in separate media pipeline
layers before packets reach the container.

## Timestamp Model

Public packets use nanoseconds:

```go
type Packet struct {
    TrackID              uint32
    TimeNS               int64
    DurationNS           int64
    ReferenceBlockTimeNS []int64
    Data                 []byte
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
When `ReferenceBlockTimeNS` is set, the muxer writes one `ReferenceBlock`
element per offset. Offsets are signed nanosecond values relative to the packet
timestamp and are stored in timestamp-scale ticks.
Negative packet durations and packet end times that overflow `int64` are
rejected before bytes are written.
Seekable mode also writes Cues for keyframe packets using Segment-relative
Cluster positions plus `CueRelativePosition` offsets to the referenced block
inside the Cluster. It also writes a SeekHead that points to Info, Tracks,
Attachments when present, and Cues. The muxer updates duration and cue state
only after the packet bytes are written successfully.
`SeekToTime` uses those Cues to jump to the nearest preceding cue. When that cue
has `CueRelativePosition`, the demuxer parses preceding Cluster metadata and
positions the next read directly on the cued block. A successful seek clears
pending laced-frame state before reading from the target cluster.
`ReadPacketAtTime` combines that cue seek with packet reads and returns the
first packet at or after the requested timestamp. The caller-provided packet
buffer must be large enough for skipped packets and the returned packet.

## Codec Mapping

Current mappings:

- Opus: `A_OPUS` with generated and parsed `OpusHead` codec-private data for
  mono/stereo tracks.
- PCMU/PCMA: `A_MS/ACM` with generated WAVEFORMATEX codec private data
- VP8: `V_VP8`
- VP9: `V_VP9`
- AV1: `V_AV1` with AV1CodecConfigurationRecord validation when codec-private
  data is provided. If AV1 private data is omitted, the first AV1 packet written
  before the header must contain a sequence header OBU with a size field so the
  muxer can generate the configuration record.
- H.264: `V_MPEG4/ISO/AVC` with AVCDecoderConfigurationRecord validation when
  codec-private data is provided. If H.264 private data is omitted, the first
  H.264 packet written before the header must be Annex B and must include SPS
  and PPS NAL units so the muxer can generate the AVC configuration record.
  Public packets use Annex B access units; stored samples use AVC
  length-prefixed NAL units when the length size is known from codec-private
  data.
- H.265: `V_MPEGH/ISO/HEVC`

WebM accepts only Opus, VP8, VP9, and AV1. It rejects H.264, H.265, PCM
variants, repair streams, retransmission streams, FEC streams, and non-WebM
Matroska document types.

## Zero-Allocation Strategy

The steady-state packet paths avoid allocations:

- The muxer writes SimpleBlock headers from fixed scratch buffers.
- Laced SimpleBlock writing uses caller-owned frame slices and fixed scratch
  buffers for lace descriptors.
- Header and track metadata may allocate before packet writing starts.
- The demuxer reuses embedded `io.LimitedReader` and fixed scratch buffers for
  block parsing.
- H.264 AVC-to-Annex B demux conversion expands length-prefixed samples into
  caller-owned packet buffers without heap allocation.
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
- `ffmpeg`
- `mkvalidator`
- `mkvinfo`
- `mkvextract`
- `mkvmerge`

Current external checks cover WebM VP8/VP9/AV1/Opus files, Matroska files
carrying the WebRTC codec set, and Matroska H.264/AV1 files whose
codec-private data is generated from the first packet. They also generate small
FFmpeg-authored Matroska/WebM files, read them through the Go demuxers, remux
the first packet through the Go muxers, and verify the remuxed output with
`ffprobe` for H.264, AV1, VP8, VP9, and Opus where valid for each container.

## Benchmark Plan

Committed benchmarks cover:

- EBML size VINT encode/decode.
- EBML element scanning.
- SimpleBlock write throughput and allocations.
- Laced SimpleBlock write throughput and allocations.
- SimpleBlock read throughput and allocations.
- Matroska WebRTC-codec corpus mux/demux throughput and allocations across
  Opus, AV1, H.264, VP9, and VP8.
- Seekable Matroska WebRTC-codec corpus mux/demux throughput, allocation
  behavior, cue-assisted `ReadPacketAtTime`, and large Cue table seek
  scalability.
- WebM-profile corpus mux/demux throughput and allocations across VP8, VP9,
  AV1, and Opus.

Future benchmarks should add large-file scan speed on real WebRTC recordings,
more lacing variants, and comparisons against `ffmpeg`, `mkvmerge`, and other
Go EBML/Matroska libraries on the same corpus.

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
err = demuxer.ReadCuedPacketAtTime(1_000_000_000, &packet)
err = demuxer.ReadPacketAtTime(1_000_000_000, &packet)
err = demuxer.ReadCuedTrackPacketAtTime(trackID, 1_000_000_000, &packet)
err = demuxer.ReadTrackPacketAtTime(trackID, 1_000_000_000, &packet)
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
- Cues writing and parsing for seekable files, including `CueRelativePosition`
  for block-level precision inside clusters. The default writer policy indexes
  audio packets and keyframe video packets; callers may force keyframe-only,
  all-packet dense indexing, or disable cues explicitly with `CuePolicy`.
  WebM output maps the zero-value default to keyframe-only cues while retaining
  explicit all-packet dense cue support.
- Cue-based `SeekToTime` for seekable demuxers, using `CueRelativePosition`
  when present and falling back to `CueBlockNumber` when relative positions are
  absent.
- Cue-assisted `ReadPacketAtTime` extraction for the first packet at or after
  a requested timestamp.
- Seekable packet-at-time reads build a one-time block-position packet index
  when Cues are absent or sparse, so `ReadPacketAtTime` and
  `ReadTrackPacketAtTime` can jump directly to uncued SimpleBlock and
  BlockGroup entries, including indexed laced frames. Track-specific reads use
  per-track subindexes so multi-track recordings avoid scanning unrelated
  packets after the index is built. `SeekToTime` and `SeekToTrackTime` use the
  same packet index when Cues are absent or unusable.
- Direct cue-backed `ReadCuedPacketAtTime` and `ReadCuedTrackPacketAtTime`
  extraction for cues at or after a requested timestamp. Exact block cues jump
  directly to `CueRelativePosition` or `CueBlockNumber`; cluster-only cues scan
  within the referenced Cluster until the cue's track/time is reached, without
  scanning uncued packets between cues.
- Track-specific cue-assisted `SeekToTrackTime` and `ReadTrackPacketAtTime`
  for multi-track recordings with per-track cue positions, with the same
  packet-index fallback when a track has no usable cue or the requested packet
  is uncued.
- BlockGroup reading and writing for single-frame and laced blocks with
  BlockDuration; packets may carry one or more `ReferenceBlock` offsets,
  reference priority, codec state, discard padding, and block additions.
  Laced BlockGroup demuxing preserves that group metadata on every emitted
  frame. Non-keyframe duration BlockGroups use `ReferenceBlock=0` when exact
  dependency information is not available.
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
  metadata. Muxing can generate AVCDecoderConfigurationRecord codec-private
  data from SPS/PPS NAL units in the first Annex B packet or laced packet
  before the header is written.
- H.264 packet muxing converts public Annex B access units, including laced
  frames, into length-prefixed AVC samples when AVC codec-private data is
  available; demuxing converts those AVC samples back into Annex B packets for
  the public API.
- H.265 tracks validate caller-provided HEVCDecoderConfigurationRecord
  codec-private data on mux and demux before exposing or writing track
  metadata. Muxing can generate HEVCDecoderConfigurationRecord codec-private
  data from VPS/SPS/PPS NAL units in the first Annex B packet or laced packet
  before the header is written.
- H.265 packet muxing converts public Annex B access units, including laced
  frames, into length-prefixed HEVC samples when HEVC codec-private data is
  available; demuxing converts those HEVC samples back into Annex B packets for
  the public API.
- AV1 tracks validate caller-provided AV1CodecConfigurationRecord codec-private
  data on mux and demux before exposing or writing track metadata; muxing can
  generate the record from a first-packet sequence header OBU before the header
  is written.
- WebM-compatible muxing for VP8/VP9/AV1 plus Opus track metadata, with
  WebM demuxers requiring the `webm` EBML document type.
- WebM profile validation rejects VP8 codec-private data and malformed VP9
  codec feature private data while accepting valid VP9 profile/level/bit-depth/
  chroma metadata.
- WebM profile validation rejects Matroska-only track content compression,
  content signatures, non-AES encryption, AES-CBC, and non-block content
  encoding scopes while preserving WebM block-scope AES-CTR encryption.
- WebM profile validation rejects non-pixel video display units while keeping
  Matroska display-unit metadata available in the Matroska package.
- WebM muxers and sequential demuxer reads reject decreasing per-track absolute
  block timecodes while preserving cross-track audio/video interleaving.
- Block-scope AES-CTR content encryption/decryption for SimpleBlock,
  BlockGroup, laced blocks, and WebM profile output; encrypted laced reads
  keep the source lace buffer retry-safe when caller packet capacity is too
  small.
- Matroska attachment mux/demux for `AttachedFile` entries, including
  filename, media type, description, UID, binary data, defensive cloning, and
  SeekHead entries in seekable output.
- EBML CRC-32 validation for finite metadata masters, including top-level
  SeekHead, Info, Tracks, Cues, Attachments and nested track, video, audio,
  colour, projection, cue, seek, and attachment masters.
- Finite unknown Segment-level elements are preserved as raw EBML bytes through
  `UnknownSegmentElements` on muxer options and demuxers, allowing remuxers to
  keep vendor/application Segment metadata the package does not understand.
- Finite unknown child elements inside known metadata records are preserved as
  raw EBML bytes on `SegmentInfo`, `Track`, `Attachment`, `ChapterEdition`,
  `Chapter`, `ChapterDisplay`, `Tag`, `TagTarget`, and recursive `SimpleTag`
  values. The muxer validates these blobs against the parent schema so known
  fields cannot be smuggled through the raw-extension path.
- Finite unknown direct children of the `Tracks`, `Attachments`, `Chapters`,
  and `Tags` metadata masters are preserved through dedicated muxer options and
  demuxer accessors, including raw-only optional metadata masters in seekable
  output.
- Finite unknown Cluster children are preserved in packet order through
  `Packet.UnknownClusterElements` and `LacedPacket.UnknownClusterElements`.
  Demuxers attach raw Cluster children to the next returned packet; muxers
  write them immediately before that packet's block inside the active Cluster.
- Format registry adapters for `av.Stream` and `av.Packet`.
- Caller-owned packet data for demuxing.
- Demux validation rejects on-disk track, cue, and block track numbers that
  cannot fit the public `uint32` track ID surface, and blocks that reference
  undeclared tracks.
- Demux validation rejects malformed `TrackEntry` records that omit required
  TrackNumber, TrackType, CodecID, or audio/video settings, duplicate singleton
  identity fields inside one TrackEntry, or reuse TrackNumber/TrackUID across
  entries.
- Demux validation rejects malformed EBML preamble and SeekHead metadata,
  including unsupported EBML/doctype read versions, invalid Matroska/WebM
  length contracts, missing or unsupported DocType values, duplicate EBML
  header singleton fields, and Seek entries without both SeekID and
  SeekPosition.
- Native fuzz targets exercise bounded EBML header/VINT parsing plus
  malformed Matroska and WebM demuxer construction/read loops with seeded
  valid, laced, and profile-constrained corpus entries.
- Demuxers enforce top-level Segment occurrence limits for SeekHead, Info,
  Tracks, Cues, Attachments, and Chapters. Seekable readers also resolve
  required Info/Tracks masters through a pre-Cluster SeekHead when those
  masters are stored after the first Cluster, then skip the same physical
  elements during the later linear packet scan.
- Demux validation rejects duplicate singleton child fields inside known Info,
  TrackEntry, TrackTranslate, BlockAdditionMapping, audio/video/colour/
  projection/mastering metadata, content encoding records, Cues, BlockGroup,
  AttachedFile, EditionEntry, ChapterAtom, ChapterDisplay, Tag, Targets, and
  SimpleTag records instead of letting later values silently overwrite earlier
  metadata.
- Mux and demux validation rejects negative or overflowing track timing,
  audio, and video metadata before it can wrap into EBML unsigned integers or
  public `int` fields.
- Demux validation rejects duplicate attachment UIDs, duplicate edition UIDs,
  and duplicate chapter UIDs across repeated top-level metadata masters and
  nested chapter trees.

## Deferred Features

These are intentionally not in the first milestone:

- Additional Matroska codec families outside the current supported set.
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
`WriteLacedPacket` writes a SimpleBlock by default. When `FrameDurationNS`,
references, reference priority, codec state, discard padding, or block
additions are set, it writes a laced BlockGroup. `FrameDurationNS` is per
emitted frame; the stored BlockDuration is the total laced block duration so
demuxing returns the same per-frame duration.
Negative packet durations and packet end times that overflow `int64` are
rejected before bytes are written.
Seekable mode also writes Cues using Segment-relative Cluster positions plus
`CueRelativePosition` offsets to the referenced block inside the Cluster. By
default, Matroska indexes audio packets and keyframe video packets; WebM maps
the default to keyframe-only cues. `CuePolicy` allows callers to force
keyframe-only indexing, force all-packet dense indexing, or disable cues. The
muxer also writes a SeekHead that points to Info, Tracks, Attachments,
Chapters, Tags, and Cues when present. The demuxer can use a
pre-Cluster SeekHead to load required Info and Tracks metadata before reading
the first Cluster even when those masters are physically stored later in the
Segment. The muxer updates duration and cue state only after the packet bytes
are written successfully.
`SeekToTime` uses those Cues to jump to the nearest preceding cue. When that cue
has `CueRelativePosition`, the demuxer parses preceding Cluster metadata and
positions the next read directly on the cued block. If a cue omits
`CueRelativePosition` but has `CueBlockNumber`, the demuxer scans the Cluster to
that block number and hands that block to the next read. A successful seek
clears pending laced-frame state before reading from the target cluster.
When Cues are absent, sparse, before the target packet, or missing for a
specific track, seekable packet-at-time reads build a packet index from
Segment-relative Cluster positions, block positions, Cluster timecodes, track
numbers, and laced-frame numbers. The demuxer also builds per-track subindexes
over that sorted packet index, so `ReadTrackPacketAtTime` and cue-free
`SeekToTrackTime` can binary-search only the requested track. The fallback seeks
directly to the indexed SimpleBlock or BlockGroup and lets the normal demux path
decode the packet, so codec conversion, content encoding, BlockGroup metadata,
and laced-frame duration handling stay shared with sequential reads.
`ReadPacketAtTime` combines that cue seek with packet reads and returns the
first packet at or after the requested timestamp. The caller-provided packet
buffer must be large enough for skipped packets and the returned packet.
`SeekToTrackTime` and `ReadTrackPacketAtTime` use CueTrackPositions to choose
the nearest cue for a specific track, then read forward until that track reaches
the requested timestamp.
`ReadCuedPacketAtTime` and `ReadCuedTrackPacketAtTime` choose the first cue at
or after the requested timestamp and read that cued packet without scanning
intervening uncued packets. If the cue has `CueRelativePosition` or
`CueBlockNumber`, the demuxer jumps directly to the block. If the cue only has
a Cluster position, the demuxer scans within that referenced Cluster until the
cue's track/time is reached. They return `ErrInvalidData` when no matching cued
packet can be resolved from the cue.

## Codec Mapping

Current mappings:

- Opus: `A_OPUS` with generated and parsed `OpusHead` codec-private data for
  mono/stereo tracks.
- PCMU/PCMA: `A_MS/ACM` with generated and parsed WAVEFORMATEX codec-private
  data for G.711 mu-law and A-law tracks.
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
- H.265: `V_MPEGH/ISO/HEVC` with HEVCDecoderConfigurationRecord validation
  when codec-private data is provided. If H.265 private data is omitted, the
  first H.265 packet written before the header must be Annex B and must include
  VPS, SPS, and PPS NAL units so the muxer can generate the HEVC configuration
  record. Public packets use Annex B access units; stored samples use HEVC
  length-prefixed NAL units when the length size is known from codec-private
  data.

WebM accepts only Opus, VP8, VP9, and AV1. It rejects H.264, H.265, PCM
variants, repair streams, retransmission streams, FEC streams, non-WebM
Matroska document types, Matroska-only content compression, content signatures,
non-AES encryption, AES-CBC, and non-block content encoding scopes. WebM
content encryption is limited to block-scope AES-CTR. VP8 tracks must not carry
codec-private data; VP9 codec-private data, when present, must use the WebM
VP9 codec feature list with supported profile, level, bit-depth, and chroma
subsampling values. WebM video display units must remain pixel units. WebM
packet writes and sequential packet reads reject decreasing per-track absolute
block timecodes; equal timestamps and lower timestamps on other tracks remain
valid so audio/video blocks can be interleaved naturally.

## Zero-Allocation Strategy

The steady-state packet paths avoid allocations:

- The muxer writes SimpleBlock headers from fixed scratch buffers.
- Laced SimpleBlock writing uses caller-owned frame slices and fixed scratch
  buffers for lace descriptors.
- Header and track metadata may allocate before packet writing starts.
- The demuxer reuses embedded `io.LimitedReader` and fixed scratch buffers for
  block parsing.
- Cue-free and sparse-cue seekable reads build the packet/block index once,
  including per-track subindexes, then use binary search and normal
  caller-owned packet buffers with 0 steady-state allocations.
- H.264 AVC-to-Annex B demux conversion expands length-prefixed samples into
  caller-owned packet buffers without heap allocation.
- `ReadPacket` writes frame bytes into caller-owned `Packet.Data` capacity.
  When a non-laced frame is too large, the demuxer returns
  `ErrPayloadTooSmall` after skipping that packet so the next read stays
  aligned; pending laced frames can be retried with a larger buffer.
- Packet-carried unknown Cluster children are transferred only when the
  associated packet is successfully returned; retryable laced-frame reads keep
  them pending, while skipped non-laced frames drop them with the skipped block.
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
codec-private data is generated from the first packet. Internal generated-path
checks also cover H.265 HEVC private-data generation and packet conversion.
The external checks also generate small
FFmpeg-authored Matroska/WebM files, read them through the Go demuxers, remux
the first packet through the Go muxers, and verify the remuxed output with
`ffprobe` for H.264, AV1, VP8, VP9, and Opus where valid for each container.
The external corpus also includes multi-packet audio/video recordings:
Matroska H.264+Opus, AV1+Opus, VP9+Opus, and VP8+Opus plus WebM VP8+Opus,
VP9+Opus, and AV1+Opus are demuxed completely, checked for monotonic per-track
timestamps, remuxed completely through the Go muxers, and validated again with
the Go demuxers and `ffprobe`.

Optional field-corpus tests and benchmarks run against real capture files when
corpus paths are provided:

```sh
GOAV_MATROSKA_FIELD_CORPUS=/path/to/mkv-or-directory \
GOAV_WEBM_FIELD_CORPUS=/path/to/webm-or-directory \
go test ./container/matroska ./container/webm \
  -run 'TestExternal(Matroska|WebM)FieldCorpus' -count=1 -v

GOAV_MATROSKA_FIELD_CORPUS=/path/to/mkv-or-directory \
GOAV_WEBM_FIELD_CORPUS=/path/to/webm-or-directory \
go test ./container/matroska ./container/webm -run '^$' \
  -bench 'BenchmarkExternal(Matroska|WebM)FieldCorpusScan' -benchtime=1x
```

`GOAV_FIELD_CORPUS_PACKET_CAP` may be set to a byte count when a capture has
packets larger than the default 16 MiB caller-owned packet buffer.

## Benchmark Plan

Committed benchmarks cover:

- EBML size VINT encode/decode.
- EBML element scanning.
- SimpleBlock write throughput and allocations.
- Laced SimpleBlock write throughput and allocations.
- SimpleBlock read throughput and allocations.
- H.264 AVC and H.265 HEVC packet conversion allocation guards.
- Matroska WebRTC-codec corpus mux/demux throughput and allocations across
  Opus, AV1, H.264, VP9, and VP8.
- Seekable Matroska WebRTC-codec corpus mux/demux throughput, allocation
  behavior, cue-assisted `ReadPacketAtTime`, track-specific
  `ReadTrackPacketAtTime`, direct `ReadCuedPacketAtTime`, and large Cue table
  seek scalability.
- WebM-profile corpus mux/demux throughput and allocations across VP8, VP9,
  AV1, and Opus.
- External head-to-head recording benchmarks on FFmpeg-authored corpora:
  Matroska H.264+Opus and WebM VP9+Opus plus AV1+Opus Go demux/remux runs are
  compared against `ffprobe -show_packets`, `ffmpeg -c copy`, `mkvinfo
  --summary`, and `mkvmerge` copy/remux on the same input files.
- External Go-library comparison benchmarks on the same Matroska
  WebRTC-shaped corpus, comparing complete packet scans against
  `github.com/luispater/matroska-go`.
- Optional field-corpus scan benchmarks for Matroska and WebM production
  captures supplied through `GOAV_MATROSKA_FIELD_CORPUS` and
  `GOAV_WEBM_FIELD_CORPUS`.
- Native fuzz harnesses for EBML header/VINT parsing and malformed
  Matroska/WebM demuxer packet-read loops.

Future benchmark runs should add committed results from real WebRTC production
recordings, subsample encrypted laced-block cases, and additional Go
EBML/Matroska libraries on the same corpus.

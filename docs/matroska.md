# Matroska and WebM

`container/matroska` and `container/webm` provide the container layer for
Matroska and WebM inside `goav`. They are pure Go, codec-implementation
agnostic, and designed to stay extractable.

The packages may depend on generic `goav` media and format vocabulary, plus
EBML helpers. They must not depend on codec implementations or protocol/runtime
internals.

## Packages

- `container/ebml`: EBML element IDs, variable-size integers, element
  read/write helpers, bounded readers, unknown-size masters, Void elements,
  CRC-32, and seekable size patching.
- `container/matroska`: Matroska tracks, packets, muxing, demuxing, timestamp
  handling, seeking, metadata, codec mapping, and format registry adapters.
- `container/webm`: a WebM profile wrapper around Matroska with stricter codec,
  document type, metadata, timestamp, and content-encoding validation.

## Basic API

```go
muxer, err := matroska.NewMuxer(w, matroska.MuxerOptions{})
trackID, err := muxer.AddTrack(matroska.Track{
    Type:  matroska.TrackVideo,
    Codec: matroska.CodecVP8,
    Video: matroska.VideoConfig{Width: 1280, Height: 720},
})
err = muxer.WritePacket(matroska.Packet{
    TrackID:  trackID,
    TimeNS:   0,
    Keyframe: true,
    Data:     frame,
})
err = muxer.Close()

demuxer, err := matroska.NewDemuxer(r, matroska.DemuxerOptions{})
tracks := demuxer.Tracks()
packet := matroska.Packet{Data: make([]byte, 0, maxPacket)}
err = demuxer.ReadPacket(&packet)
```

WebM uses the same shape through `container/webm`, but rejects tracks and
metadata outside the WebM profile.

## Current Support

This implementation is usable for internal Matroska/WebM recording, remuxing,
and WebRTC-shaped packet workflows.

Supported container features:

- EBML header and VINT parsing/writing.
- Segment, Info, Tracks, Cluster, SimpleBlock, and BlockGroup.
- Streaming output with unknown-size Segment/Cluster masters.
- Seekable output with patched Segment/Cluster sizes, Info duration, SeekHead,
  and Cues.
- Cue-assisted seeking and direct packet-at-time reads, with packet-index
  fallback when cues are absent or sparse.
- Xiph, fixed-size, and EBML lacing.
- Track timestamp scale and track offset conversion.
- Attachments, chapters, tags, CRC-32, and finite unknown metadata preservation.
- BlockGroup metadata including duration, references, discard padding, codec
  state, and block additions.
- Block-scope AES-CTR content encryption/decryption for Matroska and valid WebM
  profile output.
- Caller-owned packet buffers on demux reads.
- Format registry adapters for `av.Stream` and `av.Packet`.

Supported Matroska codecs:

- Opus
- Vorbis
- FLAC
- AAC
- PCMU and PCMA
- VP8
- VP9
- AV1
- H.264
- H.265
- S_TEXT/UTF8

Supported WebM codecs:

- Opus
- Vorbis
- VP8
- VP9
- AV1

The WebRTC codec set exposed by `av` is covered for Matroska: Opus, AV1,
H.264, VP9, and VP8. WebM covers the valid WebM subset: Opus, AV1, VP9, and
VP8.

## Codec Notes

- Opus can generate mono/stereo `OpusHead` private data when omitted.
- AV1 validates `AV1CodecConfigurationRecord`; muxing can generate it from a
  first-packet sequence header OBU.
- H.264 validates `AVCDecoderConfigurationRecord`; muxing can generate it from
  first-packet Annex B SPS/PPS and converts public Annex B packets to stored
  length-prefixed AVC samples.
- H.265 validates `HEVCDecoderConfigurationRecord`; muxing can generate it from
  first-packet Annex B VPS/SPS/PPS and converts public Annex B packets to stored
  length-prefixed HEVC samples.
- Vorbis, FLAC, and AAC require caller-provided codec-private data on mux and
  parse it on demux.
- WebM rejects Matroska-only codecs, H.264, H.265, FLAC, AAC, PCM variants,
  repair/retransmission/FEC streams, Matroska-only content encodings, non-WebM
  metadata, `TrackTimestampScale`, and `TrackOffset`.

## Validation

Committed tests cover:

- EBML VINT and element handling.
- Matroska and WebM mux/demux round trips.
- WebRTC-shaped codec coverage for Opus, AV1, H.264, VP9, and VP8.
- Lacing, BlockGroup metadata, cue seeking, sparse-cue packet indexes, and
  track-specific packet-at-time reads.
- Codec-private validation/generation for Opus, AV1, H.264, and H.265.
- Matroska/WebM profile validation.
- Malformed metadata, duplicate singleton fields, unknown metadata, CRC-32, and
  encrypted block paths.
- Allocation guards for steady-state packet write/read paths.
- Optional external compatibility checks when tools such as `ffmpeg`,
  `ffprobe`, `mkvmerge`, `mkvinfo`, `mkvextract`, and `mkvalidator` are
  installed.

Optional field-corpus tests and benchmarks are controlled by:

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

`GOAV_FIELD_CORPUS_PACKET_CAP` may be set when a corpus contains packets larger
than the default caller-owned packet buffer.

## Future Work

This is not yet a stable v1 container API or a proven drop-in replacement for
every mature Matroska/WebM implementation. Before making that claim, finish:

1. Run a larger real-file corpus covering browser WebM recordings, media-server
   archives, screen-share captures, long-running WebRTC recordings, camera
   files, edited Matroska files, and damaged/truncated files.
2. Capture benchmark results against the same corpus for this implementation,
   common command-line tools, and mature Go libraries.
3. Add differential tests that compare packet timing, track metadata, codec
   private data, cue positions, and remuxed output across those tools.
4. Harden malformed-input coverage with a dedicated corrupt corpus for deeply
   nested metadata, extreme lacing, huge cue tables, weird timestamp scales,
   negative offsets, unknown elements, CRC failures, and partial clusters.
5. Decide which additional Matroska codec families are worth supporting.
6. Review the public API for names, ownership rules, seek semantics, WebM
   restrictions, and extraction into a standalone module if the package proves
   broadly useful.
7. Commit representative benchmark numbers and performance notes before
   claiming it beats high-performance implementations.

Protocol-layer repair, retransmission, FEC, jitter buffering, and
depacketization remain outside this container package. They should produce or
consume generic packets before Matroska/WebM sees them.

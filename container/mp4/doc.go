// Package mp4 is a pure-Go ISO Base Media File Format (ISO/IEC 14496-12)
// demuxer and muxer for MP4/MOV/M4A/M4V. It registers behind the goav format
// extension point as the av.FormatMP4 demuxer and muxer.
//
// The muxer writes H.264, AV1, VP8, VP9, AAC, Opus, and FLAC tracks. To a
// seekable output it writes a progressive file, moov after the samples; to a
// pipe or a realtime output it writes fragmented MP4, a moof and mdat per
// fragment cut at video keyframes, which plays while it grows. Edit lists
// keep the tracks in sync and Opus pre-skip and B-frame delay out of
// presentation, exactly as the demuxer reads them back.
//
// The demuxer is random-access: it walks the top-level boxes to locate moov
// (which may follow mdat in non-faststart files), parses each track's sample
// tables into a flat sample index, and reads samples in decode-time order
// across tracks (ties by file offset). Edit lists are honored as encoders
// write them: a leading empty edit delays a track and the first media edit
// starts its presentation (hiding B-frame delay and AAC priming), then every
// track lifts together so no sample presents before zero. Fragmented (fMP4/CMAF) files are supported too: when a
// track carries no moov sample table, its samples are read from the moof/traf/
// trun fragment runs using the mvex/trex defaults. It needs an io.ReaderAt; a
// plain io.Reader is buffered in memory as a fallback.
//
// Supported sample entries map to av codec ids: avc1/avc3 (H264), av01 (AV1),
// vp08/vp09 (VP8/VP9), mp4a (AAC), Opus, and fLaC (FLAC). Codec configuration
// (avcC, av1C, vpcC, esds AudioSpecificConfig, dOps) is exposed as stream
// ExtraData. Tracks whose codec goav has no id for are skipped, so a file's
// supported tracks still demux.
package mp4

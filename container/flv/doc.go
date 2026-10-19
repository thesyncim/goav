// Package flv is a pure-Go FLV demuxer and muxer, the container RTMP
// carries: an RTMP audio, video, or data message is an FLV tag body. It
// registers behind the goav format extension point as av.FormatFLV.
//
// Both directions speak the legacy FLV tags (H.264 with its
// AVCDecoderConfigurationRecord and composition time, AAC with its
// AudioSpecificConfig) and Enhanced RTMP v2 (E-FLV): FourCC video and audio
// headers for HEVC, AV1, VP9, VP8, Opus, and FLAC, ModEx extensions, and
// multitrack messages carrying several tracks of one media. The muxer
// writes H.264 and AAC with legacy headers, so classic players and ingest
// servers read them, and every other codec, and every track after the first
// of its media, with enhanced ones. It writes onMetaData first; to a
// seekable output it patches the duration and file size at Close.
//
// The demuxer reads a stream, file or live: it learns the streams from
// onMetaData, the header flags, and the first tags, reading ahead a bounded
// amount, and announces a track that appears later with
// av.EventStreamAdded and a changed sequence header with
// av.EventCodecChanged. Timestamps are milliseconds, with the 32-bit
// extended timestamp unwrapped. H.264 and HEVC samples become Annex B, as
// goav carries them; stream ExtraData is the codec configuration record
// (avcC, hvcC, av1C, vpcC with its version and flags, AudioSpecificConfig,
// OpusHead, or the FLAC stream header).
package flv

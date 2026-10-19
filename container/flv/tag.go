package flv

import (
	"encoding/binary"
	"errors"

	"github.com/thesyncim/goav/av"
)

// Errors the demuxer and muxer return, matched with errors.Is.
var (
	// ErrInvalidData reports bytes that are not FLV, or a tag whose fields
	// contradict each other.
	ErrInvalidData = errors.New("flv: invalid data")
	// ErrUnsupported reports a stream FLV cannot carry.
	ErrUnsupported = errors.New("flv: unsupported")
	// ErrClosed reports a demuxer or muxer used after Close.
	ErrClosed = errors.New("flv: closed")
)

// Tag types.
const (
	tagAudio  = 8
	tagVideo  = 9
	tagScript = 18
)

// tagHeaderSize is the size of a tag header; a tag is that header, its body,
// and the 4-byte size of the whole tag that follows it (PreviousTagSize).
const tagHeaderSize = 11

// maxTagBody is the largest body a tag's 24-bit DataSize describes.
const maxTagBody = 1<<24 - 1

// Legacy codec ids: the VideoTagHeader CodecID and AudioTagHeader
// SoundFormat that goav reads (12 is the unofficial HEVC id some CDNs
// send).
const (
	legacyAVC   = 7
	legacyHEVC  = 12
	legacyAAC   = 10
	soundExHead = 9
)

// Enhanced RTMP packet types, the low nibble of an ExVideoTagHeader or
// ExAudioTagHeader (and of each multitrack entry).
const (
	packetSequenceStart = 0
	packetCodedFrames   = 1
	packetSequenceEnd   = 2
	packetCodedFramesX  = 3 // video: composition time zero
	packetMetadata      = 4 // video: AMF metadata; audio: multichannel config
	videoMultitrack     = 6
	audioMultitrack     = 5
	packetModEx         = 7
)

// Multitrack layouts (AvMultitrackType).
const (
	oneTrack             = 0
	manyTracks           = 1
	manyTracksManyCodecs = 2
)

// Video frame types.
const (
	frameKey     = 1
	frameInter   = 2
	frameCommand = 5
)

// fourCC is a codec identifier of the enhanced headers.
type fourCC uint32

func makeFourCC(s string) fourCC { return fourCC(binary.BigEndian.Uint32([]byte(s))) }

var (
	fourAVC  = makeFourCC("avc1")
	fourHEVC = makeFourCC("hvc1")
	fourAV1  = makeFourCC("av01")
	fourVP9  = makeFourCC("vp09")
	fourVP8  = makeFourCC("vp08")
	fourAAC  = makeFourCC("mp4a")
	fourOpus = makeFourCC("Opus")
	fourFLAC = makeFourCC("fLaC")
)

// codecFourCC maps the codecs goav carries in FLV to their FourCC.
var codecFourCC = map[av.CodecID]fourCC{
	av.CodecH264: fourAVC,
	av.CodecHEVC: fourHEVC,
	av.CodecAV1:  fourAV1,
	av.CodecVP9:  fourVP9,
	av.CodecVP8:  fourVP8,
	av.CodecAAC:  fourAAC,
	av.CodecOpus: fourOpus,
	av.CodecFLAC: fourFLAC,
}

// fourCCCodec is the codec a FourCC names, or CodecUnknown for one goav has
// no id for (VVC, AC-3, MP3): its tracks are skipped.
func fourCCCodec(c fourCC) av.CodecID {
	for id, code := range codecFourCC {
		if code == c {
			return id
		}
	}
	return av.CodecUnknown
}

// hasCompositionTime reports a codec whose coded frames carry an SI24
// composition time offset.
func hasCompositionTime(id av.CodecID) bool {
	return id == av.CodecH264 || id == av.CodecHEVC
}

// itemKind is what one track's part of a media tag carries.
type itemKind uint8

const (
	itemFrame itemKind = iota
	itemConfig
	itemEnd
)

// item is one track's part of an audio or video tag: a coded frame, a
// sequence start (the codec configuration), or a sequence end. data aliases
// the tag body.
type item struct {
	media    av.MediaType
	track    uint8
	codec    av.CodecID
	kind     itemKind
	keyframe bool
	cts      int32
	data     []byte
}

// errSkip ends the parse of a tag goav does not read (a command frame,
// metadata, an unknown packet type): the tag is dropped, not the stream.
var errSkip = errors.New("flv: skip tag")

// reader walks a tag body, recording a short read as ErrInvalidData.
type reader struct {
	b   []byte
	err error
}

func (r *reader) take(n int) []byte {
	if r.err != nil || n < 0 || n > len(r.b) {
		r.err = ErrInvalidData
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *reader) u8() byte {
	if b := r.take(1); b != nil {
		return b[0]
	}
	return 0
}

func (r *reader) u16() int {
	if b := r.take(2); b != nil {
		return int(binary.BigEndian.Uint16(b))
	}
	return 0
}

func (r *reader) u24() int {
	if b := r.take(3); b != nil {
		return int(b[0])<<16 | int(b[1])<<8 | int(b[2])
	}
	return 0
}

func (r *reader) si24() int32 {
	return int32(uint32(r.u24())<<8) >> 8
}

func (r *reader) fourCC() fourCC {
	if b := r.take(4); b != nil {
		return fourCC(binary.BigEndian.Uint32(b))
	}
	return 0
}

// modEx skips the ModEx extensions after an enhanced header and returns
// the packet type they end on. The only one defined, a sub-millisecond
// timestamp offset, is finer than FLV's millisecond timebase goav keeps.
func (r *reader) modEx(packetType byte) byte {
	for packetType == packetModEx && r.err == nil {
		size := int(r.u8()) + 1
		if size == 256 {
			size = r.u16() + 1
		}
		r.take(size)
		packetType = r.u8() & 0x0f
	}
	return packetType
}

// parseVideo calls fn with each track's part of a video tag body.
func parseVideo(body []byte, fn func(item) error) error {
	r := reader{b: body}
	first := r.u8()
	if r.err != nil {
		return errSkip // an empty tag carries nothing
	}
	frameType := first >> 4 & 0x07
	if first&0x80 == 0 {
		codec := av.CodecUnknown
		switch first & 0x0f {
		case legacyAVC:
			codec = av.CodecH264
		case legacyHEVC:
			codec = av.CodecHEVC
		}
		if codec == av.CodecUnknown || frameType == frameCommand {
			return errSkip
		}
		packetType := r.u8()
		cts := r.si24()
		if r.err != nil {
			return r.err
		}
		it := item{media: av.MediaVideo, codec: codec, keyframe: frameType == frameKey, cts: cts, data: r.b}
		switch packetType {
		case packetSequenceStart:
			it.kind = itemConfig
		case packetCodedFrames:
		case packetSequenceEnd:
			it.kind = itemEnd
		default:
			return errSkip
		}
		return fn(it)
	}
	packetType := r.modEx(first & 0x0f)
	if packetType != packetMetadata && frameType == frameCommand {
		return errSkip
	}
	return parseEnhanced(&r, av.MediaVideo, packetType, videoMultitrack, frameType == frameKey, fn)
}

// parseAudio calls fn with each track's part of an audio tag body.
func parseAudio(body []byte, fn func(item) error) error {
	r := reader{b: body}
	first := r.u8()
	if r.err != nil {
		return errSkip // an empty tag is the undocumented silence message
	}
	switch first >> 4 {
	case legacyAAC:
		packetType := r.u8()
		if r.err != nil {
			return r.err
		}
		it := item{media: av.MediaAudio, codec: av.CodecAAC, keyframe: true, data: r.b}
		switch packetType {
		case 0:
			it.kind = itemConfig
		case 1:
		default:
			return errSkip
		}
		return fn(it)
	case soundExHead:
		packetType := r.modEx(first & 0x0f)
		return parseEnhanced(&r, av.MediaAudio, packetType, audioMultitrack, true, fn)
	default:
		return errSkip
	}
}

// parseEnhanced reads the FourCC part of an enhanced header, single or
// multitrack, and each track's body, which the spec requires be processed
// completely.
func parseEnhanced(r *reader, media av.MediaType, packetType, multitrack byte, keyframe bool, fn func(item) error) error {
	layout := -1 // not multitrack
	var code fourCC
	if packetType == multitrack {
		b := r.u8()
		layout, packetType = int(b>>4), b&0x0f
		if packetType == multitrack || layout > manyTracksManyCodecs {
			return ErrInvalidData
		}
		if layout != manyTracksManyCodecs {
			code = r.fourCC()
		}
	} else {
		code = r.fourCC()
	}
	if r.err != nil {
		return r.err
	}
	for {
		var track uint8
		body := r
		if layout >= 0 {
			if layout == manyTracksManyCodecs {
				code = r.fourCC()
			}
			track = r.u8()
			if layout != oneTrack {
				body = &reader{b: r.take(r.u24())}
			}
			if r.err != nil {
				return r.err
			}
		}
		if err := enhancedTrack(body, media, track, code, packetType, keyframe, fn); err != nil && !errors.Is(err, errSkip) {
			return err
		}
		if layout <= oneTrack || len(r.b) == 0 {
			return nil
		}
	}
}

// enhancedTrack reads one track's body of an enhanced tag.
func enhancedTrack(r *reader, media av.MediaType, track uint8, code fourCC, packetType byte, keyframe bool, fn func(item) error) error {
	codec := fourCCCodec(code)
	if codec == av.CodecUnknown || codecMediaType(codec) != media {
		return errSkip
	}
	it := item{media: media, track: track, codec: codec, keyframe: keyframe}
	switch packetType {
	case packetSequenceStart:
		it.kind = itemConfig
	case packetCodedFrames:
		if hasCompositionTime(codec) {
			it.cts = r.si24()
		}
	case packetCodedFramesX:
		if media != av.MediaVideo {
			return errSkip
		}
	case packetSequenceEnd:
		it.kind = itemEnd
	default:
		return errSkip // metadata, multichannel config, MPEG-2 TS start
	}
	if r.err != nil {
		return r.err
	}
	it.data = r.b
	return fn(it)
}

// codecMediaType is the media a codec FLV carries is.
func codecMediaType(id av.CodecID) av.MediaType {
	switch id {
	case av.CodecAAC, av.CodecOpus, av.CodecFLAC:
		return av.MediaAudio
	default:
		return av.MediaVideo
	}
}

// appendVideoHeader appends the header of a video tag body for one track:
// legacy AVC for H.264 on track 0, which classic players read; enhanced
// otherwise, a multitrack one-track header beyond track 0.
func appendVideoHeader(dst []byte, track uint8, codec av.CodecID, kind itemKind, keyframe bool, cts int32) []byte {
	frameType := byte(frameInter)
	if keyframe || kind != itemFrame {
		frameType = frameKey
	}
	if codec == av.CodecH264 && track == 0 {
		packetType := byte(1)
		switch kind {
		case itemConfig:
			packetType = 0
		case itemEnd:
			packetType = 2
		}
		return append(dst, frameType<<4|legacyAVC, packetType, byte(cts>>16), byte(cts>>8), byte(cts))
	}
	packetType := byte(packetCodedFrames)
	switch {
	case kind == itemConfig:
		packetType = packetSequenceStart
	case kind == itemEnd:
		packetType = packetSequenceEnd
	case hasCompositionTime(codec) && cts == 0:
		packetType = packetCodedFramesX // composition time zero, left out
	}
	dst = appendEnhanced(dst, 0x80|frameType<<4, videoMultitrack, packetType, track, codecFourCC[codec])
	if packetType == packetCodedFrames && hasCompositionTime(codec) {
		dst = append(dst, byte(cts>>16), byte(cts>>8), byte(cts))
	}
	return dst
}

// appendAudioHeader appends the header of an audio tag body for one track:
// legacy AAC on track 0, enhanced otherwise.
func appendAudioHeader(dst []byte, track uint8, codec av.CodecID, kind itemKind) []byte {
	if codec == av.CodecAAC && track == 0 {
		packetType := byte(1)
		if kind == itemConfig {
			packetType = 0
		}
		// AAC is always 0xAF: 44 kHz, 16-bit, stereo, whatever it carries.
		return append(dst, legacyAAC<<4|0x0f, packetType)
	}
	packetType := byte(packetCodedFrames)
	switch kind {
	case itemConfig:
		packetType = packetSequenceStart
	case itemEnd:
		packetType = packetSequenceEnd
	}
	return appendEnhanced(dst, soundExHead<<4, audioMultitrack, packetType, track, codecFourCC[codec])
}

// appendEnhanced appends an enhanced header: the first byte's high nibble,
// then the FourCC, behind a one-track multitrack header for tracks after 0.
func appendEnhanced(dst []byte, high, multitrack, packetType, track byte, code fourCC) []byte {
	if track == 0 {
		dst = append(dst, high|packetType)
		return binary.BigEndian.AppendUint32(dst, uint32(code))
	}
	dst = append(dst, high|multitrack, oneTrack<<4|packetType)
	dst = binary.BigEndian.AppendUint32(dst, uint32(code))
	return append(dst, track)
}

// appendTagHeader appends a tag header: type, 24-bit body size, and the
// millisecond timestamp, its low 24 bits then the extended high byte.
func appendTagHeader(dst []byte, tagType byte, size int, timestamp uint32) []byte {
	return append(dst, tagType, byte(size>>16), byte(size>>8), byte(size),
		byte(timestamp>>16), byte(timestamp>>8), byte(timestamp), byte(timestamp>>24),
		0, 0, 0)
}

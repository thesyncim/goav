package mp4

import (
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/av1"
	"github.com/thesyncim/goav/internal/bitio"
	"github.com/thesyncim/goav/internal/h264"
)

// muxCodecs are the sample entries the muxer writes.
var muxCodecs = map[av.CodecID]string{
	av.CodecH264: "avc1",
	av.CodecHEVC: "hvc1",
	av.CodecAV1:  "av01",
	av.CodecVP8:  "vp08",
	av.CodecVP9:  "vp09",
	av.CodecAAC:  "mp4a",
	av.CodecOpus: "Opus",
	av.CodecFLAC: "fLaC",
}

// configure reads what the track's sample entry needs from the stream's
// declared parameters. What they leave out — an avcC, an av1C, a VP9
// profile — the first samples supply.
func (t *muxTrack) configure() error {
	extra := t.stream.Codec.ExtraData.Bytes
	switch t.codec {
	case av.CodecH264:
		switch {
		case len(extra) > 4 && extra[0] == 1:
			// Samples are written with 4-byte lengths, whatever the record said.
			t.config = append([]byte(nil), extra...)
			t.config[4] = 0xfc | 3
		case h264.HasStartCode(extra):
			if record, err := h264.ConfigFromAnnexB(extra); err == nil {
				t.config = record
			}
		}
	case av.CodecHEVC:
		t.config = hvc1Config(extra)
		if t.config == nil && h264.HasStartCode(extra) {
			t.config, _ = hvcCFromAnnexB(extra)
		}
	case av.CodecAV1:
		switch {
		case len(extra) >= 4 && extra[0] == 0x81:
			t.config = append([]byte(nil), extra...)
		case len(extra) != 0:
			if record, err := av1.ConfigFromFrames([][]byte{extra}); err == nil {
				t.config = record
			}
		}
	case av.CodecAAC:
		if len(extra) >= 2 {
			t.config = append([]byte(nil), extra...)
		}
	case av.CodecOpus:
		head := extra
		if len(head) < 19 || string(head[:8]) != "OpusHead" {
			head = defaultOpusHead(t.channels)
		}
		dops, err := dOpsFromOpusHead(head)
		if err != nil {
			return err
		}
		t.config = dops
		t.prerollNS = opusPreSkipNS(av.CodecParameters{ID: av.CodecOpus, ExtraData: av.Buffer{Bytes: head}})
		t.channels = int(head[9])
	case av.CodecFLAC:
		blocks, ok := flacMetadataBlocks(extra)
		if !ok {
			return fmt.Errorf("%w: a FLAC track needs its STREAMINFO in the stream's extradata", ErrUnsupported)
		}
		t.config = blocks
	}
	return nil
}

// sampleData turns a packet payload into the sample the mdat stores: H.264
// as 4-byte length-prefixed NAL units, AV1 without temporal delimiters, AAC
// without ADTS headers. A codec configuration the stream did not declare is
// read from the first sample that carries it. The result may alias payload or
// the track's scratch buffer.
func (t *muxTrack) sampleData(payload []byte, keyframe bool) ([]byte, error) {
	switch t.codec {
	case av.CodecH264:
		if !h264.HasStartCode(payload) {
			return payload, nil
		}
		if t.config == nil {
			if record, err := h264.ConfigFromAnnexB(payload); err == nil {
				t.config = record
			}
		}
		var err error
		t.scratch, err = h264.AppendAVC(t.scratch[:0], payload)
		return t.scratch, err
	case av.CodecHEVC:
		if !h264.HasStartCode(payload) {
			return payload, nil
		}
		if t.config == nil {
			t.config, _ = hvcCFromAnnexB(payload)
		}
		var err error
		t.scratch, err = appendHVC1(t.scratch[:0], payload)
		return t.scratch, err
	case av.CodecAV1:
		if t.config == nil {
			if record, err := av1.ConfigFromFrames([][]byte{payload}); err == nil {
				t.config = record
			}
		}
		return av1.StripTemporalDelimiters(payload)
	case av.CodecVP8, av.CodecVP9:
		if keyframe && !t.vpKnown {
			t.vp, t.vpKnown = parseVPKeyframe(t.codec, payload)
			if t.width == 0 {
				t.width, t.height = t.vp.width, t.vp.height
			}
		}
	case av.CodecAAC:
		if header, asc, ok := adtsHeader(payload); ok {
			if t.config == nil {
				t.config = asc
			}
			return payload[header:], nil
		}
	}
	return payload, nil
}

// ready reports whether the track can write its sample entry.
func (t *muxTrack) ready() error {
	switch t.codec {
	case av.CodecHEVC:
		if t.config == nil {
			return fmt.Errorf("%w: stream %q (hevc) declared no hvcC and no keyframe carried its VPS, SPS, and PPS", ErrInvalidData, t.stream.ID)
		}
	case av.CodecH264, av.CodecAV1, av.CodecAAC:
		if t.config == nil {
			return fmt.Errorf("%w: stream %q (%s) declared no codec configuration and no sample carried one", ErrInvalidData, t.stream.ID, t.codec)
		}
	}
	return nil
}

func (t *muxTrack) writeSampleEntry(w *boxWriter) {
	fourcc := muxCodecs[t.codec]
	if t.media == av.MediaVideo {
		w.start(fourcc)
		w.zeros(6)
		w.u16(1) // data_reference_index
		w.zeros(16)
		w.u16(uint16(t.width))
		w.u16(uint16(t.height))
		w.u32(0x00480000) // 72 dpi
		w.u32(0x00480000)
		w.u32(0)
		w.u16(1) // frame_count
		w.zeros(32)
		w.u16(0x0018) // depth
		w.i16(-1)
		switch t.codec {
		case av.CodecH264:
			w.box("avcC", t.config)
		case av.CodecHEVC:
			w.box("hvcC", t.config)
		case av.CodecAV1:
			w.box("av1C", t.config)
		default:
			t.writeVPCC(w)
		}
		w.end()
		return
	}
	w.start(fourcc)
	w.zeros(6)
	w.u16(1)
	w.zeros(8)
	w.u16(uint16(max(t.channels, 1)))
	w.u16(16) // samplesize
	w.u32(0)
	rate := uint32(t.sampleRate)
	if t.codec == av.CodecOpus {
		rate = 48000
	}
	if rate > 0xffff {
		rate = 0
	}
	w.u32(rate << 16)
	switch t.codec {
	case av.CodecAAC:
		writeESDS(w, t.config)
	case av.CodecOpus:
		w.box("dOps", t.config)
	case av.CodecFLAC:
		w.startFull("dfLa", 0, 0)
		w.bytes(t.config)
		w.end()
	}
	w.end()
}

// writeVPCC writes the VP codec configuration (vpcC, version 1): profile,
// level, bit depth, chroma subsampling, and unspecified colour.
func (t *muxTrack) writeVPCC(w *boxWriter) {
	vp := t.vp
	if !t.vpKnown {
		vp = vpInfo{bitDepth: 8}
	}
	w.startFull("vpcC", 1, 0)
	w.u8(vp.profile)
	w.u8(vpLevel(t.width, t.height))
	w.u8(vp.bitDepth<<4 | vp.chroma<<1)
	w.u8(2) // colour primaries: unspecified
	w.u8(2) // transfer characteristics: unspecified
	w.u8(2) // matrix coefficients: unspecified
	w.u16(0)
	w.end()
}

// vpLevel is the lowest VP9 level whose picture size holds width x height.
func vpLevel(width, height int) uint8 {
	size := width * height
	for _, level := range []struct {
		max   int
		level uint8
	}{{36864, 10}, {73728, 11}, {122880, 20}, {245760, 21}, {552960, 30}, {983040, 31}, {2228224, 40}, {8912896, 50}, {35651584, 60}} {
		if size <= level.max {
			return level.level
		}
	}
	return 62
}

// vpInfo is what a VP8 or VP9 keyframe header says about its stream.
type vpInfo struct {
	profile, bitDepth, chroma uint8
	width, height             int
}

// parseVPKeyframe reads a keyframe's profile, bit depth, chroma subsampling
// (vpcC's code: 0 4:2:0, 2 4:2:2, 3 4:4:4), and size.
func parseVPKeyframe(codec av.CodecID, data []byte) (vpInfo, bool) {
	if codec == av.CodecVP8 {
		// Frame tag, then the 0x9d012a start code and 14-bit sizes.
		if len(data) < 10 || data[0]&1 != 0 || data[3] != 0x9d || data[4] != 0x01 || data[5] != 0x2a {
			return vpInfo{}, false
		}
		return vpInfo{
			profile:  data[0] >> 1 & 7,
			bitDepth: 8,
			width:    int(binary.LittleEndian.Uint16(data[6:]) & 0x3fff),
			height:   int(binary.LittleEndian.Uint16(data[8:]) & 0x3fff),
		}, true
	}
	// A short header reads as zeros and fails a check; err reports it.
	r := bitio.Reader{Data: data, Err: ErrInvalidData}
	var err error
	bits := func(n int) uint64 {
		v, e := r.Bits(n)
		if err == nil {
			err = e
		}
		return v
	}
	var info vpInfo
	if bits(2) != 2 { // frame_marker
		return vpInfo{}, false
	}
	low, high := bits(1), bits(1)
	info.profile = uint8(high<<1 | low)
	if info.profile == 3 {
		bits(1)
	}
	if bits(1) == 1 || bits(1) != 0 { // show_existing_frame, frame_type
		return vpInfo{}, false
	}
	bits(2) // show_frame, error_resilient_mode
	if bits(24) != 0x498342 {
		return vpInfo{}, false
	}
	info.bitDepth = 8
	if info.profile >= 2 {
		info.bitDepth = 10
		if bits(1) == 1 {
			info.bitDepth = 12
		}
	}
	subX, subY := uint64(1), uint64(1)
	if bits(3) != 7 { // not sRGB
		bits(1) // color_range
		if info.profile == 1 || info.profile == 3 {
			subX, subY = bits(1), bits(1)
			bits(1)
		}
	} else {
		subX, subY = 0, 0
		if info.profile == 1 || info.profile == 3 {
			bits(1)
		}
	}
	switch {
	case subX == 1 && subY == 1:
		info.chroma = 0
	case subX == 1:
		info.chroma = 2
	default:
		info.chroma = 3
	}
	info.width = int(bits(16)) + 1
	info.height = int(bits(16)) + 1
	return info, err == nil
}

// writeESDS writes an MPEG-4 elementary stream descriptor for AAC: the
// decoder configuration with the AudioSpecificConfig as its specific info.
func writeESDS(w *boxWriter, asc []byte) {
	decoderSpecific := mpegDescriptor(0x05, asc)
	decoderConfig := mpegDescriptor(0x04, append([]byte{
		0x40,    // object type: MPEG-4 audio
		0x15,    // stream type: audio
		0, 0, 0, // buffer size
		0, 0, 0, 0, // max bitrate
		0, 0, 0, 0, // average bitrate
	}, decoderSpecific...))
	es := mpegDescriptor(0x03, append(append([]byte{0, 0, 0}, decoderConfig...), mpegDescriptor(0x06, []byte{0x02})...))
	w.startFull("esds", 0, 0)
	w.bytes(es)
	w.end()
}

// mpegDescriptor encodes an MPEG-4 descriptor: its tag, its size in 7-bit
// groups, and its payload.
func mpegDescriptor(tag byte, payload []byte) []byte {
	out := []byte{tag}
	size := len(payload)
	var groups []byte
	for {
		groups = append([]byte{byte(size & 0x7f)}, groups...)
		if size >>= 7; size == 0 {
			break
		}
	}
	for i := 0; i < len(groups)-1; i++ {
		groups[i] |= 0x80
	}
	return append(append(out, groups...), payload...)
}

// adtsHeader reads an ADTS frame header: its length and the
// AudioSpecificConfig it implies.
func adtsHeader(data []byte) (int, []byte, bool) {
	if len(data) < 7 || data[0] != 0xff || data[1]&0xf6 != 0xf0 {
		return 0, nil, false
	}
	header := 7
	if data[1]&1 == 0 { // CRC present
		header = 9
	}
	if len(data) < header {
		return 0, nil, false
	}
	objectType := data[2]>>6 + 1
	frequency := data[2] >> 2 & 0xf
	channels := data[2]&1<<2 | data[3]>>6
	asc := []byte{objectType<<3 | frequency>>1, frequency<<7 | channels<<3}
	return header, asc, true
}

// dOpsFromOpusHead converts an OpusHead (RFC 7845) to the dOps box payload
// (ISO/IEC 23003-5): the same fields, big-endian, without the magic.
func dOpsFromOpusHead(head []byte) ([]byte, error) {
	if len(head) < 19 || string(head[:8]) != "OpusHead" {
		return nil, fmt.Errorf("%w: OpusHead", ErrInvalidData)
	}
	channels, family := int(head[9]), head[18]
	dops := []byte{0, head[9]}
	dops = binary.BigEndian.AppendUint16(dops, binary.LittleEndian.Uint16(head[10:]))
	dops = binary.BigEndian.AppendUint32(dops, binary.LittleEndian.Uint32(head[12:]))
	dops = binary.BigEndian.AppendUint16(dops, binary.LittleEndian.Uint16(head[16:]))
	dops = append(dops, family)
	if family != 0 {
		if len(head) < 21+channels {
			return nil, fmt.Errorf("%w: OpusHead channel mapping", ErrInvalidData)
		}
		dops = append(dops, head[19:21+channels]...)
	}
	return dops, nil
}

// defaultOpusHead is the OpusHead of a mono or stereo stream that declared
// none, as Matroska writes it.
func defaultOpusHead(channels int) []byte {
	if channels < 1 || channels > 2 {
		channels = 2
	}
	head := make([]byte, 19)
	copy(head, "OpusHead")
	head[8] = 1
	head[9] = byte(channels)
	binary.LittleEndian.PutUint32(head[12:], 48000)
	return head
}

// flacMetadataBlocks returns the metadata blocks of a FLAC stream header
// ("fLaC" then blocks), which the dfLa box carries without the magic.
func flacMetadataBlocks(extra []byte) ([]byte, bool) {
	if len(extra) >= 4 && string(extra[:4]) == "fLaC" {
		extra = extra[4:]
	}
	if len(extra) < 4+34 || extra[0]&0x7f != 0 { // STREAMINFO first
		return nil, false
	}
	return append([]byte(nil), extra...), true
}

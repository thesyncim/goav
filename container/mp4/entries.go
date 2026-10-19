package mp4

import (
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/aac"
	"github.com/thesyncim/goav/internal/av1"
	"github.com/thesyncim/goav/internal/h264"
	"github.com/thesyncim/goav/internal/opus"
	"github.com/thesyncim/goav/internal/vpx"
)

// muxCodecs are the sample entries the muxer writes.
var muxCodecs = map[av.CodecID]string{
	av.CodecH264: "avc1",
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
			head = opus.DefaultHead(t.channels)
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
	case av.CodecAV1:
		if t.config == nil {
			if record, err := av1.ConfigFromFrames([][]byte{payload}); err == nil {
				t.config = record
			}
		}
		return av1.StripTemporalDelimiters(payload)
	case av.CodecVP8, av.CodecVP9:
		if keyframe && !t.vpKnown {
			t.vp, t.vpKnown = vpx.ParseKeyframe(t.codec, payload)
			if t.width == 0 {
				t.width, t.height = t.vp.Width, t.vp.Height
			}
		}
	case av.CodecAAC:
		if header, asc, ok := aac.ADTS(payload); ok {
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
		case av.CodecAV1:
			w.box("av1C", t.config)
		default:
			vp := t.vp
			if !t.vpKnown {
				vp = vpx.Keyframe{}
			}
			w.box("vpcC", vpx.Record(vp, t.width, t.height))
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

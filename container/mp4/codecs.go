package mp4

import (
	"bytes"

	"github.com/thesyncim/goav/av"
)

// sampleEntryCodec maps a sample-entry format (the entry box type) to an av
// codec id and media kind, returning CodecUnknown for entries goav has no codec
// for (those tracks are skipped rather than failing the whole file).
func sampleEntryCodec(format string) (av.CodecID, av.MediaType) {
	switch format {
	case "avc1", "avc3":
		return av.CodecH264, av.MediaVideo
	case "av01":
		return av.CodecAV1, av.MediaVideo
	case "vp08":
		return av.CodecVP8, av.MediaVideo
	case "vp09":
		return av.CodecVP9, av.MediaVideo
	case "mp4a":
		return av.CodecAAC, av.MediaAudio
	case "Opus":
		return av.CodecOpus, av.MediaAudio
	case "fLaC":
		return av.CodecFLAC, av.MediaAudio
	default:
		return av.CodecUnknown, av.MediaUnknown
	}
}

// videoConfigBoxes are the codec-configuration child boxes a visual sample
// entry carries; their bytes become the stream ExtraData.
var videoConfigBoxes = map[string]bool{"avcC": true, "hvcC": true, "av1C": true, "vpcC": true}

// parseVisualSampleEntry reads width/height from a visual sample entry and
// extracts its codec configuration box bytes as ExtraData.
func parseVisualSampleEntry(payload []byte) (width, height int, extradata []byte) {
	p := newParser(payload)
	p.skip(6) // reserved
	p.skip(2) // data_reference_index
	p.skip(2) // pre_defined
	p.skip(2) // reserved
	p.skip(12)
	width = int(p.u16())
	height = int(p.u16())
	p.skip(4)  // horizresolution
	p.skip(4)  // vertresolution
	p.skip(4)  // reserved
	p.skip(2)  // frame_count
	p.skip(32) // compressorname
	p.skip(2)  // depth
	p.skip(2)  // pre_defined
	if p.err() != nil {
		return 0, 0, nil
	}
	reader := bytes.NewReader(payload)
	_ = walkBoxes(reader, int64(p.pos), int64(len(payload)), func(h boxHeader) error {
		if extradata == nil && videoConfigBoxes[h.Type] {
			if pl, err := readPayload(reader, h); err == nil {
				extradata = append([]byte(nil), pl...)
			}
		}
		return nil
	})
	return width, height, extradata
}

// parseAudioSampleEntry reads channel count and sample rate from an audio
// sample entry and extracts its codec configuration as ExtraData: the
// AudioSpecificConfig from esds for AAC, or the dOps payload for Opus.
func parseAudioSampleEntry(payload []byte) (channels, sampleRate int, extradata []byte) {
	p := newParser(payload)
	p.skip(6)             // reserved
	p.skip(2)             // data_reference_index
	version := int(p.u16()) // QuickTime sound version (0 for ISO BMFF)
	p.skip(2)             // revision level
	p.skip(4)             // vendor
	channels = int(p.u16())
	p.skip(2) // sample size
	p.skip(2) // compression id / pre_defined
	p.skip(2) // packet size / reserved
	sampleRate = int(p.u32() >> 16)
	switch version {
	case 1:
		p.skip(16) // samples/bytes per packet/frame/sample
	case 2:
		p.skip(36)
	}
	if p.err() != nil {
		return 0, 0, nil
	}
	reader := bytes.NewReader(payload)
	_ = walkBoxes(reader, int64(p.pos), int64(len(payload)), func(h boxHeader) error {
		if extradata != nil {
			return nil
		}
		switch h.Type {
		case "esds":
			if pl, err := readPayload(reader, h); err == nil {
				extradata = audioSpecificConfigFromESDS(pl)
			}
		case "dOps":
			if pl, err := readPayload(reader, h); err == nil {
				extradata = append([]byte(nil), pl...)
			}
		}
		return nil
	})
	return channels, sampleRate, extradata
}

// MPEG-4 descriptor tags carried in an esds box.
const (
	esTag      = 0x03
	decConfTag = 0x04
	decSpecTag = 0x05
)

// audioSpecificConfigFromESDS walks the MPEG-4 descriptor tree in an esds box
// payload and returns the DecoderSpecificInfo (the AudioSpecificConfig that AAC
// decoders expect as ExtraData), or nil when it is absent.
func audioSpecificConfigFromESDS(payload []byte) []byte {
	p := newParser(payload)
	p.fullBox() // version + flags
	tag, ok := descriptor(p)
	if !ok || tag != esTag {
		return nil
	}
	// ES_Descriptor: ES_ID(2) + flags(1) with optional dependency/url/ocr fields.
	p.skip(2)
	flags := p.u8()
	if flags&0x80 != 0 {
		p.skip(2) // dependsOn_ES_ID
	}
	if flags&0x40 != 0 {
		urlLen := int(p.u8())
		p.skip(urlLen)
	}
	if flags&0x20 != 0 {
		p.skip(2) // OCR_ES_Id
	}
	tag, ok = descriptor(p)
	if !ok || tag != decConfTag {
		return nil
	}
	// DecoderConfigDescriptor: objectType(1)+streamType(1)+bufferSize(3)+max(4)+avg(4).
	p.skip(13)
	tag, ok = descriptor(p)
	if !ok || tag != decSpecTag {
		return nil
	}
	asc := p.take(p.remaining())
	if p.err() != nil || len(asc) == 0 {
		return nil
	}
	return append([]byte(nil), asc...)
}

// descriptor reads a tag byte and its expandable length, leaving the cursor at
// the descriptor body. The length itself is consumed but not returned because
// the descriptors above are read by their fixed field layout.
func descriptor(p *parser) (tag uint8, ok bool) {
	tag = p.u8()
	for i := 0; i < 4; i++ {
		b := p.u8()
		if b&0x80 == 0 {
			break
		}
	}
	return tag, p.err() == nil
}

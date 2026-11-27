package hls

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// nextBox returns the first whole box in b, with its size.
func nextBox(b []byte) ([]byte, int, bool, error) {
	if len(b) < 8 {
		return nil, 0, false, nil
	}
	size := uint64(binary.BigEndian.Uint32(b))
	headerSize := uint64(8)
	if size == 1 {
		if len(b) < 16 {
			return nil, 0, false, nil
		}
		size = binary.BigEndian.Uint64(b[8:])
		headerSize = 16
	}
	if size < headerSize {
		return nil, 0, false, errors.New("hls: invalid mp4 box size")
	}
	if size > uint64(len(b)) {
		return nil, 0, false, nil
	}
	return b[:size], int(size), true, nil
}

// boxPayload returns the child payload of a top-level MP4 box, handling both
// 32-bit (8-byte header) and 64-bit extended (16-byte header) boxes.
func boxPayload(b []byte) []byte {
	if len(b) < 8 {
		return nil
	}
	if binary.BigEndian.Uint32(b) == 1 {
		if len(b) < 16 {
			return nil
		}
		return b[16:]
	}
	return b[8:]
}

// readTracks reads each track's id, timescale, and handler from the moov.
func (w *writer) readTracks(moov []byte) {
	w.anchor = 0
	video := false
	forBoxes(boxPayload(moov), func(typ string, trak []byte) {
		if typ != "trak" {
			return
		}
		var id, timescale uint32
		var handler string
		forBoxes(trak, func(typ string, b []byte) {
			switch typ {
			case "tkhd":
				id = fullBoxField(b, 12, 20)
			case "mdia":
				forBoxes(b, func(typ string, b []byte) {
					switch typ {
					case "mdhd":
						timescale = fullBoxField(b, 12, 20)
					case "hdlr":
						if len(b) >= 12 {
							handler = string(b[8:12])
						}
					case "minf":
						w.readSampleEntry(b)
					}
				})
			}
		})
		if id == 0 || timescale == 0 {
			return
		}
		w.timescales[id] = timescale
		if w.anchor == 0 || handler == "vide" && !video {
			w.anchor, video = id, handler == "vide"
		}
	})
}

// fullBoxField reads the u32 a FullBox keeps at offset v0 in version 0 and
// v1 in version 1, counted from the payload start.
func fullBoxField(b []byte, v0, v1 int) uint32 {
	if len(b) < 4 {
		return 0
	}
	at := v0
	if b[0] == 1 {
		at = v1
	}
	if len(b) < at+4 {
		return 0
	}
	return binary.BigEndian.Uint32(b[at:])
}

// fragmentTimes reads where a fragment starts and ends, in seconds, from
// the anchor track's run, or the first run when the anchor has none.
func (w *writer) fragmentTimes(moof []byte) (start, end float64, independent, ok bool) {
	forBoxes(boxPayload(moof), func(typ string, traf []byte) {
		if typ != "traf" {
			return
		}
		var id uint32
		var base, duration uint64
		key := true
		forBoxes(traf, func(typ string, b []byte) {
			switch typ {
			case "tfhd":
				if len(b) >= 8 {
					id = binary.BigEndian.Uint32(b[4:])
				}
			case "tfdt":
				if len(b) >= 12 && b[0] == 1 {
					base = binary.BigEndian.Uint64(b[4:])
				} else if len(b) >= 8 {
					base = uint64(binary.BigEndian.Uint32(b[4:]))
				}
			case "trun":
				d, sync := readRun(b)
				duration += d
				key = key && sync
			}
		})
		timescale := w.timescales[id]
		if timescale == 0 || ok && id != w.anchor {
			return
		}
		start = float64(base) / float64(timescale)
		end = float64(base+duration) / float64(timescale)
		independent, ok = key, true
	})
	return start, end, independent, ok
}

// readRun sums a trun's sample durations and reports whether its first
// sample is a sync sample, which a player can start decoding at.
func readRun(b []byte) (duration uint64, sync bool) {
	if len(b) < 8 {
		return 0, false
	}
	flags := binary.BigEndian.Uint32(b) & 0xffffff
	count := int(binary.BigEndian.Uint32(b[4:]))
	at := 8
	if flags&0x1 != 0 {
		at += 4
	}
	sync = true
	const nonSync = 0x00010000
	if flags&0x4 != 0 {
		if at+4 <= len(b) {
			sync = binary.BigEndian.Uint32(b[at:])&nonSync == 0
		}
		at += 4
	}
	stride, flagsAt := 0, -1
	for _, f := range [4]uint32{0x100, 0x200, 0x400, 0x800} {
		if flags&f != 0 {
			if f == 0x400 {
				flagsAt = stride
			}
			stride += 4
		}
	}
	if flags&0x4 == 0 && flagsAt >= 0 && at+flagsAt+4 <= len(b) {
		sync = binary.BigEndian.Uint32(b[at+flagsAt:])&nonSync == 0
	}
	if flags&0x100 == 0 {
		return 0, sync
	}
	for i := 0; i < count && at+4 <= len(b); i++ {
		duration += uint64(binary.BigEndian.Uint32(b[at:]))
		at += stride
	}
	return duration, sync
}

// forBoxes calls fn with each child box's type and payload.
func forBoxes(b []byte, fn func(typ string, payload []byte)) {
	for len(b) >= 8 {
		size := int(binary.BigEndian.Uint32(b))
		header := 8
		if size == 1 && len(b) >= 16 {
			size, header = int(binary.BigEndian.Uint64(b[8:])), 16
		}
		if size < header || size > len(b) {
			return
		}
		fn(string(b[4:8]), b[header:size])
		b = b[size:]
	}
}

// readSampleEntry reads a track's codec, as an RFC 6381 codecs string, and
// a video track's size from the first sample entry under minf/stbl/stsd.
func (w *writer) readSampleEntry(minf []byte) {
	forBoxes(minf, func(typ string, stbl []byte) {
		if typ != "stbl" {
			return
		}
		forBoxes(stbl, func(typ string, stsd []byte) {
			if typ != "stsd" || len(stsd) < 8 {
				return
			}
			first := true
			forBoxes(stsd[8:], func(fourcc string, entry []byte) {
				if !first {
					return
				}
				first = false
				if codec := codecString(fourcc, entry); codec != "" {
					w.codecs = append(w.codecs, codec)
				}
				if visual[fourcc] && len(entry) >= 28 {
					w.width = int(binary.BigEndian.Uint16(entry[24:]))
					w.height = int(binary.BigEndian.Uint16(entry[26:]))
				}
			})
		})
	})
}

var visual = map[string]bool{"avc1": true, "avc3": true, "av01": true, "vp08": true, "vp09": true}

// codecString spells a sample entry as HLS CODECS wants it (RFC 6381 and
// the codecs' ISO BMFF bindings), from its configuration box.
func codecString(fourcc string, entry []byte) string {
	config := func(child string, at int) []byte {
		var found []byte
		forBoxes(entry[min(at, len(entry)):], func(typ string, b []byte) {
			if typ == child && found == nil {
				found = b
			}
		})
		return found
	}
	const visualHeader, audioHeader = 78, 28
	switch fourcc {
	case "avc1", "avc3":
		if c := config("avcC", visualHeader); len(c) >= 4 {
			return fmt.Sprintf("%s.%02x%02x%02x", fourcc, c[1], c[2], c[3])
		}
	case "av01":
		if c := config("av1C", visualHeader); len(c) >= 3 {
			tier := "M"
			if c[2]&0x80 != 0 {
				tier = "H"
			}
			depth := 8
			if c[2]&0x40 != 0 {
				depth = 10
				if c[2]&0x20 != 0 {
					depth = 12
				}
			}
			return fmt.Sprintf("av01.%d.%02d%s.%02d", c[1]>>5, c[1]&0x1f, tier, depth)
		}
	case "vp08", "vp09":
		if c := config("vpcC", visualHeader); len(c) >= 7 {
			return fmt.Sprintf("%s.%02d.%02d.%02d", fourcc, c[4], c[5], c[6]>>4)
		}
	case "mp4a":
		if c := config("esds", audioHeader); len(c) > 0 {
			if objectType, ok := aacObjectType(c); ok {
				return fmt.Sprintf("mp4a.40.%d", objectType)
			}
		}
		return "mp4a.40.2"
	case "Opus":
		return "Opus"
	case "fLaC":
		return "fLaC"
	}
	return ""
}

// aacObjectType reads the audio object type from an esds box: the first
// five bits of the AudioSpecificConfig in its ES descriptor's decoder
// configuration.
func aacObjectType(esds []byte) (byte, bool) {
	if len(esds) < 4 {
		return 0, false
	}
	p := esds[4:]
	descriptor := func(tag byte) bool {
		if len(p) < 2 || p[0] != tag {
			return false
		}
		p = p[1:]
		for len(p) > 0 && p[0]&0x80 != 0 {
			p = p[1:]
		}
		if len(p) == 0 {
			return false
		}
		p = p[1:]
		return true
	}
	skip := func(n int) bool {
		if n > len(p) {
			return false
		}
		p = p[n:]
		return true
	}
	if !descriptor(0x03) || len(p) < 3 {
		return 0, false
	}
	flags := p[2]
	p = p[3:]
	if flags&0x80 != 0 && !skip(2) {
		return 0, false
	}
	if flags&0x40 != 0 && (len(p) == 0 || !skip(1+int(p[0]))) {
		return 0, false
	}
	if flags&0x20 != 0 && !skip(2) {
		return 0, false
	}
	if !descriptor(0x04) || !skip(13) || !descriptor(0x05) || len(p) == 0 {
		return 0, false
	}
	return p[0] >> 3, true
}

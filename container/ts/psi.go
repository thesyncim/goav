package ts

import "encoding/binary"

// esInfo is one elementary stream of a PMT.
type esInfo struct {
	streamType  byte
	pid         uint16
	descriptors []byte
}

// patSection builds the PAT of a one-program stream.
func patSection(pmtPID uint16) []byte {
	s := sectionHeader(tablePAT, 1, 0)
	s = binary.BigEndian.AppendUint16(s, program)
	s = binary.BigEndian.AppendUint16(s, 0xe000|pmtPID)
	return finishSection(s)
}

// pmtSection builds the PMT of the program.
func pmtSection(pcrPID uint16, streams []esInfo) []byte {
	s := sectionHeader(tablePMT, program, 0)
	s = binary.BigEndian.AppendUint16(s, 0xe000|pcrPID)
	s = binary.BigEndian.AppendUint16(s, 0xf000) // no program descriptors
	for _, es := range streams {
		s = append(s, es.streamType)
		s = binary.BigEndian.AppendUint16(s, 0xe000|es.pid)
		s = binary.BigEndian.AppendUint16(s, 0xf000|uint16(len(es.descriptors)))
		s = append(s, es.descriptors...)
	}
	return finishSection(s)
}

// sectionHeader starts a long-form PSI section: table id, the length (set by
// finishSection), the table id extension, version 0, current, section 0 of 0.
func sectionHeader(table byte, extension uint16, version byte) []byte {
	s := []byte{table, 0xb0, 0}
	s = binary.BigEndian.AppendUint16(s, extension)
	return append(s, 0xc1|version<<1, 0, 0)
}

func finishSection(s []byte) []byte {
	length := len(s) - 3 + 4
	s[1] = 0xb0 | byte(length>>8)
	s[2] = byte(length)
	return binary.BigEndian.AppendUint32(s, crc32(s))
}

// section is a parsed long-form PSI section.
type section struct {
	table     byte
	extension uint16
	version   byte
	body      []byte // between the header and the CRC
}

// parseSection checks a whole section's syntax and CRC.
func parseSection(b []byte) (section, bool) {
	if len(b) < 12 || b[1]&0x80 == 0 || crc32(b) != 0 {
		return section{}, false
	}
	if b[5]&1 == 0 { // not yet applicable
		return section{}, false
	}
	return section{
		table:     b[0],
		extension: binary.BigEndian.Uint16(b[3:]),
		version:   b[5] >> 1 & 0x1f,
		body:      b[8 : len(b)-4],
	}, true
}

// patPrograms calls fn with each program's number and PMT PID.
func patPrograms(body []byte, fn func(number, pid uint16)) {
	for ; len(body) >= 4; body = body[4:] {
		fn(binary.BigEndian.Uint16(body), binary.BigEndian.Uint16(body[2:])&0x1fff)
	}
}

// pmtStreams reads a PMT body: the PCR PID and the elementary streams. ok
// is false for a malformed body.
func pmtStreams(body []byte) (pcrPID uint16, streams []esInfo, ok bool) {
	if len(body) < 4 {
		return 0, nil, false
	}
	pcrPID = binary.BigEndian.Uint16(body) & 0x1fff
	info := int(binary.BigEndian.Uint16(body[2:]) & 0x0fff)
	if 4+info > len(body) {
		return 0, nil, false
	}
	for rest := body[4+info:]; len(rest) > 0; {
		if len(rest) < 5 {
			return 0, nil, false
		}
		n := int(binary.BigEndian.Uint16(rest[3:]) & 0x0fff)
		if 5+n > len(rest) {
			return 0, nil, false
		}
		streams = append(streams, esInfo{
			streamType:  rest[0],
			pid:         binary.BigEndian.Uint16(rest[1:]) & 0x1fff,
			descriptors: rest[5 : 5+n],
		})
		rest = rest[5+n:]
	}
	return pcrPID, streams, true
}

// descriptor returns the payload of the first descriptor with tag, for an
// extension descriptor the one with extension tag.
func descriptor(descriptors []byte, tag, extension byte) ([]byte, bool) {
	for len(descriptors) >= 2 {
		n := int(descriptors[1])
		if 2+n > len(descriptors) {
			return nil, false
		}
		payload := descriptors[2 : 2+n]
		if descriptors[0] == tag && (tag != descriptorExtension || len(payload) > 0 && payload[0] == extension) {
			return payload, true
		}
		descriptors = descriptors[2+n:]
	}
	return nil, false
}

// sectionBuffer reassembles the PSI sections of one PID from packet
// payloads (pointer_field, sections spanning packets, several per packet).
type sectionBuffer struct {
	data []byte
	open bool
}

// maxSection is the longest PAT or PMT section (ISO/IEC 13818-1 §2.4.4).
const maxSection = 1024

// add feeds one packet payload and calls fn with each whole section.
func (s *sectionBuffer) add(payload []byte, start bool, fn func([]byte)) {
	if start {
		if len(payload) == 0 {
			s.reset()
			return
		}
		pointer := int(payload[0])
		payload = payload[1:]
		if pointer > len(payload) {
			s.reset()
			return
		}
		if s.open {
			s.data = append(s.data, payload[:pointer]...)
			s.drain(fn)
		}
		s.reset()
		s.open = true
		payload = payload[pointer:]
	}
	if !s.open {
		return
	}
	s.data = append(s.data, payload...)
	s.drain(fn)
}

// drain hands over every whole section at the head of the buffer; a
// section stuffed with 0xFF ends the packet's sections.
func (s *sectionBuffer) drain(fn func([]byte)) {
	for len(s.data) >= 3 {
		if s.data[0] == 0xff {
			s.reset()
			return
		}
		n := 3 + int(binary.BigEndian.Uint16(s.data[1:])&0x0fff)
		if n > maxSection+3 {
			s.reset()
			return
		}
		if len(s.data) < n {
			return
		}
		fn(s.data[:n])
		s.data = s.data[:copy(s.data, s.data[n:])]
	}
}

func (s *sectionBuffer) reset() {
	s.data = s.data[:0]
	s.open = false
}

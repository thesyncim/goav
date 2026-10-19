// Package opus holds the Opus identification header (OpusHead, RFC 7845)
// the containers carry as an Opus stream's codec configuration.
package opus

import "encoding/binary"

// DefaultHead is the OpusHead of a mono or stereo stream that declared
// none, as Matroska writes it: 48 kHz, no pre-skip, no gain, mapping
// family 0.
func DefaultHead(channels int) []byte {
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

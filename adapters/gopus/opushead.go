package gopus

import "encoding/binary"

// opusHeadRate is the rate RFC 7845 counts pre-skip in, whatever the decoder's
// output rate.
const opusHeadRate = 48000

// opusHead builds the RFC 7845 §5.1 identification header (channel mapping
// family 0) that Matroska CodecPrivate, MP4 dOps, and Ogg all carry: pre-skip
// in 48 kHz samples and the encoder's input rate.
func opusHead(channels, inputRate, preSkip48k int) []byte {
	head := make([]byte, 19)
	copy(head, "OpusHead")
	head[8] = 1
	head[9] = byte(channels)
	binary.LittleEndian.PutUint16(head[10:12], uint16(preSkip48k))
	binary.LittleEndian.PutUint32(head[12:16], uint32(inputRate))
	return head
}

// opusPreSkip returns the pre-skip an identification header declares, in
// 48 kHz samples; anything that is not an OpusHead (RTP streams carry none)
// declares no pre-skip.
func opusPreSkip(head []byte) int {
	if len(head) < 19 || string(head[:8]) != "OpusHead" {
		return 0
	}
	return int(binary.LittleEndian.Uint16(head[10:12]))
}

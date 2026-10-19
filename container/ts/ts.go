package ts

import "errors"

var (
	// ErrInvalidData reports bytes that are not a transport stream, or a
	// packet the muxer cannot place.
	ErrInvalidData = errors.New("ts: invalid data")
	// ErrUnsupported reports a codec or configuration the transport stream
	// mapping does not carry.
	ErrUnsupported = errors.New("ts: unsupported")
	// ErrClosed reports a use after Close.
	ErrClosed = errors.New("ts: closed")
)

const (
	packetSize  = 188
	syncByte    = 0x47
	payloadSize = packetSize - 4

	pidPAT    = 0x0000
	pidNull   = 0x1fff
	pidPMT    = 0x1000
	pidStream = 0x0100
	program   = 1

	tablePAT = 0x00
	tablePMT = 0x02

	streamTypeAAC     = 0x0f // ISO/IEC 13818-7 ADTS
	streamTypeH264    = 0x1b
	streamTypeHEVC    = 0x24
	streamTypePrivate = 0x06 // PES private data: Opus, by registration

	streamIDVideo    = 0xe0
	streamIDAudio    = 0xc0
	streamIDPrivate1 = 0xbd

	descriptorRegistration = 0x05
	descriptorLanguage     = 0x0a
	descriptorExtension    = 0x7f
	extensionOpus          = 0x80

	clock90k  = 90000
	clock27M  = 27_000_000
	wrap33    = int64(1) << 33
	pcrPerPTS = 300

	flagDiscontinuity = 0x80
	flagRandomAccess  = 0x40
	flagPCR           = 0x10
)

// crcTable is the CRC-32/MPEG-2 table: polynomial 0x04C11DB7, most
// significant bit first, no reflection.
var crcTable = func() (table [256]uint32) {
	for i := range table {
		c := uint32(i) << 24
		for range 8 {
			if c&0x80000000 != 0 {
				c = c<<1 ^ 0x04c11db7
			} else {
				c <<= 1
			}
		}
		table[i] = c
	}
	return table
}()

// crc32 is the CRC-32/MPEG-2 of b, initial value 0xFFFFFFFF, no final XOR: a
// section followed by its CRC sums to zero.
func crc32(b []byte) uint32 {
	c := uint32(0xffffffff)
	for _, v := range b {
		c = c<<8 ^ crcTable[byte(c>>24)^v]
	}
	return c
}

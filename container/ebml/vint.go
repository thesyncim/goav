package ebml

import (
	"encoding/binary"
	"io"
	"math/bits"
)

const (
	MaxIDWidth   = 4
	MaxSizeWidth = 8
)

type ID uint64

type Size struct {
	Value   uint64
	Unknown bool
	Width   int
}

func VINTWidth(first byte) (int, error) {
	if first == 0 {
		return 0, ErrInvalidVINT
	}
	return bits.LeadingZeros8(first) + 1, nil
}

func DecodeIDVINT(src []byte) (ID, int, error) {
	if len(src) == 0 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	b0 := src[0]
	if b0 == 0 {
		return 0, 0, ErrInvalidVINT
	}
	width := bits.LeadingZeros8(b0) + 1
	if width > MaxIDWidth {
		return 0, 0, ErrInvalidElementID
	}
	if len(src) < width {
		return 0, 0, io.ErrUnexpectedEOF
	}
	switch width {
	case 1:
		return ID(b0), 1, nil
	case 2:
		return ID(uint64(b0)<<8 | uint64(src[1])), 2, nil
	case 3:
		return ID(uint64(b0)<<16 | uint64(src[1])<<8 | uint64(src[2])), 3, nil
	default:
		return ID(binary.BigEndian.Uint32(src)), 4, nil
	}
}

func DecodeSizeVINT(src []byte) (Size, error) {
	if len(src) == 0 {
		return Size{}, io.ErrUnexpectedEOF
	}
	b0 := src[0]
	if b0 == 0 {
		return Size{}, ErrInvalidVINT
	}
	width := bits.LeadingZeros8(b0) + 1
	if len(src) < width {
		return Size{}, io.ErrUnexpectedEOF
	}
	v := uint64(b0 &^ (0x80 >> uint(width-1)))
	switch width {
	case 1:
		return Size{Value: v, Unknown: v == 0x7f, Width: 1}, nil
	case 2:
		v = v<<8 | uint64(src[1])
		return Size{Value: v, Unknown: v == 0x3fff, Width: 2}, nil
	case 3:
		v = v<<16 | uint64(src[1])<<8 | uint64(src[2])
		return Size{Value: v, Unknown: v == 0x1fffff, Width: 3}, nil
	case 4:
		v = v<<24 | uint64(src[1])<<16 | uint64(src[2])<<8 | uint64(src[3])
		return Size{Value: v, Unknown: v == 0x0fffffff, Width: 4}, nil
	default:
		for i := 1; i < width; i++ {
			v = (v << 8) | uint64(src[i])
		}
		return Size{Value: v, Unknown: v == maxVINTData(width), Width: width}, nil
	}
}

func DecodeUnsignedVINT(src []byte) (uint64, int, error) {
	size, err := DecodeSizeVINT(src)
	if err != nil {
		return 0, 0, err
	}
	return size.Value, size.Width, nil
}

func EncodeID(dst []byte, id ID) (int, error) {
	width, err := IDWidth(id)
	if err != nil {
		return 0, err
	}
	if len(dst) < width {
		return 0, io.ErrShortBuffer
	}
	value := uint64(id)
	for i := width - 1; i >= 0; i-- {
		dst[i] = byte(value)
		value >>= 8
	}
	return width, nil
}

func IDWidth(id ID) (int, error) {
	value := uint64(id)
	if value == 0 {
		return 0, ErrInvalidElementID
	}
	var width int
	switch {
	case value <= 0xff:
		width = 1
	case value <= 0xffff:
		width = 2
	case value <= 0xffffff:
		width = 3
	case value <= 0xffffffff:
		width = 4
	default:
		return 0, ErrInvalidElementID
	}
	var buf [MaxIDWidth]byte
	for i := width - 1; i >= 0; i-- {
		buf[i] = byte(value)
		value >>= 8
	}
	if _, _, err := DecodeIDVINT(buf[:width]); err != nil {
		return 0, err
	}
	return width, nil
}

func EncodeSizeVINT(dst []byte, value uint64) (int, error) {
	width, err := SizeVINTWidth(value)
	if err != nil {
		return 0, err
	}
	return EncodeSizeVINTWidth(dst, value, width)
}

func EncodeSizeVINTWidth(dst []byte, value uint64, width int) (int, error) {
	if width < 1 || width > MaxSizeWidth {
		return 0, ErrInvalidVINT
	}
	if value > maxKnownSize(width) {
		return 0, ErrInvalidSize
	}
	return encodeVINT(dst, value, width)
}

func EncodeUnknownSize(dst []byte, width int) (int, error) {
	if width < 1 || width > MaxSizeWidth {
		return 0, ErrInvalidVINT
	}
	return encodeVINT(dst, maxVINTData(width), width)
}

func EncodeUnsignedVINT(dst []byte, value uint64) (int, error) {
	width, err := UnsignedVINTWidth(value)
	if err != nil {
		return 0, err
	}
	return encodeVINT(dst, value, width)
}

func EncodeUnsignedVINTWidth(dst []byte, value uint64, width int) (int, error) {
	if width < 1 || width > MaxSizeWidth || value > maxVINTData(width) {
		return 0, ErrInvalidVINT
	}
	return encodeVINT(dst, value, width)
}

func SizeVINTWidth(value uint64) (int, error) {
	if value > (uint64(1)<<56)-2 {
		return 0, ErrInvalidSize
	}
	return (bits.Len64(value+1) + 6) / 7, nil
}

func UnsignedVINTWidth(value uint64) (int, error) {
	if value == 0 {
		return 1, nil
	}
	if value > (uint64(1)<<56)-1 {
		return 0, ErrVINTOverflow
	}
	return (bits.Len64(value) + 6) / 7, nil
}

func ReadID(r io.Reader, scratch *[MaxSizeWidth]byte) (ID, int, error) {
	if _, err := io.ReadFull(r, scratch[:1]); err != nil {
		return 0, 0, err
	}
	width, err := VINTWidth(scratch[0])
	if err != nil {
		return 0, 0, err
	}
	if width > MaxIDWidth {
		return 0, 0, ErrInvalidElementID
	}
	if _, err := io.ReadFull(r, scratch[1:width]); err != nil {
		return 0, 0, err
	}
	id, _, err := DecodeIDVINT(scratch[:width])
	return id, width, err
}

func ReadSize(r io.Reader, scratch *[MaxSizeWidth]byte) (Size, error) {
	if _, err := io.ReadFull(r, scratch[:1]); err != nil {
		return Size{}, err
	}
	width, err := VINTWidth(scratch[0])
	if err != nil {
		return Size{}, err
	}
	if _, err := io.ReadFull(r, scratch[1:width]); err != nil {
		return Size{}, err
	}
	return DecodeSizeVINT(scratch[:width])
}

func ReadUnsignedVINT(r io.Reader, scratch *[MaxSizeWidth]byte) (uint64, int, error) {
	size, err := ReadSize(r, scratch)
	if err != nil {
		return 0, 0, err
	}
	return size.Value, size.Width, nil
}

func encodeVINT(dst []byte, value uint64, width int) (int, error) {
	if len(dst) < width {
		return 0, io.ErrShortBuffer
	}
	switch width {
	case 1:
		if value > 0x7f {
			return 0, ErrVINTOverflow
		}
		dst[0] = byte(value) | 0x80
		return 1, nil
	case 2:
		if value > 0x3fff {
			return 0, ErrVINTOverflow
		}
		dst[0] = byte(value>>8) | 0x40
		dst[1] = byte(value)
		return 2, nil
	case 3:
		if value > 0x1fffff {
			return 0, ErrVINTOverflow
		}
		dst[0] = byte(value>>16) | 0x20
		dst[1] = byte(value >> 8)
		dst[2] = byte(value)
		return 3, nil
	case 4:
		if value > 0x0fffffff {
			return 0, ErrVINTOverflow
		}
		binary.BigEndian.PutUint32(dst, uint32(value)|0x10000000)
		return 4, nil
	}
	for i := width - 1; i >= 0; i-- {
		dst[i] = byte(value)
		value >>= 8
	}
	if value != 0 {
		return 0, ErrVINTOverflow
	}
	dst[0] |= 0x80 >> uint(width-1)
	return width, nil
}

func maxKnownSize(width int) uint64 {
	return maxVINTData(width) - 1
}

func maxVINTData(width int) uint64 {
	return (uint64(1) << uint(7*width)) - 1
}

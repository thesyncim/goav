package ebml

import (
	"io"
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
	for width := 1; width <= MaxSizeWidth; width++ {
		if first&(0x80>>uint(width-1)) != 0 {
			return width, nil
		}
	}
	return 0, ErrInvalidVINT
}

func DecodeIDVINT(src []byte) (ID, int, error) {
	if len(src) == 0 {
		return 0, 0, io.ErrUnexpectedEOF
	}
	width, err := VINTWidth(src[0])
	if err != nil {
		return 0, 0, err
	}
	if width > MaxIDWidth {
		return 0, 0, ErrInvalidElementID
	}
	if len(src) < width {
		return 0, 0, io.ErrUnexpectedEOF
	}
	var id uint64
	var data uint64
	for i := 0; i < width; i++ {
		id = (id << 8) | uint64(src[i])
		data = (data << 8) | uint64(src[i])
	}
	data &^= uint64(0x80>>uint(width-1)) << uint(8*(width-1))
	if data == 0 {
		return 0, 0, ErrInvalidElementID
	}
	return ID(id), width, nil
}

func DecodeSizeVINT(src []byte) (Size, error) {
	if len(src) == 0 {
		return Size{}, io.ErrUnexpectedEOF
	}
	width, err := VINTWidth(src[0])
	if err != nil {
		return Size{}, err
	}
	if len(src) < width {
		return Size{}, io.ErrUnexpectedEOF
	}
	value := uint64(src[0] &^ (0x80 >> uint(width-1)))
	for i := 1; i < width; i++ {
		value = (value << 8) | uint64(src[i])
	}
	allOnes := maxVINTData(width)
	return Size{Value: value, Unknown: value == allOnes, Width: width}, nil
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
	for width := 1; width <= MaxSizeWidth; width++ {
		if value <= maxKnownSize(width) {
			return width, nil
		}
	}
	return 0, ErrInvalidSize
}

func UnsignedVINTWidth(value uint64) (int, error) {
	for width := 1; width <= MaxSizeWidth; width++ {
		if value <= maxVINTData(width) {
			return width, nil
		}
	}
	return 0, ErrVINTOverflow
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

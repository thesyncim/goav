package matroska

import "errors"

var (
	ErrNilReader          = errors.New("matroska: nil reader")
	ErrNilWriter          = errors.New("matroska: nil writer")
	ErrNilPacket          = errors.New("matroska: nil packet")
	ErrClosed             = errors.New("matroska: closed")
	ErrInvalidTrack       = errors.New("matroska: invalid track")
	ErrUnknownTrack       = errors.New("matroska: unknown track")
	ErrTrackAfterWrite    = errors.New("matroska: cannot add track after writing packets")
	ErrUnsupportedCodec   = errors.New("matroska: unsupported codec")
	ErrUnsupportedElement = errors.New("matroska: unsupported element")
	ErrUnsupportedLacing  = errors.New("matroska: lacing is not supported")
	ErrInvalidData        = errors.New("matroska: invalid data")
	ErrPayloadTooSmall    = errors.New("matroska: packet data capacity too small")
	ErrTimecodeOverflow   = errors.New("matroska: block timecode out of range")
)

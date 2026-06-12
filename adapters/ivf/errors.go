package ivf

import "errors"

var (
	ErrInvalidHeader      = errors.New("ivf: invalid header")
	ErrNilReader          = errors.New("ivf: nil reader")
	ErrNilWriter          = errors.New("ivf: nil writer")
	ErrFrameCountTooLarge = errors.New("ivf: too many frames")
	ErrPayloadTooLarge    = errors.New("ivf: payload too large")
	ErrPayloadTooSmall    = errors.New("ivf: payload buffer too small")
	ErrUnsupportedCodec   = errors.New("ivf: unsupported codec")
	ErrUnsupportedStream  = errors.New("ivf: unsupported stream layout")
)

package codec

import "errors"

var (
	ErrNilResult             = errors.New("codec: nil result")
	ErrNilDecoder            = errors.New("codec: nil decoder")
	ErrNilEncoder            = errors.New("codec: nil encoder")
	ErrClosed                = errors.New("codec: closed")
	ErrOutputBufferTooSmall  = errors.New("codec: output buffer too small")
	ErrResultFull            = errors.New("codec: result capacity full")
	ErrUnavailable           = errors.New("codec: unavailable")
	ErrUnsupportedFormat     = errors.New("codec: unsupported format")
	ErrUnsupportedSampleRate = errors.New("codec: unsupported sample rate")
)

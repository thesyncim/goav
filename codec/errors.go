package codec

import "errors"

var (
	ErrNilResult             = errors.New("codec: nil result")
	ErrNilDecoder            = errors.New("codec: nil decoder")
	ErrOutputBufferTooSmall  = errors.New("codec: output buffer too small")
	ErrResultFull            = errors.New("codec: result capacity full")
	ErrUnsupportedFormat     = errors.New("codec: unsupported format")
	ErrUnsupportedSampleRate = errors.New("codec: unsupported sample rate")
)

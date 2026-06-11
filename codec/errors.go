package codec

import "errors"

// Codec sentinels, matched with errors.Is.
var (
	// ErrNilResult reports a nil result buffer handed to DecodeInto/EncodeInto.
	ErrNilResult = errors.New("codec: nil result")
	// ErrNilDecoder reports a nil decoder where one is required.
	ErrNilDecoder = errors.New("codec: nil decoder")
	// ErrNilEncoder reports a nil encoder where one is required.
	ErrNilEncoder = errors.New("codec: nil encoder")
	// ErrClosed reports a call on a closed decoder or encoder.
	ErrClosed = errors.New("codec: closed")
	// ErrOutputBufferTooSmall reports a caller-owned output buffer without the
	// capacity the operation needs; grow the buffer or the configured bounds.
	ErrOutputBufferTooSmall = errors.New("codec: output buffer too small")
	// ErrResultFull reports a result slice at its DecodeBounds capacity.
	ErrResultFull = errors.New("codec: result capacity full")
	// ErrUnavailable reports a codec whose descriptor is registered but whose
	// implementation is not (no factory).
	ErrUnavailable = errors.New("codec: unavailable")
	// ErrUnsupportedCodecSwitch reports a mid-stream codec change the decoder
	// cannot rebind to.
	ErrUnsupportedCodecSwitch = errors.New("codec: unsupported codec switch")
	// ErrUnsupportedFormat reports a sample or pixel format the codec cannot
	// consume or produce.
	ErrUnsupportedFormat = errors.New("codec: unsupported format")
	// ErrUnsupportedSampleRate reports an audio rate outside the codec's
	// supported set.
	ErrUnsupportedSampleRate = errors.New("codec: unsupported sample rate")
)

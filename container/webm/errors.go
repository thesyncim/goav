package webm

import "errors"

var (
	ErrUnsupportedWebMCodec           = errors.New("webm: unsupported codec")
	ErrUnsupportedWebMCodecPrivate    = errors.New("webm: unsupported codec private")
	ErrUnsupportedWebMDocType         = errors.New("webm: unsupported doc type")
	ErrUnsupportedWebMMetadata        = errors.New("webm: unsupported metadata")
	ErrUnsupportedWebMTrackMetadata   = errors.New("webm: unsupported track metadata")
	ErrUnsupportedWebMContentEncoding = errors.New("webm: unsupported content encoding")
	ErrNonMonotonicWebMTimecode       = errors.New("webm: non-monotonic timecode")
)

package webm

import "errors"

var (
	ErrUnsupportedWebMCodec   = errors.New("webm: unsupported codec")
	ErrUnsupportedWebMDocType = errors.New("webm: unsupported doc type")
)

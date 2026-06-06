package annexb

import "errors"

var (
	ErrNilWriter         = errors.New("annexb: nil writer")
	ErrUnsupportedStream = errors.New("annexb: unsupported stream layout")
)

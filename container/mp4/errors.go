package mp4

import "errors"

var (
	ErrNilReader   = errors.New("mp4: nil reader")
	ErrNilPacket   = errors.New("mp4: nil packet")
	ErrClosed      = errors.New("mp4: closed")
	ErrInvalidData = errors.New("mp4: invalid data")
	ErrTruncated   = errors.New("mp4: truncated box")
	ErrNoMovie     = errors.New("mp4: no moov box")
	ErrNoTracks    = errors.New("mp4: no readable tracks")
	ErrUnsupported = errors.New("mp4: unsupported")
	// ErrPayloadTooSmall reports that the caller's packet payload buffer cannot
	// hold a sample.
	ErrPayloadTooSmall = errors.New("mp4: packet data capacity too small")
)

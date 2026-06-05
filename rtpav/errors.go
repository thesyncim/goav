package rtpav

import "errors"

var (
	ErrInvalidJitterCapacity = errors.New("rtpav: invalid jitter capacity")
	ErrClosed                = errors.New("rtpav: closed")
	ErrNilReceiver           = errors.New("rtpav: nil receiver")
	ErrInvalidPayload        = errors.New("rtpav: invalid payload")
	ErrPayloadNotFound       = errors.New("rtpav: payload not found")
	ErrDepacketizerNotFound  = errors.New("rtpav: depacketizer not found")
	ErrFrameTooLarge         = errors.New("rtpav: frame too large")
	ErrResultFull            = errors.New("rtpav: result capacity full")
)

package rtpav

import "errors"

var (
	ErrInvalidJitterCapacity = errors.New("rtpav: invalid jitter capacity")
	ErrClosed                = errors.New("rtpav: closed")
	ErrNilReceiver           = errors.New("rtpav: nil receiver")
	ErrPayloadNotFound       = errors.New("rtpav: payload not found")
	ErrDepacketizerNotFound  = errors.New("rtpav: depacketizer not found")
	ErrResultFull            = errors.New("rtpav: result capacity full")
)

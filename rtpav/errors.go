package rtpav

import "errors"

var (
	ErrInvalidJitterCapacity = errors.New("rtpav: invalid jitter capacity")
	ErrResultFull            = errors.New("rtpav: result capacity full")
)

package rtpav

import "errors"

// RTP sentinels, matched with errors.Is.
var (
	// ErrInvalidJitterCapacity reports a jitter ring sized <= 0.
	ErrInvalidJitterCapacity = errors.New("rtpav: invalid jitter capacity")
	// ErrClosed reports a read on a closed source or reader.
	ErrClosed = errors.New("rtpav: closed")
	// ErrNilReceiver reports a nil PacketReader handed to Receive.
	ErrNilReceiver = errors.New("rtpav: nil receiver")
	// ErrInvalidPayload reports an RTP payload the depacketizer cannot parse.
	ErrInvalidPayload = errors.New("rtpav: invalid payload")
	// ErrPayloadNotFound reports a payload type the PayloadMap cannot resolve.
	ErrPayloadNotFound = errors.New("rtpav: payload not found")
	// ErrDepacketizerNotFound reports a codec with no registered depacketizer.
	ErrDepacketizerNotFound = errors.New("rtpav: depacketizer not found")
	// ErrFrameTooLarge reports a reassembled frame exceeding the configured
	// maximum video frame size.
	ErrFrameTooLarge = errors.New("rtpav: frame too large")
	// ErrResultFull reports a result slice at capacity.
	ErrResultFull = errors.New("rtpav: result capacity full")
)

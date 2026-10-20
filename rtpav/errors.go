package rtpav

import "errors"

// RTP sentinels, matched with errors.Is.
var (
	// ErrOrderedConfigurationRequired refuses a configuration change on an
	// explicitly static legacy reader; event channels cannot prove ordering.
	ErrOrderedConfigurationRequired = errors.New("rtpav: ordered configuration read required")
	// ErrInvalidConfiguration reports a missing map or an unannounced change.
	ErrInvalidConfiguration = errors.New("rtpav: invalid ordered configuration")
	// ErrEpochRegression reports a stream's read map moving backwards.
	ErrEpochRegression = errors.New("rtpav: configuration epoch regressed")
	// ErrAmbiguousStream reports packet configuration without enough stream
	// identity to choose among equal codecs on different tracks.
	ErrAmbiguousStream = errors.New("rtpav: ambiguous RTP stream")
	// ErrJitterStreamConflict refuses interleaved streams through one jitter
	// window. Configure a separate receive Source and jitter buffer per track.
	ErrJitterStreamConflict = errors.New("rtpav: jitter buffer requires one stream")
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
	// maximum video frame size, or an Opus packet too large for the MTU.
	ErrFrameTooLarge = errors.New("rtpav: frame too large")
	// ErrResultFull reports a result slice at capacity.
	ErrResultFull = errors.New("rtpav: result capacity full")
	// ErrPacketizerNotFound reports a codec with no built-in packetizer.
	ErrPacketizerNotFound = errors.New("rtpav: packetizer not found")
	// ErrNoTimestamp reports a packet to send with neither a PTS nor a DTS.
	ErrNoTimestamp = errors.New("rtpav: packet has no timestamp")
	// ErrNilWriter reports a nil PacketWriter handed to Send.
	ErrNilWriter = errors.New("rtpav: nil writer")
	// ErrUnknownStream reports a packet for a stream Send was not opened with.
	ErrUnknownStream = errors.New("rtpav: unknown stream")
)

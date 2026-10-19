package rtmpav

import (
	"errors"
	"fmt"
)

var (
	// ErrCodecRefused reports a stream in a codec the server does not take:
	// it negotiated Enhanced RTMP and listed codecs without this one. Encode
	// the stream to H.264 or AAC, which every server takes.
	ErrCodecRefused = errors.New("rtmpav: the server does not take the codec")
	// ErrStreamEnded reports a live stream whose publisher stopped or
	// dropped; goav.Reconnect waits for the next.
	ErrStreamEnded = errors.New("rtmpav: the live stream ended")

	errProtocol = errors.New("rtmp protocol error")
)

// StatusError is a server's refusal: the command it answered with
// _error or an error-level onStatus, and the code and description it
// gave, the stream key replaced wherever the description repeats it.
type StatusError struct {
	Command     string
	Code        string
	Description string
}

func (e *StatusError) Error() string {
	if e.Description == "" {
		return fmt.Sprintf("the server refused %s: %s", e.Command, e.Code)
	}
	return fmt.Sprintf("the server refused %s: %s (%s)", e.Command, e.Code, e.Description)
}

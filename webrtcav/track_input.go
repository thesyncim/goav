package webrtcav

import (
	"context"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/rtpav"
)

// Track adapts a remote WebRTC track into an RTP receive provider: the track
// codec is derived from the negotiated codec parameters and the source node is
// named after the track stream. Extra options layer on top of the derived
// ones, so callers can override the name or add jitter, feedback, buffer
// limits, and decode bounds.
func Track(track *webrtc.TrackRemote, options ...rtpav.ReceiveOption) *rtpav.Input {
	reader, err := NewTrackRemoteAdapter().AdaptTrack(context.Background(), RemoteTrack{Track: track})
	if err != nil {
		return failedInput(err, options...)
	}
	return trackInput(reader, options...)
}

func trackInput(reader TrackReader, options ...rtpav.ReceiveOption) *rtpav.Input {
	streams, err := reader.Streams(context.Background())
	if err != nil {
		return failedInput(err, options...)
	}
	if len(streams) == 0 {
		return failedInput(ErrUnknownStream, options...)
	}
	derived := trackReceiveOptions(streams[0])
	return rtpav.Receive(reader, append(derived, options...)...)
}

// trackReceiveOptions derives the receive intent from the adapted track
// stream: the source name and the codec the track carries.
func trackReceiveOptions(stream av.Stream) []rtpav.ReceiveOption {
	options := make([]rtpav.ReceiveOption, 0, 2)
	name := string(stream.ID)
	if name == "" {
		name = stream.Name
	}
	if name != "" {
		options = append(options, rtpav.WithName(name))
	}
	spec := codec.Spec{
		ID:         stream.Codec.ID,
		Type:       firstMediaType(stream.Codec.Type, stream.Type),
		Parameters: stream.Codec,
	}
	if spec.Parameters.Type == "" {
		spec.Parameters.Type = spec.Type
	}
	if spec.ID != "" {
		options = append(options, rtpav.WithCodec(spec))
	}
	return options
}

func firstMediaType(values ...av.MediaType) av.MediaType {
	for i := range values {
		if values[i] != "" {
			return values[i]
		}
	}
	return ""
}

// failedInput defers a track adaptation error to OpenSource, where the
// provider contract reports errors.
func failedInput(err error, options ...rtpav.ReceiveOption) *rtpav.Input {
	return rtpav.Receive(failedReader{err: err}, options...)
}

type failedReader struct {
	err error
}

func (r failedReader) Streams(context.Context) ([]av.Stream, error) {
	return nil, r.err
}

func (r failedReader) PayloadMap() rtpav.PayloadMap {
	return nil
}

func (r failedReader) ReadRTP(context.Context) (*rtp.Packet, error) {
	return nil, r.err
}

func (r failedReader) Events() <-chan av.Event {
	return nil
}

func (r failedReader) Close() error {
	return nil
}

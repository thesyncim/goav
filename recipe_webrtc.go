package goav

import (
	"context"

	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/webrtcav"
)

func WebRTCTrack(track *webrtc.TrackRemote) InputSpec {
	return webRTCRemote(webrtcav.RemoteTrack{Track: track})
}

func webRTCRemote(remote webrtcav.RemoteTrack) InputSpec {
	spec := InputSpec{
		input: format.Input{
			Protocol: av.ProtocolWebRTC,
			Realtime: true,
		},
		rtp:      &rtpInputSpec{},
		realtime: true,
	}
	reader, err := webrtcav.NewTrackRemoteAdapter().AdaptTrack(context.Background(), remote)
	if err != nil {
		spec.err = err
		return spec
	}
	spec.rtp.receiver = reader

	streams, err := reader.Streams(context.Background())
	if err != nil {
		spec.err = err
		return spec
	}
	if len(streams) == 0 {
		spec.err = webrtcav.ErrUnknownStream
		return spec
	}
	spec.applyWebRTCStream(streams[0])
	return spec
}

func (s *InputSpec) applyWebRTCStream(stream av.Stream) {
	s.name = firstNonEmpty(string(stream.ID), stream.Name)
	s.input.Name = s.name
	s.codec = codec.CodecSpec{
		ID:         stream.Codec.ID,
		Type:       firstMediaType(stream.Codec.Type, stream.Type),
		Parameters: stream.Codec,
	}
	if s.codec.Parameters.Type == "" {
		s.codec.Parameters.Type = s.codec.Type
	}
}

func firstMediaType(values ...av.MediaType) av.MediaType {
	for i := range values {
		if values[i] != "" {
			return values[i]
		}
	}
	return ""
}

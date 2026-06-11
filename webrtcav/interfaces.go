package webrtcav

import (
	"context"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/rtpav"
)

// RemoteTrack bundles one accepted Pion remote track with everything a
// reader needs: the receiver/transceiver handles, negotiated codec, the
// goav stream view, payload resolution, and the feedback path.
type RemoteTrack struct {
	Track       *webrtc.TrackRemote
	Receiver    *webrtc.RTPReceiver
	Transceiver *webrtc.RTPTransceiver
	Codec       webrtc.RTPCodecParameters
	Stream      av.Stream
	Payloads    rtpav.PayloadMap
	Feedback    rtpav.FeedbackWriter
	Metadata    av.Metadata
}

// TrackCodecUpdate carries a mid-session renegotiation: the new codec
// parameters, stream view, and payload map a live TrackReader should adopt.
type TrackCodecUpdate struct {
	Codec    webrtc.RTPCodecParameters
	Stream   av.Stream
	Payloads rtpav.PayloadMap
	Metadata av.Metadata
}

// TrackReader is an rtpav.PacketReader over one WebRTC track that also
// accepts live codec and track replacement updates, so renegotiations never
// rebuild the pipeline.
type TrackReader interface {
	Streams(context.Context) ([]av.Stream, error)
	PayloadMap() rtpav.PayloadMap
	UpdateCodec(context.Context, TrackCodecUpdate) error
	UpdateTrack(context.Context, RemoteTrack) error
	ReadRTP(context.Context) (*rtp.Packet, error)
	Events() <-chan av.Event
	Close() error
}

// TrackAdapter turns an accepted remote track into a TrackReader; custom
// adapters can wrap readers with application policy.
type TrackAdapter interface {
	AdaptTrack(context.Context, RemoteTrack) (TrackReader, error)
}

// Session is one WebRTC receive session: accept incoming tracks, exchange
// SDP/ICE, send RTCP feedback, and observe session events. NewSession returns
// the Pion-backed implementation.
type Session interface {
	PeerConnection() *webrtc.PeerConnection
	AcceptTrack(context.Context) (RemoteTrack, error)
	WriteRTCP(context.Context, []rtcp.Packet) error
	Events() <-chan av.Event
	SetRemoteDescription(context.Context, webrtc.SessionDescription) (webrtc.SessionDescription, error)
	AddICECandidate(context.Context, webrtc.ICECandidateInit) error
	Close() error
}

// SessionConfig configures a Pion-backed WebRTC receive session.
type SessionConfig struct {
	Configuration webrtc.Configuration
	API           *webrtc.API
	Metadata      av.Metadata
	MaxTracks     int
	MaxEvents     int
}

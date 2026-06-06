package webrtcav

import (
	"context"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/rtpav"
)

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

type TrackCodecUpdate struct {
	Codec    webrtc.RTPCodecParameters
	Stream   av.Stream
	Payloads rtpav.PayloadMap
	Metadata av.Metadata
}

type TrackReader interface {
	Streams(context.Context) ([]av.Stream, error)
	PayloadMap() rtpav.PayloadMap
	UpdateCodec(context.Context, TrackCodecUpdate) error
	UpdateTrack(context.Context, RemoteTrack) error
	ReadRTP(context.Context) (*rtp.Packet, error)
	Events() <-chan av.Event
	Close() error
}

// TrackReceiver is a WebRTC track reader that can also write RTCP feedback.
type TrackReceiver interface {
	TrackReader
	rtpav.FeedbackWriter
}

type TrackAdapter interface {
	AdaptTrack(context.Context, RemoteTrack) (TrackReader, error)
}

type Session interface {
	Negotiator
	PeerConnection() *webrtc.PeerConnection
	AcceptTrack(context.Context) (RemoteTrack, error)
	WriteRTCP(context.Context, []rtcp.Packet) error
	Events() <-chan av.Event
}

type Negotiator interface {
	SetRemoteDescription(context.Context, webrtc.SessionDescription) (webrtc.SessionDescription, error)
	AddICECandidate(context.Context, webrtc.ICECandidateInit) error
	Close() error
}

type SessionFactory interface {
	NewSession(context.Context, SessionConfig) (Session, error)
}

// SessionConfig configures a Pion-backed WebRTC receive session.
type SessionConfig struct {
	Configuration webrtc.Configuration
	API           *webrtc.API
	Metadata      av.Metadata
	MaxTracks     int
	MaxEvents     int
}

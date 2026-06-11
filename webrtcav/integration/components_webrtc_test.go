package integration

import (
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/webrtcav"
)

func TestComponentWebRTCTrackSetFeedsRTPSource(t *testing.T) {
	ctx := context.Background()
	initial := componentWebRTCOpusRemote("audio", 1)
	replacement := componentWebRTCOpusRemote("audio", 2)
	session := &componentWebRTCSession{tracks: []webrtcav.RemoteTrack{initial, replacement}}
	adapter := &componentTrackAdapter{
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 480},
			Payload: []byte{0xf8},
		}},
	}
	set, err := webrtcav.NewTrackSet(webrtcav.TrackSetConfig{Session: session, Adapter: adapter})
	if err != nil {
		t.Fatal(err)
	}

	added, err := set.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	replaced, err := set.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if added.Kind != webrtcav.TrackAdded || replaced.Kind != webrtcav.TrackReplaced ||
		added.Reader != replaced.Reader || replaced.Stream.Epoch != 2 || session.index != 2 ||
		len(adapter.readers) != 1 || len(adapter.readers[0].updates) != 1 ||
		adapter.readers[0].updates[0].Codec.PayloadType != 111 {
		t.Fatalf("added=%+v replaced=%+v session_index=%d readers=%d updates=%+v",
			added, replaced, session.index, len(adapter.readers), adapter.readers[0].updates)
	}
	readers := set.Readers()
	if len(readers) != 1 || readers[0] != replaced.Reader {
		t.Fatalf("readers = %+v", readers)
	}

	source, err := rtpav.NewSource(rtpav.SourceConfig{
		Name:          "webrtc",
		Detail:        "component WebRTC TrackSet reader",
		Receiver:      replaced.Reader,
		Depacketizers: []rtpav.Depacketizer{rtpav.NewOpusDepacketizer(replaced.Stream)},
		Streams:       []av.Stream{replaced.Stream},
		MaxPackets:    1,
		MaxEvents:     1,
	})
	if err != nil {
		t.Fatal(err)
	}
	sink := &componentMediaSink{name: "packets"}
	graph, err := pipeline.NewGraph(pipeline.GraphConfig{Name: "component-webrtc-trackset"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSource(source, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := graph.AddSink(sink, pipeline.BufferPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := graph.Connect(pipeline.Route{From: "webrtc", To: []string{"packets"}, Policy: pipeline.RouteAll}); err != nil {
		t.Fatal(err)
	}

	spec := graph.Spec()
	if len(spec.Nodes) != 2 || len(spec.Edges) != 1 ||
		spec.Nodes[0].Detail != "component WebRTC TrackSet reader" {
		t.Fatalf("spec = %+v", spec)
	}
	if err := graph.Run(ctx); err != nil {
		t.Fatal(err)
	}
	assertComponentSpecStable(t, graph, spec)
	if adapter.readers[0].reads != 1 || sink.packets != 1 || sink.events != 1 || sink.lastEvent != av.EventEndOfStream {
		t.Fatalf("reads=%d packets=%d events=%d last=%s",
			adapter.readers[0].reads, sink.packets, sink.events, sink.lastEvent)
	}
	if sink.lastPacketStreamID != "audio" || sink.lastPacketEpoch != 2 ||
		sink.lastPacketPTS.Value != 480 || sink.lastPacketBytes != 1 ||
		sink.lastPacketOwnership != av.BufferBorrowed {
		t.Fatalf("packet stream=%s epoch=%d pts=%+v bytes=%d ownership=%s",
			sink.lastPacketStreamID, sink.lastPacketEpoch, sink.lastPacketPTS,
			sink.lastPacketBytes, sink.lastPacketOwnership)
	}
	stats := graph.Stats()
	if stats.Packets != 1 || stats.Events != 1 || stats.Delivered != 2 {
		t.Fatalf("stats = %+v", stats)
	}
	if err := graph.Close(); err != nil {
		t.Fatal(err)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	if !adapter.readers[0].closed || session.closed {
		t.Fatalf("reader closed=%v session closed=%v", adapter.readers[0].closed, session.closed)
	}
}

type componentSpecGraph interface {
	Spec() pipeline.Spec
}

func assertComponentSpecStable(t *testing.T, graph componentSpecGraph, before pipeline.Spec) {
	t.Helper()
	if after := graph.Spec(); !reflect.DeepEqual(before, after) {
		t.Fatalf("component graph spec changed after run:\nbefore=%+v\nafter=%+v", before, after)
	}
}

type componentWebRTCSession struct {
	tracks []webrtcav.RemoteTrack
	index  int
	closed bool
}

func (s *componentWebRTCSession) PeerConnection() *webrtc.PeerConnection {
	return nil
}

func (s *componentWebRTCSession) AcceptTrack(ctx context.Context) (webrtcav.RemoteTrack, error) {
	if err := ctx.Err(); err != nil {
		return webrtcav.RemoteTrack{}, err
	}
	if s.closed {
		return webrtcav.RemoteTrack{}, webrtcav.ErrClosed
	}
	if s.index >= len(s.tracks) {
		return webrtcav.RemoteTrack{}, io.EOF
	}
	remote := s.tracks[s.index]
	s.index++
	return remote, nil
}

func (s *componentWebRTCSession) WriteRTCP(ctx context.Context, _ []rtcp.Packet) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return webrtcav.ErrClosed
	}
	return nil
}

func (s *componentWebRTCSession) Events() <-chan av.Event {
	return nil
}

func (s *componentWebRTCSession) SetRemoteDescription(ctx context.Context, _ webrtc.SessionDescription) (webrtc.SessionDescription, error) {
	if err := ctx.Err(); err != nil {
		return webrtc.SessionDescription{}, err
	}
	if s.closed {
		return webrtc.SessionDescription{}, webrtcav.ErrClosed
	}
	return webrtc.SessionDescription{}, nil
}

func (s *componentWebRTCSession) AddICECandidate(ctx context.Context, _ webrtc.ICECandidateInit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return webrtcav.ErrClosed
	}
	return nil
}

func (s *componentWebRTCSession) Close() error {
	s.closed = true
	return nil
}

type componentTrackAdapter struct {
	packets []*rtp.Packet
	readers []*componentTrackReader
}

func (a *componentTrackAdapter) AdaptTrack(_ context.Context, remote webrtcav.RemoteTrack) (webrtcav.TrackReader, error) {
	reader := &componentTrackReader{
		stream:   remote.Stream,
		payloads: remote.Payloads,
		packets:  a.packets,
	}
	a.readers = append(a.readers, reader)
	return reader, nil
}

type componentTrackReader struct {
	stream   av.Stream
	payloads rtpav.PayloadMap
	packets  []*rtp.Packet
	updates  []webrtcav.RemoteTrack
	reads    int
	closed   bool
}

func (r *componentTrackReader) Streams(context.Context) ([]av.Stream, error) {
	return []av.Stream{r.stream}, nil
}

func (r *componentTrackReader) PayloadMap() rtpav.PayloadMap {
	return r.payloads
}

func (r *componentTrackReader) UpdateCodec(_ context.Context, update webrtcav.TrackCodecUpdate) error {
	if update.Stream.ID != "" {
		r.stream = update.Stream
	}
	if update.Payloads != nil {
		r.payloads = update.Payloads
	}
	return nil
}

func (r *componentTrackReader) UpdateTrack(_ context.Context, remote webrtcav.RemoteTrack) error {
	r.updates = append(r.updates, remote)
	if remote.Stream.ID != "" {
		r.stream = remote.Stream
	}
	if remote.Payloads != nil {
		r.payloads = remote.Payloads
	}
	return nil
}

func (r *componentTrackReader) ReadRTP(context.Context) (*rtp.Packet, error) {
	if r.reads >= len(r.packets) {
		return nil, io.EOF
	}
	packet := r.packets[r.reads]
	r.reads++
	return packet, nil
}

func (r *componentTrackReader) Events() <-chan av.Event {
	return nil
}

func (r *componentTrackReader) Close() error {
	r.closed = true
	return nil
}

type componentMediaSink struct {
	name                string
	packets             int
	frames              int
	events              int
	lastEvent           av.EventType
	lastPacketStreamID  av.StreamID
	lastPacketEpoch     av.Epoch
	lastPacketPTS       av.Timestamp
	lastPacketBytes     int
	lastPacketOwnership av.BufferOwnership
	lastFrameStreamID   av.StreamID
	lastFrameEpoch      av.Epoch
	lastFramePTS        av.Timestamp
	lastAudioSamples    int
	lastAudioSampleRate int
	lastPlaneBytes      int
	lastPlaneOwnership  av.BufferOwnership
	order               [2]pipeline.MessageKind
	orderLen            int
	closed              bool
}

func (s *componentMediaSink) Name() string {
	return s.name
}

func (s *componentMediaSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil {
		return nil
	}
	switch msg.Kind {
	case pipeline.MessagePacket:
		s.packets++
		if msg.Packet != nil {
			s.lastPacketStreamID = msg.Packet.StreamID
			s.lastPacketEpoch = msg.Packet.CodecEpoch
			s.lastPacketPTS = msg.Packet.PTS
			s.lastPacketBytes = len(msg.Packet.Payload.Bytes)
			s.lastPacketOwnership = msg.Packet.Payload.Ownership
		}
	case pipeline.MessageFrame:
		s.frames++
		if msg.Frame != nil {
			s.lastFrameStreamID = msg.Frame.StreamID
			s.lastFrameEpoch = msg.Frame.CodecEpoch
			s.lastFramePTS = msg.Frame.PTS
			if msg.Frame.Audio != nil {
				s.lastAudioSamples = msg.Frame.Audio.Samples
				s.lastAudioSampleRate = msg.Frame.Audio.SampleRate
			}
			if len(msg.Frame.Planes) > 0 {
				s.lastPlaneBytes = len(msg.Frame.Planes[0].Buffer.Bytes)
				s.lastPlaneOwnership = msg.Frame.Planes[0].Buffer.Ownership
			}
		}
	case pipeline.MessageEvent:
		s.events++
		if msg.Event != nil {
			s.lastEvent = msg.Event.Type
		}
	}
	if s.orderLen < len(s.order) {
		s.order[s.orderLen] = msg.Kind
		s.orderLen++
	}
	return nil
}

func (s *componentMediaSink) Close() error {
	s.closed = true
	return nil
}

func componentWebRTCOpusRemote(id av.StreamID, epoch av.Epoch) webrtcav.RemoteTrack {
	stream := av.Stream{
		ID:       id,
		Type:     av.MediaAudio,
		Epoch:    epoch,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:           av.CodecOpus,
			Type:         av.MediaAudio,
			ClockRate:    48000,
			SampleRate:   48000,
			Channels:     1,
			SampleFormat: av.SampleFormatS16,
		},
	}
	return webrtcav.RemoteTrack{
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeOpus,
				ClockRate: 48000,
				Channels:  1,
			},
			PayloadType: 111,
		},
		Stream: stream,
		Payloads: rtpav.NewStaticPayloadMap(epoch, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    av.MIMEOpus,
			ClockRate:   48000,
			Channels:    1,
		}}),
	}
}

package webrtcav

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/rtpav"
)

type fakeTrackSetSession struct {
	tracks []RemoteTrack
	index  int
	closed bool
}

type fakeTrackSetAdapter struct {
	readers []*fakeTrackSetReader
}

type fakeTrackSetReader struct {
	stream  av.Stream
	updates []RemoteTrack
	closed  bool
}

var _ Session = (*fakeTrackSetSession)(nil)
var _ TrackAdapter = (*fakeTrackSetAdapter)(nil)
var _ TrackReader = (*fakeTrackSetReader)(nil)

func TestTrackSetAcceptAddsAndReplacesReaders(t *testing.T) {
	ctx := context.Background()
	session := &fakeTrackSetSession{
		tracks: []RemoteTrack{
			{Stream: av.Stream{ID: "video", Epoch: 1, Type: av.MediaVideo}},
			{Stream: av.Stream{ID: "audio", Epoch: 1, Type: av.MediaAudio}},
			{Stream: av.Stream{ID: "video", Epoch: 2, Type: av.MediaVideo}},
		},
	}
	adapter := &fakeTrackSetAdapter{}
	set, err := NewTrackSet(TrackSetConfig{Session: session, Adapter: adapter})
	if err != nil {
		t.Fatal(err)
	}

	video, err := set.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if video.Kind != TrackAdded || video.Stream.ID != "video" {
		t.Fatalf("video update = %+v", video)
	}
	audio, err := set.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if audio.Kind != TrackAdded || audio.Stream.ID != "audio" {
		t.Fatalf("audio update = %+v", audio)
	}
	replaced, err := set.Accept(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if replaced.Kind != TrackReplaced || replaced.Reader != video.Reader || replaced.Stream.Epoch != 2 {
		t.Fatalf("replacement = %+v video reader=%p", replaced, video.Reader)
	}
	if len(adapter.readers) != 2 {
		t.Fatalf("adapted readers = %d, want 2", len(adapter.readers))
	}
	if len(adapter.readers[0].updates) != 1 || adapter.readers[0].updates[0].Stream.Epoch != 2 {
		t.Fatalf("reader updates = %+v", adapter.readers[0].updates)
	}

	reader, ok := set.Reader("video")
	if !ok || reader != video.Reader {
		t.Fatalf("reader lookup ok=%v reader=%p want=%p", ok, reader, video.Reader)
	}
	readers := set.Readers()
	if len(readers) != 2 || readers[0] != video.Reader || readers[1] != audio.Reader {
		t.Fatalf("readers = %+v", readers)
	}
}

func TestTrackSetAcceptRemoteRejectsUnknownStream(t *testing.T) {
	set, err := NewTrackSet(TrackSetConfig{
		Session: &fakeTrackSetSession{},
		Adapter: &fakeTrackSetAdapter{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := set.AcceptRemote(context.Background(), RemoteTrack{}); !errors.Is(err, ErrUnknownStream) {
		t.Fatalf("err = %v, want ErrUnknownStream", err)
	}
}

func TestTrackSetCloseClosesReaders(t *testing.T) {
	session := &fakeTrackSetSession{tracks: []RemoteTrack{{Stream: av.Stream{ID: "audio"}}}}
	adapter := &fakeTrackSetAdapter{}
	set, err := NewTrackSet(TrackSetConfig{Session: session, Adapter: adapter})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := set.Accept(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := set.Close(); err != nil {
		t.Fatal(err)
	}
	if !adapter.readers[0].closed {
		t.Fatal("reader not closed")
	}
	if session.closed {
		t.Fatal("track set should not close the session")
	}
	if _, err := set.Accept(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("err = %v, want ErrClosed", err)
	}
}

func TestNewTrackSetRequiresSession(t *testing.T) {
	if _, err := NewTrackSet(TrackSetConfig{}); !errors.Is(err, ErrNilSession) {
		t.Fatalf("err = %v, want ErrNilSession", err)
	}
}

func (s *fakeTrackSetSession) PeerConnection() *webrtc.PeerConnection {
	return nil
}

func (s *fakeTrackSetSession) AcceptTrack(ctx context.Context) (RemoteTrack, error) {
	if err := ctx.Err(); err != nil {
		return RemoteTrack{}, err
	}
	if s.closed {
		return RemoteTrack{}, ErrClosed
	}
	if s.index >= len(s.tracks) {
		return RemoteTrack{}, io.EOF
	}
	remote := s.tracks[s.index]
	s.index++
	return remote, nil
}

func (s *fakeTrackSetSession) WriteRTCP(ctx context.Context, _ []rtcp.Packet) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	return nil
}

func (s *fakeTrackSetSession) Events() <-chan av.Event {
	return nil
}

func (s *fakeTrackSetSession) SetRemoteDescription(ctx context.Context, _ webrtc.SessionDescription) (webrtc.SessionDescription, error) {
	if err := ctx.Err(); err != nil {
		return webrtc.SessionDescription{}, err
	}
	if s.closed {
		return webrtc.SessionDescription{}, ErrClosed
	}
	return webrtc.SessionDescription{}, nil
}

func (s *fakeTrackSetSession) AddICECandidate(ctx context.Context, _ webrtc.ICECandidateInit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return ErrClosed
	}
	return nil
}

func (s *fakeTrackSetSession) Close() error {
	s.closed = true
	return nil
}

func (a *fakeTrackSetAdapter) AdaptTrack(_ context.Context, remote RemoteTrack) (TrackReader, error) {
	reader := &fakeTrackSetReader{stream: remote.Stream}
	a.readers = append(a.readers, reader)
	return reader, nil
}

func (r *fakeTrackSetReader) Streams(context.Context) ([]av.Stream, error) {
	return []av.Stream{r.stream}, nil
}

func (r *fakeTrackSetReader) PayloadMap() rtpav.PayloadMap {
	return nil
}

func (r *fakeTrackSetReader) UpdateCodec(context.Context, TrackCodecUpdate) error {
	return nil
}

func (r *fakeTrackSetReader) UpdateTrack(_ context.Context, remote RemoteTrack) error {
	r.updates = append(r.updates, remote)
	if remote.Stream.ID != "" {
		r.stream.ID = remote.Stream.ID
	}
	if remote.Stream.Epoch != 0 {
		r.stream.Epoch = remote.Stream.Epoch
	}
	if remote.Stream.Type != "" {
		r.stream.Type = remote.Stream.Type
	}
	return nil
}

func (r *fakeTrackSetReader) ReadRTP(context.Context) (*rtp.Packet, error) {
	return nil, io.EOF
}

func (r *fakeTrackSetReader) Events() <-chan av.Event {
	return nil
}

func (r *fakeTrackSetReader) Close() error {
	r.closed = true
	return nil
}

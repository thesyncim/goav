package webrtcav

import (
	"context"
	"sync"

	"github.com/thesyncim/goav/av"
)

// TrackUpdateKind says what an accepted track did to the set.
type TrackUpdateKind string

// The track update kinds: a brand-new reader, or a live replacement of an
// existing reader's underlying track.
const (
	TrackAdded    TrackUpdateKind = "added"
	TrackReplaced TrackUpdateKind = "replaced"
)

// TrackSetConfig configures a TrackSet: the session to accept from and the
// adapter that turns remote tracks into readers (defaulting to
// TrackRemoteAdapter).
type TrackSetConfig struct {
	Session Session
	Adapter TrackAdapter
}

// TrackUpdate reports whether an accepted remote track created a new reader or
// replaced the underlying track for an existing reader.
type TrackUpdate struct {
	Kind   TrackUpdateKind
	Reader TrackReader
	Stream av.Stream
}

// TrackSet keeps one long-lived TrackReader per logical stream.
//
// It is a cold-path coordinator: callers drive it by calling Accept or
// AcceptRemote. The session lifecycle remains caller-owned.
type TrackSet struct {
	session Session
	adapter TrackAdapter
	mu      sync.Mutex
	readers map[av.StreamID]TrackReader
	order   []av.StreamID
	closed  bool
}

// NewTrackSet builds a track set over the session; the session is required.
func NewTrackSet(config TrackSetConfig) (*TrackSet, error) {
	if config.Session == nil {
		return nil, ErrNilSession
	}
	adapter := config.Adapter
	if adapter == nil {
		adapter = NewTrackRemoteAdapter()
	}
	return &TrackSet{
		session: config.Session,
		adapter: adapter,
		readers: make(map[av.StreamID]TrackReader),
	}, nil
}

// Accept receives the next session track and either creates or updates a reader.
func (s *TrackSet) Accept(ctx context.Context) (TrackUpdate, error) {
	if err := ctx.Err(); err != nil {
		return TrackUpdate{}, err
	}
	if s.isClosed() {
		return TrackUpdate{}, ErrClosed
	}
	remote, err := s.session.AcceptTrack(ctx)
	if err != nil {
		return TrackUpdate{}, err
	}
	return s.AcceptRemote(ctx, remote)
}

// AcceptRemote applies one remote track without reading from the session queue.
func (s *TrackSet) AcceptRemote(ctx context.Context, remote RemoteTrack) (TrackUpdate, error) {
	if err := ctx.Err(); err != nil {
		return TrackUpdate{}, err
	}
	if s.isClosed() {
		return TrackUpdate{}, ErrClosed
	}
	remote = normalizeRemoteTrack(remote)
	key := remoteTrackID(remote)
	if key == "" {
		return TrackUpdate{}, ErrUnknownStream
	}

	reader, ok := s.Reader(key)
	if ok {
		if err := reader.UpdateTrack(ctx, remote); err != nil {
			return TrackUpdate{}, err
		}
		stream, err := firstReaderStream(ctx, reader)
		if err != nil {
			return TrackUpdate{}, err
		}
		return TrackUpdate{Kind: TrackReplaced, Reader: reader, Stream: stream}, nil
	}

	reader, err := s.adapter.AdaptTrack(ctx, remote)
	if err != nil {
		return TrackUpdate{}, err
	}
	stream, err := firstReaderStream(ctx, reader)
	if err != nil {
		reader.Close()
		return TrackUpdate{}, err
	}
	if stream.ID == "" {
		reader.Close()
		return TrackUpdate{}, ErrUnknownStream
	}
	if err := s.addReader(stream.ID, reader); err != nil {
		reader.Close()
		return TrackUpdate{}, err
	}
	return TrackUpdate{Kind: TrackAdded, Reader: reader, Stream: stream}, nil
}

// Reader returns the long-lived reader for a stream id, if one exists.
func (s *TrackSet) Reader(id av.StreamID) (TrackReader, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	reader, ok := s.readers[id]
	return reader, ok
}

// Readers lists every reader in acceptance order.
func (s *TrackSet) Readers() []TrackReader {
	s.mu.Lock()
	defer s.mu.Unlock()
	readers := make([]TrackReader, 0, len(s.order))
	for i := range s.order {
		if reader := s.readers[s.order[i]]; reader != nil {
			readers = append(readers, reader)
		}
	}
	return readers
}

// Close closes every reader; the session stays caller-owned.
func (s *TrackSet) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	readers := make([]TrackReader, 0, len(s.order))
	for i := range s.order {
		if reader := s.readers[s.order[i]]; reader != nil {
			readers = append(readers, reader)
		}
	}
	s.mu.Unlock()

	var first error
	for i := range readers {
		if err := readers[i].Close(); first == nil && err != nil {
			first = err
		}
	}
	return first
}

func (s *TrackSet) addReader(id av.StreamID, reader TrackReader) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	if existing := s.readers[id]; existing != nil {
		return ErrStreamExists
	}
	s.readers[id] = reader
	s.order = append(s.order, id)
	return nil
}

func (s *TrackSet) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func remoteTrackID(remote RemoteTrack) av.StreamID {
	if remote.Stream.ID != "" {
		return remote.Stream.ID
	}
	if remote.Track != nil {
		return av.StreamID(remote.Track.ID())
	}
	return ""
}

func firstReaderStream(ctx context.Context, reader TrackReader) (av.Stream, error) {
	streams, err := reader.Streams(ctx)
	if err != nil {
		return av.Stream{}, err
	}
	if len(streams) == 0 {
		return av.Stream{}, ErrUnknownStream
	}
	return streams[0], nil
}

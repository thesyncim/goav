package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// liveTestProvider is a minimal provider.Source fixture standing in for a
// live packet transport — the same public seam rtpav.Receive implements —
// so the compile internals can be exercised without a transport module:
// packet-domain realtime shape with declared codec facts and a scripted
// source that emits its packets and ends.
type liveTestProvider struct {
	name    string
	media   av.MediaType
	codecID av.CodecID
	streams []av.Stream
	packets []av.Packet
}

// liveAudioOpusProvider declares one Opus audio stream, the live-input
// counterpart of audioOpusTestStream.
func liveAudioOpusProvider(name string) *liveTestProvider {
	return &liveTestProvider{
		name:    name,
		media:   av.MediaAudio,
		codecID: av.CodecOpus,
		streams: []av.Stream{audioOpusTestStream()},
	}
}

// liveVideoVP8Provider declares one VP8 video stream.
func liveVideoVP8Provider(name string) *liveTestProvider {
	return &liveTestProvider{
		name:    name,
		media:   av.MediaVideo,
		codecID: av.CodecVP8,
		streams: []av.Stream{{
			ID:   "video",
			Type: av.MediaVideo,
			Codec: av.CodecParameters{
				ID:   av.CodecVP8,
				Type: av.MediaVideo,
			},
		}},
	}
}

func (p *liveTestProvider) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	streams := make([]av.Stream, len(p.streams))
	copy(streams, p.streams)
	return &liveTestSource{name: p.Name(), packets: p.packets, streams: streams}, streams, nil
}

func (p *liveTestProvider) SourceShape() shape.Spec {
	return shape.Spec{
		Domain:    shape.DomainPacket,
		MediaKind: p.media,
		Codec:     p.codecID,
		Realtime:  true,
	}
}

func (p *liveTestProvider) Name() string {
	if p.name != "" {
		return p.name
	}
	return "live"
}

func (p *liveTestProvider) Detail() string {
	if p.codecID != "" {
		return "live receive, codec=" + string(p.codecID)
	}
	return "live receive"
}

// liveTestSource is the running half of liveTestProvider: it emits the
// scripted packets, ends every declared stream, and returns.
type liveTestSource struct {
	name    string
	streams []av.Stream
	packets []av.Packet
	closed  bool
}

func (s *liveTestSource) Name() string {
	return s.name
}

func (s *liveTestSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	for i := range s.packets {
		packet := s.packets[i]
		if err := emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet}); err != nil {
			return err
		}
	}
	for i := range s.streams {
		event := av.Event{Type: av.EventEndOfStream, StreamID: s.streams[i].ID}
		if err := emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: &event}); err != nil {
			return err
		}
	}
	return nil
}

func (s *liveTestSource) Close() error {
	s.closed = true
	return nil
}

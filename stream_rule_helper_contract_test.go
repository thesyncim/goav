package goav

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

func TestStreamMatchContracts(t *testing.T) {
	stream := av.Stream{
		ID:   "voice",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			ID:   av.CodecOpus,
			Type: av.MediaAudio,
		},
	}

	if (StreamMatch{}).Matches(stream) {
		t.Fatal("empty matcher matched a stream")
	}
	if !MatchMedia(av.MediaAudio).Matches(stream) {
		t.Fatal("media matcher did not match stream.Type")
	}
	if MatchMedia(av.MediaVideo).Matches(stream) {
		t.Fatal("media matcher matched the wrong media")
	}
	codecTyped := stream
	codecTyped.Type = ""
	if !MatchMedia(av.MediaAudio).Matches(codecTyped) {
		t.Fatal("media matcher did not fall back to stream.Codec.Type")
	}
	if !MatchCodec(av.CodecOpus).Matches(stream) {
		t.Fatal("codec matcher did not match stream.Codec.ID")
	}
	if MatchCodec(av.CodecVP8).Matches(stream) {
		t.Fatal("codec matcher matched the wrong codec")
	}
	if !MatchStreamID("voice").Matches(stream) {
		t.Fatal("stream id matcher did not match stream.ID")
	}
	if MatchStreamID("other").Matches(stream) {
		t.Fatal("stream id matcher matched the wrong stream")
	}

	predicateCalls := 0
	custom := MatchStream(func(got av.Stream) bool {
		predicateCalls++
		return got.ID == "voice"
	})
	if !custom.Matches(stream) || predicateCalls != 1 {
		t.Fatalf("custom matcher calls=%d, want one successful predicate call", predicateCalls)
	}
	rejecting := MatchStream(func(av.Stream) bool { return false })
	if rejecting.Matches(stream) {
		t.Fatal("custom matcher matched after predicate rejection")
	}
	if custom.Description() != "custom" || (StreamMatch{}).Description() != "none" {
		t.Fatalf("matcher descriptions = %q / %q", custom.Description(), (StreamMatch{}).Description())
	}
}

func TestSourceEventDomainContracts(t *testing.T) {
	noopSource := func(context.Context, SourcePush) error { return nil }
	for _, tt := range []struct {
		name string
		in   InputSpec
		want shape.MediaDomain
	}{
		{
			name: "custom frame source",
			in: Source("frames",
				shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
				noopSource,
			),
			want: shape.DomainFrame,
		},
		{
			name: "custom event source",
			in:   Source("events", shape.Event(shape.Stream("events")), noopSource),
			want: shape.DomainEvent,
		},
		{
			name: "provider frame source",
			in: Input(&componentSourceProvider{
				source: &componentControllableSource{name: "provider"},
				spec:   shape.Frame(av.MediaVideo, shape.Video(640, 360, av.PixelFormatI420)),
			}),
			want: shape.DomainFrame,
		},
		{
			name: "provider event source",
			in: Input(&componentSourceProvider{
				source: &componentControllableSource{name: "events"},
				spec:   shape.Event(shape.Stream("events")),
			}),
			want: shape.DomainEvent,
		},
		{
			name: "provider without domain",
			in: Input(&componentSourceProvider{
				source: &componentControllableSource{name: "provider"},
				spec:   shape.New(shape.Media(av.MediaAudio)),
			}),
			want: shape.DomainPacket,
		},
		{
			name: "plain input",
			in:   FileInput("input.ogg", nil),
			want: shape.DomainPacket,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.in.sourceEventDomain(); got != tt.want {
				t.Fatalf("sourceEventDomain() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestBranchSpecForDiscoveredStreamClonesAndAnchors(t *testing.T) {
	dest := Sink(SinkFunc("monitor", func(context.Context, Message) error { return nil }))
	spec := Branch("watch").Copy().To(dest)
	stream := av.Stream{ID: "late", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}}

	got := branchSpecForDiscoveredStream(spec, "source", shape.DomainFrame, stream)
	if got.name != "watch-late" ||
		got.source.from != "source" ||
		got.source.policy != pipeline.RouteByStream ||
		got.source.label != "late" ||
		got.source.streamDomain != shape.DomainFrame ||
		got.source.stream == nil ||
		got.source.stream.ID != stream.ID {
		t.Fatalf("anchored branch = %+v", got)
	}

	stream.ID = "mutated"
	spec.destinations[0].dest.name = "mutated"
	if got.source.stream.ID != "late" || got.destinations[0].dest.name == "mutated" {
		t.Fatalf("branch was not cloned defensively: stream=%+v destinations=%+v", got.source.stream, got.destinations)
	}
}

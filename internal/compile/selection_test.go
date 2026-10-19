package compile

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/shape"
)

func TestAllInputBoundStreamsAndMissingSelectionDetails(t *testing.T) {
	sets := []inputStreamSet{
		{
			Name:   "mic",
			Domain: shape.DomainFrame,
			Known:  true,
			Streams: []av.Stream{
				{ID: "mic-left", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}, Name: "left"},
				{ID: "mic-right", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}, Name: "right"},
			},
		},
		{
			Name:   "events",
			Domain: shape.DomainEvent,
			Known:  true,
			Streams: []av.Stream{
				{ID: "levels", Type: av.MediaData, Name: "levels"},
			},
		},
	}

	bound := allInputBoundStreams(sets)
	if len(bound) != 3 {
		t.Fatalf("bound streams = %d, want 3", len(bound))
	}
	if bound[0].InputIndex != 0 || bound[0].InputName != "mic" || bound[0].Domain != shape.DomainFrame || bound[0].Stream.ID != "mic-left" {
		t.Fatalf("first bound stream = %+v", bound[0])
	}
	if bound[2].InputIndex != 1 || bound[2].InputName != "events" || bound[2].Domain != shape.DomainEvent || bound[2].Stream.ID != "levels" {
		t.Fatalf("third bound stream = %+v", bound[2])
	}

	_, ok, err := selectStreamAcrossInputSets(sets, av.StreamSelector{Type: av.MediaVideo}, "")
	if ok {
		t.Fatal("selectStreamAcrossInputSets ok = true, want false")
	}
	if refusal := (*builderr.Refusal)(nil); !errors.As(err, &refusal) || refusal.Code != builderr.StreamMissingCode {
		t.Fatalf("err = %v, want a stream_missing refusal", err)
	}
	if !strings.Contains(err.Error(), "input=mic audio[0] id=mic-left") ||
		!strings.Contains(err.Error(), "input=events data[2] id=levels") {
		t.Fatalf("err = %v, want all input candidates listed", err)
	}
}

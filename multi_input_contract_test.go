package goav

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/shape"
)

func TestDuplicateRealtimeInputNameErrorContract(t *testing.T) {
	err := duplicateInputNameError("mic", 0, 2)
	assertBuildErrorCode(t, err, inputDuplicateCode)
	if !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want ErrUnsupportedBuild cause", err)
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("err = %T, want *BuildError", err)
	}
	if buildErr.Node != "mic" {
		t.Fatalf("node = %q, want mic", buildErr.Node)
	}
	if !reflect.DeepEqual(buildErr.DetailLines(), []string{"first input index: 0", "second input index: 2"}) {
		t.Fatalf("details = %#v", buildErr.DetailLines())
	}

	source := func(context.Context, SourcePush) error { return nil }
	err = validateRealtimeInputNames([]InputSpec{
		Source("mic", shape.Frame(av.MediaAudio), source),
		Source("camera", shape.Frame(av.MediaVideo), source),
		Source("mic", shape.Frame(av.MediaAudio), source),
	})
	assertBuildErrorCode(t, err, inputDuplicateCode)
	if !strings.Contains(err.Error(), "realtime input name \"mic\" is defined more than once") {
		t.Fatalf("err = %v, want duplicate name reason", err)
	}
}

func TestAllInputBoundStreamsAndMissingSelectionDetails(t *testing.T) {
	sets := []inputStreamSet{
		{
			name:   "mic",
			domain: shape.DomainFrame,
			known:  true,
			streams: []av.Stream{
				{ID: "mic-left", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}, Name: "left"},
				{ID: "mic-right", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}, Name: "right"},
			},
		},
		{
			name:   "events",
			domain: shape.DomainEvent,
			known:  true,
			streams: []av.Stream{
				{ID: "levels", Type: av.MediaData, Name: "levels"},
			},
		},
	}

	bound := allInputBoundStreams(sets)
	if len(bound) != 3 {
		t.Fatalf("bound streams = %d, want 3", len(bound))
	}
	if bound[0].inputIndex != 0 || bound[0].inputName != "mic" || bound[0].domain != shape.DomainFrame || bound[0].stream.ID != "mic-left" {
		t.Fatalf("first bound stream = %+v", bound[0])
	}
	if bound[2].inputIndex != 1 || bound[2].inputName != "events" || bound[2].domain != shape.DomainEvent || bound[2].stream.ID != "levels" {
		t.Fatalf("third bound stream = %+v", bound[2])
	}

	_, ok, err := selectStreamAcrossInputSets(sets, av.StreamSelector{Type: av.MediaVideo}, "")
	if ok {
		t.Fatal("selectStreamAcrossInputSets ok = true, want false")
	}
	assertBuildErrorCode(t, err, streamMissingCode)
	if !strings.Contains(err.Error(), "input=mic audio[0] id=mic-left") ||
		!strings.Contains(err.Error(), "input=events data[2] id=levels") {
		t.Fatalf("err = %v, want all input candidates listed", err)
	}
}

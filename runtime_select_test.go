package goav

import (
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func TestStreamSelectStageAdoptsReplacementForTypeSelector(t *testing.T) {
	initial := av.Stream{ID: "video-main", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecAV1, Type: av.MediaVideo}}
	updated := initial
	updated.ID = "video-replaced"
	updated.Epoch = 2
	stage := newStreamSelectStage("select-video", initial, SelectVideo(), "video")

	if !stage.matches(&pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type:     av.EventCodecChanged,
		StreamID: updated.ID,
		Epoch:    updated.Epoch,
		Stream:   &updated,
		Codec:    &updated.Codec,
	}}) {
		t.Fatal("replacement codec-change event did not match")
	}
	if stage.stream.ID != updated.ID || stage.stream.Epoch != updated.Epoch {
		t.Fatalf("stage stream = %+v", stage.stream)
	}
	if !stage.matches(&pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: updated.ID}}) {
		t.Fatal("replacement packet did not match")
	}
	if stage.matches(&pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: initial.ID}}) {
		t.Fatal("old stream packet still matched")
	}
}

func TestStreamSelectStageAdoptsGlobalReplacementForTypeSelector(t *testing.T) {
	initial := av.Stream{ID: "video-main", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecAV1, Type: av.MediaVideo}}
	updated := initial
	updated.ID = "video-replaced"
	updated.Epoch = 2
	stage := newStreamSelectStage("select-video", initial, SelectVideo(), "video")

	if !stage.matches(&pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type:   av.EventCodecChanged,
		Epoch:  updated.Epoch,
		Stream: &updated,
		Codec:  &updated.Codec,
	}}) {
		t.Fatal("global replacement codec-change event did not match")
	}
	if stage.stream.ID != updated.ID || stage.stream.Epoch != updated.Epoch {
		t.Fatalf("stage stream = %+v", stage.stream)
	}
	if !stage.matches(&pipeline.Message{Kind: pipeline.MessagePacket, Packet: &av.Packet{StreamID: updated.ID}}) {
		t.Fatal("replacement packet did not match")
	}
}

func TestStreamSelectStageKeepsIDSelectorStrict(t *testing.T) {
	initial := av.Stream{ID: "video-main", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecAV1, Type: av.MediaVideo}}
	updated := initial
	updated.ID = "video-replaced"
	updated.Epoch = 2
	stage := newStreamSelectStage("select-main", initial, av.StreamSelector{ID: initial.ID}, "stream=video-main")

	if stage.matches(&pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type:     av.EventCodecChanged,
		StreamID: updated.ID,
		Epoch:    updated.Epoch,
		Stream:   &updated,
		Codec:    &updated.Codec,
	}}) {
		t.Fatal("ID-pinned selector adopted replacement stream")
	}
	if stage.stream.ID != initial.ID {
		t.Fatalf("stage stream = %+v", stage.stream)
	}
}

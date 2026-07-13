package goav

import (
	"context"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

type closeDecodeStateRecorder struct {
	closed bool
}

func (r *closeDecodeStateRecorder) Close() {
	r.closed = true
}

func TestCloseDecodeStateContract(t *testing.T) {
	closeDecodeState(nil)
	closeDecodeState(struct{}{})

	state := &closeDecodeStateRecorder{}
	closeDecodeState(state)
	if !state.closed {
		t.Fatal("decode state was not closed")
	}
}

func TestDecodeStreamWithSpecMergesExternalCodecMetadata(t *testing.T) {
	stream := av.Stream{
		ID: "incoming",
		Codec: av.CodecParameters{
			ID:         av.CodecID("x-source"),
			ClockRate:  44100,
			SampleRate: 44100,
			Channels:   1,
		},
	}
	if got := decodeStreamWithSpec(stream, codec.Spec{}); !reflect.DeepEqual(got, stream) {
		t.Fatalf("empty spec changed stream: got %+v want %+v", got, stream)
	}

	got := decodeStreamWithSpec(stream, codec.Spec{
		ID:   av.CodecID("x-external"),
		Type: av.MediaAudio,
		Parameters: av.CodecParameters{
			ClockRate:     48000,
			SampleRate:    48000,
			Channels:      2,
			ChannelLayout: "stereo",
			Attributes:    av.Metadata{"profile": "external"},
		},
	})
	if got.Type != av.MediaAudio {
		t.Fatalf("stream type = %q, want audio", got.Type)
	}
	if got.Codec.ID != av.CodecID("x-external") || got.Codec.Type != av.MediaAudio {
		t.Fatalf("codec identity = %+v, want external audio", got.Codec)
	}
	if got.Codec.ClockRate != 48000 || got.Codec.SampleRate != 48000 || got.Codec.Channels != 2 || got.Codec.ChannelLayout != "stereo" {
		t.Fatalf("codec parameters = %+v, want 48kHz stereo", got.Codec)
	}
	if got.Codec.Attributes["profile"] != "external" {
		t.Fatalf("codec attributes = %+v, want external profile", got.Codec.Attributes)
	}
	if got.TimeBase != (av.TimeBase{Num: 1, Den: 48000}) {
		t.Fatalf("time base = %+v, want 1/48000", got.TimeBase)
	}
}

func TestSelectBranchesBuildsPlannedBranchFanout(t *testing.T) {
	ctx := context.Background()
	var got [][]int16
	job := Select(
		From(selectTestOneShotSource("a", 100)).Audio(),
		From(selectTestOneShotSource("b", 200)).Audio(),
	).Branches(
		Branch("monitor").To(Sink(SinkFunc("monitor", func(_ context.Context, msg Message) error {
			if msg.Kind == pipeline.MessageFrame && msg.Frame != nil && msg.Frame.Audio != nil {
				got = append(got, mixTestReadS16(msg.Frame))
			}
			return nil
		}))),
	)

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, [][]int16{{100}}) {
		t.Fatalf("monitor frames = %v, want selected default arm", got)
	}
}

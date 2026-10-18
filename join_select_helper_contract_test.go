package goav

import (
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

func TestJoinStagePreallocDepthContracts(t *testing.T) {
	if got := joinStagePreallocDepth(nil); got != 1 {
		t.Fatalf("nil runtime prealloc depth = %d, want 1", got)
	}
	if got := joinStagePreallocDepth(mustNew()); got != 1 {
		t.Fatalf("direct runtime prealloc depth = %d, want 1", got)
	}
	if got := joinStagePreallocDepth(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Drop: pipeline.DropOldest}))); got != 1 {
		t.Fatalf("zero-capacity buffered prealloc depth = %d, want 1", got)
	}
	if got := joinStagePreallocDepth(mustNew(WithBufferPolicy(pipeline.BufferPolicy{Capacity: 128, Drop: pipeline.DropOldest}))); got != joinStagePreallocDepthLimit {
		t.Fatalf("large buffered prealloc depth = %d, want cap %d", got, joinStagePreallocDepthLimit)
	}
}

func TestJoinArmTransformStreamContracts(t *testing.T) {
	video := videoVP8TranscodeTestStream()
	if got := joinArmTransformStream(video, av.MediaVideo); got.ID != video.ID || got.Codec.ID != video.Codec.ID {
		t.Fatalf("video transform stream = %+v, want original stream", got)
	}

	audio := joinArmTransformStream(av.Stream{
		ID:   "voice",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			SampleRate: 16000,
		},
	}, av.MediaAudio)
	if audio.ID != "voice" || audio.Type != av.MediaAudio || audio.Codec.Channels != 1 ||
		audio.Codec.SampleFormat != av.SampleFormatS16 || audio.Codec.ClockRate != 16000 {
		t.Fatalf("audio transform stream = %+v, want normalized audio stream", audio)
	}

	preserved := joinArmTransformStream(av.Stream{
		ID:   "stereo",
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			SampleRate:   48000,
			Channels:     2,
			SampleFormat: av.SampleFormatF32,
		},
	}, av.MediaAudio)
	if preserved.Codec.Channels != 2 || preserved.Codec.SampleFormat != av.SampleFormatF32 {
		t.Fatalf("audio transform stream = %+v, want preserved channel/sample format", preserved)
	}
}

package goav

import (
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

// testBoundsProvider is a minimal decode-bounds capability fixture: per-stream
// bounds keyed by stream id, like rtpav.Input.DecodeBounds after OpenSource.
type testBoundsProvider map[av.StreamID]codec.DecodeBounds

func (p testBoundsProvider) DecodeBounds(id av.StreamID) codec.DecodeBounds {
	return p[id]
}

func TestProviderDecodeBoundsForStreamUsesMatchingInput(t *testing.T) {
	video := av.Stream{ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}}
	bounds := providerDecodeBoundsForStream(video, []decodeBoundsProvider{
		testBoundsProvider{"audio": {MaxPayloadBytes: 1024}},
		testBoundsProvider{"video": {MaxFramesPerInput: 2, MaxPayloadBytes: 4096}},
	})
	if bounds.MaxFramesPerInput != 2 || bounds.MaxPayloadBytes != 4096 {
		t.Fatalf("bounds = %+v", bounds)
	}
}

func drainTaskEvents(task Observable) []av.Event {
	var events []av.Event
	deadline := time.After(5 * time.Second)
	for {
		select {
		case event, ok := <-task.Events():
			if !ok {
				return events
			}
			events = append(events, event)
		case <-deadline:
			return events
		}
	}
}

func streamIDsEqual(got []av.StreamID, want []av.StreamID) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

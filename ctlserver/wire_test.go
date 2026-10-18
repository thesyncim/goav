package ctlserver

import (
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestWireEventCarriesTheTrack(t *testing.T) {
	stream := av.Stream{ID: "v", Index: 2, Type: av.MediaVideo, Name: "cam",
		Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo, Width: 640, Height: 360}}
	e := wireEvent(av.Event{Type: av.EventStreamAdded, StreamID: "v", Stream: &stream})
	if e.Track == nil || *e.Track != (Track{Index: 2, Type: av.MediaVideo, Codec: av.CodecVP8, Width: 640, Height: 360, Name: "cam"}) {
		t.Fatalf("stream_added track = %+v", e.Track)
	}
	codec := av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio, SampleRate: 48000, Channels: 2}
	e = wireEvent(av.Event{Type: av.EventCodecChanged, StreamID: "a", Epoch: 3, Codec: &codec})
	if e.Epoch != 3 || e.Track == nil || e.Track.Codec != av.CodecOpus || e.Track.Channels != 2 || e.Track.Index != -1 {
		t.Fatalf("codec_changed = %+v / %+v", e, e.Track)
	}
	if e = wireEvent(av.Event{Type: av.EventBranchAttached}); e.Track != nil {
		t.Fatalf("branch_attached has a track: %+v", e.Track)
	}
}

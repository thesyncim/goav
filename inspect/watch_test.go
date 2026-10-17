package inspect

import (
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestWatchFiltersMatchTypeAndStream(t *testing.T) {
	stats := av.Event{Type: av.EventStats, StreamID: "audio"}
	eos := av.Event{Type: av.EventEndOfStream, StreamID: "video"}
	types := WatchTypes(av.EventStats, av.EventQoS)
	if !types(stats) || types(eos) {
		t.Fatal("WatchTypes matched the wrong events")
	}
	stream := WatchStream("video")
	if stream(stats) || !stream(eos) {
		t.Fatal("WatchStream matched the wrong events")
	}
}

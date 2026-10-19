package audiotoolbox

import (
	"runtime"
	"testing"

	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/libcodec/kit"
)

// TestSessionsShareOneCallback opens and closes more converters than purego
// has callbacks (2000, never freed): one input procedure serves them all,
// and every handle is gone after Close.
func TestSessionsShareOneCallback(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("AudioToolbox is macOS only")
	}
	c := Codec(Path)
	for _, lib := range c.Libraries {
		if err := lib.Bind(Name); err != nil {
			t.Skip(err)
		}
	}
	desc := codec.Descriptor{Knobs: c.Knobs, Backend: codec.BackendInfo{Name: Name}}
	knobs, err := desc.ResolveKnobs(codec.CodecSettings{Bitrate: 128_000}, false)
	if err != nil {
		t.Fatal(err)
	}
	pcm := make([]int16, 2*frames*2)
	for i := range 2100 {
		session, err := c.NewAudioEncoder(kit.AudioConfig{SampleRate: 48000, Channels: 2, Knobs: knobs})
		if err != nil {
			t.Fatalf("session %d: %v", i, err)
		}
		if err := session.Encode(&kit.Samples{PCM: pcm, SampleRate: 48000, Channels: 2}, func(*kit.Packet) error { return nil }); err != nil {
			t.Fatalf("session %d: encode: %v", i, err)
		}
		if err := session.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if n := sessions.Len(); n != 0 {
		t.Fatalf("%d sessions still hold handles after Close", n)
	}
}

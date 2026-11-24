package goav

import (
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

func TestFrameDurationNSContracts(t *testing.T) {
	if got := frameDurationNS(&av.Frame{Duration: av.Duration{Value: 20, Base: av.TimeBase{Num: 1, Den: 1000}}}); got != int64(20*time.Millisecond) {
		t.Fatalf("explicit frame duration = %d", got)
	}
	if got := frameDurationNS(&av.Frame{Audio: &av.AudioFrame{SampleRate: 48000, Samples: 960}}); got != int64(20*time.Millisecond) {
		t.Fatalf("audio-derived frame duration = %d", got)
	}
	if got := frameDurationNS(&av.Frame{}); got != 0 {
		t.Fatalf("empty frame duration = %d, want 0", got)
	}
}

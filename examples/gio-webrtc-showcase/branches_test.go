package main

import (
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

func msPTS(ms int64) av.Timestamp {
	return av.Timestamp{Value: ms, Base: av.TimeBase{Num: 1, Den: 1000}}
}

// TestSampleDurationTracksPTSGaps pins that the WebRTC sample duration follows
// the real time between delivered packets — the property that keeps RTP timing
// correct when a dropping output branch sheds frames between samples.
func TestSampleDurationTracksPTSGaps(t *testing.T) {
	const fallback = 33 * time.Millisecond

	tests := []struct {
		name     string
		prev     time.Duration
		havePrev bool
		packet   *av.Packet
		want     time.Duration
		wantPrev time.Duration
		wantHave bool
	}{
		{
			name:     "first sample falls back and records pts",
			packet:   &av.Packet{PTS: msPTS(100)},
			want:     fallback,
			wantPrev: 100 * time.Millisecond,
			wantHave: true,
		},
		{
			name:     "steady cadence uses the frame gap",
			prev:     100 * time.Millisecond,
			havePrev: true,
			packet:   &av.Packet{PTS: msPTS(133)},
			want:     33 * time.Millisecond,
			wantPrev: 133 * time.Millisecond,
			wantHave: true,
		},
		{
			name:     "dropped frames stretch the duration to the real gap",
			prev:     100 * time.Millisecond,
			havePrev: true,
			packet:   &av.Packet{PTS: msPTS(700)}, // ~1.4 fps, frames shed in between
			want:     600 * time.Millisecond,
			wantPrev: 700 * time.Millisecond,
			wantHave: true,
		},
		{
			name:     "pts discontinuity beyond the cap falls back",
			prev:     100 * time.Millisecond,
			havePrev: true,
			packet:   &av.Packet{PTS: msPTS(100 + int64(3*time.Second/time.Millisecond))},
			want:     fallback,
			wantPrev: 100*time.Millisecond + 3*time.Second,
			wantHave: true,
		},
		{
			name:     "non-increasing pts falls back",
			prev:     700 * time.Millisecond,
			havePrev: true,
			packet:   &av.Packet{PTS: msPTS(700)},
			want:     fallback,
			wantPrev: 700 * time.Millisecond,
			wantHave: true,
		},
		{
			name:     "missing pts uses declared packet duration",
			packet:   &av.Packet{Duration: av.Duration{Value: 20, Base: av.TimeBase{Num: 1, Den: 1000}}},
			want:     20 * time.Millisecond,
			wantPrev: 0,
			wantHave: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, prev, have := sampleDuration(tc.prev, tc.havePrev, tc.packet, fallback)
			if got != tc.want {
				t.Fatalf("duration = %s, want %s", got, tc.want)
			}
			if prev != tc.wantPrev {
				t.Fatalf("prevPTS = %s, want %s", prev, tc.wantPrev)
			}
			if have != tc.wantHave {
				t.Fatalf("havePrev = %v, want %v", have, tc.wantHave)
			}
		})
	}
}

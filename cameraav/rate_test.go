package cameraav

import (
	"math"
	"testing"
)

func TestFrameDurationMatchesOnlyExactOrSubTickIntervals(t *testing.T) {
	for _, tc := range []struct {
		name         string
		value, scale int64
		fps          int
		want         bool
	}{
		{"exact", 1, 30, 30, true},
		{"equivalent", 1000, 30000, 30, true},
		{"UVC truncated", 333333, 10000000, 30, true},
		{"UVC rounded up", 333334, 10000000, 30, true},
		{"nanosecond rounded", 33333333, 1000000000, 30, true},
		{"240 fps truncated", 41666, 10000000, 240, true},
		{"240 fps rounded", 41667, 10000000, 240, true},
		{"one tick slow", 100001, 10000000, 100, false},
		{"one tick fast", 99999, 10000000, 100, false},
		{"29.97 fps", 1001, 30000, 30, false},
		{"59.94 fps", 1001, 60000, 60, false},
		{"23.976 fps", 1001, 24000, 24, false},
		{"different integer", 1, 29, 30, false},
		{"overflow sized", math.MaxInt64, math.MaxInt64, 240, false},
		{"zero value", 0, 30, 30, false},
		{"zero scale", 1, 0, 30, false},
		{"negative value", -1, 30, 30, false},
		{"negative scale", 1, -30, 30, false},
		{"zero fps", 1, 30, 0, false},
		{"unsupported fps", 1, 241, 241, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchesFrameDuration(tc.value, tc.scale, tc.fps); got != tc.want {
				t.Fatalf("interval %d/%d at %d fps: match=%v want %v", tc.value, tc.scale, tc.fps, got, tc.want)
			}
		})
	}
}

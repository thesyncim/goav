//go:build darwin

package player

import (
	"testing"
	"time"
)

func TestHardwareLatencyIsPlausible(t *testing.T) {
	latency := hardwareLatency()
	t.Logf("default output device latency: %v", latency)
	if latency < 0 || latency > time.Second {
		t.Fatalf("hardware latency %v is implausible", latency)
	}
}

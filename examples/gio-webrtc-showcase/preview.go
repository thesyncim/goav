package main

import (
	"context"
	"sync"
	"time"

	"github.com/thesyncim/goav/pipeline"
)

// programMetrics holds live telemetry for the studio program bus.
type programMetrics struct {
	Frames    uint64  `json:"frames"`
	FPS       float64 `json:"fps"`
	Width     int     `json:"width"`
	Height    int     `json:"height"`
	UpdatedAt string  `json:"updatedAt,omitempty"`
}

// programMonitor is a zero-allocation pipeline.Sink on Tap("program") that
// tracks live program FPS and frame dimensions. All video preview is delivered
// over WebRTC.
type programMonitor struct {
	mu          sync.Mutex
	frames      uint64
	width       int
	height      int
	windowStart time.Time
	windowCount uint64
	fps         float64
	lastFrame   time.Time
}

func newProgramMonitor() *programMonitor { return &programMonitor{} }

func (*programMonitor) Name() string { return "program-monitor" }
func (*programMonitor) Close() error { return nil }

func (m *programMonitor) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageFrame || msg.Frame == nil || msg.Frame.Video == nil {
		return nil
	}
	f := msg.Frame
	now := time.Now()

	m.mu.Lock()
	m.frames++
	m.width = f.Video.Width
	m.height = f.Video.Height
	m.lastFrame = now
	if m.windowStart.IsZero() {
		m.windowStart = now
	}
	m.windowCount++
	if elapsed := now.Sub(m.windowStart); elapsed >= 500*time.Millisecond {
		m.fps = float64(m.windowCount) / elapsed.Seconds()
		m.windowStart = now
		m.windowCount = 0
	}
	m.mu.Unlock()
	return nil
}

func (m *programMonitor) snapshot() programMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := programMetrics{
		Frames: m.frames,
		FPS:    m.fps,
		Width:  m.width,
		Height: m.height,
	}
	if !m.lastFrame.IsZero() {
		out.UpdatedAt = m.lastFrame.Format(time.RFC3339Nano)
	}
	return out
}

func (m *programMonitor) reset() {
	m.mu.Lock()
	m.windowStart = time.Time{}
	m.windowCount = 0
	m.fps = 0
	m.mu.Unlock()
}

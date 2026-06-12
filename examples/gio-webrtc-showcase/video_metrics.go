package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

const (
	videoFPSWindow       = 3 * time.Second
	videoStaleAfter      = 2 * time.Second
	videoMinHealthyFPS   = 5.0
	videoMaxExpectedFPS  = 90.0
	videoFrameTimeSlots  = 240
	videoFrameRateDigits = 10
)

type videoAnalyzer struct {
	mu          sync.Mutex
	width       int
	height      int
	pixelFormat string
	lastPTS     string
	lastFrame   time.Time
	frameTimes  [videoFrameTimeSlots]time.Time
	frameIndex  int

	frames atomic.Uint64
}

func newVideoAnalyzer() *videoAnalyzer {
	return &videoAnalyzer{}
}

func (v *videoAnalyzer) Name() string {
	return "video-analyzer"
}

func (v *videoAnalyzer) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageFrame {
		return nil
	}
	v.observeFrame(msg.Frame)
	return nil
}

func (v *videoAnalyzer) Close() error {
	return nil
}

func (v *videoAnalyzer) observeFrame(frame *av.Frame) {
	v.observeFrameAt(frame, time.Now())
}

func (v *videoAnalyzer) observeFrameAt(frame *av.Frame, now time.Time) {
	if frame == nil || frame.Video == nil {
		return
	}
	v.frames.Add(1)
	v.mu.Lock()
	v.width = frame.Video.Width
	v.height = frame.Video.Height
	v.pixelFormat = frame.Video.PixelFormat
	v.lastFrame = now
	v.frameTimes[v.frameIndex%len(v.frameTimes)] = now
	v.frameIndex++
	if d, ok := frame.PTS.ToDuration(); ok {
		v.lastPTS = d.String()
	}
	v.mu.Unlock()
}

func (v *videoAnalyzer) view() videoMetricsView {
	return v.viewAt(time.Now())
}

func (v *videoAnalyzer) viewAt(now time.Time) videoMetricsView {
	v.mu.Lock()
	width := v.width
	height := v.height
	pixelFormat := v.pixelFormat
	lastPTS := v.lastPTS
	lastFrame := v.lastFrame
	times := make([]time.Time, 0, len(v.frameTimes))
	for _, ts := range v.frameTimes {
		if !ts.IsZero() {
			times = append(times, ts)
		}
	}
	v.mu.Unlock()

	frames := v.frames.Load()
	fps := fpsFromWindow(times, now, videoFPSWindow)
	status := "waiting"
	valid := false
	warning := "waiting for decoded video frames"
	var lastFrameMS int64
	if !lastFrame.IsZero() {
		lastFrameMS = now.Sub(lastFrame).Milliseconds()
	}
	switch {
	case frames == 0:
	case lastFrame.IsZero() || now.Sub(lastFrame) > videoStaleAfter:
		status = "stale"
		warning = fmt.Sprintf("no decoded frame for %.1fs", now.Sub(lastFrame).Seconds())
	case fps == 0:
		status = "warming"
		warning = "collecting decoded frame cadence"
	case fps > 0 && fps < videoMinHealthyFPS:
		status = "low"
		warning = fmt.Sprintf("decoded FPS %.1f below %.1f", fps, videoMinHealthyFPS)
	case fps > videoMaxExpectedFPS:
		status = "high"
		warning = fmt.Sprintf("decoded FPS %.1f above %.0f", fps, videoMaxExpectedFPS)
	default:
		status = "live"
		valid = true
		warning = ""
	}

	return videoMetricsView{
		Width:       width,
		Height:      height,
		PixelFormat: pixelFormat,
		Frames:      frames,
		FPS:         roundFPS(fps),
		Status:      status,
		Valid:       valid,
		Warning:     warning,
		LastPTS:     lastPTS,
		LastFrameMS: lastFrameMS,
	}
}

func fpsFromWindow(times []time.Time, now time.Time, window time.Duration) float64 {
	cutoff := now.Add(-window)
	var first time.Time
	var last time.Time
	var count int
	for _, ts := range times {
		if ts.Before(cutoff) || ts.After(now) {
			continue
		}
		if first.IsZero() || ts.Before(first) {
			first = ts
		}
		if last.IsZero() || ts.After(last) {
			last = ts
		}
		count++
	}
	if count < 2 {
		return 0
	}
	elapsed := last.Sub(first).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(count-1) / elapsed
}

func roundFPS(fps float64) float64 {
	if fps <= 0 {
		return 0
	}
	return float64(int(fps*videoFrameRateDigits+0.5)) / videoFrameRateDigits
}

package main

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type audioMonitorSink struct {
	analyzer *audioAnalyzer
	native   nativeAudio
}

func newAudioMonitorSink(analyzer *audioAnalyzer, native nativeAudio) *audioMonitorSink {
	return &audioMonitorSink{analyzer: analyzer, native: native}
}

func (s *audioMonitorSink) Name() string {
	return "audio-monitor"
}

func (s *audioMonitorSink) Handle(ctx context.Context, msg *pipeline.Message) error {
	if s.analyzer != nil {
		if err := s.analyzer.Handle(ctx, msg); err != nil {
			return err
		}
	}
	if msg != nil && msg.Frame != nil && s.native != nil {
		s.native.HandleFrame(msg.Frame)
	}
	return nil
}

func (s *audioMonitorSink) Close() error {
	if s.native != nil {
		return s.native.Close()
	}
	return nil
}

func cloneAudioFrameForPlayback(frame *av.Frame) *av.Frame {
	if frame == nil {
		return nil
	}
	clone := *frame
	if frame.Audio != nil {
		audio := *frame.Audio
		clone.Audio = &audio
	}
	clone.Planes = make([]av.Plane, len(frame.Planes))
	for i := range frame.Planes {
		clone.Planes[i] = frame.Planes[i]
		clone.Planes[i].Buffer.Bytes = append([]byte(nil), frame.Planes[i].Buffer.Bytes...)
		clone.Planes[i].Buffer.Ownership = av.BufferOwned
	}
	return &clone
}

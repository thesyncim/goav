package main

import (
	"context"
	"encoding/binary"
	"math"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

const waveformBuckets = 96

type audioAnalyzer struct {
	mu           sync.Mutex
	sampleRate   int
	channels     int
	sampleFormat string
	rms          float64
	peak         float64
	waveform     [waveformBuckets]float32
	waveIndex    int
	lastPTS      string

	frames     atomic.Uint64
	samples    atomic.Uint64
	packets    atomic.Uint64
	bytes      atomic.Uint64
	lossEvents atomic.Uint64
	plcFrames  atomic.Uint64
}

func newAudioAnalyzer() *audioAnalyzer {
	return &audioAnalyzer{}
}

func (a *audioAnalyzer) Name() string {
	return "audio-analyzer"
}

func (a *audioAnalyzer) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil {
		return nil
	}
	switch msg.Kind {
	case pipeline.MessageFrame:
		a.observeFrame(msg.Frame)
	case pipeline.MessagePacket:
		a.observePacket(msg.Packet)
	case pipeline.MessageEvent:
		a.observeEvent(msg.Event)
	}
	return nil
}

func (a *audioAnalyzer) Close() error {
	return nil
}

func (a *audioAnalyzer) observeFrame(frame *av.Frame) {
	if frame == nil || frame.Audio == nil || len(frame.Planes) == 0 {
		return
	}
	a.frames.Add(1)
	if frame.Audio.Samples > 0 {
		a.samples.Add(uint64(frame.Audio.Samples))
	}
	if frame.Metadata != nil && frame.Metadata["plc"] != "" {
		a.plcFrames.Add(1)
	}

	data := frame.Planes[0].Buffer.Bytes
	sampleCount := len(data) / 2
	if sampleCount == 0 {
		return
	}
	var sumSquares float64
	var peak float64
	for i := 0; i < sampleCount; i++ {
		v := float64(int16(binary.LittleEndian.Uint16(data[i*2:]))) / 32768.0
		if v < 0 {
			v = -v
		}
		if v > peak {
			peak = v
		}
		sumSquares += v * v
	}
	rms := math.Sqrt(sumSquares / float64(sampleCount))

	a.mu.Lock()
	a.sampleRate = frame.Audio.SampleRate
	a.channels = frame.Audio.Channels
	a.sampleFormat = frame.Audio.SampleFormat
	a.rms = rms
	a.peak = peak
	a.waveform[a.waveIndex%len(a.waveform)] = float32(peak)
	a.waveIndex++
	if d, ok := frame.PTS.ToDuration(); ok {
		a.lastPTS = d.String()
	}
	a.mu.Unlock()
}

func (a *audioAnalyzer) observePacket(packet *av.Packet) {
	if packet == nil {
		return
	}
	a.packets.Add(1)
	a.bytes.Add(uint64(len(packet.Payload.Bytes)))
}

func (a *audioAnalyzer) observeEvent(event *av.Event) {
	if event == nil {
		return
	}
	if event.Type == av.EventPacketLoss {
		a.lossEvents.Add(1)
		a.plcFrames.Add(1)
	}
}

func (a *audioAnalyzer) view() audioMetricsView {
	a.mu.Lock()
	defer a.mu.Unlock()

	wave := make([]float32, 0, len(a.waveform))
	start := a.waveIndex
	if start < len(a.waveform) {
		start = 0
	}
	for i := 0; i < len(a.waveform); i++ {
		idx := (start + i) % len(a.waveform)
		if a.waveIndex < len(a.waveform) && idx >= a.waveIndex {
			continue
		}
		wave = append(wave, a.waveform[idx])
	}
	return audioMetricsView{
		SampleRate:   a.sampleRate,
		Channels:     a.channels,
		SampleFormat: a.sampleFormat,
		Frames:       a.frames.Load(),
		Samples:      a.samples.Load(),
		Packets:      a.packets.Load(),
		Bytes:        a.bytes.Load(),
		RMS:          a.rms,
		Peak:         a.peak,
		LossEvents:   a.lossEvents.Load(),
		PLCFrames:    a.plcFrames.Load(),
		Waveform:     wave,
		LastPTS:      a.lastPTS,
	}
}

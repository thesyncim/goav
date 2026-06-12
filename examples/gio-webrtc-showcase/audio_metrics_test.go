package main

import (
	"encoding/binary"
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestAudioAnalyzerRecordsS16Metrics(t *testing.T) {
	analyzer := newAudioAnalyzer()
	payload := make([]byte, 8)
	for i, sample := range []int16{0, 16_384, -16_384, 32_767} {
		binary.LittleEndian.PutUint16(payload[i*2:], uint16(sample))
	}

	analyzer.observeFrame(&av.Frame{
		Type: av.MediaAudio,
		PTS:  av.Timestamp{Value: 960, Base: av.TimeBase{Num: 1, Den: 48_000}},
		Audio: &av.AudioFrame{
			SampleRate:   48_000,
			Channels:     2,
			SampleFormat: av.SampleFormatS16,
			Samples:      2,
		},
		Planes: []av.Plane{{Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}, Stride: 4}},
	})
	analyzer.observeEvent(&av.Event{Type: av.EventPacketLoss})
	analyzer.observePacket(&av.Packet{Payload: av.Buffer{Bytes: []byte{1, 2, 3}}})

	view := analyzer.view()
	if view.SampleRate != 48_000 || view.Channels != 2 || view.SampleFormat != av.SampleFormatS16 {
		t.Fatalf("format = %+v", view)
	}
	if view.Frames != 1 || view.Samples != 2 || view.Packets != 1 || view.Bytes != 3 {
		t.Fatalf("counters = %+v", view)
	}
	if view.Peak < 0.99 || view.RMS <= 0 {
		t.Fatalf("levels = rms %.3f peak %.3f", view.RMS, view.Peak)
	}
	if view.LossEvents != 1 || view.PLCFrames != 1 {
		t.Fatalf("loss/plc = %+v", view)
	}
	if len(view.Waveform) != 1 || view.LastPTS == "" {
		t.Fatalf("waveform/pts = %+v", view)
	}
}

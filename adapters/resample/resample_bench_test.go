package resample

import (
	"context"
	"fmt"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
)

func BenchmarkResampleS16Kernel(b *testing.B) {
	for _, tt := range []struct {
		name           string
		inputRate      int
		outputRate     int
		inputChannels  int
		outputChannels int
		inputSamples   int
	}{
		{name: "stereo44100_to_mono48000/20ms", inputRate: 44_100, outputRate: 48_000, inputChannels: 2, outputChannels: 1, inputSamples: 882},
		{name: "mono16000_to_mono48000/20ms", inputRate: 16_000, outputRate: 48_000, inputChannels: 1, outputChannels: 1, inputSamples: 320},
		{name: "mono48000_to_mono48000/20ms", inputRate: 48_000, outputRate: 48_000, inputChannels: 1, outputChannels: 1, inputSamples: 960},
		{name: "mono48000_to_stereo48000/20ms", inputRate: 48_000, outputRate: 48_000, inputChannels: 1, outputChannels: 2, inputSamples: 960},
	} {
		b.Run(tt.name, func(b *testing.B) {
			ctx := context.Background()
			f := &Filter{}
			if err := f.Open(ctx, filter.Config{
				Stream: av.Stream{
					Type: av.MediaAudio,
					Codec: av.CodecParameters{
						Type:         av.MediaAudio,
						SampleRate:   tt.inputRate,
						Channels:     tt.inputChannels,
						SampleFormat: av.SampleFormatS16,
					},
				},
				Audio: &filter.ResampleConfig{
					SampleRate:   tt.outputRate,
					Channels:     tt.outputChannels,
					SampleFormat: av.SampleFormatS16,
				},
			}); err != nil {
				b.Fatal(err)
			}
			src := resampleBenchS16Payload(tt.inputSamples, tt.inputChannels)
			frame := &av.Frame{
				Type: av.MediaAudio,
				PTS:  av.Timestamp{Base: av.TimeBase{Num: 1, Den: int64(tt.inputRate)}},
				Audio: &av.AudioFrame{
					SampleRate:   tt.inputRate,
					Channels:     tt.inputChannels,
					SampleFormat: av.SampleFormatS16,
					Samples:      tt.inputSamples,
				},
				Planes: []av.Plane{{Buffer: av.Buffer{Bytes: src}}},
			}
			out := filter.Result{Frames: []av.Frame{preallocAudioFrame((tt.inputSamples*tt.outputRate/tt.inputRate + 2) * tt.outputChannels * 2)}[:0]}
			if err := f.FilterInto(ctx, frame, &out); err != nil {
				b.Fatal(err)
			}
			outBytes := 0
			if len(out.Frames) > 0 && len(out.Frames[0].Planes) > 0 {
				outBytes = len(out.Frames[0].Planes[0].Buffer.Bytes)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(src) + outBytes))
			b.ResetTimer()
			for b.Loop() {
				out.Reset()
				if err := f.FilterInto(ctx, frame, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkResampleS16KernelDurations(b *testing.B) {
	for _, samples := range []int{160, 960, 1920} {
		b.Run(fmt.Sprintf("stereo_to_mono/samples=%d", samples), func(b *testing.B) {
			src := resampleBenchS16Payload(samples, 2)
			dst := make([]byte, samples*2)
			b.ReportAllocs()
			b.SetBytes(int64(len(src) + len(dst)))
			for b.Loop() {
				remapS16Channels(dst, src, samples, 2, 1)
			}
		})
	}
}

func resampleBenchS16Payload(samples int, channels int) []byte {
	payload := make([]byte, samples*channels*2)
	for i := 0; i < samples*channels; i++ {
		putS16(payload, i*2, int16((i*73)%32767-16384))
	}
	return payload
}

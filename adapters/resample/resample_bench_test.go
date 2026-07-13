package resample

import (
	"fmt"
	"testing"
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
			src := resampleBenchS16Payload(tt.inputSamples, tt.inputChannels)
			outputSamples := resampledSampleCount(tt.inputSamples, tt.inputRate, tt.outputRate)
			dst := make([]byte, outputSamples*tt.outputChannels*2)
			b.ReportAllocs()
			b.SetBytes(int64(len(src) + len(dst)))
			for i := 0; i < b.N; i++ {
				resampleS16(dst, src, tt.inputSamples, tt.inputRate, tt.inputChannels, outputSamples, tt.outputRate, tt.outputChannels)
			}
		})
	}
}

func BenchmarkResampleS16KernelDurations(b *testing.B) {
	for _, samples := range []int{160, 960, 1920} {
		b.Run(fmt.Sprintf("stereo_to_mono/samples=%d", samples), func(b *testing.B) {
			src := resampleBenchS16Payload(samples, 2)
			outputSamples := resampledSampleCount(samples, 48_000, 48_000)
			dst := make([]byte, outputSamples*2)
			b.ReportAllocs()
			b.SetBytes(int64(len(src) + len(dst)))
			for i := 0; i < b.N; i++ {
				resampleS16(dst, src, samples, 48_000, 2, outputSamples, 48_000, 1)
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

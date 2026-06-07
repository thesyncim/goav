package resample

import (
	"context"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
)

func TestRegister(t *testing.T) {
	registry := filter.NewRegistry()
	Register(registry)

	factory, err := registry.Factory(filter.FactoryResample)
	if err != nil {
		t.Fatal(err)
	}
	if factory == nil {
		t.Fatal("nil factory")
	}
}

func TestDescriptor(t *testing.T) {
	desc := Descriptor()
	if desc.Name != filter.FactoryResample || desc.Input != av.MediaAudio || desc.Output != av.MediaAudio {
		t.Fatalf("descriptor = %+v", desc)
	}
	if len(desc.SampleFormats) != 1 || desc.SampleFormats[0] != av.SampleFormatS16 {
		t.Fatalf("sample formats = %+v", desc.SampleFormats)
	}
}

func TestFilterDownsamplesAndDownmixesS16(t *testing.T) {
	resampler := newTestFilter(t, filter.ResampleConfig{SampleRate: 24000, Channels: 1}, audioStream(48000, 2))
	frame := audioFrame("audio", 48000, 2, []int16{
		100, 300,
		300, 500,
		500, 700,
		700, 900,
	})
	result := filter.Result{Frames: []av.Frame{preallocAudioFrame(8)}[:0]}

	if err := resampler.FilterInto(context.Background(), &frame, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Frames) != 1 {
		t.Fatalf("frames = %d, want 1", len(result.Frames))
	}
	got := samplesFromFrame(&result.Frames[0])
	want := []int16{200, 600}
	if !s16Equal(got, want) {
		t.Fatalf("samples = %v, want %v", got, want)
	}
	if result.Frames[0].Audio == nil || result.Frames[0].Audio.SampleRate != 24000 ||
		result.Frames[0].Audio.Channels != 1 || result.Frames[0].Audio.Samples != 2 {
		t.Fatalf("audio = %+v", result.Frames[0].Audio)
	}
	if result.Frames[0].Duration != av.SamplesDuration(2, 24000) {
		t.Fatalf("duration = %+v", result.Frames[0].Duration)
	}
	if result.Frames[0].Planes[0].Buffer.Ownership != av.BufferOwned {
		t.Fatalf("ownership = %s", result.Frames[0].Planes[0].Buffer.Ownership)
	}
}

func TestFilterUpsamplesWithLinearInterpolation(t *testing.T) {
	resampler := newTestFilter(t, filter.ResampleConfig{SampleRate: 4, Channels: 1}, audioStream(2, 1))
	frame := audioFrame("audio", 2, 1, []int16{0, 1000})
	result := filter.Result{Frames: []av.Frame{preallocAudioFrame(8)}[:0]}

	if err := resampler.FilterInto(context.Background(), &frame, &result); err != nil {
		t.Fatal(err)
	}
	got := samplesFromFrame(&result.Frames[0])
	want := []int16{0, 500, 1000, 1000}
	if !s16Equal(got, want) {
		t.Fatalf("samples = %v, want %v", got, want)
	}
}

func TestFilterRejectsUnsupportedFormat(t *testing.T) {
	stream := audioStream(48000, 2)
	stream.Codec.SampleFormat = av.SampleFormatF32
	_, err := NewFactory().NewFilter(context.Background(), filter.Config{
		Stream: stream,
		Audio:  &filter.ResampleConfig{SampleRate: 24000},
	})
	if err != filter.ErrUnsupportedFormat {
		t.Fatalf("err = %v, want ErrUnsupportedFormat", err)
	}
}

func TestFilterRequiresOutputCapacity(t *testing.T) {
	resampler := newTestFilter(t, filter.ResampleConfig{SampleRate: 48000, Channels: 2}, audioStream(48000, 2))
	frame := audioFrame("audio", 48000, 2, []int16{1, 2, 3, 4})
	result := filter.Result{Frames: []av.Frame{preallocAudioFrame(2)}[:0]}

	err := resampler.FilterInto(context.Background(), &frame, &result)
	if err != filter.ErrOutputBufferTooSmall {
		t.Fatalf("err = %v, want ErrOutputBufferTooSmall", err)
	}
}

func TestFilterAllocs(t *testing.T) {
	resampler := newTestFilter(t, filter.ResampleConfig{SampleRate: 24000, Channels: 1}, audioStream(48000, 2))
	frame := audioFrame("audio", 48000, 2, []int16{
		100, 300,
		300, 500,
		500, 700,
		700, 900,
	})
	result := filter.Result{Frames: []av.Frame{preallocAudioFrame(8)}[:0]}

	if allocs := testing.AllocsPerRun(1000, func() {
		result.Reset()
		if err := resampler.FilterInto(context.Background(), &frame, &result); err != nil {
			t.Fatal(err)
		}
	}); allocs != 0 {
		t.Fatalf("allocs = %v, want 0", allocs)
	}
}

func newTestFilter(t *testing.T, config filter.ResampleConfig, stream av.Stream) *Filter {
	t.Helper()
	frameFilter, err := NewFactory().NewFilter(context.Background(), filter.Config{
		Stream: stream,
		Audio:  &config,
	})
	if err != nil {
		t.Fatal(err)
	}
	resampler, ok := frameFilter.(*Filter)
	if !ok {
		t.Fatalf("filter = %T", frameFilter)
	}
	return resampler
}

func audioStream(sampleRate int, channels int) av.Stream {
	return av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: int64(sampleRate)},
		Codec: av.CodecParameters{
			ID:           av.CodecPCM,
			Type:         av.MediaAudio,
			SampleRate:   sampleRate,
			ClockRate:    uint32(sampleRate),
			Channels:     channels,
			SampleFormat: av.SampleFormatS16,
		},
	}
}

func audioFrame(streamID av.StreamID, sampleRate int, channels int, samples []int16) av.Frame {
	data := make([]byte, len(samples)*2)
	for i := range samples {
		putS16(data, i*2, samples[i])
	}
	return av.Frame{
		StreamID: streamID,
		Type:     av.MediaAudio,
		Audio: &av.AudioFrame{
			SampleRate:   sampleRate,
			Channels:     channels,
			SampleFormat: av.SampleFormatS16,
			Samples:      len(samples) / channels,
		},
		Planes: []av.Plane{{
			Buffer: av.Buffer{Bytes: data, Ownership: av.BufferOwned},
			Stride: channels * 2,
		}},
	}
}

func preallocAudioFrame(size int) av.Frame {
	return av.Frame{
		Planes: []av.Plane{{
			Buffer: av.Buffer{Bytes: make([]byte, 0, size)},
		}},
	}
}

func samplesFromFrame(frame *av.Frame) []int16 {
	if len(frame.Planes) == 0 {
		return nil
	}
	data := frame.Planes[0].Buffer.Bytes
	out := make([]int16, len(data)/2)
	for i := range out {
		out[i] = getS16(data, i*2)
	}
	return out
}

func s16Equal(a []int16, b []int16) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

package main

import (
	"context"
	"fmt"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/goavtest"
	goavruntime "github.com/thesyncim/goav/runtime"
)

func main() {
	frames, err := runCustomFilter(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("frames:", frames)
}

func runCustomFilter(ctx context.Context) ([][]int16, error) {
	out := goavtest.NewCollector()
	err := goav.From(goavtest.Audio(8000, 1, []int16{1, 2}, []int16{3})).
		Audio().
		Resample(16000, 1).
		To(out.Sink()).
		UseRuntime(goavtest.Runtime(goavruntime.WithFilter(doubleRateDescriptor(), doubleRateFactory{}))).
		Run(ctx)
	return out.S16(), err
}

func doubleRateDescriptor() filter.Descriptor {
	return filter.Descriptor{
		Name:          filter.FactoryResample,
		Input:         av.MediaAudio,
		Output:        av.MediaAudio,
		SampleFormats: []string{av.SampleFormatS16},
		Stateless:     true,
	}
}

type doubleRateFactory struct{}

func (doubleRateFactory) NewFilter(ctx context.Context, config filter.Config) (filter.FrameFilter, error) {
	filter := &doubleRateFilter{}
	if err := filter.Open(ctx, config); err != nil {
		return nil, err
	}
	return filter, nil
}

type doubleRateFilter struct {
	channels int
	outRate  int
	audio    av.AudioFrame
}

func (f *doubleRateFilter) Descriptor() filter.Descriptor { return doubleRateDescriptor() }

func (f *doubleRateFilter) Open(_ context.Context, config filter.Config) error {
	if config.Audio == nil {
		return filter.ErrUnsupportedFormat
	}
	inputRate := config.Stream.Codec.SampleRate
	channels := config.Stream.Codec.Channels
	if inputRate <= 0 || channels <= 0 ||
		config.Audio.SampleRate != inputRate*2 ||
		(config.Audio.Channels != 0 && config.Audio.Channels != channels) {
		return filter.ErrUnsupportedFormat
	}
	f.channels = channels
	f.outRate = config.Audio.SampleRate
	f.audio = av.AudioFrame{SampleRate: f.outRate, Channels: channels, SampleFormat: av.SampleFormatS16}
	return nil
}

func (f *doubleRateFilter) FilterInto(_ context.Context, frame *av.Frame, out *filter.Result) error {
	if frame == nil || len(frame.Planes) == 0 {
		return nil
	}
	index := len(out.Frames)
	if index == cap(out.Frames) {
		return filter.ErrResultFull
	}
	src := frame.Planes[0].Buffer.Bytes
	width := f.channels * 2
	samples := len(src) / width

	out.Frames = out.Frames[:index+1]
	dstFrame := &out.Frames[index]
	dstFrame.Reset()
	if cap(dstFrame.Planes) < 1 || cap(dstFrame.Planes[:1][0].Buffer.Bytes) < 2*len(src) {
		out.Frames = out.Frames[:index]
		return filter.ErrOutputBufferTooSmall
	}
	dstFrame.Planes = dstFrame.Planes[:1]
	dst := dstFrame.Planes[0].Buffer.Bytes[:0]
	for sample := 0; sample < samples; sample++ {
		group := src[sample*width : (sample+1)*width]
		dst = append(dst, group...)
		dst = append(dst, group...)
	}
	dstFrame.Planes[0].Buffer.Bytes = dst
	dstFrame.Planes[0].Buffer.Ownership = av.BufferOwned
	dstFrame.Planes[0].Stride = width
	f.audio.Samples = samples * 2
	dstFrame.StreamID = frame.StreamID
	dstFrame.Type = av.MediaAudio
	dstFrame.PTS = frame.PTS
	dstFrame.Duration = av.SamplesDuration(samples*2, f.outRate)
	dstFrame.Audio = &f.audio
	return nil
}

func (f *doubleRateFilter) FlushInto(context.Context, *filter.Result) error { return nil }

func (f *doubleRateFilter) HandleEvent(context.Context, *av.Event) error { return nil }

func (f *doubleRateFilter) Close() error { return nil }

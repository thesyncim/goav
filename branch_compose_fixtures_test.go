package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/filter"
)

// videoVP8TranscodeTestStream is a shared VP8 video stream fixture for
// branch-composition and graph tests.
func videoVP8TranscodeTestStream() av.Stream {
	return av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:          av.CodecVP8,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       640,
			Height:      360,
			PixelFormat: av.PixelFormatYUV420P,
		},
	}
}

// transcodeTestFilterFactory / transcodeTestFilter are a shared pass-through
// audio resample filter fixture used by branch-composition and graph tests.
type transcodeTestFilterFactory struct {
	filter *transcodeTestFilter
	config filter.Config
}

func (f *transcodeTestFilterFactory) NewFilter(_ context.Context, config filter.Config) (filter.FrameFilter, error) {
	f.config = config
	if f.filter == nil {
		f.filter = &transcodeTestFilter{}
	}
	return f.filter, nil
}

type transcodeTestFilter struct {
	frames  int
	flushes int
	closed  bool
}

func (f *transcodeTestFilter) Descriptor() filter.Descriptor {
	return filter.Descriptor{Name: filter.FactoryResample, Input: av.MediaAudio, Output: av.MediaAudio}
}

func (f *transcodeTestFilter) Open(context.Context, filter.Config) error {
	return nil
}

func (f *transcodeTestFilter) FilterInto(_ context.Context, frame *av.Frame, out *filter.Result) error {
	if frame == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return filter.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	outFrame := &out.Frames[index]
	outFrame.Reset()
	*outFrame = *frame
	f.frames++
	return nil
}

func (f *transcodeTestFilter) FlushInto(context.Context, *filter.Result) error {
	f.flushes++
	return nil
}

func (f *transcodeTestFilter) HandleEvent(context.Context, *av.Event) error {
	return nil
}

func (f *transcodeTestFilter) Close() error {
	f.closed = true
	return nil
}

package main

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
)

func main() {
	frames, src, err := runProviderSource(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("provider:", src.Name())
	fmt.Println("frames:", frames)
	fmt.Println("opened:", src.opens, "started:", src.source.starts, "closed:", src.source.closes)
}

type demoProvider struct {
	name       string
	stream     av.StreamID
	sampleRate int
	channels   int
	opens      int
	source     *demoPipelineSource
}

func newDemoProvider() *demoProvider {
	return &demoProvider{
		name:       "demo-transport",
		stream:     "voice",
		sampleRate: 48000,
		channels:   1,
		source: &demoPipelineSource{
			name:       "demo-transport",
			stream:     "voice",
			sampleRate: 48000,
			channels:   1,
			frames:     [][]int16{{101, 102}, {103, 104}},
		},
	}
}

func (p *demoProvider) Name() string { return p.name }

func (p *demoProvider) Detail() string { return "copyable provider.Source" }

func (p *demoProvider) SourceShape() shape.Spec {
	return shape.Frame(av.MediaAudio,
		shape.Audio(p.sampleRate, p.channels, av.SampleFormatS16),
		shape.Stream(p.stream),
	)
}

func (p *demoProvider) OpenSource(context.Context) (pipeline.Source, []av.Stream, error) {
	p.opens++
	stream := av.Stream{
		ID:   p.stream,
		Type: av.MediaAudio,
		Codec: av.CodecParameters{
			Type:         av.MediaAudio,
			SampleRate:   p.sampleRate,
			Channels:     p.channels,
			SampleFormat: av.SampleFormatS16,
		},
	}
	return p.source, []av.Stream{stream}, nil
}

type demoPipelineSource struct {
	name       string
	stream     av.StreamID
	sampleRate int
	channels   int
	frames     [][]int16
	starts     int
	closes     int
}

func (s *demoPipelineSource) Name() string { return s.name }

func (s *demoPipelineSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	s.starts++
	var elapsed int64
	for _, samples := range s.frames {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		frame := s16Frame(s.stream, s.sampleRate, s.channels, samples, elapsed)
		elapsed += frame.Duration.Value
		if err := emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}); err != nil {
			return err
		}
	}
	event := &av.Event{Type: av.EventEndOfStream, StreamID: s.stream}
	return emitter.Emit(ctx, &pipeline.Message{Kind: pipeline.MessageEvent, Event: event})
}

func (s *demoPipelineSource) Close() error {
	s.closes++
	return nil
}

func runProviderSource(ctx context.Context) ([][]int16, *demoProvider, error) {
	src := newDemoProvider()
	out := goavtest.NewCollector()
	err := goav.From(goav.Input(src)).
		Audio().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	return out.S16(), src, err
}

func buildBrokenProvider(ctx context.Context) error {
	out := goavtest.NewCollector()
	_, err := goav.From(goav.Input(nil)).
		Audio().
		To(out.Sink()).
		Build(ctx)
	return err
}

var _ provider.Source = (*demoProvider)(nil)

func s16Frame(stream av.StreamID, sampleRate int, channels int, samples []int16, elapsed int64) *av.Frame {
	payload := make([]byte, len(samples)*2)
	for i, sample := range samples {
		binary.LittleEndian.PutUint16(payload[i*2:], uint16(sample))
	}
	perChannel := len(samples)
	if channels > 0 {
		perChannel /= channels
	}
	duration := av.SamplesDuration(perChannel, sampleRate)
	return &av.Frame{
		StreamID: stream,
		Type:     av.MediaAudio,
		PTS:      av.Timestamp{Value: elapsed, Base: duration.Base},
		Duration: duration,
		Audio: &av.AudioFrame{
			SampleRate:   sampleRate,
			Channels:     channels,
			SampleFormat: av.SampleFormatS16,
			Samples:      perChannel,
		},
		Planes: []av.Plane{{
			Buffer: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable},
			Stride: channels * 2,
		}},
	}
}

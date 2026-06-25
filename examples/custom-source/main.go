package main

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

func main() {
	frames, stats, err := runCustomSource(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println("frames:", frames)
	fmt.Println("accepted:", stats.Accepted, "dropped:", stats.Dropped)
}

type sourceStats struct {
	Accepted int
	Dropped  int
}

func runCustomSource(ctx context.Context) ([][]int16, sourceStats, error) {
	stats := &sourceStats{}
	out := goavtest.NewCollector()
	err := goav.From(pcmSource("app-mic", 48000, 1, stats,
		[]int16{10, 20},
		[]int16{30, 40},
	)).
		Audio().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	return out.S16(), *stats, err
}

func buildBrokenCustomSource(ctx context.Context) error {
	out := goavtest.NewCollector()
	_, err := goav.From(goav.Source("broken",
		shape.Frame(av.MediaAudio, shape.Audio(48000, 1, av.SampleFormatS16)),
		nil,
	)).
		Audio().
		To(out.Sink()).
		Build(ctx)
	return err
}

func pcmSource(name string, sampleRate int, channels int, stats *sourceStats, frames ...[]int16) goav.InputSpec {
	return goav.Source(name,
		shape.Frame(av.MediaAudio, shape.Audio(sampleRate, channels, av.SampleFormatS16), shape.Stream(av.StreamID(name))),
		func(_ context.Context, push source.Push) error {
			var elapsed int64
			for _, samples := range frames {
				frame := s16Frame(name, sampleRate, channels, samples, elapsed)
				elapsed += frame.Duration.Value
				result, err := push.Frame(frame)
				if result.Accepted {
					stats.Accepted++
				}
				if result.Dropped {
					stats.Dropped++
				}
				if err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

func s16Frame(stream string, sampleRate int, channels int, samples []int16, elapsed int64) *av.Frame {
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
		StreamID: av.StreamID(stream),
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

package integration

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/shape"
)

func mixTestAudioSource(id av.StreamID, samples ...int16) goav.InputSpec {
	return goav.Source(string(id),
		shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream(id)),
		func(_ context.Context, push goav.SourcePush) error {
			b := make([]byte, len(samples)*2)
			for i := range samples {
				binary.LittleEndian.PutUint16(b[i*2:], uint16(samples[i]))
			}
			frame := &av.Frame{
				StreamID: id, Type: av.MediaAudio,
				Audio:  &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: len(samples)},
				Planes: []av.Plane{{Buffer: av.Buffer{Bytes: b, Ownership: av.BufferImmutable}}},
			}
			if _, err := push.Frame(frame); err != nil {
				return err
			}
			return push.EOS()
		})
}

func TestMixAcceptsRTPArmsViaUnifiedOpener(t *testing.T) {
	// RTP resolves through the same source opener as custom/file — it must not
	// be special-rejected as an unsupported source kind (it may still fail later
	// for fixture reasons, which is fine; the point is it is no longer a special
	// case in the opener).
	_, err := goav.Mix(
		goav.From(mixTestAudioSource("a", 1)).Audio(),
		goav.From(goav.Input(rtpav.Receive(&runtimeRTPReceiver{streams: []av.Stream{{ID: "rtp-b", Type: av.MediaAudio}}}))).Audio(),
	).To(goav.Sink(component.SinkFunc("out", func(context.Context, component.Message) error { return nil }))).
		Build(context.Background())
	var buildErr *goav.BuildError
	if errors.As(err, &buildErr) && buildErr.Code == "source_unsupported" {
		t.Fatalf("RTP arm still special-rejected by the opener: %v", err)
	}
}

package goav

import (
	"context"
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/shape"
)

// Shared in-package test helpers.

func assertBuildErrorCode(t *testing.T, err error, code errcode.Code) {
	t.Helper()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) {
		t.Fatalf("error = %v, want *BuildError %s", err, code)
	}
	if buildErr.Code != code {
		t.Fatalf("BuildError code = %s, want %s (err=%v)", buildErr.Code, code, err)
	}
}

func buildErrorHasCodeOperation(err error, code errcode.Code, operation string) bool {
	var buildErr *BuildError
	return errors.As(err, &buildErr) && buildErr.Code == code && buildErr.Operation == operation
}

func branchBuilderTestSink(name string) Destination {
	return Sink(SinkFunc(name, func(context.Context, Message) error { return nil }))
}

type tapArmTestDecoderFactory struct {
	decoder *tapArmTestDecoder
}

func (f tapArmTestDecoderFactory) NewDecoder(_ context.Context, _ codec.DecodeConfig) (codec.Decoder, error) {
	return f.decoder, nil
}

// tapArmTestDecoder turns each packet's payload bytes into one mono S16 frame
// verbatim, so mixed sums are exact and decode invocations are countable.
type tapArmTestDecoder struct {
	decodes int
}

func (d *tapArmTestDecoder) Descriptor() codec.Descriptor {
	return codec.Descriptor{ID: "x_pcm_mono"}
}

func (d *tapArmTestDecoder) Open(context.Context, codec.DecodeConfig) error { return nil }

func (d *tapArmTestDecoder) DecodeInto(_ context.Context, packet *av.Packet, out *codec.DecodeResult) error {
	if packet == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return codec.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	frame.StreamID = packet.StreamID
	frame.Type = av.MediaAudio
	frame.Audio = &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: len(packet.Payload.Bytes) / 2}
	if cap(frame.Planes) < 1 {
		frame.Planes = make([]av.Plane, 1)
	} else {
		frame.Planes = frame.Planes[:1]
	}
	frame.Planes[0].Buffer.Bytes = append(frame.Planes[0].Buffer.Bytes[:0], packet.Payload.Bytes...)
	frame.Planes[0].Stride = 2
	d.decodes++
	return nil
}

func (d *tapArmTestDecoder) FlushInto(context.Context, *codec.DecodeResult) error { return nil }
func (d *tapArmTestDecoder) HandleEvent(context.Context, *av.Event) error         { return nil }
func (d *tapArmTestDecoder) Close() error                                         { return nil }

func tapArmTestPacketSource(id av.StreamID, codecID av.CodecID, samples ...int16) InputSpec {
	payload := make([]byte, len(samples)*2)
	for i := range samples {
		payload[i*2] = byte(samples[i])
		payload[i*2+1] = byte(samples[i] >> 8)
	}
	return Source(string(id),
		shape.Packet(av.MediaAudio, codecID, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream(id)),
		func(_ context.Context, push SourcePush) error {
			if _, err := push.Packet(&av.Packet{StreamID: id, Type: av.MediaAudio, Payload: av.Buffer{Bytes: payload, Ownership: av.BufferImmutable}}); err != nil {
				return err
			}
			return push.EOS()
		})
}

// compileOutputs compiles Outputs on rt with options, as Describe or Build
// does, for tests that read the compiled recipe.
func compileOutputs(rt *Runtime, options recipeCompileOptions, outs ...Output) (recipeResolved, error) {
	g, err := compileGraph(outs)
	if err != nil {
		return recipeResolved{}, err
	}
	return compileRecipe(g, rt, options)
}

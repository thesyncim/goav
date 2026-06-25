package goav

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

func mixTestS16Frame(id av.StreamID, samples ...int16) *av.Frame {
	b := make([]byte, len(samples)*2)
	for i := range samples {
		binary.LittleEndian.PutUint16(b[i*2:], uint16(samples[i]))
	}
	return &av.Frame{
		StreamID: id,
		Type:     av.MediaAudio,
		Audio:    &av.AudioFrame{SampleRate: 48000, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: len(samples)},
		Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: b, Ownership: av.BufferImmutable}}},
	}
}

func mixTestReadS16(frame *av.Frame) []int16 {
	b := frame.Planes[0].Buffer.Bytes
	out := make([]int16, len(b)/2)
	for i := range out {
		out[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
	}
	return out
}

type mixTestEmitter struct {
	frames []*av.Frame
	events []*av.Event
}

func (c *mixTestEmitter) Emit(_ context.Context, m *pipeline.Message) error {
	switch m.Kind {
	case pipeline.MessageFrame:
		c.frames = append(c.frames, cloneMixTestFrame(m.Frame))
	case pipeline.MessageEvent:
		c.events = append(c.events, m.Event)
	}
	return nil
}

func cloneMixTestFrame(frame *av.Frame) *av.Frame {
	if frame == nil {
		return nil
	}
	clone := *frame
	if frame.Audio != nil {
		audio := *frame.Audio
		clone.Audio = &audio
	}
	if frame.Video != nil {
		video := *frame.Video
		clone.Video = &video
	}
	clone.Planes = make([]av.Plane, len(frame.Planes))
	for i := range frame.Planes {
		clone.Planes[i] = frame.Planes[i]
		payload := frame.Planes[i].Buffer.Bytes
		bytes := make([]byte, len(payload))
		copy(bytes, payload)
		clone.Planes[i].Buffer = av.Buffer{Bytes: bytes, Ownership: av.BufferOwned}
	}
	return &clone
}

func TestAudioMixStageSumsAlignedS16(t *testing.T) {
	mix := newAudioMixStage("mix", []av.StreamID{"a", "b"}, "mix", joinSyncArrival)
	emit := &mixTestEmitter{}
	ctx := context.Background()

	// One arm ready is not enough — the mix only advances when every arm has a frame.
	if err := mix.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixTestS16Frame("a", 100, 200)}, emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.frames) != 0 {
		t.Fatalf("emitted %d before both inputs ready, want 0", len(emit.frames))
	}
	if err := mix.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixTestS16Frame("b", 50, -50)}, emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.frames) != 1 {
		t.Fatalf("frames=%d, want 1", len(emit.frames))
	}
	if got, want := mixTestReadS16(emit.frames[0]), []int16{150, 150}; !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed=%v, want %v", got, want)
	}
	if emit.frames[0].StreamID != "mix" {
		t.Fatalf("output stream=%s, want mix", emit.frames[0].StreamID)
	}
}

func TestAudioMixStageClampsOverflow(t *testing.T) {
	mix := newAudioMixStage("mix", []av.StreamID{"a", "b"}, "mix", joinSyncArrival)
	emit := &mixTestEmitter{}
	ctx := context.Background()
	_ = mix.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixTestS16Frame("a", 30000, -30000)}, emit)
	_ = mix.Handle(ctx, &pipeline.Message{Kind: pipeline.MessageFrame, Frame: mixTestS16Frame("b", 30000, -30000)}, emit)
	if got, want := mixTestReadS16(emit.frames[0]), []int16{32767, -32768}; !reflect.DeepEqual(got, want) {
		t.Fatalf("clamped=%v, want %v", got, want)
	}
}

func TestAudioMixStageEmitsEOSWhenAllInputsEnd(t *testing.T) {
	mix := newAudioMixStage("mix", []av.StreamID{"a", "b"}, "mix", joinSyncArrival)
	emit := &mixTestEmitter{}
	ctx := context.Background()
	eos := func(id av.StreamID) *pipeline.Message {
		return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventEndOfStream, StreamID: id}}
	}
	_ = mix.Handle(ctx, eos("a"), emit)
	if len(emit.events) != 0 {
		t.Fatal("emitted EOS before all inputs ended")
	}
	_ = mix.Handle(ctx, eos("b"), emit)
	if len(emit.events) != 1 || emit.events[0].Type != av.EventEndOfStream || emit.events[0].StreamID != "mix" {
		t.Fatalf("events=%+v, want one mix EOS", emit.events)
	}
}

func mixTestAudioSource(id av.StreamID, samples ...int16) InputSpec {
	return Source(string(id),
		shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream(id)),
		func(_ context.Context, push SourcePush) error {
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

// TestMixRunsTwoAudioSourcesIntoSink moved to goavtest_dogfood_test.go: it is
// now the consumer-side Mix acceptance test written against goavtest.

func TestMixDescribeShowsConvergentJoin(t *testing.T) {
	spec, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 1)).Audio(),
	).To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer spec.Close()
	graph := spec.Describe()
	var into int
	var mixNode string
	for _, n := range graph.Nodes {
		if n.Kind == pipeline.NodeStage {
			mixNode = string(n.Name)
		}
	}
	for _, e := range graph.Edges {
		if string(e.To) == mixNode {
			into++
		}
	}
	if into != 2 {
		t.Fatalf("edges into mix node = %d, want 2 (convergence); spec=%+v", into, graph)
	}
}

func TestMixRequiresTwoArms(t *testing.T) {
	_, err := Mix(From(mixTestAudioSource("a", 1)).Audio()).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).
		Build(context.Background())
	var buildErr *BuildError
	if !errorsAsMix(err, &buildErr) || buildErr.Code != "mix_inputs" {
		t.Fatalf("err = %v, want mix_inputs", err)
	}
}

func TestMixRawFramesRequireSinkDestination(t *testing.T) {
	_, err := Mix(
		From(mixTestAudioSource("a", 1)).Audio(),
		From(mixTestAudioSource("b", 2)).Audio(),
	).To(Write("mix.ogg", io.Discard)).
		Build(context.Background())
	var buildErr *BuildError
	if !errorsAsMix(err, &buildErr) || buildErr.Code != "mix_destination" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want mix_destination wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), ".Encode(codec.Opus") ||
		!strings.Contains(err.Error(), "goav.Sink") {
		t.Fatalf("err = %v, want encode-or-sink guidance", err)
	}
}

func errorsAsMix(err error, target **BuildError) bool { return errors.As(err, target) }

func TestMixBuilderOptionsCarryIntoJoinSpec(t *testing.T) {
	var nilMix *mixStream
	if arm := nilMix.joinArm(); arm.join != nil || arm.region != nil {
		t.Fatalf("nil mix join arm = %+v, want zero", arm)
	}

	require := shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Stereo, av.SampleFormatS16))
	prefer := shape.Frame(av.MediaAudio, shape.Audio(0, 0, av.SampleFormatF32))
	mix := Mix(
		From(mixTestAudioSource("a", 100, 200)).Audio(),
		From(mixTestAudioSource("b", 50, -50)).Audio(),
	).
		SyncByPTS().
		Auto(shape.AllowResample(), shape.AllowConvert()).
		Require(require).
		Prefer(prefer).
		Encode(codec.Opus(codec.Bitrate(128_000), codec.Channels(codec.Stereo))).
		Tap(FrameTap("mixed"))

	if mix.sync != joinSyncPTS {
		t.Fatalf("sync = %v, want PTS", mix.sync)
	}
	if mix.encode == nil ||
		mix.encode.ID != av.CodecOpus ||
		mix.encode.Settings.Bitrate != 128_000 ||
		mix.encode.Settings.Channels != codec.Stereo {
		t.Fatalf("encode = %+v", mix.encode)
	}
	if len(mix.taps) != 1 || mix.taps[0].name != "mixed" {
		t.Fatalf("taps = %+v", mix.taps)
	}
	if len(mix.operations) != 3 {
		t.Fatalf("operations = %+v, want auto/require/prefer", mix.operations)
	}
	if mix.operations[0].Auto == nil ||
		!mix.operations[0].Auto.AllowsResample() ||
		!mix.operations[0].Auto.AllowsConvert() ||
		mix.operations[0].Auto.AllowsResize() {
		t.Fatalf("auto operation = %+v", mix.operations[0])
	}
	if mix.operations[1].Require == nil || *mix.operations[1].Require != require {
		t.Fatalf("require operation = %+v", mix.operations[1])
	}
	if mix.operations[2].Prefer == nil || *mix.operations[2].Prefer != prefer {
		t.Fatalf("prefer operation = %+v", mix.operations[2])
	}

	arm := mix.joinArm()
	if arm.region != nil {
		t.Fatalf("mix join arm region = %+v, want nil", arm.region)
	}
	if arm.join == nil ||
		arm.join.kind != joinMix ||
		arm.join.sync != joinSyncPTS ||
		arm.join.encode == nil ||
		arm.join.encode.ID != av.CodecOpus ||
		len(arm.join.taps) != 1 ||
		len(arm.join.operations) != 3 {
		t.Fatalf("join arm = %+v", arm.join)
	}

	mix.operations[0] = operationSpec{}
	if arm.join.operations[0].Auto == nil || !arm.join.operations[0].Auto.AllowsResample() {
		t.Fatalf("join arm operations were not cloned: %+v", arm.join.operations)
	}
}

func TestMixDecodesPacketArmsBeforeMixing(t *testing.T) {
	ctx := context.Background()
	pcm := av.CodecID("x_pcm_s16")
	desc := codec.Descriptor{ID: pcm, Name: "PCM", Type: av.MediaAudio, Capabilities: codec.Capabilities{SampleFormats: []string{av.SampleFormatS16}}}
	rt := MustNew(WithDecoder(desc, recipePCMDecoderFactory{}))

	packetSrc := func(id av.StreamID) InputSpec {
		return Source(string(id),
			shape.Packet(av.MediaAudio, pcm, shape.Audio(48000, codec.Stereo, av.SampleFormatS16), shape.Stream(id)),
			func(_ context.Context, push SourcePush) error {
				if _, err := push.Packet(&av.Packet{StreamID: id, Type: av.MediaAudio, Payload: av.Buffer{Bytes: []byte{0, 0}, Ownership: av.BufferImmutable}}); err != nil {
					return err
				}
				return push.EOS()
			})
	}

	var frames int
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil {
			frames++
		}
		return nil
	}))

	task, err := Mix(
		From(packetSrc("a")).Audio(),
		From(packetSrc("b")).Audio(),
	).To(sink).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if frames != 1 {
		t.Fatalf("mixed frames at sink = %d, want 1 (packet arms auto-decoded then mixed)", frames)
	}
}

func TestMixEncodesMixedOutput(t *testing.T) {
	ctx := context.Background()
	rt := MustNew(WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}))

	var packets int
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessagePacket && m.Packet != nil {
			packets++
		}
		return nil
	}))

	task, err := Mix(
		From(mixTestAudioSource("a", 100, 200)).Audio(),
		From(mixTestAudioSource("b", 50, -50)).Audio(),
	).Encode(codec.Opus()).To(sink).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if packets < 1 {
		t.Fatalf("encoded packets at sink = %d, want >=1 (mix -> encode -> sink)", packets)
	}
}

func TestMixEncodesToFile(t *testing.T) {
	ctx := context.Background()
	muxers := &remuxTestMuxerFactory{}
	rt := MustNew(
		withTestFormats(testFormatMuxer(av.FormatOgg, muxers)),
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)

	task, err := Mix(
		From(mixTestAudioSource("a", 100, 200)).Audio(),
		From(mixTestAudioSource("b", 50, -50)).Audio(),
	).Encode(codec.Opus()).To(Write("mix.ogg", io.Discard, Format(av.FormatOgg))).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes < 1 {
		t.Fatalf("muxer writes = %+v, want one mux with >=1 write (mix -> encode -> file)", muxers.muxers)
	}
}

// writerTestMuxerFactory builds muxers that copy packet payloads straight to
// the destination's writer, so fanout tests can compare the bytes each
// destination received.
type writerTestMuxerFactory struct{}

func (writerTestMuxerFactory) NewMuxer(context.Context, av.FormatID) (format.Muxer, error) {
	return &writerTestMuxer{}, nil
}

type writerTestMuxer struct {
	writer io.Writer
}

func (m *writerTestMuxer) Format() av.FormatID {
	return av.FormatOgg
}

func (m *writerTestMuxer) Open(_ context.Context, output format.Output, _ []av.Stream, _ format.OpenOptions) error {
	m.writer = output.Writer
	return nil
}

func (m *writerTestMuxer) Write(_ context.Context, packet *av.Packet, _ *format.WriteResult) error {
	if m.writer == nil || len(packet.Payload.Bytes) == 0 {
		return nil
	}
	_, err := m.writer.Write(packet.Payload.Bytes)
	return err
}

func (m *writerTestMuxer) Close() error { return nil }

func TestMixEncodesToMultipleFileDestinations(t *testing.T) {
	ctx := context.Background()
	rt := MustNew(
		withTestFormats(testFormatMuxer(av.FormatOgg, writerTestMuxerFactory{})),
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)

	var first, second bytes.Buffer
	task, err := Mix(
		From(mixTestAudioSource("a", 100, 200)).Audio(),
		From(mixTestAudioSource("b", 50, -50)).Audio(),
	).Encode(codec.Opus()).To(
		Write("first.ogg", &first, Format(av.FormatOgg)),
		Write("second.ogg", &second, Format(av.FormatOgg)),
	).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if first.Len() == 0 {
		t.Fatal("first destination received no bytes (mix -> encode -> two files)")
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatalf("destinations diverge: first=%v second=%v, want byte-equal fanout", first.Bytes(), second.Bytes())
	}
}

func TestMixRejectsSameDestinationHandleTwice(t *testing.T) {
	rt := MustNew(
		withTestFormats(testFormatMuxer(av.FormatOgg, writerTestMuxerFactory{})),
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)
	out := Write("mix.ogg", io.Discard, Format(av.FormatOgg))

	_, err := Mix(
		From(mixTestAudioSource("a", 100)).Audio(),
		From(mixTestAudioSource("b", 50)).Audio(),
	).Encode(codec.Opus()).To(out, out).UseRuntime(rt).Build(context.Background())

	// One handle listed twice is the same refusal a chain's .To(out, out)
	// raises: the joined stream reaches each destination once.
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != errcode.OutputDuplicate {
		t.Fatalf("err = %v, want %s", err, errcode.OutputDuplicate)
	}
}

func mixTestAudioSourceRate(id av.StreamID, rate int) InputSpec {
	return Source(string(id),
		shape.Frame(av.MediaAudio, shape.Audio(rate, codec.Mono, av.SampleFormatS16), shape.Stream(id)),
		func(_ context.Context, push SourcePush) error {
			b := make([]byte, 960*2) // 960 mono S16 samples
			for i := 0; i < 960; i++ {
				binary.LittleEndian.PutUint16(b[i*2:], uint16(int16(100)))
			}
			frame := &av.Frame{
				StreamID: id, Type: av.MediaAudio,
				Audio:  &av.AudioFrame{SampleRate: rate, Channels: 1, SampleFormat: av.SampleFormatS16, Samples: 960},
				Planes: []av.Plane{{Buffer: av.Buffer{Bytes: b, Ownership: av.BufferImmutable}}},
			}
			if _, err := push.Frame(frame); err != nil {
				return err
			}
			return push.EOS()
		})
}

func TestMixResamplesMismatchedArms(t *testing.T) {
	ctx := context.Background()
	rt := MustNew(testBundleFilters())

	var frames int
	var peak int16
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil {
			frames++
			if got := m.Frame.Audio.SampleRate; got != 48000 {
				t.Errorf("mixed frame sample rate = %d, want 48000 (the 24kHz arm must be resampled before mixing)", got)
			}
			for _, plane := range m.Frame.Planes {
				for i := 0; i+1 < len(plane.Buffer.Bytes); i += 2 {
					if v := int16(binary.LittleEndian.Uint16(plane.Buffer.Bytes[i:])); v > peak {
						peak = v
					}
				}
			}
		}
		return nil
	}))

	// arm "a" is 48kHz (the target); arm "b" is 24kHz and must auto-resample.
	task, err := Mix(
		From(mixTestAudioSourceRate("a", 48000)).Audio(),
		From(mixTestAudioSourceRate("b", 24000)).Audio(),
	).To(sink).UseRuntime(rt).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if frames < 1 {
		t.Fatalf("mixed frames at sink = %d, want >=1 (24kHz arm auto-resampled to 48kHz, then mixed)", frames)
	}
	// Both arms carry a constant 100: where they overlap the mix sums to ~200,
	// so a peak well above one arm's level proves both actually contributed.
	if peak < 150 {
		t.Fatalf("peak mixed amplitude = %d, want >=150 (both constant-100 arms summed)", peak)
	}
}

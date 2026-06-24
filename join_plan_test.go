package goav

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// These guards pin the planned-join contract: a Mix/Composite/Select job is
// compiled by the ONE recipe compile, so Job.Describe() (the planner spec) must
// equal the built graph node-for-node and edge-for-edge. No hand-assembled
// join graph exists to drift from the plan.

func joinPlanGuard(t *testing.T, job *Job) pipeline.Spec {
	t.Helper()
	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	task, err := job.Build(context.Background())
	if err != nil {
		t.Fatalf("Build(): %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if specText(planned) != specText(built) {
		t.Fatalf("Describe() != Build():\nplanned:\n%s\nbuilt:\n%s", specText(planned), specText(built))
	}
	return planned
}

// TestJoinDescribeEqualsBuildMix covers the deepest mix shape: one packet arm
// (auto-decoded), one mismatched frame arm (auto-resampled), the mix encoded
// and muxed to a file.
func TestJoinDescribeEqualsBuildMix(t *testing.T) {
	pcm := av.CodecID("x_pcm_s16")
	desc := codec.Descriptor{ID: pcm, Name: "PCM", Type: av.MediaAudio, Capabilities: codec.Capabilities{SampleFormats: []string{av.SampleFormatS16}}}
	rt := MustNew(
		testStdFilters(),
		WithDecoder(desc, recipePCMDecoderFactory{decoder: &recipePCMDecoder{}}),
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		withTestFormats(testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{})),
	)
	packetArm := Source("a",
		shape.Packet(av.MediaAudio, pcm, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream("a")),
		func(_ context.Context, push SourcePush) error {
			if _, err := push.Packet(&av.Packet{StreamID: "a", Type: av.MediaAudio, Payload: av.Buffer{Bytes: []byte{0, 0}, Ownership: av.BufferImmutable}}); err != nil {
				return err
			}
			return push.EOS()
		})

	job := Mix(
		From(packetArm).Audio(),
		From(mixTestAudioSourceRate("b", 24000)).Audio(),
	).Tap(FrameTap("mixed")).
		Encode(codec.Opus()).
		To(File("mix.ogg", io.Discard, Format(av.FormatOgg))).
		UseRuntime(rt)

	planned := joinPlanGuard(t, job)
	text := specText(planned)
	for _, want := range []string{
		"mix-decode-a",                 // packet arm auto-decode
		"mix-resample-b",               // mismatched frame arm auto-resample
		"-> mix",                       // convergence into the join node
		"encode-mix-encode -> mix.ogg", // joined chain: encode then mux
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("planned mix spec missing %q:\n%s", want, text)
		}
	}
}

// TestJoinDescribeEqualsBuildComposite covers a composite with per-arm regions
// delivered to a frame sink.
// TestJoinDescribeEqualsBuildMixMultiDestination pins the variadic-join-To
// contract: the planned spec keeps the encoded join fanning out to every
// destination and equals the built graph.
func TestJoinDescribeEqualsBuildMixMultiDestination(t *testing.T) {
	rt := MustNew(
		WithEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
		withTestFormats(testFormatMuxer(av.FormatOgg, &remuxTestMuxerFactory{})),
	)

	job := Mix(
		From(mixTestAudioSource("a", 100)).Audio(),
		From(mixTestAudioSource("b", 50)).Audio(),
	).Encode(codec.Opus()).
		To(
			File("first.ogg", io.Discard, Format(av.FormatOgg)),
			File("second.ogg", io.Discard, Format(av.FormatOgg)),
		).
		UseRuntime(rt)

	planned := joinPlanGuard(t, job)
	text := specText(planned)
	for _, want := range []string{
		"encode-mix-encode -> first.ogg",
		"encode-mix-encode -> second.ogg",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("planned mix spec missing %q:\n%s", want, text)
		}
	}
}

func TestJoinDescribeEqualsBuildComposite(t *testing.T) {
	job := Composite(
		From(compositeTestVideoSource("a", 4, 4, 100, 10, 20)).Video().Region(0, 0),
		From(compositeTestVideoSource("b", 4, 4, 200, 30, 40)).Video().Region(4, 0),
	).To(Sink(SinkFunc("canvas", func(context.Context, Message) error { return nil })))

	planned := joinPlanGuard(t, job)
	text := specText(planned)
	if !strings.Contains(text, "composite -> canvas") {
		t.Fatalf("planned composite spec missing joined chain:\n%s", text)
	}
}

// TestJoinDescribeEqualsBuildSelect covers the passthrough switch with its
// pinned DropBlock selector buffer: the planner spec must keep the sources
// feeding the selector directly (the control entry row SelectActive broadcasts
// to) and equal the built graph.
func TestJoinDescribeEqualsBuildSelect(t *testing.T) {
	job := Select(
		From(selectTestOneShotSource("a", 100)).Audio(),
		From(selectTestOneShotSource("b", 200)).Audio(),
	).Tap(Tap("switched")).
		To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil })))

	planned := joinPlanGuard(t, job)
	entries := 0
	for _, edge := range planned.Edges {
		if (edge.From == "a" || edge.From == "b") && edge.To == "select" {
			entries++
		}
	}
	if entries != 2 {
		t.Fatalf("planned select spec must feed the selector from both sources (entry row), got %d:\n%s", entries, specText(planned))
	}
}

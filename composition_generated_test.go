package goav_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

const compositionLawGeneratorSeed int64 = 0x60A7

type generatedCompositionCase struct {
	name       string
	sampleRate int
	samples    []int16
	resample   bool
	bitrate    int
}

func TestCompositionLawsHoldForGeneratedRecipes(t *testing.T) {
	for _, tc := range generatedCompositionCorpus() {
		t.Run(tc.name, func(t *testing.T) {
			input := goavtest.Audio(tc.sampleRate, 1, tc.samples)
			archive, monitor := goav.Write("archive.ogg", io.Discard), goav.Write("monitor.ogg", io.Discard)
			stream := generatedCompositionStream(input, tc)
			split := requireGeneratedCompositionDescribeMatchesBuild(t, stream.To(archive), stream.To(monitor))
			merged := requireGeneratedCompositionDescribeMatchesBuild(t, stream.To(archive, monitor))
			if !reflect.DeepEqual(split, merged) {
				t.Fatalf("s.To(a), s.To(b) != s.To(a, b) for generated case %s\nsplit:\n%s\nmerged:\n%s",
					tc.name, specText(split), specText(merged))
			}

			requireGeneratedCompositionRuntimeAttachMatchesBuildTimeBranch(t, tc)
			requireGeneratedNestedMixMatchesFlatMix(t, tc)
			requireGeneratedForksShareTheirWork(t, tc)
			requireGeneratedJoinsRunTogetherAsApart(t, tc)
		})
	}
}

func generatedCompositionCorpus() []generatedCompositionCase {
	rng := rand.New(rand.NewSource(compositionLawGeneratorSeed))
	count := 8
	if os.Getenv("GOAV_COMPOSITION_LONG") != "" {
		count = 64
	}
	cases := make([]generatedCompositionCase, 0, count)
	for i := 0; i < count; i++ {
		sampleRate := 48_000
		resample := rng.Intn(2) == 0
		if resample {
			sampleRate = 44_100
		}
		samples := make([]int16, 4+rng.Intn(5))
		for j := range samples {
			samples[j] = int16(rng.Intn(600) - 300)
		}
		cases = append(cases, generatedCompositionCase{
			name:       fmt.Sprintf("seed_%x_case_%02d", compositionLawGeneratorSeed, i),
			sampleRate: sampleRate,
			samples:    samples,
			resample:   resample,
			bitrate:    64_000 + rng.Intn(4)*16_000,
		})
	}
	return cases
}

// generatedCompositionStream is the generated case's encoded stream: one
// Stream value, so every Output that routes it shares its work.
func generatedCompositionStream(input goav.InputSpec, tc generatedCompositionCase) goav.Stream {
	stream := input.Audio()
	if tc.resample {
		stream = stream.Resample(48_000, codec.Mono)
	}
	return stream.Encode(codec.Opus(codec.Bitrate(tc.bitrate)))
}

func requireGeneratedCompositionDescribeMatchesBuild(t *testing.T, outs ...goav.Output) pipeline.Spec {
	t.Helper()
	ctx := context.Background()
	planned, err := goavtest.Runtime().Describe(outs...)
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	task, err := goavtest.Runtime().Build(ctx, outs...)
	if err != nil {
		t.Fatalf("BuildLive(): %v", err)
	}
	defer task.Close()
	built := task.Spec()
	if !reflect.DeepEqual(planned, built) {
		t.Fatalf("Describe() != BuildLive().Spec()\nplanned:\n%s\nbuilt:\n%s", specText(planned), specText(built))
	}
	return planned
}

func requireGeneratedCompositionRuntimeAttachMatchesBuildTimeBranch(t *testing.T, tc generatedCompositionCase) {
	t.Helper()
	ctx := context.Background()
	const tapName = "generated.frames"
	input := generatedNamedCompositionAudioInput("generated-audio", tc.sampleRate)
	branchSink := generatedNoopCompositionSink("monitor")
	buildTimePlan := requireGeneratedCompositionDescribeMatchesBuild(t, generatedMonitorComposition(input.Audio().Tap(tapName), branchSink, tc))

	baseInput := generatedNamedCompositionAudioInput("generated-audio", tc.sampleRate)
	base := baseInput.
		Audio().
		Tap(tapName).
		To(generatedNoopCompositionSink("main"))
	task, err := goavtest.Runtime().Build(ctx, base)
	if err != nil {
		t.Fatalf("BuildLive base for runtime attach: %v", err)
	}
	defer task.Close()
	if _, err := task.Attach(ctx, generatedMonitorComposition(task.Tap(tapName), generatedNoopCompositionSink("monitor"), tc)); err != nil {
		t.Fatalf("Attach generated branch: %v", err)
	}

	runtimePlan := generatedCompositionNormalizeRuntimeBranchSpec(
		generatedCompositionSpecWithout(task.Spec(), "main"),
		"monitor/",
	)
	if !reflect.DeepEqual(buildTimePlan, runtimePlan) {
		t.Fatalf("build-time branch != runtime Attach branch for generated case %s\nbuild-time:\n%s\nruntime:\n%s",
			tc.name, specText(buildTimePlan), specText(runtimePlan))
	}
}

func generatedNamedCompositionAudioInput(name string, sampleRate int) goav.InputSpec {
	return goavtest.NewTestSource(name,
		shape.Frame(av.MediaAudio, shape.Audio(sampleRate, 1, av.SampleFormatS16)),
	).Input()
}

func generatedMonitorComposition(from goav.Stream, sink goav.Destination, tc generatedCompositionCase) goav.Output {
	if tc.resample {
		from = from.Resample(48_000, codec.Mono)
	}
	return from.Encode(codec.Opus(codec.Bitrate(tc.bitrate))).As("monitor").To(sink)
}

func generatedNoopCompositionSink(name string) goav.Destination {
	return goav.Sink(goav.SinkFunc(name, func(context.Context, goav.Message) error {
		return nil
	}))
}

func generatedCompositionSpecWithout(spec pipeline.Spec, nodeNames ...string) pipeline.Spec {
	remove := make(map[string]struct{}, len(nodeNames))
	for _, name := range nodeNames {
		remove[name] = struct{}{}
	}
	out := spec
	out.Nodes = make([]pipeline.NodeSpec, 0, len(spec.Nodes))
	for _, node := range spec.Nodes {
		if _, ok := remove[node.Name]; !ok {
			out.Nodes = append(out.Nodes, node)
		}
	}
	out.Edges = make([]pipeline.EdgeSpec, 0, len(spec.Edges))
	for _, edge := range spec.Edges {
		if _, ok := remove[string(edge.From)]; ok {
			continue
		}
		if _, ok := remove[string(edge.To)]; ok {
			continue
		}
		out.Edges = append(out.Edges, edge)
	}
	return out
}

func generatedCompositionNormalizeRuntimeBranchSpec(spec pipeline.Spec, prefix string) pipeline.Spec {
	for i := range spec.Nodes {
		spec.Nodes[i].Name = generatedCompositionTrimNodePrefix(spec.Nodes[i].Name, prefix)
	}
	for i := range spec.Edges {
		spec.Edges[i].From = pipeline.NodeRef(generatedCompositionTrimNodePrefix(string(spec.Edges[i].From), prefix))
		spec.Edges[i].To = pipeline.NodeRef(generatedCompositionTrimNodePrefix(string(spec.Edges[i].To), prefix))
	}
	return spec
}

func generatedCompositionTrimNodePrefix(name string, prefix string) string {
	if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
		return name[len(prefix):]
	}
	return name
}

func requireGeneratedNestedMixMatchesFlatMix(t *testing.T, tc generatedCompositionCase) {
	t.Helper()
	a := generatedOffsetCompositionSamples(tc.samples, -7)
	b := generatedOffsetCompositionSamples(tc.samples, 3)
	c := generatedOffsetCompositionSamples(tc.samples, 11)
	flat := runGeneratedFlatCompositionMix(t, a, b, c)
	nested := runGeneratedNestedCompositionMix(t, a, b, c)
	if !reflect.DeepEqual(flat, nested) {
		t.Fatalf("nested mix != flat mix for generated case %s\nflat:   %v\nnested: %v", tc.name, flat, nested)
	}
}

func runGeneratedFlatCompositionMix(t *testing.T, a, b, c []int16) [][]int16 {
	t.Helper()
	out := goavtest.NewCollector()
	job := goav.Mix(
		goavtest.Audio(48_000, 1, a).Audio(),
		goavtest.Audio(48_000, 1, b).Audio(),
		goavtest.Audio(48_000, 1, c).Audio(),
	).To(out.Sink())
	requireGeneratedCompositionDescribeMatchesBuild(t, job)
	if err := goavtest.Runtime().Run(context.Background(), job); err != nil {
		t.Fatalf("Run generated flat mix: %v", err)
	}
	return out.S16()
}

func runGeneratedNestedCompositionMix(t *testing.T, a, b, c []int16) [][]int16 {
	t.Helper()
	out := goavtest.NewCollector()
	job := goav.Mix(
		goav.Mix(
			goavtest.Audio(48_000, 1, a).Audio(),
			goavtest.Audio(48_000, 1, b).Audio(),
		),
		goavtest.Audio(48_000, 1, c).Audio(),
	).To(out.Sink())
	requireGeneratedCompositionDescribeMatchesBuild(t, job)
	if err := goavtest.Runtime().Run(context.Background(), job); err != nil {
		t.Fatalf("Run generated nested mix: %v", err)
	}
	return out.S16()
}

func generatedOffsetCompositionSamples(samples []int16, delta int16) []int16 {
	out := make([]int16, len(samples))
	for i, sample := range samples {
		out[i] = sample + delta
	}
	return out
}

// requireGeneratedForksShareTheirWork pins that work before a fork runs once:
// two encodings forked after the case's conversion read one converted stream.
func requireGeneratedForksShareTheirWork(t *testing.T, tc generatedCompositionCase) {
	t.Helper()
	shared := goavtest.Audio(tc.sampleRate, 1, tc.samples).Audio().Resample(48_000, codec.Stereo)
	spec := requireGeneratedCompositionDescribeMatchesBuild(t,
		shared.Encode(codec.Opus(codec.Bitrate(tc.bitrate))).As("high").To(generatedNoopCompositionSink("high")),
		shared.Encode(codec.Opus(codec.Bitrate(32_000))).As("low").To(generatedNoopCompositionSink("low")),
	)
	resamples, readers := 0, 0
	for _, node := range spec.Nodes {
		if node.Detail != "" && len(node.Name) >= len("resample") && node.Name[:len("resample")] == "resample" {
			resamples++
		}
	}
	for _, edge := range spec.Edges {
		if len(edge.From) >= len("resample") && string(edge.From)[:len("resample")] == "resample" {
			readers++
		}
	}
	if resamples != 1 || readers != 2 {
		t.Fatalf("forks of one converted stream: %d resample nodes read by %d nodes, want 1 read by both encoders\n%s", resamples, readers, specText(spec))
	}
}

// requireGeneratedJoinsRunTogetherAsApart pins that two joins in one recipe
// run as each would alone: every sink receives what its join produces.
func requireGeneratedJoinsRunTogetherAsApart(t *testing.T, tc generatedCompositionCase) {
	t.Helper()
	a := generatedOffsetCompositionSamples(tc.samples, -5)
	b := generatedOffsetCompositionSamples(tc.samples, 9)
	c := generatedOffsetCompositionSamples(tc.samples, 2)
	mix := func(x, y []int16, out *goavtest.Collector) goav.Output {
		return goav.Mix(goavtest.Audio(48_000, 1, x).Audio(), goavtest.Audio(48_000, 1, y).Audio()).To(out.Sink())
	}
	run := func(outs ...goav.Output) {
		t.Helper()
		requireGeneratedCompositionDescribeMatchesBuild(t, outs...)
		if err := goavtest.Runtime().Run(context.Background(), outs...); err != nil {
			t.Fatalf("Run generated joins: %v", err)
		}
	}
	firstAlone, secondAlone := goavtest.NewCollector(), goavtest.NewCollector()
	run(mix(a, b, firstAlone))
	run(mix(b, c, secondAlone))
	first, second := goavtest.NewCollector(), goavtest.NewCollector()
	run(mix(a, b, first), mix(b, c, second))
	if !reflect.DeepEqual(first.S16(), firstAlone.S16()) || !reflect.DeepEqual(second.S16(), secondAlone.S16()) {
		t.Fatalf("joins run together != apart for generated case %s\ntogether: %v %v\napart:    %v %v",
			tc.name, first.S16(), second.S16(), firstAlone.S16(), secondAlone.S16())
	}
}

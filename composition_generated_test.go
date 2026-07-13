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
			dest := goav.Write("archive.ogg", io.Discard)
			direct := generatedDirectCompositionJob(input, dest, tc)
			branched := generatedMainBranchCompositionJob(input, dest, tc)

			directPlan := requireGeneratedCompositionDescribeMatchesBuild(t, direct)
			branchPlan := requireGeneratedCompositionDescribeMatchesBuild(t, branched)
			if !reflect.DeepEqual(directPlan, branchPlan) {
				t.Fatalf("direct chain != Branch(\"main\") for generated case %s\ndirect:\n%s\nbranch:\n%s",
					tc.name, specText(directPlan), specText(branchPlan))
			}

			requireGeneratedCompositionRuntimeAttachMatchesBuildTimeBranch(t, tc)
			requireGeneratedNestedMixMatchesFlatMix(t, tc)
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

func generatedDirectCompositionJob(input goav.InputSpec, dest goav.Destination, tc generatedCompositionCase) *goav.Job {
	chain := goav.From(input).Audio()
	if tc.resample {
		chain = chain.Resample(48_000, codec.Mono)
	}
	return chain.Encode(codec.Opus(codec.Bitrate(tc.bitrate))).To(dest).UseRuntime(goavtest.Runtime())
}

func generatedMainBranchCompositionJob(input goav.InputSpec, dest goav.Destination, tc generatedCompositionCase) *goav.Job {
	branch := goav.Branch("main")
	if tc.resample {
		branch = branch.Resample(48_000, codec.Mono)
	}
	spec := branch.Encode(codec.Opus(codec.Bitrate(tc.bitrate))).To(dest)
	return goav.From(input).Audio().Branches(spec).UseRuntime(goavtest.Runtime())
}

func requireGeneratedCompositionDescribeMatchesBuild(t *testing.T, job *goav.Job) pipeline.Spec {
	t.Helper()
	ctx := context.Background()
	planned, err := job.Describe()
	if err != nil {
		t.Fatalf("Describe(): %v", err)
	}
	task, err := job.BuildLive(ctx)
	if err != nil {
		t.Fatalf("BuildLive(): %v", err)
	}
	defer task.Close()
	built := task.Describe()
	if !reflect.DeepEqual(planned, built) {
		t.Fatalf("Describe() != BuildLive().Describe()\nplanned:\n%s\nbuilt:\n%s", specText(planned), specText(built))
	}
	return planned
}

func requireGeneratedCompositionRuntimeAttachMatchesBuildTimeBranch(t *testing.T, tc generatedCompositionCase) {
	t.Helper()
	ctx := context.Background()
	const tapName = "generated.frames"
	input := generatedNamedCompositionAudioInput("generated-audio", tc.sampleRate)
	branchSink := generatedNoopCompositionSink("monitor")
	buildTime := goav.From(input).Audio().Tap(goav.FrameTap(tapName)).
		Branches(generatedMonitorCompositionBranch(tapName, branchSink, tc)).
		UseRuntime(goavtest.Runtime())

	buildTimePlan := requireGeneratedCompositionDescribeMatchesBuild(t, buildTime)

	baseInput := generatedNamedCompositionAudioInput("generated-audio", tc.sampleRate)
	base := goav.From(baseInput).Audio().Tap(goav.FrameTap(tapName)).
		To(generatedNoopCompositionSink("main")).
		UseRuntime(goavtest.Runtime())
	task, err := base.BuildLive(ctx)
	if err != nil {
		t.Fatalf("BuildLive base for runtime attach: %v", err)
	}
	defer task.Close()
	if _, err := task.Attach(ctx, generatedMonitorCompositionBranch(tapName, generatedNoopCompositionSink("monitor"), tc)); err != nil {
		t.Fatalf("Attach generated branch: %v", err)
	}

	runtimePlan := generatedCompositionNormalizeRuntimeBranchSpec(
		generatedCompositionSpecWithout(task.Describe(), "main"),
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

func generatedMonitorCompositionBranch(tapName string, sink goav.Destination, tc generatedCompositionCase) goav.BranchSpec {
	branch := goav.Branch("monitor").From(goav.FrameTap(tapName))
	if tc.resample {
		branch = branch.Resample(48_000, codec.Mono)
	}
	return branch.Encode(codec.Opus(codec.Bitrate(tc.bitrate))).To(sink)
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
		goav.From(goavtest.Audio(48_000, 1, a)).Audio(),
		goav.From(goavtest.Audio(48_000, 1, b)).Audio(),
		goav.From(goavtest.Audio(48_000, 1, c)).Audio(),
	).To(out.Sink()).UseRuntime(goavtest.Runtime())
	requireGeneratedCompositionDescribeMatchesBuild(t, job)
	if err := job.Run(context.Background()); err != nil {
		t.Fatalf("Run generated flat mix: %v", err)
	}
	return out.S16()
}

func runGeneratedNestedCompositionMix(t *testing.T, a, b, c []int16) [][]int16 {
	t.Helper()
	out := goavtest.NewCollector()
	job := goav.Mix(
		goav.Mix(
			goav.From(goavtest.Audio(48_000, 1, a)).Audio(),
			goav.From(goavtest.Audio(48_000, 1, b)).Audio(),
		),
		goav.From(goavtest.Audio(48_000, 1, c)).Audio(),
	).To(out.Sink()).UseRuntime(goavtest.Runtime())
	requireGeneratedCompositionDescribeMatchesBuild(t, job)
	if err := job.Run(context.Background()); err != nil {
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

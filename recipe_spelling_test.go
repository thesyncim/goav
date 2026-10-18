package goav

import (
	"context"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	sourcepkg "github.com/thesyncim/goav/source"
)

// The in-package tests spell recipes as chains: recipeFrom(input).Audio()...
// .To(dest), then a runtime call. Each chain here is a value Stream and a job
// is the Outputs its chains end in, so these tests compile through the one
// compiler exactly as rt.Build(ctx, outputs...) does.

// testJob is a recipe: its inputs, the Outputs its chains ended in, and the
// runtime it builds against (nil builds against no adapters).
type testJob struct {
	inputs []InputSpec
	outs   []Output
	rt     *Runtime
	sync   []flow.SyncPolicy
	// rtSet marks a runtime the test named, nil included.
	rtSet bool
}

func recipeFrom(inputs ...InputSpec) *testJob { return &testJob{inputs: inputs} }

// selectInputName narrows a chain of a several-input job to one input.
type selectInputName string

func (j *testJob) And(inputs ...InputSpec) *testJob {
	j.inputs = append(j.inputs, inputs...)
	return j
}

func (j *testJob) UseRuntime(rt *Runtime) *testJob {
	j.rt, j.rtSet = rt, true
	return j
}

// Copy is the whole-input copy input.To spells; it needs no step.
func (j *testJob) Copy() *testJob { return j }

// Sync gates the chains the job starts after it.
func (j *testJob) Sync(policy flow.SyncPolicy) *testJob {
	j.sync = append(j.sync, policy)
	return j
}

// To copies every input whole to the destinations.
func (j *testJob) To(destinations ...Destination) *testJob {
	for _, in := range j.inputs {
		j.outs = append(j.outs, in.To(destinations...))
	}
	return j
}

func (j *testJob) Audio(options ...any) *testChain { return j.chain(av.MediaAudio, options) }
func (j *testJob) Video(options ...any) *testChain { return j.chain(av.MediaVideo, options) }
func (j *testJob) Stream(options ...any) *testChain {
	return j.chain("", options)
}

func (j *testJob) chain(media av.MediaType, options []any) *testChain {
	var streamOptions []StreamOption
	in := j.inputFor(media)
	for _, option := range options {
		switch option := option.(type) {
		case StreamOption:
			streamOptions = append(streamOptions, option)
		case selectInputName:
			for _, candidate := range j.inputs {
				if candidate.inputName(candidate.graphSourceNodeName()) == string(option) {
					in = candidate
				}
			}
		}
	}
	var s Stream
	switch media {
	case av.MediaAudio:
		s = in.Audio(streamOptions...)
	case av.MediaVideo:
		s = in.Video(streamOptions...)
	default:
		s = in.Stream(streamOptions...)
	}
	for _, policy := range j.sync {
		s = s.Sync(policy)
	}
	return &testChain{job: j, s: s}
}

// runtime is the job's runtime: a bare one when the test named none.
func (j *testJob) runtime() *Runtime {
	if j.rt == nil && !j.rtSet {
		return mustNew()
	}
	return j.rt
}

func (j *testJob) Describe() (pipeline.Spec, error) { return j.runtime().Describe(j.outs...) }

func (j *testJob) Explain(ctx context.Context) (plan.Report, error) {
	return j.runtime().Explain(ctx, j.outs...)
}

func (j *testJob) Build(ctx context.Context) (Task, error) { return j.BuildLive(ctx) }

func (j *testJob) BuildLive(ctx context.Context) (LiveTask, error) {
	return j.runtime().Build(ctx, j.outs...)
}

func (j *testJob) Run(ctx context.Context) error { return j.runtime().Run(ctx, j.outs...) }

// testArm is a join arm: a chain, a placed chain, or a nested join.
type testArm interface{ armLayer() Layer }

// testChain is one Stream under construction; Region places it as a
// composite arm.
type testChain struct {
	job    *testJob
	s      Stream
	x, y   int
	placed bool
}

func (c *testChain) with(s Stream) *testChain {
	next := *c
	next.s = s
	return &next
}

func (c *testChain) armLayer() Layer {
	if c.placed {
		return At(c.s, c.x, c.y)
	}
	return c.s
}

func (c *testChain) Region(x, y int) *testChain {
	next := *c
	next.x, next.y, next.placed = x, y, true
	return &next
}

func (c *testChain) Decode(options ...codec.Option) *testChain { return c.with(c.s.Decode(options...)) }
func (c *testChain) Encode(spec codec.Spec) *testChain         { return c.with(c.s.Encode(spec)) }
func (c *testChain) Copy() *testChain                          { return c }
func (c *testChain) Tap(tap tapRef) *testChain                 { return c.with(c.s.Tap(tap.name)) }
func (c *testChain) Do(stages ...pipeline.Stage) *testChain    { return c.with(c.s.Do(stages...)) }
func (c *testChain) Sync(policy flow.SyncPolicy) *testChain    { return c.with(c.s.Sync(policy)) }
func (c *testChain) Playout(policy flow.PlayoutPolicy) *testChain {
	return c.with(c.s.Playout(policy))
}
func (c *testChain) Auto(policies ...shape.Policy) *testChain { return c.with(c.s.Auto(policies...)) }
func (c *testChain) Require(spec shape.Spec) *testChain       { return c.with(c.s.Require(spec)) }
func (c *testChain) Prefer(spec shape.Spec) *testChain        { return c.with(c.s.Prefer(spec)) }
func (c *testChain) Shape(spec shape.Spec) *testChain         { return c.with(c.s.Shape(spec)) }
func (c *testChain) As(name string) *testChain                { return c.with(c.s.As(name)) }
func (c *testChain) Buffer(buffer flow.BranchBuffer) *testChain {
	return c.with(c.s.Buffer(buffer))
}
func (c *testChain) Resize(width, height int, options ...resizeOption) *testChain {
	return c.with(c.s.Resize(width, height, options...))
}
func (c *testChain) Resample(sampleRate, channels int, options ...audioOption) *testChain {
	return c.with(c.s.Resample(sampleRate, channels, options...))
}

func (c *testChain) To(destinations ...Destination) *testJob {
	c.job.outs = append(c.job.outs, c.s.To(destinations...))
	return c.job
}

// Branches forks the chain: each branch reads the stream where the chain
// ends.
func (c *testChain) Branches(branches ...*testBranch) *testJob {
	c.job.outs = append(c.job.outs, forkOutputs(c.s, branches)...)
	return c.job
}

// testJoin is a join under construction.
type testJoin struct {
	job *testJob
	s   Stream
}

func joinArms(arms []testArm) []Stream {
	out := make([]Stream, 0, len(arms))
	for _, arm := range arms {
		layer := arm.armLayer()
		stream, _, _, _ := layer.compositeLayer()
		out = append(out, stream)
	}
	return out
}

func legacyMix(arms ...testArm) *testJoin {
	return &testJoin{job: armJob(arms), s: Mix(joinArms(arms)...)}
}

func legacySelect(arms ...testArm) *testJoin {
	return &testJoin{job: armJob(arms), s: Select(joinArms(arms)...)}
}

func legacyComposite(arms ...testArm) *testJoin {
	layers := make([]Layer, 0, len(arms))
	for _, arm := range arms {
		layers = append(layers, arm.armLayer())
	}
	return &testJoin{job: armJob(arms), s: Composite(layers...)}
}

func legacyJoin(name string, stage pipeline.Stage, arms ...testArm) *testJoin {
	return &testJoin{job: armJob(arms), s: Join(name, stage, joinArms(arms)...)}
}

// armJob is the job a join's Outputs land in: a fresh one, built against the
// runtime its first chain arm named.
func armJob(arms []testArm) *testJob {
	job := &testJob{}
	for _, arm := range arms {
		if chain, ok := arm.(*testChain); ok && chain.job != nil {
			job.rt, job.rtSet = chain.job.rt, chain.job.rtSet
			break
		}
	}
	return job
}

func (m *testJoin) armLayer() Layer                         { return m.s }
func (m *testJoin) with(s Stream) *testJoin                 { return &testJoin{job: m.job, s: s} }
func (m *testJoin) SyncByPTS() *testJoin                    { return m.with(m.s.SyncByPTS()) }
func (m *testJoin) Auto(policies ...shape.Policy) *testJoin { return m.with(m.s.Auto(policies...)) }
func (m *testJoin) Require(spec shape.Spec) *testJoin       { return m.with(m.s.Require(spec)) }
func (m *testJoin) Prefer(spec shape.Spec) *testJoin        { return m.with(m.s.Prefer(spec)) }
func (m *testJoin) Encode(spec codec.Spec) *testJoin        { return m.with(m.s.Encode(spec)) }
func (m *testJoin) Tap(tap tapRef) *testJoin                { return m.with(m.s.Tap(tap.name)) }
func (m *testJoin) Region(x, y int) *testChain {
	return (&testChain{job: m.job, s: m.s}).Region(x, y)
}

func (m *testJoin) To(destinations ...Destination) *testJob {
	m.job.outs = append(m.job.outs, m.s.To(destinations...))
	return m.job
}

func (m *testJoin) Branches(branches ...*testBranch) *testJob {
	m.job.outs = append(m.job.outs, forkOutputs(m.s, branches)...)
	return m.job
}

// testBranch is a named fork: its anchor (a tap, a track, or a graph route)
// when it attaches to a running task, its steps, and its destinations.
type testBranch struct {
	name   string
	from   branchSource
	policy pipeline.RoutePolicy
	label  string
	steps  []func(Stream) Stream
	dests  []Destination
}

func newBranch(name string) *testBranch { return &testBranch{name: name} }

func (b *testBranch) step(fn func(Stream) Stream) *testBranch {
	next := *b
	next.steps = append(append([]func(Stream) Stream(nil), b.steps...), fn)
	return &next
}

func (b *testBranch) From(source branchSource) *testBranch {
	next := *b
	next.from = source
	return &next
}

func (b *testBranch) Stream(id av.StreamID) *testBranch {
	next := *b
	next.policy, next.label = pipeline.RouteByStream, string(id)
	return &next
}

func (b *testBranch) Event(event av.EventType) *testBranch {
	next := *b
	next.policy, next.label = pipeline.RouteByEvent, string(event)
	return &next
}

func (b *testBranch) Copy() *testBranch { return b }
func (b *testBranch) Decode(options ...codec.Option) *testBranch {
	return b.step(func(s Stream) Stream { return s.Decode(options...) })
}
func (b *testBranch) Encode(spec codec.Spec) *testBranch {
	return b.step(func(s Stream) Stream { return s.Encode(spec) })
}
func (b *testBranch) Do(stages ...pipeline.Stage) *testBranch {
	return b.step(func(s Stream) Stream { return s.Do(stages...) })
}
func (b *testBranch) Tap(tap tapRef) *testBranch {
	return b.step(func(s Stream) Stream { return s.Tap(tap.name) })
}
func (b *testBranch) Sync(policy flow.SyncPolicy) *testBranch {
	return b.step(func(s Stream) Stream { return s.Sync(policy) })
}
func (b *testBranch) Playout(policy flow.PlayoutPolicy) *testBranch {
	return b.step(func(s Stream) Stream { return s.Playout(policy) })
}
func (b *testBranch) Auto(policies ...shape.Policy) *testBranch {
	return b.step(func(s Stream) Stream { return s.Auto(policies...) })
}
func (b *testBranch) Require(spec shape.Spec) *testBranch {
	return b.step(func(s Stream) Stream { return s.Require(spec) })
}
func (b *testBranch) Prefer(spec shape.Spec) *testBranch {
	return b.step(func(s Stream) Stream { return s.Prefer(spec) })
}
func (b *testBranch) Shape(spec shape.Spec) *testBranch {
	return b.step(func(s Stream) Stream { return s.Shape(spec) })
}
func (b *testBranch) Buffer(buffer flow.BranchBuffer) *testBranch {
	return b.step(func(s Stream) Stream { return s.Buffer(buffer) })
}
func (b *testBranch) Resize(width, height int, options ...resizeOption) *testBranch {
	return b.step(func(s Stream) Stream { return s.Resize(width, height, options...) })
}
func (b *testBranch) Resample(sampleRate, channels int, options ...audioOption) *testBranch {
	return b.step(func(s Stream) Stream { return s.Resample(sampleRate, channels, options...) })
}

func (b *testBranch) To(destinations ...Destination) *testBranch {
	next := *b
	next.dests = append([]Destination(nil), destinations...)
	return &next
}

// output is the branch as an Output read from s.
func (b *testBranch) output(s Stream) Output {
	for _, step := range b.steps {
		s = step(s)
	}
	if b.name != "" {
		s = s.As(b.name)
	}
	return s.To(b.dests...)
}

func forkOutputs(s Stream, branches []*testBranch) []Output {
	out := make([]Output, 0, len(branches))
	for _, b := range branches {
		out = append(out, b.output(s))
	}
	return out
}

// legacyAttach attaches branches to a running task, each from its anchor.
func legacyAttach(live LiveTask, ctx context.Context, branches ...*testBranch) (Attachment, error) {
	outs := make([]Output, 0, len(branches))
	for _, b := range branches {
		outs = append(outs, b.output(b.anchor(live)))
	}
	return live.Attach(ctx, outs...)
}

func (b *testBranch) anchor(live LiveTask) Stream {
	switch from := b.from.(type) {
	case tapRef:
		return live.Tap(from.name)
	case inputStreamAnchor:
		return live.Track(from.input, from.stream)
	case graphRouteAnchor:
		route := from.Route()
		if b.policy != "" {
			route.Policy, route.Label = b.policy, b.label
		}
		return live.Route(route)
	case tapAnchor:
		binding := from.branchSource()
		if binding.tap != "" {
			return live.Tap(binding.tap)
		}
		route := pipeline.Route{From: binding.from, Policy: binding.policy, Label: binding.label}
		if b.policy != "" {
			route.Policy, route.Label = b.policy, b.label
		}
		return live.Route(route)
	case nil:
		return Stream{}
	default:
		return Stream{node: &streamNode{err: branchSourceInvalidError(firstNonEmpty(b.name, "branch"))}}
	}
}

// on is the branch as an Output read from its anchor on a running task, the
// replacement a Rebranch takes.
func (b *testBranch) on(live LiveTask) Output { return b.output(b.anchor(live)) }

// OnStream adds a stream rule on the job's first input: each branch forks
// every stream the rule matches.
func (j *testJob) OnStream(match sourcepkg.StreamMatch, branches ...*testBranch) *testJob {
	in := InputSpec{}
	if len(j.inputs) != 0 {
		in = j.inputs[0]
	}
	j.outs = append(j.outs, forkOutputs(in.OnStream(match), branches)...)
	return j
}

// testFlow is reusable work: steps a chain or a branch applies in order.
type testFlow struct{ steps []func(Stream) Stream }

func newFlow(string) *testFlow { return &testFlow{} }

func (f *testFlow) step(fn func(Stream) Stream) *testFlow {
	return &testFlow{steps: append(append([]func(Stream) Stream(nil), f.steps...), fn)}
}

func (f *testFlow) Audio() *testFlow { return f }
func (f *testFlow) Video() *testFlow { return f }
func (f *testFlow) Decode(options ...codec.Option) *testFlow {
	return f.step(func(s Stream) Stream { return s.Decode(options...) })
}
func (f *testFlow) Encode(spec codec.Spec) *testFlow {
	return f.step(func(s Stream) Stream { return s.Encode(spec) })
}
func (f *testFlow) Do(stages ...pipeline.Stage) *testFlow {
	return f.step(func(s Stream) Stream { return s.Do(stages...) })
}
func (f *testFlow) Tap(tap tapRef) *testFlow {
	return f.step(func(s Stream) Stream { return s.Tap(tap.name) })
}
func (f *testFlow) Sync(policy flow.SyncPolicy) *testFlow {
	return f.step(func(s Stream) Stream { return s.Sync(policy) })
}
func (f *testFlow) Playout(policy flow.PlayoutPolicy) *testFlow {
	return f.step(func(s Stream) Stream { return s.Playout(policy) })
}
func (f *testFlow) Auto(policies ...shape.Policy) *testFlow {
	return f.step(func(s Stream) Stream { return s.Auto(policies...) })
}
func (f *testFlow) Require(spec shape.Spec) *testFlow {
	return f.step(func(s Stream) Stream { return s.Require(spec) })
}
func (f *testFlow) Prefer(spec shape.Spec) *testFlow {
	return f.step(func(s Stream) Stream { return s.Prefer(spec) })
}
func (f *testFlow) Shape(spec shape.Spec) *testFlow {
	return f.step(func(s Stream) Stream { return s.Shape(spec) })
}
func (f *testFlow) Resize(width, height int, options ...resizeOption) *testFlow {
	return f.step(func(s Stream) Stream { return s.Resize(width, height, options...) })
}
func (f *testFlow) Resample(sampleRate, channels int, options ...audioOption) *testFlow {
	return f.step(func(s Stream) Stream { return s.Resample(sampleRate, channels, options...) })
}

func (c *testChain) Apply(f *testFlow) *testChain {
	s := c.s
	for _, step := range f.steps {
		s = step(s)
	}
	return c.with(s)
}

func (b *testBranch) Apply(f *testFlow) *testBranch {
	next := b
	for _, step := range f.steps {
		next = next.step(step)
	}
	return next
}

func (f *testFlow) Copy() *testFlow { return f }

// compileJobRecipe compiles a job the way Describe and Explain do;
// compileJobRecipeForBuild the way Build does, with every preflight.
func compileJobRecipe(j *testJob) (recipeResolved, error) {
	return j.compile(recipeCompileOptions{})
}

func compileJobRecipeForBuild(j *testJob) (recipeResolved, error) {
	return compileJobRecipeForBuildContext(context.Background(), j)
}

func compileJobRecipeForBuildContext(ctx context.Context, j *testJob) (recipeResolved, error) {
	return j.compile(buildCompileOptions(ctx))
}

func (j *testJob) compile(options recipeCompileOptions) (recipeResolved, error) {
	g, err := compileGraph(j.outs)
	if err != nil {
		return recipeResolved{}, err
	}
	return compileRecipe(g, j.runtime(), options)
}

// inputFor is the input a chain of media reads: the one input whose declared
// stream is that media, else the first.
func (j *testJob) inputFor(media av.MediaType) InputSpec {
	if len(j.inputs) == 0 {
		return InputSpec{}
	}
	var matches []InputSpec
	for _, in := range j.inputs {
		if spec, ok := declaredSourceShape(in); ok && (media == "" || spec.MediaKind == media) {
			matches = append(matches, in)
		}
	}
	if len(matches) == 1 {
		return matches[0]
	}
	return j.inputs[0]
}

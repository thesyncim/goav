// The From(...) job grammar: the Job builder, its stream and destination wiring, and Build/Run.

package goav

import (
	"context"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// BuildError is the one structured refusal goav raises from build,
// validation, attach, and explain paths. Code identifies the refusal class
// (see errors.go for the catalog), Operation/Node say where, Reason says why,
// Details carry machine-readable facts (key=value lines), and Suggestions
// carry concrete fixes. Cause is a sentinel (ErrUnsupportedBuild, ErrNilSink,
// ...) reachable through errors.Is.
type BuildError struct {
	Code        ErrorCode
	Operation   string
	Node        string
	Reason      string
	Details     []string
	Suggestions []string
	Cause       error
}

func (e *BuildError) Error() string {
	if e == nil {
		return ""
	}
	var out strings.Builder
	out.WriteString("goav")
	if e.Operation != "" {
		out.WriteString(": cannot ")
		out.WriteString(e.Operation)
	} else {
		out.WriteString(": build failed")
	}
	if e.Node != "" {
		out.WriteString(" for ")
		out.WriteString(e.Node)
	}
	if e.Reason != "" {
		out.WriteString(": ")
		out.WriteString(e.Reason)
	}
	if len(e.Details) != 0 {
		out.WriteString("\nDetails:")
		for i := range e.Details {
			out.WriteString("\n  - ")
			out.WriteString(e.Details[i])
		}
	}
	if len(e.Suggestions) != 0 {
		out.WriteString("\nSuggestions:")
		for i := range e.Suggestions {
			out.WriteString("\n  - ")
			out.WriteString(e.Suggestions[i])
		}
	}
	return out.String()
}

func (e *BuildError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

type Job struct {
	name               string
	runtime            Runtime
	inputs             []InputSpec
	outputs            []destinationSpec
	outputNames        []string
	streams            []*jobStreamBuild
	branchStreams      []streamBuild
	branchDestinations []namedDestinationSpec
	streamRules        []streamRule
	join               *joinSpec
	err                error
}

type jobStreamBuild struct {
	name        string
	selector    av.StreamSelector
	input       string
	operations  []OperationSpec
	codecChange CodecChangePolicy
	outputs     []destinationSpec
	outputNames []string
}

type chainStep struct {
	stage     pipeline.Stage
	shape     shape.Spec
	transform TransformSpec
	tap       string
	tapDomain shape.MediaDomain
}

// From starts a recipe from one or more inputs. With several inputs each
// stream chain selects across all of them: an unambiguous .Audio()/.Video()
// match just works, and goav.InputName(...) narrows a chain to one input.
func From(inputs ...InputSpec) *Job {
	job := newJob("from")
	job.inputs = append(job.inputs, inputs...)
	return job
}

func (j *Job) Copy() *Job {
	return j
}

func newJob(name string) *Job {
	return &Job{name: name, runtime: Default()}
}

func (j *Job) UseRuntime(runtime Runtime) *Job {
	if j != nil {
		j.runtime = runtime
	}
	return j
}

func (j *Job) setErr(err error) {
	if j.err == nil {
		j.err = err
	}
}

func (j *Job) To(destinations ...Destination) *Job {
	if len(j.branchStreams) != 0 || (j.join != nil && len(j.join.branches) != 0) {
		j.setErr(branchOutputScopeError("branches"))
		return j
	}
	for i := range destinations {
		destination := destinations[i]
		binding, err := destinationBindingFromDestination(destination)
		if err != nil {
			j.setErr(jobDestinationInvalidError("job", err.Error()))
			return j
		}
		output, name, err := destinationFromBinding("build job", "job", binding, i)
		if err != nil {
			j.setErr(err)
			return j
		}
		j.outputs = append(j.outputs, output)
		j.outputNames = append(j.outputNames, name)
	}
	return j
}

func (j *Job) addBranchDestinations(destinations ...destinationRef) error {
	seen := make(map[string]string, len(j.branchDestinations)+len(destinations))
	for i := range j.branchDestinations {
		seen[j.branchDestinations[i].name] = destinationIdentity(j.branchDestinations[i])
	}
	for i := range destinations {
		destination := cloneDestinationRef(destinations[i])
		if destination.err != nil {
			return destination.err
		}
		if destination.name == "" {
			return destinationNameMissingError(destination.dest)
		}
		destination.dest = destination.dest.withName(firstNonEmpty(destination.dest.name, destination.name))
		named := namedDestinationSpec{name: destination.name, output: destination.dest}
		identity := destinationIdentity(named)
		if existing, ok := seen[named.name]; ok {
			if existing != identity {
				return branchDestinationDuplicateError(named.name)
			}
			continue
		}
		seen[named.name] = identity
		j.branchDestinations = append(j.branchDestinations, named)
	}
	return nil
}

func (j *Job) And(inputs ...InputSpec) *Job {
	j.inputs = append(j.inputs, inputs...)
	return j
}

func (j *Job) Audio(options ...streamOption) *jobStreamBuilder {
	return j.streamBuilder("audio", av.MediaAudio, options...)
}

func (j *Job) Video(options ...streamOption) *jobStreamBuilder {
	return j.streamBuilder("video", av.MediaVideo, options...)
}

func (j *Job) Stream(options ...streamOption) *jobStreamBuilder {
	return j.streamBuilder("stream", "", options...)
}

func (j *Job) streamBuilder(name string, media av.MediaType, options ...streamOption) *jobStreamBuilder {
	config := newStreamSelectConfig(media, options...)
	stream := &jobStreamBuild{
		name:     name,
		selector: config.selector,
		input:    config.input,
	}
	if last := j.currentStream(); last != nil {
		if len(j.branchStreams) != 0 {
			j.streams = []*jobStreamBuild{stream}
			return &jobStreamBuilder{job: j, stream: stream}
		}
		if len(last.outputs) == 0 {
			// A new chain may only start once the previous one is routed; an
			// unfinished chain followed by another selection is still an error.
			j.err = duplicateJobStreamError(last, stream)
			return &jobStreamBuilder{job: j, stream: stream}
		}
	}
	j.streams = append(j.streams, stream)
	return &jobStreamBuilder{job: j, stream: stream}
}

func (j *Job) currentStream() *jobStreamBuild {
	if j == nil || len(j.streams) == 0 {
		return nil
	}
	return j.streams[len(j.streams)-1]
}

// checkSharedStreamDestination lets several chains share ONE Destination handle
// (one mux group) while rejecting two different handles that would collide on
// the same destination label.
func (j *Job) checkSharedStreamDestination(current *jobStreamBuild, output destinationSpec, name string) error {
	label := firstNonEmpty(name, output.label(""))
	if label == "" {
		return nil
	}
	for i := range j.streams {
		stream := j.streams[i]
		if stream == nil || stream == current {
			continue
		}
		for k := range stream.outputs {
			existingLabel := jobOutputDestinationName(stream.outputs, stream.outputNames, k)
			if existingLabel != label {
				continue
			}
			existing := destinationIdentity(namedDestinationSpec{name: existingLabel, output: stream.outputs[k]})
			next := destinationIdentity(namedDestinationSpec{name: label, output: output})
			if existing != next {
				return duplicateDestinationHandleError("build stream", label)
			}
		}
	}
	return nil
}

func (j *Job) plan() Intent {
	intent := Intent{Name: j.name}
	if runtime, ok := j.runtime.(*runtime); ok {
		intent.Policies.Realtime = runtime.realtime
	}
	for i := range j.inputs {
		intent.Inputs = append(intent.Inputs, j.inputs[i].intent())
	}
	if len(j.branchStreams) != 0 {
		for i := range j.branchStreams {
			intent.Streams = append(intent.Streams, branchStreamIntent(j.branchStreams[i]))
		}
		for i := range j.branchDestinations {
			intent.Destinations = append(intent.Destinations, j.branchDestinations[i].output.intentWithName(j.branchDestinations[i].name))
		}
		return intent
	} else if len(j.streams) == 1 {
		stream := j.streams[0]
		intent.Streams = append(intent.Streams, jobStreamIntent(stream))
		for i := range j.outputs {
			name := ""
			if i < len(j.outputNames) {
				name = j.outputNames[i]
			}
			intent.Destinations = append(intent.Destinations, j.outputs[i].intentWithName(name))
		}
		for i := range stream.outputs {
			name := ""
			if i < len(stream.outputNames) {
				name = stream.outputNames[i]
			}
			intent.Destinations = append(intent.Destinations, stream.outputs[i].intentWithName(name))
		}
		return intent
	} else if len(j.streams) > 1 {
		names := uniqueJobStreamNames(j.streams)
		for i := range j.streams {
			stream := jobStreamIntent(j.streams[i])
			stream.Name = names[i]
			intent.Streams = append(intent.Streams, stream)
		}
		for i := range j.outputs {
			name := ""
			if i < len(j.outputNames) {
				name = j.outputNames[i]
			}
			intent.Destinations = append(intent.Destinations, j.outputs[i].intentWithName(name))
		}
		outputs, outputNames := dedupedJobStreamOutputs(j.streams)
		for i := range outputs {
			intent.Destinations = append(intent.Destinations, outputs[i].intentWithName(outputNames[i]))
		}
		return intent
	}
	outputs := j.allOutputs()
	outputNames := j.allOutputNames()
	for i := range outputs {
		name := ""
		if i < len(outputNames) {
			name = outputNames[i]
		}
		intent.Destinations = append(intent.Destinations, outputs[i].intentWithName(name))
	}
	return intent
}

func (j *Job) Describe() (pipeline.Spec, error) {
	resolved, err := compileJobRecipe(j)
	if err != nil {
		return pipeline.Spec{}, err
	}
	return resolved.Describe()
}

func (j *Job) Build(ctx context.Context) (Task, error) {
	resolved, err := compileJobRecipeForBuildContext(ctx, j)
	if err != nil {
		return nil, err
	}
	built, err := resolved.Build(ctx)
	if err != nil {
		return nil, err
	}
	j.installStreamRules(built)
	return built, nil
}

// installStreamRules binds the job's OnStream rules to the built task: the
// rules anchor on the job's single source node (the compile validated the
// input count) and react from the task's event stream.
func (j *Job) installStreamRules(built Task) {
	if len(j.streamRules) == 0 || len(j.inputs) != 1 {
		return
	}
	runtimeTask, ok := built.(*task)
	if !ok || runtimeTask == nil {
		return
	}
	input := j.inputs[0]
	runtimeTask.installStreamRules(
		graphSourceNodeNames(j.inputs)[0],
		input.sourceEventDomain(),
		cloneStreamRules(j.streamRules),
	)
}

func (j *Job) Run(ctx context.Context) error {
	task, err := j.Build(ctx)
	if err != nil {
		return err
	}
	defer task.Close()
	return task.Run(ctx)
}

func validateJobOutputBindings(operation string, stream streamIntent, outputs []destinationSpec, destinationNames []string) error {
	destinations := jobOutputDestinationSet(outputs, destinationNames)
	for _, destinationName := range stream.Destinations {
		if _, ok := destinations[destinationName]; ok {
			continue
		}
		return jobDestinationReferenceMissingError(operation, stream, destinationName)
	}
	return nil
}

func jobOutputDestinationSet(outputs []destinationSpec, destinationNames []string) map[string]struct{} {
	destinations := make(map[string]struct{}, len(outputs))
	for i := range outputs {
		destinations[jobOutputDestinationName(outputs, destinationNames, i)] = struct{}{}
	}
	return destinations
}

func (j *Job) allOutputs() []destinationSpec {
	if len(j.branchDestinations) != 0 {
		outputs := make([]destinationSpec, 0, len(j.branchDestinations))
		for i := range j.branchDestinations {
			outputs = append(outputs, j.branchDestinations[i].output)
		}
		return outputs
	}
	streamOutputs, _ := j.streamOutputsAndNames()
	return jobAllOutputs(j.outputs, streamOutputs)
}

func (j *Job) allOutputNames() []string {
	if len(j.branchDestinations) != 0 {
		names := make([]string, 0, len(j.branchDestinations))
		for i := range j.branchDestinations {
			names = append(names, j.branchDestinations[i].name)
		}
		return names
	}
	_, streamOutputNames := j.streamOutputsAndNames()
	return jobAllOutputNames(j.outputNames, streamOutputNames)
}

// streamOutputsAndNames collects the stream-chain destinations: verbatim for a
// single chain (today's behavior) and deduplicated by destination label for
// several chains so one shared Destination handle lowers to one mux group.
func (j *Job) streamOutputsAndNames() ([]destinationSpec, []string) {
	if len(j.streams) > 1 {
		return dedupedJobStreamOutputs(j.streams)
	}
	return jobStreamOutputs(j.currentStream()), jobStreamOutputNames(j.currentStream())
}

func dedupedJobStreamOutputs(streams []*jobStreamBuild) ([]destinationSpec, []string) {
	outputs := make([]destinationSpec, 0, len(streams))
	names := make([]string, 0, len(streams))
	seen := make(map[string]struct{}, len(streams))
	for i := range streams {
		stream := streams[i]
		if stream == nil {
			continue
		}
		for k := range stream.outputs {
			label := jobOutputDestinationName(stream.outputs, stream.outputNames, k)
			if _, ok := seen[label]; ok {
				continue
			}
			seen[label] = struct{}{}
			outputs = append(outputs, stream.outputs[k])
			names = append(names, label)
		}
	}
	return outputs, names
}

// uniqueJobStreamNames keeps chain names stable ("video", "audio") and only
// suffixes repeats ("video-2") so each stream lowers to a uniquely named branch.
func uniqueJobStreamNames(streams []*jobStreamBuild) []string {
	names := make([]string, 0, len(streams))
	counts := make(map[string]int, len(streams))
	for i := range streams {
		name := jobStreamName(streams[i])
		counts[name]++
		if counts[name] > 1 {
			name = fmt.Sprintf("%s-%d", name, counts[name])
		}
		names = append(names, name)
	}
	return names
}

func jobAllOutputs(outputs []destinationSpec, streamOutputs []destinationSpec) []destinationSpec {
	if len(streamOutputs) == 0 {
		return append([]destinationSpec(nil), outputs...)
	}
	all := make([]destinationSpec, 0, len(outputs)+len(streamOutputs))
	all = append(all, outputs...)
	all = append(all, streamOutputs...)
	return all
}

func jobAllOutputNames(outputNames []string, streamOutputNames []string) []string {
	if len(streamOutputNames) == 0 {
		return append([]string(nil), outputNames...)
	}
	all := make([]string, 0, len(outputNames)+len(streamOutputNames))
	all = append(all, outputNames...)
	all = append(all, streamOutputNames...)
	return all
}

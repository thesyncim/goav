// The From(...) job grammar: the Job builder, its stream and destination wiring, and Build/Run.

package goav

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// buildErrorDetail is one typed machine-readable fact attached to a
// BuildError. Callers read details through BuildError.Detail and
// BuildError.DetailLines so the root API does not expose implementation DTOs.
type buildErrorDetail struct {
	Key   string
	Value any
}

func (d buildErrorDetail) String() string {
	if d.Key == "" {
		return fmt.Sprint(d.Value)
	}
	return d.Key + "=" + fmt.Sprint(d.Value)
}

// buildErrorFix is one concrete way to repair a BuildError.
type buildErrorFix struct {
	Message string
}

func (f buildErrorFix) String() string {
	return f.Message
}

const (
	phaseBuild = "build"
	phaseOpen  = "open"
	phaseRun   = "run"
	phaseClose = "close"
)

// BuildError is the one structured refusal goav raises from build,
// validation, attach, and explain paths. Family identifies the stable
// application branch key, Code identifies the detailed diagnostic leaf (see the
// errcode package), Phase says when in the lifecycle it happened,
// Operation/Node say where, Reason says why, Detail exposes typed
// machine-readable facts, and DetailLines and FixLines expose rendered details
// and repair actions. Match build-shape refusals through Family and Code;
// Unwrap preserves low-level causes for errors.Is where a pipeline or runtime
// sentinel is still part of that lower-level contract.
type BuildError struct {
	Phase     string
	Family    errcode.Family
	Code      errcode.Code
	Operation string
	Node      string
	Reason    string
	fields    []buildErrorDetail
	fixes     []buildErrorFix
	cause     error
}

// Error renders the one goav error shape: "goav: cannot <operation> for
// <node>: <reason>" followed by Details and Suggestions lines.
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
	details := e.detailLines()
	if len(details) != 0 {
		out.WriteString("\nDetails:")
		for i := range details {
			out.WriteString("\n  - ")
			out.WriteString(details[i])
		}
	}
	suggestions := e.suggestionLines()
	if len(suggestions) != 0 {
		out.WriteString("\nSuggestions:")
		for i := range suggestions {
			out.WriteString("\n  - ")
			out.WriteString(suggestions[i])
		}
	}
	return out.String()
}

// Detail returns the typed value for key when the BuildError carries it.
func (e *BuildError) Detail(key string) (any, bool) {
	if e == nil || key == "" {
		return nil, false
	}
	for i := range e.fields {
		if e.fields[i].Key == key {
			return e.fields[i].Value, true
		}
	}
	return nil, false
}

// DetailLines returns the human-readable detail lines rendered in Error.
func (e *BuildError) DetailLines() []string {
	return append([]string(nil), e.detailLines()...)
}

// FixLines returns the human-readable fix lines rendered in Error.
func (e *BuildError) FixLines() []string {
	return append([]string(nil), e.suggestionLines()...)
}

// Unwrap exposes the underlying cause to errors.Is.
func (e *BuildError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// MarshalJSON renders the BuildError as a stable machine-readable document:
// phase, family, code, operation, node, reason, typed key/value details, and
// concrete fixes. It is the JSON contract consumed by tooling and pinned by the
// golden error tests; Detail values keep their real type.
func (e *BuildError) MarshalJSON() ([]byte, error) {
	if e == nil {
		return []byte("null"), nil
	}
	type detailJSON struct {
		Key   string `json:"key,omitempty"`
		Value any    `json:"value,omitempty"`
	}
	doc := struct {
		Phase     string         `json:"phase,omitempty"`
		Family    errcode.Family `json:"family,omitempty"`
		Code      errcode.Code   `json:"code,omitempty"`
		Operation string         `json:"operation,omitempty"`
		Node      string         `json:"node,omitempty"`
		Reason    string         `json:"reason,omitempty"`
		Details   []detailJSON   `json:"details,omitempty"`
		Fixes     []string       `json:"fixes,omitempty"`
	}{
		Phase:     e.Phase,
		Family:    e.Family,
		Code:      e.Code,
		Operation: e.Operation,
		Node:      e.Node,
		Reason:    e.Reason,
		Fixes:     e.suggestionLines(),
	}
	for i := range e.fields {
		doc.Details = append(doc.Details, detailJSON{Key: e.fields[i].Key, Value: e.fields[i].Value})
	}
	return json.Marshal(doc)
}

// EffectivePhase returns the BuildError's lifecycle phase. Every BuildError now
// sets Phase explicitly (pinned by TestEveryBuildErrorHasPhaseFamilyCodeReason),
// so this is a nil-safe reader of that stated fact — no inference from the
// operation string.
func (e *BuildError) EffectivePhase() string {
	if e == nil {
		return ""
	}
	return e.Phase
}

func (e *BuildError) detailLines() []string {
	if e == nil {
		return nil
	}
	return detailsToLines(e.fields)
}

func (e *BuildError) suggestionLines() []string {
	if e == nil {
		return nil
	}
	if len(e.fixes) == 0 {
		return nil
	}
	out := make([]string, 0, len(e.fixes))
	for i := range e.fixes {
		if e.fixes[i].Message == "" {
			continue
		}
		out = append(out, e.fixes[i].String())
	}
	return out
}

// errDetail builds one typed machine-readable BuildError detail from an explicit
// key and value. It replaces splitting a "key=value" string at construction:
// the value keeps its real type (int, string, ...) and is read back through
// BuildError.Detail(key).
func errDetail(key string, value any) buildErrorDetail {
	return buildErrorDetail{Key: key, Value: value}
}

// errNote builds one keyless detail: a machine-readable note with no key.
func errNote(message string) buildErrorDetail {
	return buildErrorDetail{Value: message}
}

// errDetails collects typed details, dropping empties. It is the typed builder
// that replaced buildErrorFields([]string{"key=value"}).
func errDetails(details ...buildErrorDetail) []buildErrorDetail {
	if len(details) == 0 {
		return nil
	}
	out := make([]buildErrorDetail, 0, len(details))
	for _, d := range details {
		if d.Key == "" && (d.Value == nil || d.Value == "") {
			continue
		}
		out = append(out, d)
	}
	return out
}

// errDetailLines converts a dynamically-built slice of "key=value" (or plain)
// lines into typed details, splitting each at the first '='. It is the runtime
// path for call sites whose detail keys are not known statically; literal detail
// lists use errDetail/errNote instead.
func errDetailLines(lines []string) []buildErrorDetail {
	if len(lines) == 0 {
		return nil
	}
	out := make([]buildErrorDetail, 0, len(lines))
	for i := range lines {
		if lines[i] == "" {
			continue
		}
		key, value, ok := strings.Cut(lines[i], "=")
		if !ok || key == "" {
			out = append(out, buildErrorDetail{Value: lines[i]})
			continue
		}
		out = append(out, buildErrorDetail{Key: key, Value: value})
	}
	return out
}

func buildErrorFixes(lines []string) []buildErrorFix {
	if len(lines) == 0 {
		return nil
	}
	out := make([]buildErrorFix, 0, len(lines))
	for i := range lines {
		if lines[i] == "" {
			continue
		}
		out = append(out, buildErrorFix{Message: lines[i]})
	}
	return out
}

func detailsToLines(details []buildErrorDetail) []string {
	if len(details) == 0 {
		return nil
	}
	out := make([]string, 0, len(details))
	for i := range details {
		if details[i].Key == "" && details[i].Value == nil {
			continue
		}
		out = append(out, details[i].String())
	}
	return out
}

// Job is a recipe under construction: the inputs, stream chains, branches,
// joins, and destinations declared so far. Construct one with From; the zero
// value is intentionally not a valid recipe. A Job is inert until Describe,
// Explain, Build, or Run compiles it; construction errors are deferred and
// surface on those calls as structured BuildErrors.
type Job struct {
	origin             jobOrigin
	name               string
	runtime            *Runtime
	runtimeSet         bool
	copy               bool
	inputs             []InputSpec
	outputs            []destinationSpec
	outputNames        []string
	syncOperations     []operationSpec
	streams            []*jobStreamBuild
	branchStreams      []streamBuild
	branchDestinations []namedDestinationSpec
	streamRules        []streamRule
	join               *joinSpec
	err                error
	errs               []error
}

type jobOrigin uint8

const (
	jobOriginZero jobOrigin = iota
	jobOriginConstructed
)

type jobStreamBuild struct {
	name        string
	selector    av.StreamSelector
	input       string
	operations  []operationSpec
	codecChange codecChangePolicy
	outputs     []destinationSpec
	outputNames []string
}

type chainStep struct {
	stage     pipeline.Stage
	shape     shape.Spec
	transform transformSpec
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

// Copy declares packet-preserving passthrough for a whole-job shortcut such
// as From(input).Copy().To(out): packets flow to the destinations without
// decode. On a selected stream, use the stream chain's Copy instead.
func (j *Job) Copy() *Job {
	if j != nil {
		j.copy = true
	}
	return j
}

// Sync places the job's subsequent stream chains on the given media timeline.
// If a chain is currently under construction, the sync gate is appended there;
// otherwise the policy is applied to stream chains started after this call.
func (j *Job) Sync(policy flow.SyncPolicy) *Job {
	if j == nil {
		return j
	}
	operation := operationSpecForSync(policy)
	if stream := j.currentStream(); stream != nil && len(stream.outputs) == 0 {
		stream.operations = append(stream.operations, operation)
		return j
	}
	j.syncOperations = append(j.syncOperations, operation)
	return j
}

func newJob(name string) *Job {
	return &Job{origin: jobOriginConstructed, name: name}
}

// UseRuntime compiles the job against the given runtime: the seam for custom
// registries, bundled adapter sets, offline pacing, or injected clocks.
func (j *Job) UseRuntime(runtime *Runtime) *Job {
	if j != nil {
		j.runtime = runtime
		j.runtimeSet = true
	}
	return j
}

func (j *Job) runtimeOrNil() *Runtime {
	if j == nil {
		return nil
	}
	if j.runtimeSet {
		return j.runtime
	}
	return nil
}

func (j *Job) setErr(err error) {
	if err == nil {
		return
	}
	if j.err == nil {
		j.err = err
	}
	j.errs = append(j.errs, err)
}

func (j *Job) recipeErr() error {
	if j == nil {
		return nil
	}
	if len(j.errs) == 0 {
		return j.err
	}
	return errors.Join(j.errs...)
}

// To routes the whole job to one or more destinations (a fanout when several
// are given). It applies to direct whole-job chains; once Branches are
// declared, destinations belong to the branches.
func (j *Job) To(destinations ...Destination) *Job {
	if len(j.branchStreams) != 0 || (j.join != nil && len(j.join.branches) != 0) {
		j.setErr(branchOutputScopeError("branches"))
		return j
	}
	for i := range destinations {
		output, err := destinationSpecFromDestination(destinations[i])
		if err != nil {
			j.setErr(jobDestinationInvalidError("job", err.Error()))
			return j
		}
		j.outputs = append(j.outputs, output)
		j.outputNames = append(j.outputNames, "")
	}
	return j
}

func (j *Job) addBranchDestinations(destinations ...destinationRef) error {
	updated, err := appendNamedBranchDestinations(j.branchDestinations, destinations...)
	if err != nil {
		return err
	}
	j.branchDestinations = updated
	return nil
}

func appendNamedBranchDestinations(existing []namedDestinationSpec, destinations ...destinationRef) ([]namedDestinationSpec, error) {
	out := append([]namedDestinationSpec(nil), existing...)
	seen := make(map[string]namedDestinationSpec, len(existing)+len(destinations))
	for i := range existing {
		seen[existing[i].name] = existing[i]
	}
	for i := range destinations {
		destination := cloneDestinationRef(destinations[i])
		destination.dest = destination.dest.withName(firstNonEmpty(destination.dest.name, destination.name))
		named := namedDestinationSpec{name: destination.name, output: destination.dest}
		if existing, ok := seen[named.name]; ok {
			if !destinationsShareExplicitGroup(existing, named) {
				return nil, branchDestinationDuplicateError(named.name)
			}
			continue
		}
		seen[named.name] = named
		out = append(out, named)
	}
	return out, nil
}

// And appends more inputs to the job, equivalent to listing them in From.
func (j *Job) And(inputs ...InputSpec) *Job {
	j.inputs = append(j.inputs, inputs...)
	return j
}

// Audio starts a chain on the job's audio stream. With several candidates the
// selector options (InputName, StreamID, StreamIndex) narrow the
// match; an ambiguous selection fails the build listing the candidates.
func (j *Job) Audio(options ...streamOption) *jobStreamBuilder {
	return j.streamBuilder("audio", av.MediaAudio, options...)
}

// Video starts a chain on the job's video stream. With several candidates the
// selector options (InputName, StreamID, StreamIndex) narrow the
// match; an ambiguous selection fails the build listing the candidates.
func (j *Job) Video(options ...streamOption) *jobStreamBuilder {
	return j.streamBuilder("video", av.MediaVideo, options...)
}

// Stream starts a chain on any media type — useful when the input carries one
// stream whose kind the recipe does not care about. Selector options narrow
// the match exactly as for Audio and Video.
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
	if len(j.syncOperations) != 0 {
		stream.operations = append(stream.operations, cloneOperationSpecs(j.syncOperations)...)
	}
	if last := j.currentStream(); last != nil {
		if len(j.branchStreams) != 0 {
			j.streams = []*jobStreamBuild{stream}
			return &jobStreamBuilder{job: j, stream: stream}
		}
		if len(last.outputs) == 0 {
			// A new chain may only start once the previous one is routed; an
			// unfinished chain followed by another selection is still an error.
			j.setErr(duplicateJobStreamError(last, stream))
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
			existing := namedDestinationSpec{name: existingLabel, output: stream.outputs[k]}
			next := namedDestinationSpec{name: label, output: output}
			if !destinationsShareExplicitGroup(existing, next) {
				return duplicateDestinationHandleError("build stream", label)
			}
		}
	}
	return nil
}

func (j *Job) plan() intent {
	intent := intent{Name: j.name, Copy: j.copy}
	if j.runtime != nil {
		intent.Policies.Realtime = j.runtime.realtime
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

// Describe compiles the job and returns the planned graph spec — node for
// node what Build will create — without opening any resource. Rendering
// lives outside core (see graphrender).
func (j *Job) Describe() (pipeline.Spec, error) {
	resolved, err := compileJobRecipe(j)
	if err != nil {
		return pipeline.Spec{}, err
	}
	return resolved.Describe()
}

// Build compiles and materializes the job into the narrow runnable Task
// lifecycle: sources resolve, the graph is wired, and OnStream rules install.
// The task does not flow until Run. Use BuildLive when the application needs
// inspection, events, live control, or late attachment.
func (j *Job) Build(ctx context.Context) (Task, error) {
	return j.BuildLive(ctx)
}

// BuildLive compiles and materializes the job into the full live task
// capability surface for inspection, events, live control, or late attachment.
// Prefer Build when the caller only needs Run and Close.
func (j *Job) BuildLive(ctx context.Context) (LiveTask, error) {
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

// Run is the one-shot shortcut: Build, Run to completion, then Close. It
// returns the first build refusal or runtime error; destinations commit on
// success and abort on failure. Finalize failures surface here too: if both
// the run and close/finalization fail, Run returns errors.Join(runErr,
// closeErr) so callers can match either cause.
func (j *Job) Run(ctx context.Context) error {
	task, err := j.Build(ctx)
	if err != nil {
		return err
	}
	runErr := task.Run(ctx)
	closeErr := task.Close()
	return errors.Join(runErr, closeErr)
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

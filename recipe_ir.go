package goav

import (
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

type recipeCompileSnapshot struct {
	recipe recipeir.Recipe

	runtime         *Runtime
	runtimeExplicit bool
	recipeErr       error

	jobPresent               bool
	branchCompositionPresent bool

	inputAttachments       []InputSpec
	jobOutputCount         int
	outputAttachments      []destinationSpec
	outputDestinationNames []string
	streamRules            []streamRule

	branchInputAttachment        InputSpec
	branchDestinationAttachments []namedDestinationSpec
	branchCompositionSplit       bool
	branchPlan                   branchComposePlan
	branchPlanErr                error

	joinAttachment *joinSpec
}

func newJobRecipeSnapshot(job *Job) recipeCompileSnapshot {
	switch {
	case job == nil:
		return recipeCompileSnapshot{recipe: recipeir.Recipe{Kind: recipeir.KindJob}}
	case job.join != nil:
		return newJoinRecipeSnapshot(job)
	case len(job.branchStreams) != 0:
		return newBranchJobRecipeSnapshot(job)
	default:
		return newLinearJobRecipeSnapshot(job)
	}
}

func newLinearJobRecipeSnapshot(job *Job) recipeCompileSnapshot {
	streamOutputs, _ := job.streamOutputsAndNames()
	outputs := jobAllOutputs(job.outputs, streamOutputs)
	outputNames := job.allOutputNames()
	recipe := recipeIRFromIntent(job.plan(), recipeir.KindJob)
	annotateRecipeIRInputsFromSpecs(&recipe, job.inputs)
	annotateRecipeIRDestinationsFromSpecs(&recipe, outputs)
	recipe.StreamRules = recipeIRStreamRulesFromRoot(job.streamRules)
	recipeErr := job.err
	if job.origin != jobOriginConstructed && recipeErr == nil {
		recipeErr = unconstructedJobError()
	}
	return recipeCompileSnapshot{
		recipe:                 recipe,
		runtime:                job.runtimeOrNil(),
		runtimeExplicit:        job.runtimeSet,
		recipeErr:              recipeErr,
		jobPresent:             true,
		inputAttachments:       append([]InputSpec(nil), job.inputs...),
		jobOutputCount:         len(job.outputs),
		outputAttachments:      outputs,
		outputDestinationNames: outputNames,
		streamRules:            cloneStreamRules(job.streamRules),
	}
}

func newJoinRecipeSnapshot(job *Job) recipeCompileSnapshot {
	spec := cloneJoinSpec(job.join)
	outputs, names := joinOutputAttachments(spec)
	inputs := joinArmInputs(spec)
	recipe := recipeIRFromIntent(joinIntentFromSpec(job, spec), recipeir.KindJoin)
	annotateRecipeIRInputsFromSpecs(&recipe, inputs)
	annotateRecipeIRDestinationsFromSpecs(&recipe, outputs)
	recipe.Join = recipeIRJoinFromSpec(spec, len(inputs))
	recipe.StreamRules = recipeIRStreamRulesFromRoot(job.streamRules)
	return recipeCompileSnapshot{
		recipe:                 recipe,
		runtime:                job.runtimeOrNil(),
		runtimeExplicit:        job.runtimeSet,
		recipeErr:              job.err,
		inputAttachments:       inputs,
		outputAttachments:      outputs,
		outputDestinationNames: names,
		streamRules:            cloneStreamRules(job.streamRules),
		joinAttachment:         spec,
	}
}

func newBranchJobRecipeSnapshot(job *Job) recipeCompileSnapshot {
	input := InputSpec{}
	recipeErr := job.err
	if len(job.inputs) == 1 {
		input = job.inputs[0]
	} else if recipeErr == nil {
		recipeErr = branchInputCountError("branches", len(job.inputs))
	}
	runtime := job.runtimeOrNil()
	destinations := cloneNamedDestinationSpecs(job.branchDestinations)
	recipe := recipeir.Recipe{
		Kind: recipeir.KindBranchComposition,
		Name: firstNonEmpty(job.name, "branch-composition"),
		Inputs: []recipeir.Input{
			recipeIRInputFromSpec(input),
		},
	}
	if runtime != nil {
		recipe.Policies.Realtime = runtime.realtime
	}
	for i := range job.branchStreams {
		recipe.Streams = append(recipe.Streams, recipeIRStreamFromIntent(branchStreamIntent(job.branchStreams[i])))
	}
	for i := range destinations {
		destination := recipeIRDestinationFromIntent(destinations[i].output.intentWithName(destinations[i].name))
		destination.Kind = recipeIRDestinationKindFromSpec(destinations[i].output)
		recipe.Destinations = append(recipe.Destinations, destination)
	}
	recipe.StreamRules = recipeIRStreamRulesFromRoot(job.streamRules)
	branchPlan, branchPlanErr := planBranchCompositionRecipe(recipe, input, destinations)
	return recipeCompileSnapshot{
		recipe:                       recipe,
		runtime:                      runtime,
		runtimeExplicit:              job.runtimeSet,
		recipeErr:                    recipeErr,
		branchCompositionPresent:     true,
		branchInputAttachment:        input,
		branchDestinationAttachments: destinations,
		branchCompositionSplit:       true,
		branchPlan:                   branchPlan,
		branchPlanErr:                branchPlanErr,
		streamRules:                  cloneStreamRules(job.streamRules),
	}
}

func recipeIRFromIntent(in intent, kind recipeir.Kind) recipeir.Recipe {
	out := recipeir.Recipe{
		Kind: kind,
		Name: in.Name,
		Policies: recipeir.Policies{
			Realtime: in.Policies.Realtime,
		},
		Copy: in.Copy,
	}
	for i := range in.Inputs {
		out.Inputs = append(out.Inputs, recipeIRInputFromIntent(in.Inputs[i]))
	}
	for i := range in.Streams {
		out.Streams = append(out.Streams, recipeIRStreamFromIntent(in.Streams[i]))
	}
	for i := range in.Destinations {
		out.Destinations = append(out.Destinations, recipeIRDestinationFromIntent(in.Destinations[i]))
	}
	return out
}

func intentFromRecipeIR(in recipeir.Recipe) intent {
	out := intent{
		Name: in.Name,
		Policies: policyIntent{
			Realtime: in.Policies.Realtime,
		},
		Copy: in.Copy,
	}
	for i := range in.Inputs {
		out.Inputs = append(out.Inputs, inputIntentFromRecipeIR(in.Inputs[i]))
	}
	for i := range in.Streams {
		out.Streams = append(out.Streams, streamIntentFromRecipeIR(in.Streams[i]))
	}
	for i := range in.Destinations {
		out.Destinations = append(out.Destinations, destinationIntentFromRecipeIR(in.Destinations[i]))
	}
	return out
}

func recipeIRInputFromIntent(in inputIntent) recipeir.Input {
	return recipeir.Input{
		Name:     in.Name,
		URI:      in.URI,
		Protocol: in.Protocol,
		MIMEType: in.MIMEType,
		Codec:    cloneCodecSpec(in.Codec),
		Realtime: in.Realtime,
	}
}

func recipeIRInputFromSpec(input InputSpec) recipeir.Input {
	out := recipeIRInputFromIntent(input.intent())
	out.Kind = recipeIRInputKindFromSpec(input)
	if sourceShape, ok := declaredSourceShape(input); ok {
		out.SourceShape = sourceShape
	}
	return out
}

func annotateRecipeIRInputsFromSpecs(recipe *recipeir.Recipe, inputs []InputSpec) {
	if recipe == nil {
		return
	}
	for i := range recipe.Inputs {
		if i >= len(inputs) {
			return
		}
		recipe.Inputs[i] = recipeIRInputFromSpec(inputs[i])
	}
}

func recipeIRInputsFromSpecs(inputs []InputSpec) []recipeir.Input {
	if len(inputs) == 0 {
		return nil
	}
	out := make([]recipeir.Input, 0, len(inputs))
	for i := range inputs {
		out = append(out, recipeIRInputFromSpec(inputs[i]))
	}
	return out
}

func recipeIRInputKindFromSpec(input InputSpec) recipeir.InputKind {
	switch {
	case input.source != nil:
		return recipeir.InputKindCustomSource
	case input.provider != nil:
		return recipeir.InputKindProvider
	case input.input.Name != "" || input.input.URI != "" || input.input.Protocol != "" || input.input.MIMEType != "" || input.input.Reader != nil || input.input.ReaderAt != nil:
		return recipeir.InputKindByteStream
	default:
		return recipeir.InputKindUnknown
	}
}

func recipeIRInputSourceShape(input recipeir.Input) (shape.Spec, bool) {
	switch input.Kind {
	case recipeir.InputKindCustomSource, recipeir.InputKindProvider:
	default:
		return shape.Spec{}, false
	}
	if input.SourceShape.Domain == "" {
		return shape.Spec{}, false
	}
	return normalizeCustomSourceShape(input.Name, input.SourceShape), true
}

func inputIntentFromRecipeIR(in recipeir.Input) inputIntent {
	return inputIntent{
		Name:     in.Name,
		URI:      in.URI,
		Protocol: in.Protocol,
		MIMEType: in.MIMEType,
		Codec:    cloneCodecSpec(in.Codec),
		Realtime: in.Realtime,
	}
}

func inputIntentsFromRecipeIR(inputs []recipeir.Input) []inputIntent {
	if len(inputs) == 0 {
		return nil
	}
	out := make([]inputIntent, 0, len(inputs))
	for i := range inputs {
		out = append(out, inputIntentFromRecipeIR(inputs[i]))
	}
	return out
}

func recipeIRStreamFromIntent(in streamIntent) recipeir.Stream {
	out := recipeir.Stream{
		Name:        in.Name,
		Selector:    in.Select,
		From:        recipeIRTapRefFromRoot(in.From),
		CodecChange: recipeIRCodecChangeFromRoot(in.CodecChange),
	}
	for i := range in.Operations {
		out.Operations = append(out.Operations, recipeIROperationFromSpec(in.Operations[i]))
	}
	for i := range in.Destinations {
		out.Outputs = append(out.Outputs, recipeir.OutputRef(in.Destinations[i]))
	}
	return out
}

func streamIntentFromRecipeIR(in recipeir.Stream) streamIntent {
	out := streamIntent{
		Name:        in.Name,
		Select:      in.Selector,
		From:        rootTapRefFromRecipeIR(in.From),
		CodecChange: rootCodecChangeFromRecipeIR(in.CodecChange),
	}
	for i := range in.Operations {
		out.Operations = append(out.Operations, operationSpecFromRecipeIR(in.Operations[i]))
	}
	for i := range in.Outputs {
		out.Destinations = append(out.Destinations, string(in.Outputs[i]))
	}
	return out
}

func recipeIRDestinationFromIntent(in destinationIntent) recipeir.Destination {
	return recipeir.Destination{
		Name:     in.Name,
		URI:      in.URI,
		Protocol: in.Protocol,
		MIMEType: in.MIMEType,
		Format:   in.Format,
	}
}

func annotateRecipeIRDestinationsFromSpecs(recipe *recipeir.Recipe, destinations []destinationSpec) {
	if recipe == nil {
		return
	}
	for i := range recipe.Destinations {
		if i >= len(destinations) {
			return
		}
		recipe.Destinations[i].Kind = recipeIRDestinationKindFromSpec(destinations[i])
	}
}

func recipeIRDestinationKindFromSpec(destination destinationSpec) recipeir.DestinationKind {
	switch {
	case destination.sink != nil:
		return recipeir.DestinationKindSink
	case destinationSpecHasOutput(destination), destination.custom != nil:
		return recipeir.DestinationKindByteStream
	default:
		return recipeir.DestinationKindUnknown
	}
}

func recipeIRStreamRulesFromRoot(rules []streamRule) []recipeir.StreamRule {
	if len(rules) == 0 {
		return nil
	}
	out := make([]recipeir.StreamRule, 0, len(rules))
	for i := range rules {
		out = append(out, recipeIRStreamRuleFromRoot(rules[i]))
	}
	return out
}

func recipeIRStreamRuleFromRoot(rule streamRule) recipeir.StreamRule {
	out := recipeir.StreamRule{
		MatchDescription: rule.match.Description(),
	}
	for i := range rule.branches {
		operations := make([]recipeir.Operation, 0, len(rule.branches[i].operations))
		for j := range rule.branches[i].operations {
			operations = append(operations, recipeIROperationFromSpec(rule.branches[i].operations[j]))
		}
		out.Branches = append(out.Branches, recipeir.StreamRuleBranch{
			Name:         rule.branches[i].name,
			Media:        rule.branches[i].media,
			Operations:   operations,
			Destinations: branchDestinationNames(rule.branches[i].destinations),
			Buffer:       rule.branches[i].branchBuffer,
		})
	}
	return out
}

func recipeIRJoinFromSpec(spec *joinSpec, inputCount int) recipeir.Join {
	if spec == nil {
		return recipeir.Join{}
	}
	join := recipeir.Join{
		Kind:           string(spec.kind),
		ArmCount:       len(spec.arms),
		InputCount:     inputCount,
		BranchCount:    len(spec.branches),
		OperationCount: len(spec.operations),
		TapCount:       len(spec.taps),
		HasEncode:      spec.encode != nil,
		Custom:         spec.custom != nil,
	}
	if len(spec.branches) != 0 {
		named, _ := joinBranchNamedDestinations(spec.branches)
		join.DestinationCount = len(named)
	} else {
		join.DestinationCount = len(spec.dests)
	}
	return join
}

func destinationIntentFromRecipeIR(in recipeir.Destination) destinationIntent {
	return destinationIntent{
		Name:     in.Name,
		URI:      in.URI,
		Protocol: in.Protocol,
		MIMEType: in.MIMEType,
		Format:   in.Format,
	}
}

func recipeIROperationFromSpec(in operationSpec) recipeir.Operation {
	out := recipeir.Operation{
		Kind:      in.Kind,
		Component: in.Component,
		Detail:    in.Detail,
		Stage:     in.Stage,
		Shape:     in.Shape,
		Transform: recipeIRTransformFromRoot(in.Transform),
		Tap:       recipeIRTapFromRoot(in.Tap),
		Decode:    cloneCodecSpec(in.Decode),
		Encode:    cloneCodecSpec(in.Encode),
		Shared:    in.Shared,
	}
	if in.Auto != nil {
		policy := *in.Auto
		out.Auto = &policy
	}
	if in.Require != nil {
		required := *in.Require
		out.Require = &required
	}
	if in.Prefer != nil {
		preferred := *in.Prefer
		out.Prefer = &preferred
	}
	return out
}

func recipeIROperationsFromSpecs(operations []operationSpec) []recipeir.Operation {
	if len(operations) == 0 {
		return nil
	}
	out := make([]recipeir.Operation, 0, len(operations))
	for i := range operations {
		out = append(out, recipeIROperationFromSpec(operations[i]))
	}
	return out
}

func operationSpecFromRecipeIR(in recipeir.Operation) operationSpec {
	out := operationSpec{
		Kind:      in.Kind,
		Component: in.Component,
		Detail:    in.Detail,
		Stage:     in.Stage,
		Shape:     in.Shape,
		Transform: rootTransformFromRecipeIR(in.Transform),
		Tap:       rootTapFromRecipeIR(in.Tap),
		Decode:    cloneCodecSpec(in.Decode),
		Encode:    cloneCodecSpec(in.Encode),
		Shared:    in.Shared,
	}
	if in.Auto != nil {
		policy := *in.Auto
		out.Auto = &policy
	}
	if in.Require != nil {
		required := *in.Require
		out.Require = &required
	}
	if in.Prefer != nil {
		preferred := *in.Prefer
		out.Prefer = &preferred
	}
	return out
}

func recipeIRTapFromRoot(in tapIntent) recipeir.Tap {
	return recipeir.Tap{
		Name:      in.Name,
		MediaKind: in.MediaKind,
		Domain:    in.Domain,
		After:     in.After,
	}
}

func rootTapFromRecipeIR(in recipeir.Tap) tapIntent {
	return tapIntent{
		Name:      in.Name,
		MediaKind: in.MediaKind,
		Domain:    in.Domain,
		After:     in.After,
	}
}

func recipeIRTapRefFromRoot(in tapRef) recipeir.TapRef {
	return recipeir.TapRef{Name: in.name, Domain: in.domain}
}

func rootTapRefFromRecipeIR(in recipeir.TapRef) tapRef {
	return tapRef{name: in.Name, domain: in.Domain}
}

func recipeIRRuntimeBranchSourceFromRoot(in branchSourceBinding) recipeir.RuntimeBranchSource {
	out := recipeir.RuntimeBranchSource{
		From:         in.from,
		Tap:          recipeIRTapRefFromRoot(tapRef{name: in.tap, domain: in.tapDomain}),
		Policy:       in.policy,
		Label:        in.label,
		StreamDomain: in.streamDomain,
	}
	if in.stream != nil {
		out.Stream = *in.stream
		out.StreamPresent = true
	}
	return out
}

func recipeIRCodecChangeFromRoot(in codecChangePolicy) recipeir.CodecChangePolicy {
	return recipeir.CodecChangePolicy{
		RebindCompatible:     in.RebindCompatible,
		RequestKeyframe:      in.RequestKeyframe,
		DropUntilSync:        in.DropUntilSync,
		FailOnDifferentCodec: in.FailOnDifferentCodec,
	}
}

func rootCodecChangeFromRecipeIR(in recipeir.CodecChangePolicy) codecChangePolicy {
	return codecChangePolicy{
		RebindCompatible:     in.RebindCompatible,
		RequestKeyframe:      in.RequestKeyframe,
		DropUntilSync:        in.DropUntilSync,
		FailOnDifferentCodec: in.FailOnDifferentCodec,
	}
}

func recipeIRTransformFromRoot(in transformSpec) recipeir.Transform {
	switch {
	case in.resize != nil:
		return recipeir.Transform{
			Kind:   recipeir.TransformResize,
			Resize: *in.resize,
		}
	case in.resample != nil:
		return recipeir.Transform{
			Kind:     recipeir.TransformResample,
			Resample: *in.resample,
		}
	default:
		return recipeir.Transform{}
	}
}

func rootTransformFromRecipeIR(in recipeir.Transform) transformSpec {
	switch in.Kind {
	case recipeir.TransformResize:
		config := in.Resize
		return transformSpec{resize: &config}
	case recipeir.TransformResample:
		config := in.Resample
		return transformSpec{resample: &config}
	default:
		return transformSpec{}
	}
}

func cloneNamedDestinationSpecs(destinations []namedDestinationSpec) []namedDestinationSpec {
	if len(destinations) == 0 {
		return nil
	}
	out := make([]namedDestinationSpec, 0, len(destinations))
	for i := range destinations {
		out = append(out, namedDestinationSpec{
			name:   destinations[i].name,
			output: cloneDestinationSpec(destinations[i].output),
		})
	}
	return out
}

func cloneDestinationSpecs(destinations []destinationSpec) []destinationSpec {
	if len(destinations) == 0 {
		return nil
	}
	out := make([]destinationSpec, 0, len(destinations))
	for i := range destinations {
		out = append(out, cloneDestinationSpec(destinations[i]))
	}
	return out
}

func cloneJoinSpec(spec *joinSpec) *joinSpec {
	if spec == nil {
		return nil
	}
	out := *spec
	out.arms = append([]joinArm(nil), spec.arms...)
	out.dests = append([]Destination(nil), spec.dests...)
	if spec.encode != nil {
		encode := cloneCodecSpec(*spec.encode)
		out.encode = &encode
	}
	out.operations = cloneOperationSpecs(spec.operations)
	out.taps = append([]tapRef(nil), spec.taps...)
	out.branches = cloneBranchSpecs(spec.branches)
	return &out
}

func joinIntentFromSpec(job *Job, spec *joinSpec) intent {
	in := intent{Name: string(spec.kind)}
	if job.runtime != nil {
		in.Policies.Realtime = job.runtime.realtime
	}
	for _, input := range joinLeafInputSpecs(spec) {
		in.Inputs = append(in.Inputs, input.intent())
	}
	if len(spec.branches) != 0 {
		named, _ := joinBranchNamedDestinations(spec.branches)
		for i := range named {
			in.Destinations = append(in.Destinations, named[i].output.intentWithName(named[i].name))
		}
		return in
	}
	for i := range spec.dests {
		in.Destinations = append(in.Destinations, spec.dests[i].spec.intentWithName(""))
	}
	return in
}

func recipeCompileStateFromSnapshot(snapshot recipeCompileSnapshot, options recipeCompileOptions) recipeCompileState {
	return recipeCompileState{
		operation:                    recipeCompileOperation(snapshot),
		recipe:                       cloneRecipeIRRecipe(snapshot.recipe),
		intent:                       intentFromRecipeIR(snapshot.recipe),
		inputFacts:                   cloneRecipeIRInputs(snapshot.recipe.Inputs),
		destinationKinds:             recipeIRDestinationKinds(snapshot.recipe),
		joinFacts:                    snapshot.recipe.Join,
		streamRuleFacts:              cloneRecipeIRStreamRules(snapshot.recipe.StreamRules),
		runtime:                      snapshot.runtime,
		runtimeExplicit:              snapshot.runtimeExplicit,
		options:                      options,
		jobPresent:                   snapshot.jobPresent,
		branchCompositionPresent:     snapshot.branchCompositionPresent,
		recipeErr:                    snapshot.recipeErr,
		inputAttachments:             append([]InputSpec(nil), snapshot.inputAttachments...),
		jobOutputCount:               snapshot.jobOutputCount,
		outputAttachments:            cloneDestinationSpecs(snapshot.outputAttachments),
		outputDestinationNames:       append([]string(nil), snapshot.outputDestinationNames...),
		streamRules:                  cloneStreamRules(snapshot.streamRules),
		branchInputAttachment:        snapshot.branchInputAttachment,
		branchDestinationAttachments: cloneNamedDestinationSpecs(snapshot.branchDestinationAttachments),
		branchInputProbeReady:        false,
		branchCompositionSplit:       snapshot.branchCompositionSplit,
		joinAttachment:               cloneJoinSpec(snapshot.joinAttachment),
		plan:                         snapshot.branchPlan,
		planErr:                      snapshot.branchPlanErr,
	}
}

func cloneRecipeIRRecipe(recipe recipeir.Recipe) recipeir.Recipe {
	out := recipe
	out.Inputs = cloneRecipeIRInputs(recipe.Inputs)
	out.Streams = cloneRecipeIRStreams(recipe.Streams)
	out.Destinations = append([]recipeir.Destination(nil), recipe.Destinations...)
	out.StreamRules = cloneRecipeIRStreamRules(recipe.StreamRules)
	return out
}

func cloneRecipeIRInputs(inputs []recipeir.Input) []recipeir.Input {
	if len(inputs) == 0 {
		return nil
	}
	out := make([]recipeir.Input, len(inputs))
	copy(out, inputs)
	return out
}

func cloneRecipeIRStreams(streams []recipeir.Stream) []recipeir.Stream {
	if len(streams) == 0 {
		return nil
	}
	out := make([]recipeir.Stream, len(streams))
	for i := range streams {
		out[i] = streams[i]
		out[i].Operations = cloneRecipeIROperations(streams[i].Operations)
		out[i].Outputs = append([]recipeir.OutputRef(nil), streams[i].Outputs...)
	}
	return out
}

func cloneRecipeIRStreamRules(rules []recipeir.StreamRule) []recipeir.StreamRule {
	if len(rules) == 0 {
		return nil
	}
	out := make([]recipeir.StreamRule, len(rules))
	for i := range rules {
		out[i] = recipeir.StreamRule{
			MatchDescription: rules[i].MatchDescription,
			Branches:         append([]recipeir.StreamRuleBranch(nil), rules[i].Branches...),
		}
		for j := range out[i].Branches {
			out[i].Branches[j].Operations = cloneRecipeIROperations(rules[i].Branches[j].Operations)
			out[i].Branches[j].Destinations = append([]string(nil), rules[i].Branches[j].Destinations...)
		}
	}
	return out
}

func cloneRecipeIROperations(operations []recipeir.Operation) []recipeir.Operation {
	if len(operations) == 0 {
		return nil
	}
	out := make([]recipeir.Operation, len(operations))
	for i := range operations {
		out[i] = recipeIROperationFromSpec(operationSpecFromRecipeIR(operations[i]))
	}
	return out
}

func recipeIRDestinationKinds(recipe recipeir.Recipe) []recipeir.DestinationKind {
	if len(recipe.Destinations) == 0 {
		return nil
	}
	kinds := make([]recipeir.DestinationKind, len(recipe.Destinations))
	for i := range recipe.Destinations {
		kinds[i] = recipe.Destinations[i].Kind
	}
	return kinds
}

func recipeCompileOperation(snapshot recipeCompileSnapshot) string {
	switch snapshot.recipe.Kind {
	case recipeir.KindJoin:
		return "build " + snapshot.recipe.Name
	case recipeir.KindBranchComposition:
		return branchCompositionOperation
	default:
		return "build job"
	}
}

func recipeCompilePhasesForSnapshot(snapshot recipeCompileSnapshot) recipeCompilePhaseSet {
	switch snapshot.recipe.Kind {
	case recipeir.KindJoin:
		return joinRecipeCompilePhases()
	case recipeir.KindBranchComposition:
		return branchCompositionRecipeCompilePhases()
	default:
		return jobRecipeCompilePhases()
	}
}

func recipeIRHasOperationKind(recipe recipeir.Recipe, kind plan.OperationKind) bool {
	for i := range recipe.Streams {
		for j := range recipe.Streams[i].Operations {
			if recipe.Streams[i].Operations[j].Kind == kind {
				return true
			}
		}
	}
	return false
}

func recipeIRHasFrameTap(recipe recipeir.Recipe) bool {
	for i := range recipe.Streams {
		for j := range recipe.Streams[i].Operations {
			if recipe.Streams[i].Operations[j].Kind == plan.OpTap && recipe.Streams[i].Operations[j].Tap.Domain == shape.DomainFrame {
				return true
			}
		}
	}
	return false
}

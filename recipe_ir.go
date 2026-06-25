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
	return recipeCompileSnapshot{
		recipe:                 recipeIRFromIntent(job.plan(), recipeir.KindJob),
		runtime:                job.runtimeOrNil(),
		runtimeExplicit:        job.runtimeSet,
		recipeErr:              job.err,
		jobPresent:             true,
		inputAttachments:       append([]InputSpec(nil), job.inputs...),
		jobOutputCount:         len(job.outputs),
		outputAttachments:      jobAllOutputs(job.outputs, streamOutputs),
		outputDestinationNames: job.allOutputNames(),
		streamRules:            cloneStreamRules(job.streamRules),
	}
}

func newJoinRecipeSnapshot(job *Job) recipeCompileSnapshot {
	spec := cloneJoinSpec(job.join)
	outputs, names := joinOutputAttachments(spec)
	return recipeCompileSnapshot{
		recipe:                 recipeIRFromIntent(joinIntentFromSpec(job, spec), recipeir.KindJoin),
		runtime:                job.runtimeOrNil(),
		runtimeExplicit:        job.runtimeSet,
		recipeErr:              job.err,
		inputAttachments:       joinArmInputs(spec),
		outputAttachments:      outputs,
		outputDestinationNames: names,
		streamRules:            cloneStreamRules(job.streamRules),
		joinAttachment:         spec,
	}
}

func newBranchJobRecipeSnapshot(job *Job) recipeCompileSnapshot {
	branchJob := &branchCompositionJob{
		runtime:         job.runtimeOrNil(),
		runtimeExplicit: job.runtimeSet,
		name:            job.name,
		streams:         cloneStreamBuilds(job.branchStreams),
		outputs:         cloneNamedDestinationSpecs(job.branchDestinations),
		streamRules:     cloneStreamRules(job.streamRules),
		err:             job.err,
		fromBranchSplit: true,
	}
	if len(job.inputs) == 1 {
		branchJob.input = job.inputs[0]
	} else if branchJob.err == nil {
		branchJob.err = branchInputCountError("branches", len(job.inputs))
	}
	return newBranchCompositionRecipeSnapshot(branchJob)
}

func newBranchCompositionRecipeSnapshot(job *branchCompositionJob) recipeCompileSnapshot {
	if job == nil {
		return recipeCompileSnapshot{recipe: recipeir.Recipe{Kind: recipeir.KindBranchComposition}}
	}
	branchPlan, branchPlanErr := job.composePlan()
	return recipeCompileSnapshot{
		recipe:                       recipeIRFromIntent(job.plan(), recipeir.KindBranchComposition),
		runtime:                      job.runtime,
		runtimeExplicit:              job.runtimeExplicit,
		recipeErr:                    job.err,
		branchCompositionPresent:     true,
		branchInputAttachment:        job.input,
		branchDestinationAttachments: cloneNamedDestinationSpecs(job.outputs),
		branchCompositionSplit:       job.fromBranchSplit,
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
		Transform: cloneTransformSpec(in.Transform),
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

func recipeIRTapRefFromRoot(in TapRef) recipeir.TapRef {
	return recipeir.TapRef{Name: in.name, Domain: in.domain}
}

func rootTapRefFromRecipeIR(in recipeir.TapRef) TapRef {
	return TapRef{name: in.Name, domain: in.Domain}
}

func recipeIRCodecChangeFromRoot(in CodecChangePolicy) recipeir.CodecChangePolicy {
	return recipeir.CodecChangePolicy{
		RebindCompatible:     in.RebindCompatible,
		RequestKeyframe:      in.RequestKeyframe,
		DropUntilSync:        in.DropUntilSync,
		FailOnDifferentCodec: in.FailOnDifferentCodec,
	}
}

func rootCodecChangeFromRecipeIR(in recipeir.CodecChangePolicy) CodecChangePolicy {
	return CodecChangePolicy{
		RebindCompatible:     in.RebindCompatible,
		RequestKeyframe:      in.RequestKeyframe,
		DropUntilSync:        in.DropUntilSync,
		FailOnDifferentCodec: in.FailOnDifferentCodec,
	}
}

func rootTransformFromRecipeIR(in any) TransformSpec {
	switch transform := in.(type) {
	case TransformSpec:
		return cloneTransformSpec(transform)
	case *TransformSpec:
		if transform != nil {
			return cloneTransformSpec(*transform)
		}
	}
	return TransformSpec{}
}

func cloneStreamBuilds(streams []streamBuild) []streamBuild {
	if len(streams) == 0 {
		return nil
	}
	out := make([]streamBuild, 0, len(streams))
	for i := range streams {
		stream := streams[i]
		stream.operations = cloneOperationSpecs(stream.operations)
		stream.sharedOps = cloneOperationSpecs(stream.sharedOps)
		stream.privateOps = cloneOperationSpecs(stream.privateOps)
		stream.destinationNames = append([]string(nil), stream.destinationNames...)
		out = append(out, stream)
	}
	return out
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
	out.arms = append([]JoinArm(nil), spec.arms...)
	out.dests = append([]Destination(nil), spec.dests...)
	if spec.encode != nil {
		encode := cloneCodecSpec(*spec.encode)
		out.encode = &encode
	}
	out.operations = cloneOperationSpecs(spec.operations)
	out.taps = append([]TapRef(nil), spec.taps...)
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
		named, _ := joinBranchNamedDestinations(string(spec.kind), spec.branches)
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
		intent:                       intentFromRecipeIR(snapshot.recipe),
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

func recipeIRRoundTripIntent(in intent, kind recipeir.Kind) intent {
	return intentFromRecipeIR(recipeIRFromIntent(in, kind))
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

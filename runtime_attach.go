package goav

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

var runtimeAttachmentSeq atomic.Uint64

// contextMutex is a control-plane mutex whose acquisition can honor a caller's
// deadline. Ordinary attachment operations retain the sync.Locker interface.
type contextMutex struct {
	once sync.Once
	gate chan struct{}
}

func (m *contextMutex) LockContext(ctx context.Context) error {
	m.once.Do(func() { m.gate = make(chan struct{}, 1) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *contextMutex) Lock()   { _ = m.LockContext(context.Background()) }
func (m *contextMutex) Unlock() { <-m.gate }

// Register before any node becomes graph-owned, so a racing Close cannot
// finalize a partially installed attachment with an undecided disposition.
// Only transactions and stages are read through this lifecycle registry;
// those slices are immutable before publication.
func (t *task) beginAttachmentLifecycle(a *runtimeAttachment) bool {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	if t.closeStarted {
		return false
	}
	if t.activeAttachments == nil {
		t.activeAttachments = make(map[*runtimeAttachment]struct{})
	}
	t.activeAttachments[a] = struct{}{}
	if t.finished {
		for _, tx := range a.transactions {
			if t.runErr == nil {
				tx.Succeed()
			} else {
				tx.Fail()
			}
		}
	}
	return true
}

func (t *task) setAttachmentDisposition(a *runtimeAttachment, disposition oldBranchDisposition) {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	if disposition == oldBranchDrain {
		a.draining = true
		if t.closeStarted {
			disposition = oldBranchAbort
		}
	}
	a.applyDetachDisposition(disposition)
}

// An accepted drain remains owned until removal finishes. A racing whole-task
// Close must still abandon it if upstream work has not finished draining.
func (t *task) endAttachmentLifecycle(a *runtimeAttachment) {
	t.lifecycleMu.Lock()
	defer t.lifecycleMu.Unlock()
	a.draining = false
	delete(t.activeAttachments, a)
}

// attachDestination is one validated destination of an attaching branch: stable
// recipe facts plus the concrete handle kept for the mutation edge.
type attachDestination struct {
	recipe recipeir.RuntimeDestination
	dest   destinationSpec
	sink   pipeline.Sink
}

type runtimeBranchGroupDestinations struct {
	sharedSinkKeys map[string]struct{}
	sharedMuxKeys  map[string]struct{}
}

type runtimeBranchRecipe struct {
	branch recipeir.RuntimeBranch
}

type runtimeAttachBranchInput struct {
	recipe       runtimeBranchRecipe
	destinations []attachDestination
	failure      BranchFailurePolicy
}

type runtimeAttachInput struct {
	branches          []runtimeAttachBranchInput
	groupDestinations runtimeBranchGroupDestinations
	name              string
}

type runtimeRebranchInput struct {
	attach      runtimeAttachInput
	group       *switchGroup
	disposition oldBranchDisposition
}

// Attachment is a live runtime branch attached to a task.
type Attachment interface {
	// ID is the task-unique attachment identifier; Name is the branch name,
	// taken from each Output's .As or else its first destination (a name may
	// be reused across attachments).
	ID() string
	Name() string
	// Spec returns the branch's private graph patch as a structured spec, and
	// Stats its counters, over the same node set; once the branch detaches
	// both report its final nodes. Snapshot returns the branch-owned
	// point-in-time view (lifecycle state, spec, stats, published taps).
	Spec() pipeline.Spec
	Stats() pipeline.GraphStats
	Snapshot() BranchSnapshot
	// Pause stops delivery to this branch without touching the source or its
	// siblings; messages are skipped while paused. Resume restores delivery.
	Pause(context.Context) error
	Resume(context.Context) error
	// Rebranch replaces this branch with new ones in place: the replacements are
	// attached and start receiving before this branch is detached (no gap), so a
	// live subscriber can be switched (e.g. to a different simulcast layer)
	// without rebuilding the task. On attach failure this branch is left intact
	// by default. Pass replacement Outputs (built from task.Tap or task.Track,
	// like Attach) plus lifecycle policies:
	// SwitchAt(NextFrame()/NextKeyframe()) delays
	// the switch to that stream boundary — the replacements shed media until the
	// boundary and this branch detaches at it — and
	// DrainBranch/AbortBranch select whether this
	// branch's destinations commit or abort on detach.
	// Without options the switch is immediate, exactly like Detach after Attach.
	Rebranch(context.Context, ...RebranchArg) (Attachment, error)
	// Close detaches this branch and any dependent branches anchored on its
	// taps; equivalent to task.Detach.
	Close(context.Context) error
}

// attach lowers the branches below their anchors through the build's
// lowering, opens every planned node, and then applies the patch atomically
// to the running graph: a failed attach rolls every applied node back and
// leaves the task unchanged.
func (t *task) attach(ctx context.Context, specs ...attachBranch) (Attachment, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	input, err := runtimeAttachInputFromBranchSpecs(specs)
	if err != nil {
		return nil, err
	}
	return t.attachRuntimeBranches(ctx, input)
}

func runtimeAttachInputFromBranchSpecs(specs []attachBranch) (runtimeAttachInput, error) {
	if len(specs) == 0 {
		return runtimeAttachInput{}, runtimeBranchInvalidError("no runtime branches to attach", "pass one or more Outputs: task.Attach(ctx, task.Tap(name).To(destination))")
	}
	branches := make([]runtimeAttachBranchInput, 0, len(specs))
	for i := range specs {
		branchDestinations, err := attachBranchDestinations(specs[i])
		if err != nil {
			return runtimeAttachInput{}, err
		}
		if err := validateAttachBranchSpec(specs[i], branchDestinations); err != nil {
			return runtimeAttachInput{}, err
		}
		branches = append(branches, runtimeAttachBranchInput{
			recipe:       runtimeBranchRecipeFromBranchSpec(specs[i]),
			destinations: branchDestinations,
			failure:      specs[i].failure,
		})
	}
	return runtimeAttachInputFromBranchInputs(branches, runtimeAttachmentName(specs))
}

func runtimeAttachInputFromBranchInputs(branches []runtimeAttachBranchInput, name string) (runtimeAttachInput, error) {
	groupDestinations, err := validateRuntimeBranchGroupDestinations(branches)
	if err != nil {
		return runtimeAttachInput{}, err
	}
	return runtimeAttachInput{
		branches:          branches,
		groupDestinations: groupDestinations,
		name:              firstNonEmpty(name, runtimeAttachmentNameFromBranchInputs(branches)),
	}, nil
}

func runtimeBranchRecipeFromBranchSpec(spec attachBranch) runtimeBranchRecipe {
	return runtimeBranchRecipe{
		branch: recipeir.RuntimeBranch{
			Name:       spec.name,
			Media:      spec.media,
			Source:     runtimeBranchSourceOf(spec.source),
			Operations: append([]recipeir.Operation(nil), spec.operations...),
			Buffer:     spec.buffer,
		},
	}
}

// runtimeBranchSourceOf is the IR of an attach branch's anchor.
func runtimeBranchSourceOf(in branchSourceBinding) recipeir.RuntimeBranchSource {
	out := recipeir.RuntimeBranchSource{
		From:         in.from,
		Tap:          recipeir.TapRef{Name: in.tap, Domain: in.tapDomain},
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

func runtimeRebranchInputFromArgs(ctx context.Context, args []RebranchArg) (runtimeRebranchInput, error) {
	policy := rebranchPolicyFromOptions(args)
	if len(policy.specs) == 0 {
		return runtimeRebranchInput{}, runtimeBranchInvalidError("rebranch runtime branch", "pass one or more replacement Outputs built from task.Tap(name) or task.Track(input, stream)")
	}
	if policy.invalid != "" {
		return runtimeRebranchInput{}, runtimeBranchInvalidError(policy.invalid, "pass SwitchAt(AtMediaTime(position)) with position >= 0")
	}
	if err := ctx.Err(); err != nil {
		return runtimeRebranchInput{}, err
	}
	specs := policy.specs
	var group *switchGroup
	if policy.boundary != switchImmediate {
		group = newSwitchGroup(policy.boundary, policy.mediaTime)
		specs = gatedBranchSpecs(specs, group)
	}
	attach, err := runtimeAttachInputFromBranchSpecs(specs)
	if err != nil {
		return runtimeRebranchInput{}, err
	}
	return runtimeRebranchInput{
		attach:      attach,
		group:       group,
		disposition: policy.disposition,
	}, nil
}

// attachBranchDestinations validates and clones the branch's destinations,
// carrying the share keys that group explicit Mux destination groups.
func attachBranchDestinations(spec attachBranch) ([]attachDestination, error) {
	if len(spec.dests) == 0 {
		return nil, runtimeBranchInvalidError("branch destination is missing", "finish the branch with .To(goav.Sink(sink)) or .To(goav.Write(name, writer))")
	}
	destinations := make([]attachDestination, 0, len(spec.dests))
	for i := range spec.dests {
		ref := cloneDestinationRef(spec.dests[i])
		destination := cloneDestinationSpec(ref.dest)
		name := firstNonEmpty(ref.name, spec.name, "branch")
		if err := destination.validate("attach runtime branch", name); err != nil {
			return nil, err
		}
		destinations = append(destinations, attachDestination{
			recipe: runtimeDestinationFromSpec(name, destination, destinationShareKey(ref.dest)),
			dest:   destination,
			sink:   destination.sink,
		})
	}
	return destinations, nil
}

func runtimeDestinationFromSpec(name string, destination destinationSpec, shareKey string) recipeir.RuntimeDestination {
	return recipeir.RuntimeDestination{
		Name:     name,
		Kind:     recipeIRDestinationKindFromSpec(destination),
		Format:   destinationOpenFormat(destination),
		ShareKey: shareKey,
	}
}

func validateAttachBranchSpec(spec attachBranch, destinations []attachDestination) error {
	if spec.name == "" {
		return runtimeBranchInvalidError("branch name is empty", "name the Output with .As(\"name\")")
	}
	if spec.source.from == "" && spec.source.tap == "" {
		return runtimeBranchInvalidError("branch source is empty", "start the Output from task.Track(input, stream) for an input stream, task.Tap(name) with a tap from task.Taps(), or task.Route(node.Out().Route()) with an expert graph handle")
	}
	if spec.source.stream != nil && spec.source.label == "" {
		return runtimeBranchInvalidError("branch source stream id is empty", "set av.Stream.ID before passing the stream to task.Track(input, stream)")
	}
	seen := make(map[string]int, len(destinations))
	for i := range destinations {
		name := firstNonEmpty(destinations[i].recipe.Name, destinations[i].dest.label(fmt.Sprintf("target%d", i+1)))
		if firstIndex, ok := seen[name]; ok {
			return duplicateRuntimeBranchDestinationRefError(spec.name, name, firstIndex, i)
		}
		seen[name] = i
	}
	return nil
}

func validateRuntimeBranchGroupDestinations(branches []runtimeAttachBranchInput) (runtimeBranchGroupDestinations, error) {
	var group runtimeBranchGroupDestinations
	seen := make(map[string]attachDestination)
	seenBranch := make(map[string]string)
	for i := range branches {
		recipe := branches[i].recipe
		branchName := firstNonEmpty(recipe.branch.Name, fmt.Sprintf("branch-%d", i+1))
		for j := range branches[i].destinations {
			destination := branches[i].destinations[j]
			label := destination.recipe.Name
			if label == "" {
				continue
			}
			if first, ok := seen[label]; ok {
				if first.recipe.ShareKey != "" && first.recipe.ShareKey == destination.recipe.ShareKey {
					switch {
					case runtimeBranchDestinationCanShareSink(first) && runtimeBranchDestinationCanShareSink(destination):
						if group.sharedSinkKeys == nil {
							group.sharedSinkKeys = make(map[string]struct{})
						}
						group.sharedSinkKeys[first.recipe.ShareKey] = struct{}{}
						continue
					case runtimeBranchDestinationCanShareMux(first) && runtimeBranchDestinationCanShareMux(destination):
						if group.sharedMuxKeys == nil {
							group.sharedMuxKeys = make(map[string]struct{})
						}
						group.sharedMuxKeys[first.recipe.ShareKey] = struct{}{}
						continue
					}
				}
				return group, &BuildError{
					Phase:     phaseRun,
					Family:    errcode.FamilyForCode(destinationDuplicateCode),
					Code:      destinationDuplicateCode,
					Operation: "attach runtime branches",
					Node:      firstNonEmpty(recipe.branch.Name, "branch"),
					Reason:    "runtime branch group reuses one destination name",
					fields:    errDetails(errNote("destination: "+label), errNote("first branch: "+seenBranch[label]), errNote("second branch: "+branchName)),
					fixes: buildErrorFixes([]string{
						"pass the same destination value to each branch in one Attach call to share it",
						"create distinct destination values with distinct names for independent runtime destinations",
						"use a sink destination for runtime diagnostic groups or a mux destination for runtime recording groups",
					}),
					cause: errUnsupportedBuild,
				}
			}
			seen[label] = destination
			seenBranch[label] = branchName
		}
	}
	return group, nil
}

func runtimeBranchDestinationCanShareSink(destination attachDestination) bool {
	return destination.recipe.Kind == recipeir.DestinationKindSink
}

func runtimeBranchDestinationCanShareMux(destination attachDestination) bool {
	return destination.recipe.Kind == recipeir.DestinationKindByteStream
}

func runtimeMuxDestinationFormat(ctx context.Context, rt *Runtime, dest destinationSpec, index int) (av.FormatID, error) {
	formatID := destinationOpenFormat(dest)
	if formatID != "" {
		return formatID, nil
	}
	if rt == nil {
		return "", runtimeBranchInvalidError(
			"runtime branch mux destinations require the bundled runtime",
			"build tasks with bundle.MustNew(...) from github.com/thesyncim/goav/bundle before attaching file or URI branches",
		)
	}
	result, err := rt.formats.Probe(ctx, outputProbeRequest(dest.output))
	if err != nil {
		return "", outputFormatProbeError(dest.output, index, err)
	}
	return result.Format, nil
}

func runtimeMuxCompatibilityIssue(destinationName string, formatID av.FormatID, branches []string, streams []av.Stream, rt *Runtime) (compile.MuxCompatibilityIssue, bool) {
	output := compile.WorkDestination{
		Name:      destinationName,
		Operation: plan.OpMux,
		Format:    formatID,
		Branches:  append([]string(nil), branches...),
	}
	plannedStreams := make([]compile.PlannedMuxStream, 0, len(streams))
	for i := range streams {
		branch := "branch"
		if i < len(branches) && branches[i] != "" {
			branch = branches[i]
		}
		stream := streams[i]
		plannedStreams = append(plannedStreams, compile.PlannedMuxStream{
			Branch:   branch,
			Codec:    stream.Codec.ID,
			Media:    compile.FirstNonEmptyMedia(stream.Type, stream.Codec.Type, compile.CodecMedia(stream.Codec.ID)),
			TimeBase: compile.MuxStreamTimeBase(stream),
		})
	}
	return compile.CheckKnownMuxCompatibility(output, plannedStreams, rt.compileEnv())
}

func runtimeAttachmentName(specs []attachBranch) string {
	if len(specs) == 1 {
		return specs[0].name
	}
	names := make([]string, 0, len(specs))
	for i := range specs {
		if specs[i].name != "" {
			names = append(names, specs[i].name)
		}
	}
	if len(names) == 0 {
		return "branches"
	}
	return strings.Join(names, "+")
}

func runtimeAttachmentNameFromBranchInputs(branches []runtimeAttachBranchInput) string {
	if len(branches) == 1 {
		return branches[0].recipe.branch.Name
	}
	names := make([]string, 0, len(branches))
	for i := range branches {
		if branches[i].recipe.branch.Name != "" {
			names = append(names, branches[i].recipe.branch.Name)
		}
	}
	if len(names) == 0 {
		return "branches"
	}
	return strings.Join(names, "+")
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for i := range values {
		value := values[i]
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

// discoveredStreamAnchorTap synthesizes the anchor tap for a source+stream
// anchor (a branch attached to a stream the source discovered at runtime):
// the source node is the anchor and the announced av.Stream supplies the
// shape facts a tap would normally carry.
func discoveredStreamAnchorTap(source recipeir.RuntimeBranchSource) TapSnapshot {
	domain := source.StreamDomain
	if domain == "" {
		domain = shape.DomainPacket
	}
	spec := shape.FromStream(source.Stream, domain)
	return TapSnapshot{
		Node:      pipeline.NodeRef(source.From),
		Domain:    domain,
		MediaKind: spec.MediaKind,
		Shape:     spec,
	}
}

// attachRuntimeBranches plans the group below its anchors, opens it, and
// applies it to the running graph as one attachment.
func (t *task) attachRuntimeBranches(ctx context.Context, input runtimeAttachInput) (Attachment, error) {
	if err := t.attachMu.LockContext(ctx); err != nil {
		return nil, err
	}
	defer t.attachMu.Unlock()
	t.lifecycleMu.Lock()
	closing := t.closeStarted
	t.lifecycleMu.Unlock()
	if closing {
		return nil, runtimeBranchGraphError("attach branch", input.name, pipeline.ErrClosed)
	}
	p, err := t.planAttach(ctx, input)
	if err != nil {
		return nil, err
	}
	attachment, err := p.apply()
	if err != nil {
		return nil, err
	}
	t.trackAttachmentLocked(attachment)
	t.addAttachmentTapsLocked(attachment.taps)
	t.publishBranchLifecycleEvent(av.EventBranchAttached, attachment, oldBranchDetach)
	return attachment, nil
}

// validateAttachBranchShapeContract validates the branch's operation list
// against the live anchor shape with the same shape algebra the build path
// uses, and rejects mux destinations whose final shape is not packet-domain.
func validateAttachBranchShapeContract(stream recipeir.Stream, operations []recipeir.Operation, destinations []attachDestination, initial shape.Spec) error {
	if err := publicError(compile.ValidateRecipeIROperationShapes("attach runtime branch", stream, operations, initial)); err != nil {
		return err
	}
	final := compile.NormalizeTapShape(initial)
	for _, operation := range operations {
		final = compile.RecipeIROperationOutputShape(final, operation)
	}
	for i, destination := range destinations {
		destinationName := firstNonEmpty(destination.recipe.Name, destination.dest.label(fmt.Sprintf("destination%d", i+1)))
		if destination.recipe.Kind == recipeir.DestinationKindSink {
			if err := publicError(compile.ValidateSinkShape("attach runtime branch", stream.Name, destinationName, compileDestinationFromSpec(destination.dest), final)); err != nil {
				return err
			}
			continue
		}
		if destination.recipe.Kind != recipeir.DestinationKindByteStream || final.Domain == shape.DomainPacket {
			continue
		}
		return publicError(compile.DestinationShapeMismatchError("attach runtime branch", stream.Name, destinationName, compileDestinationFromSpec(destination.dest), final))
	}
	return nil
}

func (d runtimeBranchGroupDestinations) sharesSink(key string) bool {
	_, ok := d.sharedSinkKeys[key]
	return key != "" && ok
}

func (d runtimeBranchGroupDestinations) sharesMux(key string) bool {
	_, ok := d.sharedMuxKeys[key]
	return key != "" && ok
}

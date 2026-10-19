package goav

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
)

var runtimeAttachmentSeq atomic.Uint64

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
	// Spec returns the branch's private graph patch as a structured spec.
	Spec() pipeline.Spec
	// Stats returns branch-scoped counters; Snapshot returns the branch-owned
	// point-in-time view (lifecycle state, stats, published taps).
	Stats() pipeline.GraphStats
	Snapshot() snapshot.Branch
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
	// lifecycle.SwitchAt(lifecycle.NextFrame()/lifecycle.NextKeyframe()) delays
	// the switch to that stream boundary — the replacements shed media until the
	// boundary and this branch detaches at it — and
	// lifecycle.DrainOldBranch/lifecycle.AbortOldBranch select whether this
	// branch's destinations commit or abort on detach.
	// Without options the switch is immediate, exactly like Detach after Attach.
	Rebranch(context.Context, ...lifecycle.RebranchArg) (Attachment, error)
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

func runtimeRebranchInputFromArgs(ctx context.Context, args []lifecycle.RebranchArg) (runtimeRebranchInput, error) {
	policy := rebranchPolicyFromOptions(args)
	if len(policy.specs) == 0 {
		return runtimeRebranchInput{}, runtimeBranchInvalidError("rebranch runtime branch", "pass one or more replacement Outputs built from task.Tap(name) or task.Track(input, stream)")
	}
	if policy.invalid != "" {
		return runtimeRebranchInput{}, runtimeBranchInvalidError(policy.invalid, "pass lifecycle.SwitchAt(lifecycle.AtMediaTime(position)) with position >= 0")
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
func discoveredStreamAnchorTap(source recipeir.RuntimeBranchSource) snapshot.Tap {
	domain := source.StreamDomain
	if domain == "" {
		domain = shape.DomainPacket
	}
	spec := shape.FromStream(source.Stream, domain)
	return snapshot.Tap{
		Node:      pipeline.NodeRef(source.From),
		Domain:    domain,
		MediaKind: spec.MediaKind,
		Shape:     spec,
	}
}

func (t *task) rollbackRuntimeBranch(nodes []pipeline.NodeRef) {
	for _, node := range slices.Backward(nodes) {
		_ = t.graph.Remove(node)
	}
}

func (t *task) trackAttachmentLocked(attachment *runtimeAttachment) {
	if t.attachments == nil {
		t.attachments = make(map[*runtimeAttachment]struct{})
	}
	t.attachments[attachment] = struct{}{}
}

func (t *task) tapsLocked() []snapshot.Tap {
	base := t.taps
	out := make([]snapshot.Tap, 0, len(base)+len(t.branchTaps))
	seen := make(map[string]struct{}, len(base)+len(t.branchTaps))
	appendTap := func(tap snapshot.Tap) {
		if tap.Name == "" {
			return
		}
		if _, ok := seen[tap.Name]; ok {
			return
		}
		seen[tap.Name] = struct{}{}
		out = append(out, tap)
	}
	for i := range base {
		appendTap(base[i])
	}
	for i := range t.branchTaps {
		appendTap(t.branchTaps[i])
	}
	return out
}

func (t *task) addAttachmentTapsLocked(taps []snapshot.Tap) {
	if len(taps) == 0 {
		return
	}
	t.branchTaps = append(t.branchTaps, taps...)
}

func (t *task) removeAttachmentTapsLocked(attachment *runtimeAttachment) {
	if attachment == nil || len(attachment.taps) == 0 || len(t.branchTaps) == 0 {
		return
	}
	remove := make(map[string]struct{}, len(attachment.taps))
	for i := range attachment.taps {
		remove[attachment.taps[i].Name] = struct{}{}
	}
	out := t.branchTaps[:0]
	for i := range t.branchTaps {
		if _, ok := remove[t.branchTaps[i].Name]; ok {
			continue
		}
		out = append(out, t.branchTaps[i])
	}
	t.branchTaps = out
}

func (t *task) stopAttachment(ctx context.Context, input runtimeDetachInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
	return t.stopAttachmentLocked(ctx, input)
}

func (t *task) stopAttachmentLocked(ctx context.Context, input runtimeDetachInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	attachment := input.runtime
	disposition := input.disposition
	if attachment == nil {
		return nil
	}
	first, stranded := t.stopAttachmentChildrenLocked(ctx, input)
	if stranded {
		// A child that could not stop still reads this branch: it stays
		// attached, and a later detach finishes both.
		return first
	}
	attachment.stopMu.Lock()
	defer attachment.stopMu.Unlock()
	if attachment.stopped {
		delete(t.attachments, attachment)
		return first
	}
	attachment.applyDetachDisposition(disposition)
	err := attachment.stopLocked(t.graph)
	attachment.stopped = true
	t.removeAttachmentTapsLocked(attachment)
	delete(t.attachments, attachment)
	if err == nil {
		t.publishAttachmentDestinationLifecycleEvents(attachment, disposition)
		t.publishBranchLifecycleEvent(av.EventBranchDetached, attachment, disposition)
	} else {
		t.publishAttachmentDestinationCommitErrors(attachment, err)
	}
	if first != nil {
		return first
	}
	return err
}

func (t *task) publishAttachmentDestinationLifecycleEvents(attachment *runtimeAttachment, disposition oldBranchDisposition) {
	if attachment == nil {
		return
	}
	var kind av.EventType
	switch disposition {
	case oldBranchDrain:
		kind = av.EventDestinationCommitted
	case oldBranchAbort:
		kind = av.EventDestinationAborted
	default:
		return
	}
	for i := range attachment.destinations {
		t.publishDestinationLifecycleEvent(kind, attachment.destinations[i].Name, attachment, nil)
	}
}

func (t *task) publishAttachmentDestinationCommitErrors(attachment *runtimeAttachment, cause error) {
	if attachment == nil || cause == nil {
		return
	}
	for i := range attachment.destinations {
		t.publishDestinationLifecycleEvent(av.EventDestinationCommitError, attachment.destinations[i].Name, attachment, cause)
	}
}

// stopAttachmentChildrenLocked stops the attachments anchored on this one's
// taps and nodes. Each child is tried once: one that does not stop (its
// context ended) is never reselected, and stranded reports that one is
// still attached.
func (t *task) stopAttachmentChildrenLocked(ctx context.Context, input runtimeDetachInput) (first error, stranded bool) {
	if input.runtime == nil {
		return nil, false
	}
	tried := map[*runtimeAttachment]bool{}
	for {
		child := t.childAttachmentLocked(input.runtime, tried)
		if child == nil {
			return first, stranded
		}
		tried[child] = true
		childInput := runtimeDetachInputForRuntimeAttachment(child, input.disposition)
		if err := t.stopAttachmentLocked(ctx, childInput); first == nil && err != nil {
			first = err
		}
		if !child.stopped {
			stranded = true
		}
	}
}

func (t *task) publishBranchLifecycleEvent(kind av.EventType, attachment *runtimeAttachment, disposition oldBranchDisposition) {
	if t == nil || attachment == nil {
		return
	}
	reason := "runtime branch attached"
	if kind == av.EventBranchDetached {
		reason = "runtime branch detached"
	}
	event := av.Event{
		Type:   kind,
		Reason: reason,
		Metadata: av.Metadata{
			av.MetadataAttachmentID:   attachment.ID(),
			av.MetadataAttachmentName: attachment.Name(),
		},
	}
	if kind == av.EventBranchDetached {
		event.Metadata[av.MetadataDetachDisposition] = detachDispositionLabel(disposition)
	}
	t.watch.publish(event)
}

func detachDispositionLabel(disposition oldBranchDisposition) string {
	switch disposition {
	case oldBranchDrain:
		return "drain"
	case oldBranchAbort:
		return "abort"
	default:
		return "detach"
	}
}

func (t *task) childAttachmentLocked(parent *runtimeAttachment, skip map[*runtimeAttachment]bool) *runtimeAttachment {
	if parent == nil {
		return nil
	}
	taps := make(map[string]struct{}, len(parent.taps))
	for i := range parent.taps {
		if parent.taps[i].Name != "" {
			taps[parent.taps[i].Name] = struct{}{}
		}
	}
	nodes := make(map[string]struct{}, len(parent.nodes))
	for i := range parent.nodes {
		nodes[parent.nodes[i].String()] = struct{}{}
	}
	for attachment := range t.attachments {
		if attachment == nil || attachment == parent || attachment.stopped || skip[attachment] {
			continue
		}
		for _, anchor := range attachment.allAnchorTaps() {
			if _, ok := taps[anchor]; ok && anchor != "" {
				return attachment
			}
		}
		for _, anchor := range attachment.allAnchorNodes() {
			if _, ok := nodes[anchor]; ok && anchor != "" {
				return attachment
			}
		}
	}
	return nil
}

type runtimeAttachment struct {
	id           string
	name         string
	owner        *task
	anchorTap    string
	anchorNode   string
	anchorTaps   []string
	anchorNodes  []string
	nodes        []pipeline.NodeRef
	routes       []pipeline.Route
	taps         []snapshot.Tap
	stages       []pipeline.Stage
	destinations []compile.WorkDestination
	stopMu       sync.Mutex
	stopped      bool
	// detachOutcome records the lifecycle.DestinationState a disposition detach
	// (DrainOldBranch/AbortOldBranch) chose for this branch's destinations;
	// snapshots report it instead of the plain lifecycle.DestinationClosed.
	detachOutcome atomic.Value
	// removedStats holds the final counters of the branch's nodes, taken as
	// detach removed them: removed nodes leave the graph's Stats.
	removedStats atomic.Pointer[map[string]pipeline.NodeStats]
}

func (a *runtimeAttachment) allAnchorTaps() []string {
	if a == nil {
		return nil
	}
	if len(a.anchorTaps) == 0 {
		if a.anchorTap == "" {
			return nil
		}
		return []string{a.anchorTap}
	}
	return a.anchorTaps
}

func (a *runtimeAttachment) allAnchorNodes() []string {
	if a == nil {
		return nil
	}
	if len(a.anchorNodes) == 0 {
		if a.anchorNode == "" {
			return nil
		}
		return []string{a.anchorNode}
	}
	return a.anchorNodes
}

func (a *runtimeAttachment) ID() string {
	if a == nil {
		return ""
	}
	return a.id
}

func (a *runtimeAttachment) Name() string {
	if a == nil {
		return ""
	}
	return a.name
}

func (a *runtimeAttachment) Spec() pipeline.Spec {
	if a == nil || a.owner == nil {
		return pipeline.Spec{}
	}
	return a.specFromGraph(a.owner.Describe())
}

func (a *runtimeAttachment) specFromGraph(graph pipeline.Spec) pipeline.Spec {
	if a == nil {
		return pipeline.Spec{}
	}
	nodes := make(map[string]struct{}, len(a.nodes))
	for i := range a.nodes {
		nodes[a.nodes[i].String()] = struct{}{}
	}
	spec := pipeline.Spec{Name: a.name, Realtime: graph.Realtime}
	for i := range graph.Nodes {
		if _, ok := nodes[graph.Nodes[i].Name]; ok {
			spec.Nodes = append(spec.Nodes, graph.Nodes[i])
		}
	}
	for i := range graph.Edges {
		if _, ok := nodes[graph.Edges[i].To.String()]; ok {
			spec.Edges = append(spec.Edges, graph.Edges[i])
		}
	}
	return spec
}

func (a *runtimeAttachment) Stats() pipeline.GraphStats {
	if a == nil || a.owner == nil {
		return pipeline.GraphStats{}
	}
	return branchStatsForNodes(withRemovedStats(a.owner.Stats(), a.removedStats.Load()), a.nodes)
}

func (a *runtimeAttachment) Snapshot() snapshot.Branch {
	if a == nil {
		return snapshot.Branch{}
	}
	stats := pipeline.GraphStats{}
	if a.owner == nil {
		return a.branchSnapshotLocked(stats)
	}
	stats = withRemovedStats(a.owner.Stats(), a.removedStats.Load())
	a.owner.attachMu.Lock()
	defer a.owner.attachMu.Unlock()
	return a.branchSnapshotLocked(stats)
}

func (a *runtimeAttachment) branchSnapshotLocked(taskStats pipeline.GraphStats) snapshot.Branch {
	if a == nil {
		return snapshot.Branch{}
	}
	state := lifecycle.BranchAttached
	destinationState := lifecycle.DestinationOpen
	spec := pipeline.Spec{}
	if a.owner != nil {
		_, destinationState = a.owner.lifecycleStates()
		spec = a.specFromGraph(a.owner.Describe())
	}
	if a.stopped {
		state = lifecycle.BranchDetached
		destinationState = lifecycle.DestinationClosed
		if outcome, ok := a.detachOutcome.Load().(lifecycle.DestinationState); ok && outcome != "" {
			destinationState = outcome
		}
	}
	return snapshot.Branch{
		ID:           a.id,
		Name:         a.name,
		State:        state,
		AnchorTaps:   append([]string(nil), a.allAnchorTaps()...),
		AnchorNodes:  append([]string(nil), a.allAnchorNodes()...),
		Nodes:        append([]pipeline.NodeRef(nil), a.nodes...),
		Taps:         append([]snapshot.Tap(nil), a.taps...),
		Destinations: destinationSnapshotsFromWork(a.destinations, destinationState),
		Spec:         spec,
		Stats:        branchStatsForNodes(taskStats, a.nodes),
	}
}

func (a *runtimeAttachment) Pause(ctx context.Context) error {
	return a.setPaused(ctx, true)
}

func (a *runtimeAttachment) Resume(ctx context.Context) error {
	return a.setPaused(ctx, false)
}

func (a *runtimeAttachment) setPaused(ctx context.Context, paused bool) error {
	if a == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.stopMu.Lock()
	defer a.stopMu.Unlock()
	if a.stopped {
		return runtimeBranchInvalidError("pause runtime branch", "branch is already detached")
	}
	if a.owner == nil {
		return nil
	}
	pauser, ok := a.owner.graph.(pipeline.NodePauser)
	if !ok {
		return runtimeBranchInvalidError("pause runtime branch", "this task graph does not support pausing branches")
	}
	var first error
	for i := range a.nodes {
		if err := pauser.SetNodePaused(a.nodes[i], paused); err != nil && !isStoppedAttachmentError(err) && first == nil {
			first = err
		}
	}
	return first
}

func (a *runtimeAttachment) Rebranch(ctx context.Context, options ...lifecycle.RebranchArg) (Attachment, error) {
	if a == nil || a.owner == nil {
		return nil, runtimeBranchInvalidError("rebranch runtime branch", "attachment has no owning task")
	}
	options, err := a.owner.rebranchArgs(options)
	if err != nil {
		return nil, err
	}
	input, err := runtimeRebranchInputFromArgs(ctx, options)
	if err != nil {
		return nil, err
	}
	return a.rebranchRuntimeAttachment(ctx, input)
}

func (a *runtimeAttachment) rebranchRuntimeAttachment(ctx context.Context, input runtimeRebranchInput) (Attachment, error) {
	// Attach the replacements first so they are live before the old branch goes
	// away (no delivery gap); if that fails, leave the old branch untouched.
	next, err := a.owner.attachRuntimeBranches(ctx, input.attach)
	if err != nil {
		return nil, err
	}
	if input.group == nil {
		if err := a.detachReplaced(ctx, input.disposition); err != nil {
			return next, err
		}
		return next, nil
	}
	go a.detachReplacedAtBoundary(input.group, input.disposition)
	return next, nil
}

// detachReplacedAtBoundary waits for the switch boundary — the first
// replacement gate opening — and detaches this branch with the chosen
// disposition. When every gate closes before any opened (teardown reached the
// replacements first), the switch never happened and this branch's own
// teardown finalizes it instead.
func (a *runtimeAttachment) detachReplacedAtBoundary(group *switchGroup, disposition oldBranchDisposition) {
	select {
	case <-group.opened:
	case <-group.abandoned:
		select {
		case <-group.opened:
		default:
			return
		}
	}
	_ = a.detachReplaced(context.Background(), disposition)
}

// detachReplaced detaches this branch as the replaced side of a rebranch:
// drain commits its destinations, abort marks its prepared stages failed so
// closing aborts them, and the default stays a plain detach.
func (a *runtimeAttachment) detachReplaced(ctx context.Context, disposition oldBranchDisposition) error {
	return a.owner.stopAttachment(ctx, runtimeDetachInputForRuntimeAttachment(a, disposition))
}

func (a *runtimeAttachment) applyDetachDisposition(disposition oldBranchDisposition) {
	switch disposition {
	case oldBranchDrain:
		a.detachOutcome.Store(lifecycle.DestinationCommitted)
	case oldBranchAbort:
		for _, stage := range a.stages {
			markPipelineStageFailed(stage)
		}
		a.detachOutcome.Store(lifecycle.DestinationAborted)
	case oldBranchDetach:
	}
}

func (a *runtimeAttachment) Close(ctx context.Context) error {
	if a == nil {
		return nil
	}
	if a.owner != nil {
		return a.owner.stopAttachment(ctx, runtimeDetachInputForRuntimeAttachment(a, oldBranchDetach))
	}
	return nil
}

// stopLocked takes the branch out of the graph without losing its tail: the
// routes into it are cut first, then its nodes are removed upstream first.
// Removing a node delivers what it already queued (pipeline.Graph.Remove), so
// each node drains into downstream nodes that are still attached. An abort
// detach has marked its destinations failed already, so what drains into them
// is discarded with the aborted write.
func (a *runtimeAttachment) stopLocked(graph pipeline.Graph) error {
	var first error
	for i := range a.routes {
		if slices.Contains(a.nodes, pipeline.NodeRef(a.routes[i].From)) {
			continue // leaves with its node
		}
		if err := graph.Disconnect(a.routes[i]); err != nil && !isStoppedAttachmentError(err) && first == nil {
			first = err
		}
	}
	removed := make(map[string]pipeline.NodeStats, len(a.nodes))
	for _, v := range upstreamFirst(a.nodes, a.routes) {
		if err := removeBranchNode(graph, v, removed); err != nil && !isStoppedAttachmentError(err) && first == nil {
			first = err
		}
	}
	a.removedStats.Store(&removed)
	return first
}

// upstreamFirst orders a branch's nodes so that none comes before a node
// routed into it; ties, and a cycle should one exist, keep the attach order.
func upstreamFirst(nodes []pipeline.NodeRef, routes []pipeline.Route) []pipeline.NodeRef {
	left := slices.Clone(nodes)
	out := make([]pipeline.NodeRef, 0, len(nodes))
	fed := func(node pipeline.NodeRef) bool {
		for _, route := range routes {
			if route.From != node.String() && slices.Contains(left, pipeline.NodeRef(route.From)) && slices.Contains(route.To, node.String()) {
				return true
			}
		}
		return false
	}
	for len(left) > 0 {
		next := slices.IndexFunc(left, func(node pipeline.NodeRef) bool { return !fed(node) })
		if next < 0 {
			next = 0
		}
		out = append(out, left[next])
		left = slices.Delete(left, next, next+1)
	}
	return out
}

func branchStatsForNodes(stats pipeline.GraphStats, nodes []pipeline.NodeRef) pipeline.GraphStats {
	if len(nodes) == 0 || len(stats.Nodes) == 0 {
		return pipeline.GraphStats{}
	}
	out := pipeline.GraphStats{
		DropReasons: make(map[pipeline.DropPolicy]uint64),
		Nodes:       make(map[string]pipeline.NodeStats, len(nodes)),
	}
	for i := range nodes {
		name := nodes[i].String()
		nodeStats, ok := stats.Nodes[name]
		if !ok {
			continue
		}
		out.Nodes[name] = clonePublicNodeStats(nodeStats)
		out.Messages += nodeStats.OutMessages
		out.Packets += nodeStats.OutPackets
		out.Frames += nodeStats.OutFrames
		out.Events += nodeStats.OutEvents
		out.Delivered += nodeStats.InMessages
		out.Dropped += nodeStats.Dropped
		for reason, count := range nodeStats.DropReasons {
			out.DropReasons[reason] += count
		}
		if nodeStats.LastEventPresent {
			out.LastEvent = nodeStats.LastEvent
			out.LastEventPresent = true
		}
	}
	if len(out.DropReasons) == 0 {
		out.DropReasons = nil
	}
	if len(out.Nodes) == 0 {
		out.Nodes = nil
	}
	return out
}

func clonePublicNodeStats(stats pipeline.NodeStats) pipeline.NodeStats {
	cloned := stats
	if stats.DropReasons != nil {
		cloned.DropReasons = make(map[pipeline.DropPolicy]uint64, len(stats.DropReasons))
		maps.Copy(cloned.DropReasons, stats.DropReasons)
	}
	return cloned
}

func isStoppedAttachmentError(err error) bool {
	return errors.Is(err, pipeline.ErrInvalidLink) ||
		errors.Is(err, pipeline.ErrUnknownNode) ||
		errors.Is(err, pipeline.ErrClosed)
}

func destinationSpecHasOutput(dest destinationSpec) bool {
	return dest.output.Name != "" ||
		dest.output.URI != "" ||
		dest.output.Writer != nil ||
		dest.format != "" ||
		dest.resolvedFormat != ""
}

func runtimeBranchStreamMedia(stream av.Stream) av.MediaType {
	if stream.Type != "" {
		return stream.Type
	}
	return stream.Codec.Type
}

func runtimeBranchAnchorShape(anchor snapshot.Tap) shape.Spec {
	shape := anchor.Shape
	if shape.Domain == "" {
		shape.Domain = anchor.Domain
	}
	if shape.MediaKind == "" {
		shape.MediaKind = anchor.MediaKind
	}
	return shape
}

func streamFromRuntimeBranchShape(name string, shape shape.Spec) av.Stream {
	stream := av.Stream{
		ID:   av.StreamID(firstNonEmpty(string(shape.StreamID), name, "runtime-branch")),
		Name: firstNonEmpty(name, "runtime-branch"),
		Type: shape.MediaKind,
		Codec: av.CodecParameters{
			ID:            shape.Codec,
			Type:          shape.MediaKind,
			SampleRate:    shape.SampleRate,
			Channels:      shape.Channels,
			ChannelLayout: "",
			Width:         shape.Width,
			Height:        shape.Height,
			PixelFormat:   shape.PixelFormat,
			SampleFormat:  shape.SampleFormat,
		},
	}
	if shape.SampleRate > 0 {
		stream.Codec.ClockRate = uint32(shape.SampleRate)
		stream.TimeBase = av.TimeBase{Num: 1, Den: int64(shape.SampleRate)}
	}
	return stream
}

func mediaShapeFromRuntimeBranchStream(stream av.Stream, previous shape.Spec) shape.Spec {
	spec := previous
	spec.Domain = shape.DomainFrame
	if stream.Type != "" {
		spec.MediaKind = stream.Type
	}
	if stream.ID != "" {
		spec.StreamID = stream.ID
	}
	if stream.Codec.ID != "" {
		spec.Codec = stream.Codec.ID
	}
	if stream.Codec.Width != 0 {
		spec.Width = stream.Codec.Width
	}
	if stream.Codec.Height != 0 {
		spec.Height = stream.Codec.Height
	}
	if stream.Codec.PixelFormat != "" {
		spec.PixelFormat = stream.Codec.PixelFormat
	}
	if stream.Codec.SampleRate != 0 {
		spec.SampleRate = stream.Codec.SampleRate
	}
	if stream.Codec.Channels != 0 {
		spec.Channels = stream.Codec.Channels
	}
	if stream.Codec.SampleFormat != "" {
		spec.SampleFormat = stream.Codec.SampleFormat
	}
	return spec
}

func streamPacketShapeFromRuntimeBranchStream(stream av.Stream, previous shape.Spec) shape.Spec {
	spec := mediaShapeFromRuntimeBranchStream(stream, previous)
	spec.Domain = shape.DomainPacket
	return spec
}

func runtimeBranchTap(name string, node pipeline.NodeRef, spec shape.Spec, after plan.OperationKind) snapshot.Tap {
	domain := spec.Domain
	if domain == "" {
		domain = shape.DomainPacket
	}
	media := spec.MediaKind
	if spec.Domain == "" {
		spec.Domain = domain
	}
	if spec.MediaKind == "" {
		spec.MediaKind = media
	}
	return snapshot.Tap{
		Name:      name,
		MediaKind: media,
		Domain:    domain,
		After:     after,
		Shape:     spec,
		Node:      node,
	}
}

func nextRuntimeAttachmentID(name string) string {
	seq := runtimeAttachmentSeq.Add(1)
	if name == "" {
		return "attachment-" + strconv.FormatUint(seq, 10)
	}
	return name + "-" + strconv.FormatUint(seq, 10)
}

func specHasNode(spec pipeline.Spec, name string) bool {
	for i := range spec.Nodes {
		if spec.Nodes[i].Name == name {
			return true
		}
	}
	return false
}

func runtimeBranchInvalidError(reason string, suggestion string) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchInvalidCode),
		Code:      runtimeBranchInvalidCode,
		Operation: "attach runtime branch",
		Reason:    reason,
		fixes: buildErrorFixes([]string{
			suggestion,
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchAnchorMissingError(node string) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchAnchorMissingCode),
		Code:      runtimeBranchAnchorMissingCode,
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "branch source node does not exist in the running task graph",
		fixes: buildErrorFixes([]string{
			"call task.Taps() and attach from task.Tap(name) for stable media outlets",
			"keep the GraphNode returned by expert.Graph(runtime).Source/Stage/Sink for expert graph attachments",
			"attach from a stable decoded-frame tap when the branch needs raw frames",
		}),
		cause: pipeline.ErrUnknownNode,
	}
}

func runtimeBranchTapMissingError(name string, taps []snapshot.Tap) error {
	details := make([]string, 0, len(taps))
	for i := range taps {
		details = append(details, taps[i].Name+": "+string(taps[i].Domain)+" "+string(taps[i].MediaKind))
	}
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchTapMissingCode),
		Code:      runtimeBranchTapMissingCode,
		Operation: "attach runtime branch",
		Node:      name,
		Reason:    "branch source tap does not exist in the running task",
		fields:    errDetailLines(details),
		fixes: buildErrorFixes([]string{
			"add .Tap(" + strconv.Quote(name) + ") at the point you want to attach",
			"call task.Taps() before attaching runtime branches",
			"if the tap came from a detached runtime branch, reattach that branch or choose a tap still listed by task.Taps()",
			"use task.Route(node.Out().Route()) only for expert graph-node attachments",
		}),
		cause: pipeline.ErrUnknownNode,
	}
}

func runtimeBranchNodeDuplicateError(node string) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchNodeDuplicateCode),
		Code:      runtimeBranchNodeDuplicateCode,
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "branch node name already exists in the task graph",
		fixes: buildErrorFixes([]string{
			"give each attached Output a unique name with .As(name)",
			"use distinct stage and sink names inside repeated runtime branches",
		}),
		cause: pipeline.ErrNodeExists,
	}
}

func duplicateRuntimeBranchDestinationRefError(branch string, label string, firstIndex int, secondIndex int) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(destinationDuplicateCode),
		Code:      destinationDuplicateCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    fmt.Sprintf("branch routes to destination %q more than once", label),
		fields:    errDetails(errNote(fmt.Sprintf("first destination index: %d", firstIndex)), errNote(fmt.Sprintf("second destination index: %d", secondIndex))),
		fixes: buildErrorFixes([]string{
			"list each destination once in .To(...)",
			"route one runtime branch to multiple destinations with distinct values such as .To(archive, monitor)",
			"pass the same destination value to each grouped attachment branch",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchTapDuplicateError(name string) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchTapDuplicateCode),
		Code:      runtimeBranchTapDuplicateCode,
		Operation: "attach runtime branch",
		Node:      name,
		Reason:    "branch tap name already exists in the task",
		fixes: buildErrorFixes([]string{
			"use a unique tap name for each runtime branch outlet",
			"call task.Taps() before attaching to inspect the current tap names",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchTransformMediaError(branch string, transform string, expected av.MediaType, actual av.MediaType) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(transformMediaMismatchCode),
		Code:      transformMediaMismatchCode,
		Operation: "attach runtime branch",
		Node:      branch,
		Reason:    transform + " applies to " + string(expected) + " frame taps",
		fields:    errDetails(errDetail("expected_shape", shape.Frame(expected).String()), errDetail("actual_shape", shape.Frame(actual).String())),
		fixes: buildErrorFixes([]string{
			"use in.Video().Decode().Tap(name) or a video transform tap before attaching .Resize(...)",
			"use in.Audio().Decode().Tap(name) or an audio transform tap before attaching .Resample(...)",
			"call task.Taps() and choose a tap with matching media kind",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchTransformError(node string, cause error) error {
	if cause == nil {
		return nil
	}
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchTransformErrorCode),
		Code:      runtimeBranchTransformErrorCode,
		Operation: "attach runtime branch",
		Node:      node,
		Reason:    "runtime branch transform could not be opened",
		fixes: buildErrorFixes([]string{
			"register a matching resize or resample filter adapter",
			"import github.com/thesyncim/goav/bundle and build with bundle.MustNew(...), or goav.New(bundle.Filters(), ...) for the bundled filters alone",
			"attach from a frame tap with media shape that match the requested transform",
		}),
		cause: cause,
	}
}

func runtimeBranchEncodeMissingError(branch string) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchEncodeMissingCode),
		Code:      runtimeBranchEncodeMissingCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "muxed runtime branches need packet copy or an encoder",
		fixes: buildErrorFixes([]string{
			"attach from a packet tap without .Decode() to copy its packets",
			"call " + encodeHint("") + " when attaching from a frame tap",
			"use .To(goav.Sink(...)) when the runtime branch should receive raw frames",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchEncodeDomainError(branch string, shape shape.Spec) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchEncodeDomainMismatchCode),
		Code:      runtimeBranchEncodeDomainMismatchCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch encoding requires a frame tap",
		fields:    errDetailLines(runtimeBranchShapeDetails(shape)),
		fixes: buildErrorFixes([]string{
			"attach from a tap declared after Decode, Resize, Resample, or a frame-stage .Do(...)",
			"route a packet tap without .Encode(...) when no re-encode is intended: task.Tap(name).To(dst)",
			"call task.Taps() and choose a tap with domain=frame",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchDecodeDomainError(branch string, shape shape.Spec) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchDecodeDomainMismatchCode),
		Code:      runtimeBranchDecodeDomainMismatchCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch decoding requires a packet tap",
		fields:    errDetailLines(runtimeBranchShapeDetails(shape)),
		fixes: buildErrorFixes([]string{
			"attach from a tap declared before Decode (on received packets) or after Encode",
			"omit .Decode() when attaching from a frame tap",
			"call task.Taps() and choose a tap with domain=packet",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchDecodeCodecMissingError(branch string, shape shape.Spec) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchDecodeCodecMissingCode),
		Code:      runtimeBranchDecodeCodecMissingCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch decode needs packet codec metadata",
		fields:    errDetailLines(runtimeBranchShapeDetails(shape)),
		fixes: buildErrorFixes([]string{
			"attach from a recipe tap with codec shape",
			"declare the input codec (provider codec intent or file metadata) before building the task",
			"call task.Taps() and choose a packet tap that reports codec=...",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchCopyDomainError(branch string, shape shape.Spec) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchCopyDomainMismatchCode),
		Code:      runtimeBranchCopyDomainMismatchCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch packet copy requires a packet tap",
		fields:    errDetailLines(runtimeBranchShapeDetails(shape)),
		fixes: buildErrorFixes([]string{
			"attach from a tap declared before Decode or after Encode",
			"encode frame taps with " + encodeHint(shape.MediaKind) + " before writing a muxed destination",
			"call task.Taps() and choose a tap with domain=packet",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchMuxCodecMissingError(branch string, shape shape.Spec) error {
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchMuxCodecMissingCode),
		Code:      runtimeBranchMuxCodecMissingCode,
		Operation: "attach runtime branch",
		Node:      firstNonEmpty(branch, "branch"),
		Reason:    "runtime branch mux destination needs codec metadata",
		fields:    errDetailLines(runtimeBranchShapeDetails(shape)),
		fixes: buildErrorFixes([]string{
			"attach from a recipe tap with codec shape",
			"set an explicit encoder with " + encodeHint(shape.MediaKind),
			"use .To(goav.Sink(...)) when the branch should stay raw",
		}),
		cause: errUnsupportedBuild,
	}
}

func runtimeBranchShapeDetails(shape shape.Spec) []string {
	details := []string{
		"domain=" + string(shape.Domain),
		"media=" + string(shape.MediaKind),
	}
	if shape.Codec != "" {
		details = append(details, "codec="+string(shape.Codec))
	}
	if shape.StreamID != "" {
		details = append(details, "stream="+string(shape.StreamID))
	}
	return details
}

func runtimeBranchGraphError(operation string, node string, cause error) error {
	fixes := []string{
		"build the branch before Run when using buffered execution",
	}
	if errors.Is(cause, pipeline.ErrBufferedEdgesUnsupported) {
		// A buffered branch (.Buffer(...)) needs a buffered task graph; offline
		// file tasks run direct.
		fixes = []string{
			"drop the branch's .Buffer(...) policy: this task runs a direct (offline) graph",
			"or build the task buffered: declare the buffered fork in the recipe itself (v.Buffer(...).To(dst)), or build the runtime with runconfig.WithBufferPolicy(...)",
		}
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(runtimeBranchGraphErrorCode),
		Code:      runtimeBranchGraphErrorCode,
		Operation: operation,
		Node:      node,
		Reason:    "runtime graph rejected the branch attachment",
		fixes:     buildErrorFixes(fixes),
		cause:     cause,
	}
}

// attachRuntimeBranches plans the group below its anchors, opens it, and
// applies it to the running graph as one attachment.
func (t *task) attachRuntimeBranches(ctx context.Context, input runtimeAttachInput) (Attachment, error) {
	t.attachMu.Lock()
	defer t.attachMu.Unlock()
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
		if destination.recipe.Kind != recipeir.DestinationKindByteStream || final.Domain == shape.DomainPacket {
			continue
		}
		destinationName := firstNonEmpty(destination.recipe.Name, destination.dest.label(fmt.Sprintf("destination%d", i+1)))
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

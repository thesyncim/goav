package goav

import (
	"context"
	"errors"
	"slices"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
)

func (t *Task) rollbackRuntimeBranch(nodes []pipeline.NodeRef) {
	for _, node := range slices.Backward(nodes) {
		_ = t.graph.Remove(node)
	}
}

func (t *Task) trackAttachmentLocked(attachment *runtimeAttachment) {
	t.pruneIsolatedLocked()
	if t.attachments == nil {
		t.attachments = make(map[*runtimeAttachment]struct{})
	}
	t.attachments[attachment] = struct{}{}
}

// A timed-out detach can still report a real failure from an in-flight
// handler. Keep that domain until retirement settles, then prune only healthy
// records. Check Failed after Retired so a concurrent last handler cannot be
// mistaken for a healthy completion.
func (t *Task) pruneIsolatedLocked() {
	t.isolated = slices.DeleteFunc(t.isolated, func(b *isolatedBranch) bool {
		if b.attachment == nil {
			return false
		}
		if _, attached := t.attachments[b.attachment]; attached {
			return false
		}
		return b.domain.Snapshot().Retired && !b.domain.Failed()
	})
}

func (t *Task) tapsLocked() []TapSnapshot {
	base := t.taps
	out := make([]TapSnapshot, 0, len(base)+len(t.branchTaps))
	seen := make(map[string]struct{}, len(base)+len(t.branchTaps))
	appendTap := func(tap TapSnapshot) {
		if tap.Name == "" {
			return
		}
		if _, ok := seen[tap.Name]; ok {
			return
		}
		for _, branch := range t.isolated {
			if branch.attachment != nil {
				if _, attached := t.attachments[branch.attachment]; !attached {
					continue
				}
			}
			if branch.domain.Failed() && slices.Contains(branch.plan.nodes, tap.Node) {
				return
			}
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

func (t *Task) addAttachmentTapsLocked(taps []TapSnapshot) {
	if len(taps) == 0 {
		return
	}
	t.branchTaps = append(t.branchTaps, taps...)
}

func (t *Task) removeAttachmentTapsLocked(attachment *runtimeAttachment) {
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

// A commit-intent detach must be able to observe execution failure before
// Run returns. Refuse unsupported graphs before removing children, connecting
// replacements, or changing any destination disposition.
func (t *Task) validateAttachmentDisposition(disposition oldBranchDisposition) error {
	if disposition != oldBranchDrain {
		return nil
	}
	if _, ok := t.graph.(pipeline.RunErrorReporter); ok {
		return nil
	}
	return &BuildError{
		Phase:     phaseRun,
		Family:    errcode.FamilyForCode(runtimeBranchGraphErrorCode),
		Code:      runtimeBranchGraphErrorCode,
		Operation: "drain runtime branch",
		Reason:    "graph does not implement pipeline.RunErrorReporter",
		fixes:     buildErrorFixes([]string{"implement or forward pipeline.RunErrorReporter before requesting DrainBranch"}),
		cause:     pipeline.ErrDynamicGraphUnsupported,
	}
}

func (t *Task) stopAttachment(ctx context.Context, input runtimeDetachInput) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := t.attachMu.LockContext(ctx); err != nil {
		return err
	}
	defer t.attachMu.Unlock()
	return t.stopAttachmentLocked(ctx, input)
}

func (t *Task) stopAttachmentLocked(ctx context.Context, input runtimeDetachInput) error {
	attachment := input.runtime
	disposition := input.disposition
	if attachment == nil {
		return nil
	}
	if err := t.validateAttachmentDisposition(disposition); err != nil {
		return err
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
		waitErr := attachment.waitRetirement(ctx)
		if waitErr == nil && attachment.detachOutcome.Load() == DestinationFinalizing {
			attachment.detachOutcome.Store(DestinationAborted)
		}
		return errors.Join(first, waitErr)
	}
	t.setAttachmentDisposition(attachment, disposition)
	err := attachment.stopLocked(ctx, t.graph)
	if disposition == oldBranchDrain {
		if runErr := t.graph.(pipeline.RunErrorReporter).RunError(); runErr != nil && !isStoppedAttachmentError(runErr) {
			err = errorsJoinUnique(err, runErr)
		}
	}
	attachment.stopped = true
	err = errors.Join(err, attachment.waitRetirement(ctx))
	if err != nil {
		t.setAttachmentDisposition(attachment, oldBranchAbort)
	}
	aborted := t.endAttachmentLifecycle(attachment)
	if aborted {
		disposition = oldBranchAbort
	}
	t.removeAttachmentTapsLocked(attachment)
	delete(t.attachments, attachment)
	t.pruneIsolatedLocked()
	if err == nil {
		switch disposition {
		case oldBranchDrain:
			attachment.detachOutcome.Store(DestinationCommitted)
		case oldBranchAbort:
			attachment.detachOutcome.Store(DestinationAborted)
		}
		t.publishAttachmentDestinationLifecycleEvents(attachment, disposition)
		t.publishBranchLifecycleEvent(av.EventBranchDetached, attachment, disposition)
	} else {
		if errors.Is(err, pipeline.ErrCloseWait) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			attachment.detachOutcome.Store(DestinationFinalizing)
		} else {
			attachment.detachOutcome.Store(DestinationFailed)
		}
		t.publishAttachmentDestinationCommitErrors(attachment, err)
	}
	if first != nil {
		return first
	}
	return err
}

func (t *Task) publishAttachmentDestinationLifecycleEvents(attachment *runtimeAttachment, disposition oldBranchDisposition) {
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
		if attachment.hasTransaction(attachment.destinations[i].Name) {
			continue
		}
		t.publishDestinationLifecycleEvent(kind, attachment.destinations[i].Name, attachment, nil)
	}
}

func (t *Task) publishAttachmentDestinationCommitErrors(attachment *runtimeAttachment, cause error) {
	if attachment == nil || cause == nil {
		return
	}
	for i := range attachment.destinations {
		if attachment.hasTransaction(attachment.destinations[i].Name) {
			continue
		}
		t.publishDestinationLifecycleEvent(av.EventDestinationCommitError, attachment.destinations[i].Name, attachment, cause)
	}
}

// stopAttachmentChildrenLocked stops the attachments anchored on this one's
// taps and nodes. Each child is tried once: one that does not stop (its
// context ended) is never reselected, and stranded reports that one is
// still attached.
func (t *Task) stopAttachmentChildrenLocked(ctx context.Context, input runtimeDetachInput) (first error, stranded bool) {
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

func (t *Task) publishBranchLifecycleEvent(kind av.EventType, attachment *runtimeAttachment, disposition oldBranchDisposition) {
	if t == nil || attachment == nil {
		return
	}
	reason := "runtime branch attached"
	if kind == av.EventBranchDetached {
		reason = "runtime branch detached"
	}
	event := av.Event{
		Type:           kind,
		Reason:         reason,
		AttachmentID:   attachment.ID(),
		AttachmentName: attachment.Name(),
	}
	if kind == av.EventBranchDetached {
		event.DetachDisposition = detachDispositionLabel(disposition)
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

func (t *Task) childAttachmentLocked(parent *runtimeAttachment, skip map[*runtimeAttachment]bool) *runtimeAttachment {
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

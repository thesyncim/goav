package goav

import (
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/shape"
)

// tapRef is a typed handle to a stable media attach point.
type tapRef struct {
	name   string
	domain shape.MediaDomain
}

func (t tapRef) branchSource() branchSourceBinding {
	return branchSourceBinding{tap: t.name, tapDomain: t.domain}
}

// frameTapRef names a frame-domain attach point.
func frameTapRef(name string) tapRef {
	return tapRef{name: name, domain: shape.DomainFrame}
}

// packetTapRef names a packet-domain attach point.
func packetTapRef(name string) tapRef {
	return tapRef{name: name, domain: shape.DomainPacket}
}

// Name returns the tap's stable name, the key LiveTask.Taps and Branch.From use.
func (t tapRef) Name() string {
	return t.name
}

// Domain returns the asserted media domain: frame, packet, or empty when the
// tap infers its domain from the chain point.
func (t tapRef) Domain() shape.MediaDomain {
	return t.domain
}

func tapWithDomain(tap tapRef, domain shape.MediaDomain) tapRef {
	if tap.domain == "" {
		tap.domain = domain
	}
	return tap
}

func validateTapDomain(operation string, node string, tap tapRef, actual shape.MediaDomain) error {
	if tap.domain == "" || actual == "" || tap.domain == actual {
		return nil
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(errcode.TapDomainMismatch),
		Code:      errcode.TapDomainMismatch,
		Operation: operation,
		Node:      firstNonEmpty(node, "tap"),
		Reason:    "tap domain does not match this stream point",
		fields:    errDetails(errDetail("tap", tap.name), errDetail("wanted", string(tap.domain)), errDetail("actual", string(actual))),
		fixes: buildErrorFixes([]string{
			"move .Tap(name) after decode, resize, resample, or custom frame stages to tap frames",
			"move .Tap(name) before .Decode() or after an encoder to tap packets",
		}),
		cause: errUnsupportedBuild,
	}
}

func branchSourceInvalidError(node string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(branchSourceInvalidCode),
		Code:      branchSourceInvalidCode,
		Operation: "build branch",
		Node:      firstNonEmpty(node, "branch"),
		Reason:    "branch source must be a task tap, track, or expert graph route",
		fixes: buildErrorFixes([]string{
			"start the Output from task.Tap(name) for tap branches",
			"use task.Route(node.Out().Route()) only for expert graph-handle attachments",
		}),
		cause: errUnsupportedBuild,
	}
}

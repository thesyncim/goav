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

// FrameTap names a frame-domain attach point.
func FrameTap(name string) tapRef {
	return tapRef{name: name, domain: shape.DomainFrame}
}

// PacketTap names a packet-domain attach point.
func PacketTap(name string) tapRef {
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
		Family:    errcode.FamilyForCode(errcode.TapDomainMismatch),
		Code:      errcode.TapDomainMismatch,
		Operation: operation,
		Node:      firstNonEmpty(node, "tap"),
		Reason:    "typed tap domain does not match this chain point",
		Fields: buildErrorFields([]string{
			"tap=" + tap.name,
			"wanted=" + string(tap.domain),
			"actual=" + string(actual),
		}),
		Fixes: buildErrorFixes([]string{
			"use goav.FrameTap(name) after decode, resize, resample, or custom frame stages",
			"use goav.PacketTap(name) after .Copy() or an encoder",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

func branchSourceInvalidError(node string) error {
	return &BuildError{
		Family:    errcode.FamilyForCode(errcode.BranchSourceInvalid),
		Code:      errcode.BranchSourceInvalid,
		Operation: "build branch",
		Node:      firstNonEmpty(node, "branch"),
		Reason:    "branch source must be a typed tap or expert graph handle",
		Fixes: buildErrorFixes([]string{
			"use .From(goav.FrameTap(name)) or .From(goav.PacketTap(name)) for tap branches",
			"use .From(graphNode) only for expert graph-handle attachments",
		}),
		Cause: ErrUnsupportedBuild,
	}
}

package goav

import (
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/shape"
)

// TapRef is a typed handle to a stable media attach point.
type TapRef struct {
	name   string
	domain shape.MediaDomain
}

func (t TapRef) branchSource() branchSourceBinding {
	return branchSourceBinding{tap: t.name, tapDomain: t.domain}
}

// Tap names an attach point whose media domain is inferred from where it is
// declared in the chain: frame after decode/resize/resample/custom frame stages,
// packet after .Copy() or an encoder. Prefer Tap for everyday use; reach for
// FrameTap/PacketTap only to assert the domain early.
func Tap(name string) TapRef {
	return TapRef{name: name}
}

// FrameTap names a frame-domain attach point. It is Tap with an early
// frame-domain assertion.
func FrameTap(name string) TapRef {
	return TapRef{name: name, domain: shape.DomainFrame}
}

// PacketTap names a packet-domain attach point.
func PacketTap(name string) TapRef {
	return TapRef{name: name, domain: shape.DomainPacket}
}

// Name returns the tap's stable name, the key Inspectable.Taps and Branch.From use.
func (t TapRef) Name() string {
	return t.name
}

// Domain returns the asserted media domain: frame, packet, or empty when the
// tap infers its domain from the chain point.
func (t TapRef) Domain() shape.MediaDomain {
	return t.domain
}

func tapWithDomain(tap TapRef, domain shape.MediaDomain) TapRef {
	if tap.domain == "" {
		tap.domain = domain
	}
	return tap
}

func validateTapDomain(operation string, node string, tap TapRef, actual shape.MediaDomain) error {
	if tap.domain == "" || actual == "" || tap.domain == actual {
		return nil
	}
	return &BuildError{
		Code:      errcode.TapDomainMismatch,
		Operation: operation,
		Node:      firstNonEmpty(node, "tap"),
		Reason:    "typed tap domain does not match this chain point",
		Details: []string{
			"tap=" + tap.name,
			"wanted=" + string(tap.domain),
			"actual=" + string(actual),
		},
		Suggestions: []string{
			"use goav.FrameTap(name) after decode, resize, resample, or custom frame stages",
			"use goav.PacketTap(name) after .Copy() or an encoder",
		},
		Cause: ErrUnsupportedBuild,
	}
}

func branchSourceInvalidError(node string) error {
	return &BuildError{
		Code:      errcode.BranchSourceInvalid,
		Operation: "build branch",
		Node:      firstNonEmpty(node, "branch"),
		Reason:    "branch source must be a typed tap or expert graph handle",
		Suggestions: []string{
			"use .From(goav.FrameTap(name)) or .From(goav.PacketTap(name)) for tap branches",
			"use .From(graphNode) only for expert graph-handle attachments",
		},
		Cause: ErrUnsupportedBuild,
	}
}

package goav

// TapRef is a typed handle to a stable media attach point.
type TapRef struct {
	name   string
	domain MediaDomain
}

// FrameTap names a frame-domain attach point.
func FrameTap(name string) TapRef {
	return TapRef{name: name, domain: DomainFrame}
}

// PacketTap names a packet-domain attach point.
func PacketTap(name string) TapRef {
	return TapRef{name: name, domain: DomainPacket}
}

func (t TapRef) Name() string {
	return t.name
}

func (t TapRef) Domain() MediaDomain {
	return t.domain
}

func validateTapDomain(operation string, node string, tap TapRef, actual MediaDomain) error {
	if tap.domain == "" || actual == "" || tap.domain == actual {
		return nil
	}
	return &BuildError{
		Code:      "tap_domain_mismatch",
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
		Code:      "branch_source_invalid",
		Operation: "build branch",
		Node:      firstNonEmpty(node, "branch"),
		Reason:    "branch source must be a typed tap or expert graph node name",
		Suggestions: []string{
			"use .From(goav.FrameTap(name)) or .From(goav.PacketTap(name)) for tap branches",
			"use .From(\"node\") only for expert graph-node attachments",
		},
		Cause: ErrUnsupportedBuild,
	}
}

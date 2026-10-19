package goav

import (
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/shape"
)

// packetTapRef names a packet-domain attach point.
func packetTapRef(name string) compile.TapRef {
	return compile.TapRef{Name: name, Domain: shape.DomainPacket}
}

func tapWithDomain(tap compile.TapRef, domain shape.MediaDomain) compile.TapRef {
	if tap.Domain == "" {
		tap.Domain = domain
	}
	return tap
}

func validateTapDomain(operation string, node string, tap compile.TapRef, actual shape.MediaDomain) error {
	if tap.Domain == "" || actual == "" || tap.Domain == actual {
		return nil
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(errcode.TapDomainMismatch),
		Code:      errcode.TapDomainMismatch,
		Operation: operation,
		Node:      firstNonEmpty(node, "tap"),
		Reason:    "tap domain does not match this stream point",
		fields:    errDetails(errDetail("tap", tap.Name), errDetail("wanted", string(tap.Domain)), errDetail("actual", string(actual))),
		fixes: buildErrorFixes([]string{
			"move .Tap(name) after decode, resize, resample, or custom frame stages to tap frames",
			"move .Tap(name) before .Decode() or after an encoder to tap packets",
		}),
		cause: errUnsupportedBuild,
	}
}

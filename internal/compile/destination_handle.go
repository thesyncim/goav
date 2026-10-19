package compile

import (
	"cmp"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
)

func duplicateBranchDecodeError(node string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.BranchDecodeDuplicateCode),
		Code:      builderr.BranchDecodeDuplicateCode,
		Operation: "build branch",
		Node:      node,
		Reason:    "branch already decodes its input packets",
		Fixes: builderr.Fixes([]string{
			"call .Decode() once before frame operations",
			"remove the second .Decode() call",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func branchDecodeDomainError(node string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.BranchDecodeDomainMismatchCode),
		Code:      builderr.BranchDecodeDomainMismatchCode,
		Operation: "build branches",
		Node:      node,
		Reason:    "branch decoding requires a packet-domain stream point",
		Fixes: builderr.Fixes([]string{
			"omit .Decode() when the fork already starts after the stream's .Decode()",
			"fork before .Decode() when a packet copy and a decoding fork share the stream: s := in.Video()",
			"attach runtime decode branches from packet taps",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

func branchDestinationInvalidError(name string, reason string) error {
	return DestinationInvalidError("build branch", cmp.Or(name, "branch"), reason)
}

func DestinationInvalidError(operation string, node string, reason string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.DestinationInvalidCode),
		Code:      builderr.DestinationInvalidCode,
		Operation: operation,
		Node:      node,
		Reason:    reason,
		Fixes: builderr.Fixes([]string{
			"use goav.Write(...), goav.URI(...), or goav.Sink(...) for a real destination",
			"pass one destination value to every chain that should share it",
			"use distinct destination values for independent outputs",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

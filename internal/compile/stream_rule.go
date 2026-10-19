package compile

import (
	"cmp"

	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
)

func StreamRuleInvalidError(node string, reason string, suggestion string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(errcode.StreamRuleInvalid),
		Code:      errcode.StreamRuleInvalid,
		Operation: "build stream rule",
		Node:      cmp.Or(node, "rule"),
		Reason:    reason,
		Fixes: builderr.Fixes([]string{
			suggestion,
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

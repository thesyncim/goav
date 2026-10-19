package compile

import (
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
)

func GraphPlanInvalidError(reason string, details []string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.GraphPlanInvalidCode),
		Code:      builderr.GraphPlanInvalidCode,
		Operation: "build graph plan",
		Reason:    reason,
		Details:   builderr.DetailLines(append([]string(nil), details...)),
		Fixes: builderr.Fixes([]string{
			"compile recipes through Streams, joins, and Outputs: rt.Run(ctx, outputs...)",
			"keep graph-plan nodes, edges, operations, and destinations in sync",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

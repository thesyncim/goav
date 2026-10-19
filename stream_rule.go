package goav

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

func explainStreamRuleFacts(rules []recipeir.StreamRule) []plan.Decision {
	if len(rules) == 0 {
		return nil
	}
	out := make([]plan.Decision, 0, len(rules))
	for i := range rules {
		names := make([]string, 0, len(rules[i].Branches))
		destinations := make([]string, 0)
		for j := range rules[i].Branches {
			names = append(names, rules[i].Branches[j].Name+"-<stream>")
			destinations = append(destinations, rules[i].Branches[j].Destinations...)
		}
		match := firstNonEmpty(rules[i].MatchDescription, "none")
		out = append(out, plan.Decision{
			Code:   compile.DiagnosticStreamRule,
			Branch: strings.Join(names, "+"),
			Message: fmt.Sprintf("on discovered stream (%s): attach %s to %s per matched stream",
				match, strings.Join(names, ", "), strings.Join(destinations, ", ")),
		})
	}
	return out
}

// sourceEventDomain reports the media domain this input's source produces, so
// discovered-stream anchors carry the right domain (frame-domain custom
// sources announce frame streams; everything else is packet-domain).
func (s InputSpec) sourceEventDomain() shape.MediaDomain {
	if spec, ok := declaredSourceShape(s); ok && spec.Domain != "" {
		return spec.Domain
	}
	return shape.DomainPacket
}

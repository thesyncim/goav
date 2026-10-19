package compile

import (
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/internal/builderr"
)

func SelectDecodeStream(streams []av.Stream, selector av.StreamSelector) (av.Stream, error) {
	return selectStreamWithCodecRequirement(streams, selector, true)
}

func SelectStream(streams []av.Stream, selector av.StreamSelector) (av.Stream, error) {
	return selectStreamWithCodecRequirement(streams, selector, false)
}

func selectStreamWithCodecRequirement(streams []av.Stream, selector av.StreamSelector, requireCodec bool) (av.Stream, error) {
	var selected av.Stream
	matches := 0
	matched := make([]av.Stream, 0, len(streams))
	for i := range streams {
		if !StreamMatchesSelector(streams[i], selector) {
			continue
		}
		selected = streams[i]
		matches++
		matched = append(matched, streams[i])
	}
	if matches == 0 {
		return av.Stream{}, streamSelectionError(builderr.StreamMissingCode, selector, streams)
	}
	if matches > 1 {
		return av.Stream{}, streamSelectionError(builderr.StreamAmbiguousCode, selector, matched)
	}
	if requireCodec && selected.Codec.ID == "" {
		return av.Stream{}, &builderr.Refusal{
			Phase:     builderr.PhaseBuild,
			Family:    errcode.FamilyForCode(builderr.StreamCodecMissingCode),
			Code:      builderr.StreamCodecMissingCode,
			Operation: "select stream",
			Node:      selectorDetail(selector),
			Reason:    "selected stream has no codec id",
			Details:   builderr.Details(builderr.Note(StreamDiagnostic(selected, 0))),
			Fixes: builderr.Fixes([]string{
				"provide codec metadata on the input stream",
				"declare the receive codec on the source provider (e.g. a codec intent option)",
			}),
			Cause: builderr.ErrUnsupportedBuild,
		}
	}
	return selected, nil
}

func StreamMatchesSelector(stream av.Stream, selector av.StreamSelector) bool {
	if selector.ID != "" && stream.ID != selector.ID {
		return false
	}
	if selectorHasIndex(selector) && stream.Index != selector.Index {
		return false
	}
	if selector.Type != "" && stream.Type != selector.Type {
		return false
	}
	if selector.Codec != "" && stream.Codec.ID != selector.Codec {
		return false
	}
	if selector.Name != "" && stream.Name != selector.Name {
		return false
	}
	return true
}

func selectorHasIndex(selector av.StreamSelector) bool {
	return selector.UseIndex || selector.Index != 0
}

func streamSelectionError(code errcode.Code, selector av.StreamSelector, streams []av.Stream) error {
	operation := "select stream"
	node := selectorDetail(selector)
	reason := "no stream matches " + ReadableSelector(selector)
	if code == builderr.StreamAmbiguousCode {
		reason = "multiple streams match " + ReadableSelector(selector)
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: operation,
		Node:      node,
		Reason:    reason,
		Details:   builderr.DetailLines(streamDiagnostics(streams)),
		Fixes:     builderr.Fixes(streamSelectionSuggestions(selector, streams)),
		Cause:     builderr.ErrUnsupportedBuild,
	}
}

func ReadableSelector(selector av.StreamSelector) string {
	detail := selectorDetail(selector)
	if detail == "" {
		return "the requested selector"
	}
	return detail
}

func streamDiagnostics(streams []av.Stream) []string {
	if len(streams) == 0 {
		return nil
	}
	details := make([]string, len(streams))
	for i := range streams {
		details[i] = StreamDiagnostic(streams[i], i)
	}
	return details
}

func StreamDiagnostic(stream av.Stream, fallbackIndex int) string {
	media := string(stream.Type)
	if media == "" {
		media = "stream"
	}
	index := stream.Index
	if index == 0 {
		index = fallbackIndex
	}
	parts := []string{media + "[" + strconv.Itoa(index) + "]"}
	if stream.ID != "" {
		parts = append(parts, "id="+string(stream.ID))
	}
	if stream.Codec.ID != "" {
		parts = append(parts, "codec="+string(stream.Codec.ID))
	}
	if stream.Name != "" {
		parts = append(parts, "name="+stream.Name)
	}
	if stream.Language != "" {
		parts = append(parts, "lang="+stream.Language)
	}
	return strings.Join(parts, " ")
}

// streamSelectionSuggestions offers selections that can work: streams of the
// requested kind by id (or index), or — when the input has none of that kind —
// the kinds it does carry.
func streamSelectionSuggestions(selector av.StreamSelector, streams []av.Stream) []string {
	call, recipeCall := streamOptionCall(selector)
	var matching []av.Stream
	for i := range streams {
		if selector.Type == "" || streams[i].Type == selector.Type {
			matching = append(matching, streams[i])
		}
	}
	if len(matching) == 0 && selector.Type != "" {
		var have []string
		seen := map[string]bool{}
		for i := range streams {
			if c, ok := streamOptionCall(av.StreamSelector{Type: streams[i].Type}); ok && !seen[c] {
				seen[c] = true
				have = append(have, "input"+c+"()")
			}
		}
		if len(have) == 0 {
			return []string{"the input carries no " + string(selector.Type) + " stream; check that it is the input you meant"}
		}
		return []string{"the input carries no " + string(selector.Type) + " stream; select one it has: " + strings.Join(have, " or ")}
	}
	suggestions := make([]string, 0, 2)
	for i := range matching {
		if matching[i].ID == "" {
			continue
		}
		if recipeCall {
			suggestions = append(suggestions, call+"(goav.StreamID("+strconv.Quote(string(matching[i].ID))+"))")
		} else {
			suggestions = append(suggestions, "select stream id "+strconv.Quote(string(matching[i].ID)))
		}
		break
	}
	if len(matching) != 0 {
		index := matching[0].Index
		if recipeCall {
			suggestions = append(suggestions, call+"(goav.StreamIndex("+strconv.Itoa(index)+"))")
		} else {
			suggestions = append(suggestions, "select stream index "+strconv.Itoa(index))
		}
	}
	if len(suggestions) == 0 {
		suggestions = append(suggestions, "choose a more specific stream selector")
	}
	return suggestions
}

func streamOptionCall(selector av.StreamSelector) (string, bool) {
	switch selector.Type {
	case av.MediaAudio:
		return ".Audio", true
	case av.MediaVideo:
		return ".Video", true
	default:
		return "", false
	}
}

func DecodeNodeName(selector av.StreamSelector) string {
	if selector.Name != "" {
		return "decode-" + selector.Name
	}
	if selector.ID != "" {
		return "decode-" + string(selector.ID)
	}
	if selector.Codec != "" {
		return "decode-" + string(selector.Codec)
	}
	if selector.Type != "" {
		return "decode-" + string(selector.Type)
	}
	if selectorHasIndex(selector) {
		return "decode-" + strconv.Itoa(selector.Index)
	}
	return "decode"
}

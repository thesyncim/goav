package goav

import (
	"fmt"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/shape"
)

// inputStreamSet is one job input with its name and the streams it is known to
// carry. It is the unit of multi-input stream selection: a stream chain selects
// across the union of all sets, and goav.InputName(...) narrows the search to
// one set.
type inputStreamSet struct {
	name    string
	domain  shape.MediaDomain
	streams []av.Stream
	known   bool
}

// jobInputStreamSets builds one inputStreamSet per job input from what the
// compile state knows: declared custom-source shapes, probe results, and live
// (realtime) input codec intent. Inputs whose streams cannot be known yet (for
// example a file destination before probing) stay known=false so selection
// validation skips them instead of guessing.
func jobInputStreamSets(inputs []inputIntent, attachments []InputSpec, probes []format.ProbeResult) []inputStreamSet {
	sets := make([]inputStreamSet, 0, len(inputs))
	for i := range inputs {
		set := inputStreamSet{
			name:   inputIntentName(inputs[i], i),
			domain: shape.DomainPacket,
		}
		switch {
		case i < len(attachments) && attachments[i].source != nil:
			spec, _ := customSourceShape(attachments[i])
			set.domain = spec.Domain
			set.streams = customSourceStreams(attachments[i])
			set.known = true
		case i < len(probes) && len(probes[i].Streams) != 0:
			set.streams = append([]av.Stream(nil), probes[i].Streams...)
			set.known = true
		case inputs[i].Realtime && inputs[i].Codec.ID != "":
			if stream, ok := liveIntentStream(inputs[i], i); ok {
				set.streams = []av.Stream{stream}
				set.known = true
			}
		}
		sets = append(sets, set)
	}
	return sets
}

// inputSpecStreamSets derives the per-input stream sets straight from the input
// specs (no probes), for callers outside the compile state such as the builder
// and the branch-compose graph constructor.
func inputSpecStreamSets(inputs []InputSpec) []inputStreamSet {
	intents := make([]inputIntent, 0, len(inputs))
	for i := range inputs {
		intents = append(intents, inputs[i].intent())
	}
	return jobInputStreamSets(intents, inputs, nil)
}

func inputIntentName(input inputIntent, index int) string {
	return firstNonEmpty(input.Name, input.URI, fmt.Sprintf("input-%d", index))
}

// resolveInputSetIndex resolves which input a selector binds to without
// producing errors: the named input when narrowed, otherwise the single known
// input whose streams match. ok=false when unknown or ambiguous.
func resolveInputSetIndex(sets []inputStreamSet, selector av.StreamSelector, inputName string) (int, bool) {
	if inputName != "" {
		for i := range sets {
			if sets[i].name == inputName {
				return i, true
			}
		}
		return 0, false
	}
	matched := -1
	for i := range sets {
		if !sets[i].known {
			continue
		}
		for k := range sets[i].streams {
			if !streamMatchesSelector(sets[i].streams[k], selector) {
				continue
			}
			if matched >= 0 && matched != i {
				return 0, false
			}
			matched = i
		}
	}
	if matched < 0 {
		return 0, false
	}
	return matched, true
}

// inputBoundStream is one selection candidate with its input attribution.
type inputBoundStream struct {
	inputIndex int
	inputName  string
	domain     shape.MediaDomain
	stream     av.Stream
}

// selectStreamAcrossInputSets selects exactly one stream across all inputs:
// the multi-input dual of selectDecodeStream. With inputName set the search is
// narrowed to that input. ok=false (with nil error) means the inputs' streams
// are not known yet and selection must be deferred; an error means selection
// definitively failed (unknown input name, no match, or several matches — the
// ambiguity error lists every candidate with its input and suggests
// goav.InputName/goav.StreamID narrowing).
func selectStreamAcrossInputSets(sets []inputStreamSet, selector av.StreamSelector, inputName string) (inputBoundStream, bool, error) {
	candidates := sets
	if inputName != "" {
		index := -1
		for i := range sets {
			if sets[i].name == inputName {
				index = i
				break
			}
		}
		if index < 0 {
			return inputBoundStream{}, false, unknownInputNameError(selector, inputName, sets)
		}
		candidates = sets[index : index+1]
	}
	unknown := false
	matches := make([]inputBoundStream, 0, 2)
	for i := range candidates {
		set := candidates[i]
		if !set.known {
			unknown = true
			continue
		}
		for k := range set.streams {
			if !streamMatchesSelector(set.streams[k], selector) {
				continue
			}
			matches = append(matches, inputBoundStream{
				inputIndex: inputSetIndex(sets, set.name),
				inputName:  set.name,
				domain:     set.domain,
				stream:     set.streams[k],
			})
		}
	}
	switch {
	case len(matches) == 1:
		selected := matches[0]
		if selected.domain != shape.DomainFrame && selected.stream.Codec.ID == "" {
			// Mirror selectDecodeStream's codec requirement for packet media.
			_, err := selectDecodeStream([]av.Stream{selected.stream}, selector)
			return inputBoundStream{}, false, err
		}
		return selected, true, nil
	case len(matches) > 1:
		return inputBoundStream{}, false, multiInputStreamSelectionError(errcode.StreamAmbiguous, selector, inputName, matches, sets)
	case unknown:
		return inputBoundStream{}, false, nil
	default:
		return inputBoundStream{}, false, multiInputStreamSelectionError(errcode.StreamMissing, selector, inputName, allInputBoundStreams(sets), sets)
	}
}

func inputSetIndex(sets []inputStreamSet, name string) int {
	for i := range sets {
		if sets[i].name == name {
			return i
		}
	}
	return 0
}

func allInputBoundStreams(sets []inputStreamSet) []inputBoundStream {
	out := make([]inputBoundStream, 0, len(sets))
	for i := range sets {
		for k := range sets[i].streams {
			out = append(out, inputBoundStream{
				inputIndex: i,
				inputName:  sets[i].name,
				domain:     sets[i].domain,
				stream:     sets[i].streams[k],
			})
		}
	}
	return out
}

func multiInputStreamSelectionError(code errcode.Code, selector av.StreamSelector, inputName string, candidates []inputBoundStream, sets []inputStreamSet) error {
	reason := "no stream across the inputs matches " + readableSelector(selector)
	if code == errcode.StreamAmbiguous {
		reason = "multiple streams across the inputs match " + readableSelector(selector)
	}
	if inputName != "" {
		reason = reason + " on input " + strconv.Quote(inputName)
	}
	details := make([]string, 0, len(candidates))
	for i := range candidates {
		details = append(details, "input="+candidates[i].inputName+" "+streamDiagnostic(candidates[i].stream, i))
	}
	return &BuildError{
		Code:        code,
		Operation:   "select stream",
		Node:        selectorDetail(selector),
		Reason:      reason,
		Details:     details,
		Suggestions: multiInputSelectionSuggestions(selector, candidates),
		Cause:       ErrUnsupportedBuild,
	}
}

func multiInputSelectionSuggestions(selector av.StreamSelector, candidates []inputBoundStream) []string {
	call, recipeCall := streamOptionCall(selector)
	suggestions := make([]string, 0, 3)
	seen := make(map[string]struct{}, len(candidates))
	for i := range candidates {
		name := candidates[i].inputName
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		if recipeCall {
			suggestions = append(suggestions, call+"(goav.InputName("+strconv.Quote(name)+"))")
		} else {
			suggestions = append(suggestions, "narrow the selection to input "+strconv.Quote(name))
		}
		if len(suggestions) == 2 {
			break
		}
	}
	for i := range candidates {
		if candidates[i].stream.ID != "" {
			if recipeCall {
				suggestions = append(suggestions, call+"(goav.StreamID("+strconv.Quote(string(candidates[i].stream.ID))+"))")
			} else {
				suggestions = append(suggestions, "select stream id "+strconv.Quote(string(candidates[i].stream.ID)))
			}
			break
		}
	}
	if len(suggestions) == 0 {
		suggestions = append(suggestions, "give each input a stable name with goav.Source(name, ...) or the goav.Name(...) option")
	}
	return suggestions
}

func unknownInputNameError(selector av.StreamSelector, inputName string, sets []inputStreamSet) error {
	details := make([]string, 0, len(sets))
	for i := range sets {
		details = append(details, "input="+sets[i].name)
	}
	return &BuildError{
		Code:      errcode.InputUnknown,
		Operation: "select stream",
		Node:      selectorDetail(selector),
		Reason:    "no job input is named " + strconv.Quote(inputName),
		Details:   details,
		Suggestions: []string{
			"use one of the listed input names with goav.InputName(...)",
			"name inputs with goav.Source(name, ...), goav.FileInput(name, ...), or the goav.Name(...) option",
		},
		Cause: ErrUnsupportedBuild,
	}
}

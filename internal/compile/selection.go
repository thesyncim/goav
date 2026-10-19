package compile

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/shape"
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

// inputStreamSet is one job input with its name and the streams it is known to
// carry. It is the unit of multi-input stream selection: a stream chain selects
// across the union of all sets, and goav.InputName(...) narrows the search to
// one set.
type inputStreamSet struct {
	Name    string
	Domain  shape.MediaDomain
	Streams []av.Stream
	Known   bool
}

// jobInputStreamSets builds one inputStreamSet per job input from what the
// compile state knows: declared custom-source shapes, probe results, and live
// (realtime) input codec intent. Inputs whose streams cannot be known yet (for
// example a file destination before probing) stay known=false so selection
// validation skips them instead of guessing.
func jobInputStreamSets(inputs []recipeir.Input, probes []format.ProbeResult) []inputStreamSet {
	sets := make([]inputStreamSet, 0, len(inputs))
	for i := range inputs {
		set := inputStreamSet{
			Name:   recipeIRInputName(inputs[i], i),
			Domain: shape.DomainPacket,
		}
		switch {
		case inputs[i].SourceShape.Domain != "":
			spec, _ := recipeIRInputSourceShape(inputs[i])
			set.Domain = spec.Domain
			set.Streams = []av.Stream{recipeIRInputDeclaredStream(inputs[i], spec)}
			set.Known = true
		case i < len(probes) && len(probes[i].Streams) != 0:
			set.Streams = append([]av.Stream(nil), probes[i].Streams...)
			set.Known = true
		case inputs[i].Realtime && inputs[i].Codec.ID != "":
			if stream, ok := liveInputStream(inputs[i], i); ok {
				set.Streams = []av.Stream{stream}
				set.Known = true
			}
		}
		sets = append(sets, set)
	}
	return sets
}

func recipeIRInputDeclaredStream(input recipeir.Input, spec shape.Spec) av.Stream {
	shape := recipeir.NormalizeSourceShape(input.Name, spec)
	stream := av.Stream{
		ID:   shape.StreamID,
		Type: shape.MediaKind,
		Codec: av.CodecParameters{
			ID:           shape.Codec,
			Type:         shape.MediaKind,
			SampleRate:   shape.SampleRate,
			Channels:     shape.Channels,
			Width:        shape.Width,
			Height:       shape.Height,
			PixelFormat:  shape.PixelFormat,
			SampleFormat: shape.SampleFormat,
		},
		Name: string(shape.StreamID),
	}
	recipeir.FillStreamCodecParameters(&stream, input.Codec)
	return stream
}

func recipeIRInputName(input recipeir.Input, index int) string {
	return cmp.Or(input.Name, input.URI, fmt.Sprintf("input-%d", index))
}

// inputBoundStream is one selection candidate with its input attribution.
type inputBoundStream struct {
	InputIndex int
	InputName  string
	Domain     shape.MediaDomain
	Stream     av.Stream
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
			if sets[i].Name == inputName {
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
		if !set.Known {
			unknown = true
			continue
		}
		for k := range set.Streams {
			if !StreamMatchesSelector(set.Streams[k], selector) {
				continue
			}
			matches = append(matches, inputBoundStream{
				InputIndex: inputSetIndex(sets, set.Name),
				InputName:  set.Name,
				Domain:     set.Domain,
				Stream:     set.Streams[k],
			})
		}
	}
	switch {
	case len(matches) == 1:
		selected := matches[0]
		if selected.Domain != shape.DomainFrame && selected.Stream.Codec.ID == "" {
			// Mirror selectDecodeStream's codec requirement for packet media.
			_, err := SelectDecodeStream([]av.Stream{selected.Stream}, selector)
			return inputBoundStream{}, false, err
		}
		return selected, true, nil
	case len(matches) > 1:
		return inputBoundStream{}, false, multiInputStreamSelectionError(builderr.StreamAmbiguousCode, selector, inputName, matches)
	case unknown:
		return inputBoundStream{}, false, nil
	default:
		return inputBoundStream{}, false, multiInputStreamSelectionError(builderr.StreamMissingCode, selector, inputName, allInputBoundStreams(sets))
	}
}

func inputSetIndex(sets []inputStreamSet, name string) int {
	for i := range sets {
		if sets[i].Name == name {
			return i
		}
	}
	return 0
}

func allInputBoundStreams(sets []inputStreamSet) []inputBoundStream {
	out := make([]inputBoundStream, 0, len(sets))
	for i := range sets {
		for k := range sets[i].Streams {
			out = append(out, inputBoundStream{
				InputIndex: i,
				InputName:  sets[i].Name,
				Domain:     sets[i].Domain,
				Stream:     sets[i].Streams[k],
			})
		}
	}
	return out
}

func multiInputStreamSelectionError(code errcode.Code, selector av.StreamSelector, inputName string, candidates []inputBoundStream) error {
	reason := "no stream across the inputs matches " + ReadableSelector(selector)
	if code == builderr.StreamAmbiguousCode {
		reason = "multiple streams across the inputs match " + ReadableSelector(selector)
	}
	if inputName != "" {
		reason = reason + " on input " + strconv.Quote(inputName)
	}
	details := make([]string, 0, len(candidates))
	for i := range candidates {
		details = append(details, "input="+candidates[i].InputName+" "+StreamDiagnostic(candidates[i].Stream, i))
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(code),
		Code:      code,
		Operation: "select stream",
		Node:      selectorDetail(selector),
		Reason:    reason,
		Details:   builderr.DetailLines(details),
		Fixes:     builderr.Fixes(multiInputSelectionSuggestions(selector, candidates)),
		Cause:     builderr.ErrUnsupportedBuild,
	}
}

func multiInputSelectionSuggestions(selector av.StreamSelector, candidates []inputBoundStream) []string {
	call, recipeCall := streamOptionCall(selector)
	suggestions := make([]string, 0, 3)
	seen := make(map[string]struct{}, len(candidates))
	for i := range candidates {
		name := candidates[i].InputName
		if name == "" {
			continue
		}
		if _, ok := seen[name]; ok {
			continue
		}
		seen[name] = struct{}{}
		if recipeCall {
			suggestions = append(suggestions, "select from the input named "+strconv.Quote(name)+" directly: input"+call+"()")
		} else {
			suggestions = append(suggestions, "narrow the selection to input "+strconv.Quote(name))
		}
		if len(suggestions) == 2 {
			break
		}
	}
	for i := range candidates {
		if candidates[i].Stream.ID != "" {
			if recipeCall {
				suggestions = append(suggestions, call+"(goav.StreamID("+strconv.Quote(string(candidates[i].Stream.ID))+"))")
			} else {
				suggestions = append(suggestions, "select stream id "+strconv.Quote(string(candidates[i].Stream.ID)))
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
		details = append(details, "input="+sets[i].Name)
	}
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.InputUnknownCode),
		Code:      builderr.InputUnknownCode,
		Operation: "select stream",
		Node:      selectorDetail(selector),
		Reason:    "no job input is named " + strconv.Quote(inputName),
		Details:   builderr.DetailLines(details),
		Fixes: builderr.Fixes([]string{
			"select from one of the listed inputs directly: input.Audio() or input.Video()",
			"name inputs with goav.Source(name, ...), goav.FileInput(name, ...), or the goav.Name(...) option",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

// Stream selection options and the refusals the stream validator raises.

package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
)

// StreamOption narrows the stream selection made by the input selectors
// ([InputSpec.Audio], [InputSpec.Video], [InputSpec.Stream]). Build one with
// [StreamID] or [StreamIndex]. It is sealed: only goav option constructors
// implement it.
type StreamOption interface {
	applyStream(*streamSelectConfig)
}

type streamSelectConfig struct {
	selector av.StreamSelector
}

func newStreamSelectConfig(media av.MediaType, options ...StreamOption) streamSelectConfig {
	config := streamSelectConfig{selector: av.StreamSelector{Type: media}}
	for i := range options {
		if options[i] != nil {
			options[i].applyStream(&config)
		}
	}
	return config
}

type streamIDOption av.StreamID

func (o streamIDOption) applyStream(config *streamSelectConfig) {
	config.selector.ID = av.StreamID(o)
}

type streamIndexOption int

func (o streamIndexOption) applyStream(config *streamSelectConfig) {
	config.selector.Index = int(o)
	config.selector.UseIndex = true
}

// StreamID narrows a stream selection to the stream with the given id, as
// probed or declared by the input.
func StreamID(id av.StreamID) StreamOption {
	return streamIDOption(id)
}

// StreamIndex narrows a stream selection to the stream at the given probe
// index (0-based).
func StreamIndex(index int) StreamOption {
	return streamIndexOption(index)
}

// joinLayerNilError refuses a nil join argument: every argument is a Stream,
// a Stream placed with At, or a join option.
func joinLayerNilError(join string, index int) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamInvalidCode),
		Code:      streamInvalidCode,
		Operation: "build recipe",
		Node:      join,
		Reason:    fmt.Sprintf("%s input %d is nil", join, index+1),
		fixes: buildErrorFixes([]string{
			"pass every join argument as a Stream, a placed Stream, or a join option: goav.Composite(cam, goav.At(screen, 640, 0), goav.ByPTS())",
		}),
		cause: errUnsupportedBuild,
	}
}

// joinAtOutsideCompositeError refuses At outside a Composite: only a canvas
// places its inputs.
func joinAtOutsideCompositeError(join string, index int) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamInvalidCode),
		Code:      streamInvalidCode,
		Operation: "build recipe",
		Node:      join,
		Reason:    fmt.Sprintf("%s input %d is placed with At, which positions Composite inputs only", join, index+1),
		fixes: buildErrorFixes([]string{
			"pass the Stream itself: goav.Mix(a, b)",
			"place inputs on a canvas with goav.Composite(cam, goav.At(screen, 640, 0))",
		}),
		cause: errUnsupportedBuild,
	}
}

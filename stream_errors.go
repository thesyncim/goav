// Stream selection options and the refusals the stream validator raises.

package goav

import (
	"fmt"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
)

// StreamOption narrows the stream selection made by the input selectors
// (input.Audio, input.Video, input.Stream); construct values with StreamID or
// StreamIndex. It is sealed: the config it acts on is unexported, so those
// constructors are the only way to build one.
type StreamOption func(*streamSelectConfig)

type streamSelectConfig struct {
	selector av.StreamSelector
}

func newStreamSelectConfig(media av.MediaType, options ...StreamOption) streamSelectConfig {
	config := streamSelectConfig{selector: av.StreamSelector{Type: media}}
	for i := range options {
		if options[i] != nil {
			options[i](&config)
		}
	}
	return config
}

// StreamID narrows a stream selection to the stream with the given id, as
// probed or declared by the input.
func StreamID(id av.StreamID) StreamOption {
	return func(config *streamSelectConfig) {
		config.selector.ID = id
	}
}

// StreamIndex narrows a stream selection to the stream at the given probe
// index (0-based).
func StreamIndex(index int) StreamOption {
	return func(config *streamSelectConfig) {
		config.selector.Index = index
		config.selector.UseIndex = true
	}
}

// joinLayerNilError refuses a nil Composite layer: every layer is a Stream
// or a Stream placed with At.
func joinLayerNilError(join string, index int) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(streamInvalidCode),
		Code:      streamInvalidCode,
		Operation: "build recipe",
		Node:      join,
		Reason:    fmt.Sprintf("%s input %d is nil", join, index+1),
		fixes: buildErrorFixes([]string{
			"pass every Composite layer as a Stream or a placed Stream: goav.Composite(cam, goav.At(screen, 640, 0))",
		}),
		cause: errUnsupportedBuild,
	}
}

package native_test

import (
	"testing"

	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/native"
	"github.com/thesyncim/goav/native/codectest"
)

// Each backend gets the whole conformance suite in one line.

func TestOpenH264(t *testing.T) { codectest.Run(t, native.OpenH264()) }

func TestAudioToolbox(t *testing.T) { codectest.Run(t, native.AudioToolbox()) }

func TestVideoToolbox(t *testing.T) { codectest.Run(t, native.VideoToolbox()) }

// TestAllNative runs every native backend together, so the matrix decodes each
// one's output with the others: VideoToolbox through OpenH264 and back.
func TestAllNative(t *testing.T) {
	codectest.Run(t, native.VideoToolbox(), native.OpenH264(), native.AudioToolbox())
}

// TestBundle runs the same suite on the pure-Go codecs, which register
// through the same registry.
func TestBundle(t *testing.T) { codectest.Run(t, bundle.Codecs()) }

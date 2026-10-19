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

// TestBundle runs the same suite on the pure-Go codecs, which register
// through the same registry.
func TestBundle(t *testing.T) { codectest.Run(t, bundle.Codecs()) }

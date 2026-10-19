package libcodec_test

import (
	"testing"

	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/libcodec"
	"github.com/thesyncim/goav/libcodec/codectest"
)

// Each backend gets the whole conformance suite in one line.

func TestOpenH264(t *testing.T) { codectest.Run(t, libcodec.OpenH264()) }

func TestAudioToolbox(t *testing.T) { codectest.Run(t, libcodec.AudioToolbox()) }

func TestVideoToolbox(t *testing.T) { codectest.Run(t, libcodec.VideoToolbox()) }

// TestAllLibcodec runs every libcodec backend together, so the matrix decodes each
// one's output with the others: VideoToolbox through OpenH264 and back.
func TestAllLibcodec(t *testing.T) {
	codectest.Run(t, libcodec.VideoToolbox(), libcodec.OpenH264(), libcodec.AudioToolbox())
}

// TestBundle runs the same suite on the pure-Go codecs, which register
// through the same registry.
func TestBundle(t *testing.T) { codectest.Run(t, bundle.Codecs()) }

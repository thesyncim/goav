package codectest

import (
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

// runMatrix decodes every encoder's output with every decoder of the same
// codec, so backends that pass alone also interoperate. Pairs of two
// reference codecs are left to their own suites.
func runMatrix(t *testing.T, under, refs []backend) {
	var all []backend
	seen := map[string]bool{}
	for _, b := range append(append([]backend(nil), under...), refs...) {
		if key := b.name() + "/" + string(b.mode); !seen[key] {
			seen[key] = true
			all = append(all, b)
		}
	}
	tested := map[string]bool{}
	for _, b := range under {
		tested[b.name()] = true
	}
	for _, e := range all {
		if e.mode != codec.ModeEncode {
			continue
		}
		var out encoded
		encodedOnce := false
		for _, d := range decodersOf(e.desc.ID, under, refs) {
			if !tested[e.name()] && !tested[d.name()] {
				continue
			}
			t.Run(e.name()+"->"+d.desc.Backend.Name, func(t *testing.T) {
				if !encodedOnce {
					var ok bool
					if e.desc.Type == av.MediaAudio {
						out, ok = encodeAudio(t, e)
					} else {
						out, ok = encodeVideo(t, e, withBitrate(e, testBitrate))
					}
					if !ok {
						return
					}
					encodedOnce = true
				}
				frames := decodeAll(t, d, out.stream, out.packets)
				if e.desc.Type == av.MediaAudio {
					checkAudio(t, d, sourceTimeline(), frames, minSNR)
				} else {
					checkDecoded(t, d, frames, minPSNR)
				}
			})
		}
	}
}

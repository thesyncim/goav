package libcodec

import (
	"github.com/thesyncim/goav/libcodec/internal/audiotoolbox"
	"github.com/thesyncim/goav/libcodec/internal/openh264"
	"github.com/thesyncim/goav/libcodec/internal/videotoolbox"
	"github.com/thesyncim/goav/libcodec/kit"
	"github.com/thesyncim/goav/runconfig"
)

// Option configures one backend.
type Option func(*options)

type options struct {
	library string
}

// Library loads the backend's library from path, which must be absolute,
// instead of the system install: a downloaded Cisco OpenH264 binary, say.
func Library(path string) Option {
	return func(o *options) { o.library = path }
}

// OpenH264 registers Cisco's OpenH264 as an h264 encoder and decoder
// (h264/openh264, lib). A libopenh264 from 2.3 up to 3 binds when the
// runtime is built; without it the backend registers as not-installed and
// selection passes it over.
func OpenH264(opts ...Option) runconfig.Option {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	c := openh264.Codec()
	if o.library != "" {
		c.Libraries[0].Path = o.library
	}
	return kit.Register(c)
}

// AudioToolbox registers Apple's AAC-LC encoder and decoder
// (aac/audiotoolbox, lib) on macOS; elsewhere it registers as
// not-installed and selection passes it over.
func AudioToolbox() runconfig.Option {
	return kit.Register(audiotoolbox.Codec())
}

// VideoToolbox registers the hardware H.264 and HEVC encoders and decoders
// of macOS (h264/videotoolbox and hevc/videotoolbox, hardware). Each checks
// for its hardware when the runtime is built; a Mac without it, or another
// OS, registers it not-installed and selection passes it over.
func VideoToolbox() runconfig.Option {
	return kit.Register(videotoolbox.Codecs()...)
}

// Declarations are the backends this module offers, as plain values: their
// identities, knobs, licenses, and releases, read without loading anything.
// Installers, lockfiles, and license pages read them.
func Declarations() []kit.Codec {
	return append([]kit.Codec{openh264.Codec(), audiotoolbox.Codec()}, videotoolbox.Codecs()...)
}

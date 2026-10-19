package native

import (
	"github.com/thesyncim/goav/native/internal/openh264"
	"github.com/thesyncim/goav/native/kit"
	"github.com/thesyncim/goav/runconfig"
)

// Option configures one backend.
type Option func(*options)

type options struct {
	library string
}

// Library loads the backend's library from path instead of the platform's
// default names: a downloaded Cisco OpenH264 binary, say.
func Library(path string) Option {
	return func(o *options) { o.library = path }
}

func apply(opts []Option) options {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// OpenH264 registers Cisco's OpenH264 as an h264 encoder and decoder
// (h264/openh264, native). libopenh264 2.3 or later loads on first use.
func OpenH264(opts ...Option) runconfig.Option {
	return kit.Register(openh264.Codec(apply(opts).library))
}

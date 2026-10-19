package native

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/thesyncim/goav/native/internal/openh264"
	"github.com/thesyncim/goav/native/kit"
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

func apply(opts []Option) options {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o
}

// resolve picks the library a backend binds: the explicit path, else the
// first system path that exists. Paths are absolute, so loading never
// searches the working directory. macOS frameworks live in the dyld shared
// cache, not on disk, so their paths are taken as given.
func resolve(explicit string, system []string) string {
	if explicit != "" {
		return explicit
	}
	for _, path := range system {
		if !filepath.IsAbs(path) {
			continue
		}
		if runtime.GOOS == "darwin" && strings.HasPrefix(path, "/System/Library/") {
			return path
		}
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return ""
}

// OpenH264 registers Cisco's OpenH264 as an h264 encoder and decoder
// (h264/openh264, native). libopenh264 2.3 or later binds when the runtime
// is built; without it the backend registers as not-installed and
// selection passes it over.
func OpenH264(opts ...Option) runconfig.Option {
	return kit.Register(openh264.Codec(resolve(apply(opts).library, openh264.SystemPaths())))
}

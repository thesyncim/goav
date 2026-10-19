package kit

import (
	"slices"

	"github.com/thesyncim/goav/runconfig"
)

// Release is one published build of a backend's library: its version and,
// per platform, where it is fetched from and the SHA-256 it must have. A
// backend lists the releases it can bind in Codec.Releases, inside the ABI
// window its Version check accepts; installers and lockfiles read them
// through Releases, so a backend from any module is installed like the ones
// goav ships.
type Release struct {
	Version string
	Files   []ReleaseFile
}

// ReleaseFile is a release's library for one platform (GOOS/GOARCH, such as
// linux/amd64): the URL to fetch, the SHA-256 of what it serves, and the
// library's file name once unpacked (a .bz2 or .gz URL is unpacked).
type ReleaseFile struct {
	Platform string
	URL      string
	SHA256   string
	Name     string
}

// declared remembers every backend Register saw, by codec and backend, for
// Releases.
var declared = map[string]*Codec{}

// Releases returns the releases of every kit backend the options register,
// by backend identity (h264/openh264).
func Releases(options ...runconfig.Option) (map[string][]Release, error) {
	config, err := runconfig.NewConfig(options...)
	if err != nil {
		return nil, err
	}
	out := map[string][]Release{}
	for _, desc := range config.Codecs.Descriptors() {
		key := string(desc.ID) + "/" + desc.Backend.Name
		declaredMu.Lock()
		c := declared[key]
		declaredMu.Unlock()
		if c != nil && len(c.Releases) != 0 {
			out[key] = slices.Clone(c.Releases)
		}
	}
	return out, nil
}

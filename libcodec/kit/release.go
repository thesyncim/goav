package kit

import (
	"strconv"
	"strings"
)

// Release is one published build of a backend's library: its version and,
// per platform, where it is fetched from and the SHA-256 it must have. A
// backend lists in Codec.Releases the releases inside its ABI window that it
// can bind; installers and lockfiles read them from the declaration, so a
// backend from any module installs like the ones goav ships.
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

// Window is a range of library versions, From inclusive and Before
// exclusive, compared number by number (2.9 < 2.10): the releases whose
// structure layouts a backend mirrors. An empty bound is open.
type Window struct {
	From   string
	Before string
}

// Contains reports whether version falls in the window.
func (w Window) Contains(version string) bool {
	return (w.From == "" || compareVersions(version, w.From) >= 0) &&
		(w.Before == "" || compareVersions(version, w.Before) < 0)
}

func (w Window) String() string {
	switch {
	case w.From == "" && w.Before == "":
		return "any version"
	case w.Before == "":
		return w.From + " or later"
	case w.From == "":
		return "before " + w.Before
	}
	return w.From + " or later, before " + w.Before
}

// compareVersions orders dotted versions by their numbers; a missing number
// is zero, so 3 equals 3.0.0.
func compareVersions(a, b string) int {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := range max(len(as), len(bs)) {
		x, y := versionPart(as, i), versionPart(bs, i)
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

func versionPart(parts []string, i int) uint64 {
	if i >= len(parts) {
		return 0
	}
	n, _ := strconv.ParseUint(parts[i], 10, 64)
	return n
}

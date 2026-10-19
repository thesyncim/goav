package kit

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// find returns the first of paths a library can load from: an absolute path
// to a file that exists, or on macOS a system library or framework, which
// lives in the dyld shared cache rather than on disk. It returns "" when none
// does, which Library reports as not installed. Relative paths are skipped,
// so loading never searches the working directory.
func find(paths ...string) string {
	for _, path := range paths {
		if !filepath.IsAbs(path) {
			continue
		}
		if runtime.GOOS == "darwin" && (strings.HasPrefix(path, "/System/Library/") || strings.HasPrefix(path, "/usr/lib/")) {
			return path
		}
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path
		}
	}
	return ""
}

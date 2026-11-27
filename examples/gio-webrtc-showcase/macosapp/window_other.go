//go:build !darwin || (!amd64 && !arm64)

package macosapp

import "errors"

// Run returns ErrUnsupported on non-macOS platforms; callers fall back to headless mode.
func Run(title, url string, width, height float64) error {
	return errors.New("macosapp: native window requires macOS (run with -headless)")
}

//go:build !((darwin && arm64) || (linux && arm64))

package goaac

// targetSupported mirrors the goaac backend's build tags: elsewhere the
// backend is a stub whose decoder open fails with ErrNativeUnavailable.
const targetSupported = false

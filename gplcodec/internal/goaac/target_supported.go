//go:build (darwin && arm64) || (linux && arm64)

package goaac

// targetSupported mirrors the goaac backend's build tags: the pure-Go FAAD2
// port is generated for these targets only.
const targetSupported = true

//go:build !darwin || (!amd64 && !arm64)

package main

func ensureMacOSAppBundle(bool) {}

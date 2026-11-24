//go:build !windows

package main

import (
	"os"
	"syscall"
)

func stopSignals() []os.Signal {
	return []os.Signal{os.Interrupt, syscall.SIGTERM}
}

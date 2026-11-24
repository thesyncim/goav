//go:build windows

package main

import "os"

func stopSignals() []os.Signal {
	return []os.Signal{os.Interrupt}
}

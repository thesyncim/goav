//go:build (!darwin && !linux && !windows) || ((linux || windows || darwin) && !amd64 && !arm64)

package cameraav

import "context"

func devices(context.Context) ([]Device, error)                 { return nil, ErrUnsupported }
func openDevice(context.Context, Config) (captureDevice, error) { return nil, ErrUnsupported }
func requestAccess(context.Context) error                       { return ErrUnsupported }

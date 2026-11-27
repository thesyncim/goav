//go:build !darwin || (!amd64 && !arm64)

package screenav

import "context"

func displays(context.Context) ([]Display, error)                { return nil, ErrUnsupported }
func hasAccess() bool                                            { return false }
func requestAccess() bool                                        { return false }
func openDisplay(context.Context, Config) (captureDevice, error) { return nil, ErrUnsupported }

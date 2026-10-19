//go:build goav_noplay

package main

import (
	"context"
	"errors"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/launch"
)

// A goav_noplay build leaves the window out, and with it the windowing and
// audio libraries: on Linux the binary is then fully static again.
type noWindow struct{}

func newWindow() window { return noWindow{} }

var errNoPlay = errors.New("this goav was built with -tags goav_noplay: play is unavailable; route to file or null, or rebuild without the tag")

func (noWindow) Element() launch.Element {
	return launch.Sink("play", "play in a window (unavailable: built with goav_noplay)", func(struct{}) (goav.Destination, error) {
		return goav.Destination{}, errNoPlay
	})
}

func (noWindow) Used() bool                               { return false }
func (noWindow) Clock() av.Clock                          { return av.MonotonicClock() }
func (noWindow) Run(context.Context, goav.LiveTask) error { return errNoPlay }
func (noWindow) summary() *playStats                      { return nil }

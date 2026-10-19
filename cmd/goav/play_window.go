//go:build !goav_noplay

package main

import "github.com/thesyncim/goav/player"

type playerWindow struct{ *player.Window }

func newWindow() window { return playerWindow{player.New()} }

func (w playerWindow) summary() *playStats {
	s := w.Stats()
	return &playStats{
		PositionMS:    ms(s.Position.Seconds()),
		Presented:     s.Presented,
		Dropped:       s.Dropped,
		Underruns:     s.Underruns,
		MaxAVOffsetMS: ms(s.MaxAVOffset.Seconds()),
	}
}

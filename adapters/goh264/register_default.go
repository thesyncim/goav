//go:build !goav_goh264

package goh264

import "github.com/thesyncim/goav/codec"

func Register(registry *codec.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterDescriptor(Descriptor())
}

//go:build !goav_goav1

package goav1

import "github.com/thesyncim/goav/codec"

func Register(registry *codec.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterDescriptor(Descriptor())
}

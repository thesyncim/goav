//go:build !goav_govpx

package govpx

import "github.com/thesyncim/goav/codec"

func Register(registry *codec.SimpleRegistry) {
	if registry == nil {
		return
	}
	descriptors := Descriptors()
	for i := range descriptors {
		registry.RegisterDescriptor(descriptors[i])
	}
}

package compile

import (
	"github.com/thesyncim/goav/shape"
)

// TapRef is a typed handle to a stable media attach point.
type TapRef struct {
	Name   string
	Domain shape.MediaDomain
}

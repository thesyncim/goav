package compile

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
)

type EncodeRequest struct {
	Name     string
	Selector av.StreamSelector
	Config   codec.EncodeConfig
}

type DecodeRequest struct {
	Selector av.StreamSelector
	Config   codec.Spec
}

package goav_test

import (
	"context"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
)

// normalizeVoice is the kind of helper the exported Chain type allows: a
// reusable step sequence that takes and returns a chain.
func normalizeVoice(c *goav.Chain) *goav.Chain {
	return c.Decode().Resample(16_000, codec.Mono)
}

func TestChainHelperComposes(t *testing.T) {
	out := goavtest.NewCollector()
	input := goavtest.Packets(av.CodecOpus, av.Packet{Payload: av.Buffer{Bytes: []byte{1}}})
	err := normalizeVoice(goav.From(input).Audio()).
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(context.Background())
	if err != nil {
		t.Fatalf("helper chain: %v", err)
	}
}

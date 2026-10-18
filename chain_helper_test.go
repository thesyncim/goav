package goav_test

import (
	"context"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
)

// normalizeVoice is the kind of helper Stream values allow: a reusable step
// sequence that takes and returns a Stream.
func normalizeVoice(s goav.Stream) goav.Stream {
	return s.Decode().Resample(16_000, codec.Mono)
}

func TestChainHelperComposes(t *testing.T) {
	out := goavtest.NewCollector()
	input := goavtest.Packets(av.CodecOpus, av.Packet{Payload: av.Buffer{Bytes: []byte{1}}})
	if err := goavtest.Runtime().Run(context.Background(), normalizeVoice(input.Audio()).To(out.Sink())); err != nil {
		t.Fatalf("helper chain: %v", err)
	}
}

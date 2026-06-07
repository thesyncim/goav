package matroska

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func FuzzDemuxerMalformedInputs(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0x1a, 0x45, 0xdf, 0xa3})
	f.Add(makeMatroskaData(f, 1))
	f.Add(makeLacedMatroskaData(f, simpleBlockLacingXiph, [][]byte{{1}, {2}}, 20_000_000))

	f.Fuzz(func(t *testing.T, data []byte) {
		demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{
			MaxElementSize: 1 << 20,
			MaxLaceFrames:  16,
			MaxLacePayload: 1 << 16,
		})
		if err != nil {
			return
		}

		packet := Packet{
			Data:                   make([]byte, 0, 1<<16),
			ReferenceBlockTimeNS:   make([]int64, 0, 4),
			CodecState:             make([]byte, 0, 1024),
			BlockAdditions:         make([]BlockAddition, 0, 4),
			UnknownClusterElements: make([]UnknownElement, 0, 4),
		}
		for i := 0; i < 16; i++ {
			err := demuxer.ReadPacket(&packet)
			switch {
			case err == nil:
				packet.Reset()
			case errors.Is(err, io.EOF):
				return
			default:
				return
			}
		}
	})
}

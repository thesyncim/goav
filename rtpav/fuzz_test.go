package rtpav

import (
	"context"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
)

const fuzzMaxFrame = 1 << 12

// FuzzDepacketizers feeds arbitrary RTP payloads, split into a run of
// sequential packets, to every depacketizer. None may panic; errors are fine;
// whatever comes out stays bounded: no more packets than were pushed, and a
// video frame never exceeds the configured frame cap.
func FuzzDepacketizers(f *testing.F) {
	// Real payloads from the depacketizer tests, one per codec.
	f.Add(uint8(0), []byte{0x10, 0x00, 0x11, 0x22})                               // VP8 keyframe descriptor
	f.Add(uint8(0), []byte{0x90, 0x80, 0x01, 0x10, 0x00, 0x11, 0x22})             // VP8 with extension
	f.Add(uint8(1), []byte{0x8c, 0x82, 0x49, 0x83, 0x42, 0x00, 0x00})             // VP9 start
	f.Add(uint8(2), []byte{0x10, 0x0a, 0x0b, 0x00, 0x01})                         // AV1 OBU aggregation
	f.Add(uint8(3), []byte{0x65, 0xaa, 0xbb})                                     // H.264 IDR single NALU
	f.Add(uint8(3), []byte{0x18, 0x00, 0x02, 0x67, 0x42, 0x00, 0x02, 0x68, 0xce}) // STAP-A
	f.Add(uint8(3), []byte{0x7c, 0x85, 0x01, 0x02, 0x03})                         // FU-A start
	f.Add(uint8(4), []byte{0xfc, 0x01, 0x02, 0x03})                               // Opus
	f.Add(uint8(0), []byte{})

	f.Fuzz(func(t *testing.T, kind uint8, data []byte) {
		ctx := context.Background()
		opts := []VideoDepacketizerOption{WithMaxVideoFrameSize(fuzzMaxFrame)}
		var d Depacketizer
		clock := uint32(90000)
		switch kind % 5 {
		case 0:
			d = NewVP8Depacketizer(videoTestStream(av.CodecVP8), opts...)
		case 1:
			d = NewVP9Depacketizer(videoTestStream(av.CodecVP9), opts...)
		case 2:
			d = NewAV1Depacketizer(videoTestStream(av.CodecAV1), opts...)
		case 3:
			d = NewH264Depacketizer(videoTestStream(av.CodecH264), opts...)
		default:
			d = NewOpusDepacketizer(av.Stream{ID: "a", Type: av.MediaAudio})
			clock = 48000
		}
		opus := kind%5 == 4
		payload := PayloadCodec{ClockRate: clock}
		// Cut the input into packets: a length byte, then that many payload bytes.
		pushed, emitted := 0, 0
		var seq uint16
		for len(data) > 0 && pushed < 64 {
			n := int(data[0])%64 + 1
			data = data[1:]
			if n > len(data) {
				n = len(data)
			}
			chunk := data[:n]
			data = data[n:]
			pkt := &rtp.Packet{Header: rtp.Header{SequenceNumber: seq, Timestamp: uint32(pushed/3) * 3000, Marker: pushed%3 == 2}, Payload: chunk}
			seq++
			pushed++
			out := DepacketizeResult{Packets: make([]av.Packet, 0, 4)}
			if err := d.PushInto(ctx, pkt, payload, &out); err != nil {
				continue
			}
			if len(out.Packets) > 4 {
				t.Fatalf("%d packets overflow the result's capacity", len(out.Packets))
			}
			for _, p := range out.Packets {
				emitted++
				size := len(p.Payload.Bytes)
				if opus && size > len(chunk) {
					t.Fatalf("opus packet grew: %d > %d", size, len(chunk))
				}
				if !opus && size > fuzzMaxFrame {
					t.Fatalf("frame of %d bytes exceeds the %d cap", size, fuzzMaxFrame)
				}
			}
		}
		if emitted > pushed {
			t.Fatalf("%d packets out of %d pushed", emitted, pushed)
		}
		var out DepacketizeResult
		_ = d.FlushInto(ctx, &out)
	})
}

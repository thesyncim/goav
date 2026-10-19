package ts

import (
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/h264"
)

// syntheticAV builds n packets of an H.264 video stream (PID 256, 25 fps,
// a keyframe every 10 frames carrying its parameter sets, B-frame reorder)
// and an AAC stream (PID 257, 48 kHz, 1024-sample frames), interleaved by
// DTS from start ticks of 90 kHz.
func syntheticAV(t testing.TB, start int64, n int) ([]av.Stream, []av.Packet) {
	t.Helper()
	sps := []byte{0x67, 0x42, 0xc0, 0x1e, 0xda, 0x02, 0x80, 0xbf, 0xe5, 0x84, 0x00, 0x00, 0x03, 0x00, 0x04, 0x00, 0x00, 0x03, 0x00, 0xca, 0x3c, 0x58, 0xba, 0x80}
	pps := []byte{0x68, 0xce, 0x0f, 0x2c, 0x80}
	streams := []av.Stream{
		{ID: "256", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo}, TimeBase: av.TimeBase{Num: 1, Den: 90000}},
		{ID: "257", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecAAC, Type: av.MediaAudio, SampleRate: 48000, Channels: 2,
			ExtraData: av.Buffer{Bytes: aacConfig{objectType: 2, rateIndex: 3, channels: 2}.audioSpecificConfig()}}, TimeBase: av.TimeBase{Num: 1, Den: 48000}},
	}
	var packets []av.Packet
	video, audio := 0, 0
	for len(packets) < n {
		vdts := start + int64(video)*3600 - 3600
		adts := start*48000/90000 + int64(audio)*1024
		if vdts*48000 <= adts*90000 {
			payload := []byte{0, 0, 0, 1}
			key := video%10 == 0
			if key {
				payload = append(append(append(append(payload, sps...), 0, 0, 0, 1), pps...), 0, 0, 1, 0x65)
			} else {
				payload = append(payload, 0x41)
			}
			for i := range 100 + video*37%900 {
				payload = append(payload, byte(i*7+video))
			}
			pts := vdts + 3600
			if !key && video%2 == 1 {
				pts += 3600
			}
			packets = append(packets, av.Packet{StreamID: "256", Type: av.MediaVideo, Payload: av.Buffer{Bytes: payload}, Keyframe: key,
				PTS: av.Timestamp{Value: pts, Base: av.TimeBase{Num: 1, Den: 90000}}, DTS: av.Timestamp{Value: vdts, Base: av.TimeBase{Num: 1, Den: 90000}}})
			video++
			continue
		}
		payload := make([]byte, 200+audio%50)
		for i := range payload {
			payload[i] = byte(i + audio)
		}
		ts := av.Timestamp{Value: adts, Base: av.TimeBase{Num: 1, Den: 48000}}
		packets = append(packets, av.Packet{StreamID: "257", Type: av.MediaAudio, Payload: av.Buffer{Bytes: payload}, Keyframe: true, PTS: ts, DTS: ts})
		audio++
	}
	return streams, packets
}

// avcC builds the decoder configuration record of Annex B parameter sets.
func avcC(sets []byte) ([]byte, error) { return h264.ConfigFromAnnexB(sets) }

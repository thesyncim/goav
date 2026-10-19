package ts

import (
	"bytes"
	"testing"

	"github.com/thesyncim/goav/av"
)

// FuzzDemux feeds arbitrary bytes, seeded with real streams, to the
// demuxer: it never panics or hangs, and every packet it returns belongs to
// a stream it announced, Open or later.
func FuzzDemux(f *testing.F) {
	for _, name := range []string{"h264_aac.ts", "hevc_opus.ts"} {
		data, err := readFixture(name)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(data)
		f.Add(data[:len(data)/3])
	}
	streams, packets := syntheticAV(f, 1<<33-9000, 30)
	f.Add(muxBytes(f, MuxerOptions{MuxRate: 1_000_000}, streams, packets))
	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := demux(data)
		if err != nil {
			return
		}
		known := map[av.StreamID]bool{}
		for _, s := range got.streams {
			known[s.ID] = true
		}
		events := got.events
		for _, p := range got.packets {
			for !known[p.StreamID] && len(events) > 0 {
				if e := events[0]; e.Type == av.EventStreamAdded && e.Stream != nil {
					known[e.Stream.ID] = true
				}
				events = events[1:]
			}
			if !known[p.StreamID] {
				t.Fatalf("packet of stream %q, never announced", p.StreamID)
			}
			if !p.PTS.Base.Valid() || len(p.Payload.Bytes) > maxPES {
				t.Fatalf("packet %+v", p)
			}
		}
	})
}

// FuzzRoundTrip muxes fuzzed payloads and timestamps and reads back the
// same packets.
func FuzzRoundTrip(f *testing.F) {
	f.Add([]byte{1, 2, 3}, []byte{0xff, 0xf1, 0x00}, int64(0), uint16(3600), uint8(2))
	f.Add(bytes.Repeat([]byte{0, 0, 1}, 70), []byte{0x47}, int64(1<<33-1), uint16(1), uint8(0))
	f.Fuzz(func(t *testing.T, video, audio []byte, start int64, step uint16, reorder uint8) {
		if start < 0 || len(video) > 1<<16 || len(audio) > 4000 || step == 0 {
			return
		}
		streams, _ := syntheticAV(t, 0, 0)
		unit := append([]byte{0, 0, 0, 1, 0x65}, video...)
		var packets []av.Packet
		base := av.TimeBase{Num: 1, Den: 90000}
		// From the PCR delay, so nothing shifts, to below 2^33, the epoch the
		// demuxer counts from: the packets may still run past the wrap.
		start = 63000 + start%(1<<33-1<<20)
		for i := range 6 {
			dts := start + int64(i)*int64(step)
			pts := dts + int64(reorder)*int64(step)
			packets = append(packets,
				av.Packet{StreamID: "256", Type: av.MediaVideo, Keyframe: true, Payload: av.Buffer{Bytes: unit},
					PTS: av.Timestamp{Value: pts, Base: base}, DTS: av.Timestamp{Value: dts, Base: base}},
				av.Packet{StreamID: "257", Type: av.MediaAudio, Keyframe: true, Payload: av.Buffer{Bytes: append([]byte{0}, audio...)},
					PTS: av.Timestamp{Value: dts, Base: base}, DTS: av.Timestamp{Value: dts, Base: base}})
		}
		muxed := muxBytes(t, MuxerOptions{}, streams, packets)
		got := demuxBytes(t, muxed)
		for i := range packets {
			if packets[i].StreamID == "257" {
				packets[i].Duration = got.packets[i].Duration
			}
		}
		assertSamePackets(t, got.packets, packets, 0)
	})
}

package mp4

import (
	"bytes"
	"sort"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

// fuzzPackets derives a VP9 + Opus packet list from fuzz bytes: sample sizes,
// keyframe flags and increasing timestamps all come from data. The first video
// sample is always a keyframe; audio steps 20 ms from before zero (the pre-skip).
func fuzzPackets(data []byte) []muxPacket {
	next := func() byte {
		if len(data) == 0 {
			return 0
		}
		b := data[0]
		data = data[1:]
		return b
	}
	take := func(n int) []byte {
		out := make([]byte, n)
		for i := range out {
			out[i] = next()
		}
		return out
	}
	var packets []muxPacket
	nv := 1 + int(next()%24)
	var vt time.Duration
	for i := 0; i < nv; i++ {
		key := i == 0 || next()%4 == 0
		payload := take(1 + int(next()%48))
		if key {
			payload = append(vp9Keyframe(), payload...)
		}
		packets = append(packets, muxPacket{stream: "v", pts: vt, dts: vt, key: key, payload: payload})
		vt += time.Duration(1+int(next())%100) * time.Millisecond
	}
	na := 2 + int(next()%32)
	for i := 0; i < na; i++ {
		at := time.Duration(i)*20*time.Millisecond - 6500*time.Microsecond
		packets = append(packets, muxPacket{stream: "a", pts: at, dts: at, key: true, payload: append([]byte{0xfc}, take(1+int(next()%32))...)})
	}
	sort.SliceStable(packets, func(i, j int) bool { return packets[i].dts < packets[j].dts })
	return packets
}

// FuzzMuxRoundTrip muxes fuzz-shaped packets progressive (seekable) and
// fragmented (plain writer) and demuxes them: every sample must come back
// with its bytes, keyframe flag, and time within one tick.
func FuzzMuxRoundTrip(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{3, 0, 4, 1, 2, 3, 4, 10, 1, 2, 5, 0, 0, 0, 0, 0, 20, 4, 9, 9, 9, 9, 9})
	seed := make([]byte, 200)
	for i := range seed {
		seed[i] = byte(i * 7)
	}
	f.Add(seed)

	f.Fuzz(func(t *testing.T, data []byte) {
		streams, _ := avPackets(t)
		packets := fuzzPackets(data)
		progressive := &sliceWriteSeeker{}
		writeMP4(t, progressive, streams, packets, false)
		var fragmented bytes.Buffer
		writeMP4(t, &fragmented, streams, packets, true)
		for name, out := range map[string][]byte{"progressive": progressive.data, "fragmented": fragmented.Bytes()} {
			gotStreams, got := readMP4(t, out)
			if len(gotStreams) != 2 {
				t.Fatalf("%s: %d streams", name, len(gotStreams))
			}
			ids := map[av.StreamID]av.StreamID{"v": gotStreams[0].ID, "a": gotStreams[1].ID}
			for _, id := range []av.StreamID{"v", "a"} {
				var want, have []muxPacket
				for _, p := range packets {
					if p.stream == id {
						want = append(want, p)
					}
				}
				for _, p := range got {
					if p.stream == ids[id] {
						have = append(have, p)
					}
				}
				if len(have) != len(want) {
					t.Fatalf("%s %s: %d samples back, want %d", name, id, len(have), len(want))
				}
				tick := time.Second / 90000
				if id == "a" {
					tick = time.Second / 48000
				}
				tick++ // ns rounding
				for i := range want {
					if d := have[i].pts - want[i].pts; d > tick || d < -tick {
						t.Fatalf("%s %s sample %d at %v, want %v", name, id, i, have[i].pts, want[i].pts)
					}
					if have[i].key != want[i].key || !bytes.Equal(have[i].payload, want[i].payload) {
						t.Fatalf("%s %s sample %d = key %v % x, want key %v % x", name, id, i, have[i].key, have[i].payload, want[i].key, want[i].payload)
					}
				}
			}
		}
	})
}

package ts

import (
	"bytes"
	"context"
	"errors"
	"github.com/thesyncim/goav/format"
	"io"
	"os"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
)

func readFixture(name string) ([]byte, error) { return os.ReadFile("testdata/" + name) }

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	data, err := readFixture(name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// TestDemuxResyncsAfterGarbage puts junk before the stream and between two
// packets: the demuxer finds the packet boundary again, every PES the junk
// did not touch reads back whole, and the one it cut is marked.
func TestDemuxResyncsAfterGarbage(t *testing.T) {
	data := fixture(t, "h264_aac.ts")
	clean := demuxBytes(t, data)
	junk := bytes.Repeat([]byte{0x47, 0x00, 0x13, 0x47, 0x47, 0xff}, 50)
	at := 20 * packetSize
	dirty := append(append(append(append([]byte(nil), junk...), data[:at+77]...), junk...), data[at+77:]...)
	got := demuxBytes(t, dirty)
	if len(got.packets) < len(clean.packets)-2 {
		t.Fatalf("read %d packets through the junk, %d without", len(got.packets), len(clean.packets))
	}
	marked := 0
	for _, p := range got.packets {
		if p.Corrupt || p.LossBefore {
			marked++
		}
	}
	if marked == 0 {
		t.Fatal("no packet marked across the cut")
	}
	last, want := got.packets[len(got.packets)-1], clean.packets[len(clean.packets)-1]
	if !bytes.Equal(last.Payload.Bytes, want.Payload.Bytes) || last.PTS != want.PTS {
		t.Fatal("the stream after the junk does not read back")
	}
}

// TestDemuxContinuityLoss drops one packet from the middle of a video PES
// and repeats another: the damaged access unit is Corrupt, the next one
// LossBefore, and the repeated packet is ignored.
func TestDemuxContinuityLoss(t *testing.T) {
	data := fixture(t, "h264_aac.ts")
	clean := demuxBytes(t, data)
	var video []int // packet indexes of PID 256 that continue a PES
	for i := 0; i < len(data); i += packetSize {
		if pid := int(data[i+1]&0x1f)<<8 | int(data[i+2]); pid == 256 && data[i+1]&0x40 == 0 && data[i+3]&0x10 != 0 {
			video = append(video, i)
		}
	}
	drop := video[1]
	repeat := video[len(video)-1]
	damaged := append(append([]byte(nil), data[:drop]...), data[drop+packetSize:repeat+packetSize]...)
	damaged = append(append(damaged, data[repeat:repeat+packetSize]...), data[repeat+packetSize:]...)
	got := demuxBytes(t, damaged)
	if len(got.packets) != len(clean.packets) {
		t.Fatalf("read %d packets, want %d", len(got.packets), len(clean.packets))
	}
	var corrupt, loss []int
	for i, p := range got.packets {
		if p.Corrupt {
			corrupt = append(corrupt, i)
		}
		if p.LossBefore {
			loss = append(loss, i)
		}
	}
	if len(corrupt) != 1 || len(loss) != 1 || got.packets[corrupt[0]].StreamID != "256" || loss[0] <= corrupt[0] {
		t.Fatalf("corrupt %v, loss %v", corrupt, loss)
	}
	if got.packets[loss[0]].StreamID != "256" || !bytes.Equal(got.packets[loss[0]].Payload.Bytes, clean.packets[loss[0]].Payload.Bytes) {
		t.Fatal("the access unit after the loss does not read back whole")
	}
}

// TestDemuxDiscontinuity re-times the muxer mid-stream (a packet marked
// Discontinuous, its clock an hour back): the PCR carries
// discontinuity_indicator, and the demuxer emits av.EventDiscontinuity and
// marks the first packet of the new time base.
func TestDemuxDiscontinuity(t *testing.T) {
	streams, packets := syntheticAV(t, 90000*36000, 40)
	for i := 20; i < len(packets); i++ {
		packets[i].PTS.Value -= 3600 * packets[i].PTS.Base.Den
		packets[i].DTS.Value -= 3600 * packets[i].DTS.Base.Den
	}
	packets[20].Discontinuous = true
	muxed := muxBytes(t, MuxerOptions{}, streams, packets)
	validateTS(t, muxed)
	got := demuxBytes(t, muxed)
	if len(got.events) != 1 || got.events[0].Type != av.EventDiscontinuity {
		t.Fatalf("events = %+v", got.events)
	}
	// The first packet of each stream on the new time base is marked.
	broken := map[av.StreamID]int{}
	for i, p := range got.packets {
		if p.Discontinuous {
			if _, twice := broken[p.StreamID]; twice || i < 20 {
				t.Fatalf("packet %d of %s marked discontinuous", i, p.StreamID)
			}
			broken[p.StreamID] = i
		}
	}
	if len(broken) != 2 || broken[packets[20].StreamID] != 20 {
		t.Fatalf("discontinuous packets %v, want packet 20 and the other stream's next", broken)
	}
	assertSamePackets(t, got.packets[21:], packets[21:], 0)
}

// TestDemux33BitWrap runs timestamps across 2^33 ticks (26.5 hours): the
// stream carries them modulo 2^33 and the demuxer reads them back as they
// were, past the wrap.
func TestDemux33BitWrap(t *testing.T) {
	streams, packets := syntheticAV(t, 1<<33-90000, 100)
	if last := packets[len(packets)-1].DTS.Value; last < 1<<33 {
		t.Fatalf("the packets end at %d, before the wrap", last)
	}
	muxed := muxBytes(t, MuxerOptions{}, streams, packets)
	validateTS(t, muxed)
	got := demuxBytes(t, muxed)
	assertSamePackets(t, got.packets, packets, 0)
}

// TestDemuxPMTChanges follows a PMT that adds an audio stream, then drops
// it: Open reports the video stream, av.EventStreamAdded announces the
// audio stream before its first packet, and av.EventStreamRemoved its end.
func TestDemuxPMTChanges(t *testing.T) {
	streams, packets := syntheticAV(t, 90000, 30)
	var video []av.Packet
	for _, p := range packets {
		if p.StreamID == "256" {
			video = append(video, p)
		}
	}
	first := muxBytes(t, MuxerOptions{}, streams[:1], video[:5])
	both := withPMTVersion(t, muxBytes(t, MuxerOptions{}, streams, packets[10:20]), 1)
	last := withPMTVersion(t, muxBytes(t, MuxerOptions{}, streams[:1], video[10:]), 2)
	got := demuxBytes(t, append(append(first, both...), last...))
	if len(got.streams) != 1 || got.streams[0].ID != "256" {
		t.Fatalf("Open found %+v", got.streams)
	}
	if len(got.events) != 2 || got.events[0].Type != av.EventStreamAdded || got.events[0].Stream == nil ||
		got.events[0].Stream.ID != "257" || got.events[0].Stream.Codec.ID != av.CodecAAC || got.events[0].Stream.Codec.SampleRate != 48000 ||
		got.events[1].Type != av.EventStreamRemoved || got.events[1].StreamID != "257" {
		t.Fatalf("events = %+v", got.events)
	}
}

// withPMTVersion rewrites the version of every PMT section in a muxed
// stream, its CRC with it.
func withPMTVersion(t *testing.T, data []byte, version byte) []byte {
	t.Helper()
	for i := 0; i < len(data); i += packetSize {
		p := data[i : i+packetSize]
		if pid := int(p[1]&0x1f)<<8 | int(p[2]); pid != pidPMT {
			continue
		}
		s := p[5:]
		n := 3 + (int(s[1]&0x0f)<<8 | int(s[2]))
		s[5] = s[5]&^0x3e | version<<1
		crc := bitwiseCRC(s[:n-4])
		s[n-4], s[n-3], s[n-2], s[n-1] = byte(crc>>24), byte(crc>>16), byte(crc>>8), byte(crc)
	}
	return data
}

// TestDemuxRefusesNonTransportStreams refuses input with no program.
func TestDemuxRefusesNonTransportStreams(t *testing.T) {
	for name, data := range map[string][]byte{
		"empty":   nil,
		"text":    bytes.Repeat([]byte("not a transport stream "), 100),
		"no PSI":  bytes.Repeat(append([]byte{0x47, 0x01, 0x00, 0x10}, make([]byte, 184)...), 20),
		"mp4 box": fixture(t, "../../mp4/testdata/h264_aac.mp4"),
	} {
		if _, err := demux(data); !errors.Is(err, ErrInvalidData) {
			t.Errorf("%s: %v, want ErrInvalidData", name, err)
		}
	}
}

// liveReader serves data in 1316-byte reads, as a UDP or SRT input does,
// then blocks as a live input with nothing more to say.
type liveReader struct {
	data []byte
	wait chan struct{}
}

func (r *liveReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		<-r.wait
		return 0, io.EOF
	}
	n := copy(p[:min(len(p), 1316)], r.data)
	r.data = r.data[n:]
	return n, nil
}

// TestDemuxDeliversWithoutReadingAhead reads a live input that goes quiet:
// every PES with a known length, the last packet's included, is delivered
// without waiting for more bytes.
func TestDemuxDeliversWithoutReadingAhead(t *testing.T) {
	data := fixture(t, "h264_aac.ts")
	want := 0
	for _, p := range demuxBytes(t, data).packets {
		if p.Type == av.MediaAudio {
			want++
		}
	}
	r := &liveReader{data: data, wait: make(chan struct{})}
	defer close(r.wait)
	d := NewFormatDemuxer()
	ctx := context.Background()
	if err := d.Open(ctx, format.Input{Reader: r}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	got := make(chan int)
	go func() {
		audio := 0
		result := format.ReadResult{Packet: &av.Packet{}, Events: make([]av.Event, 0, 1)}
		for d.ReadInto(ctx, &result) == nil {
			if result.PacketReady && result.Packet.Type == av.MediaAudio {
				if audio++; audio == want {
					got <- audio
				}
			}
			result.Reset()
		}
	}()
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("the last audio frame waits for bytes after it")
	}
}

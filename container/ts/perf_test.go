package ts

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// benchStreams is a 1080p-sized H.264 stream with AAC: 30 fps access units
// of 40 KB (a 200 KB keyframe every second) and 48 kHz audio frames.
func benchStreams(tb testing.TB) ([]av.Stream, []av.Packet) {
	streams, packets := syntheticAV(tb, 90000, 64)
	for i := range packets {
		p := &packets[i]
		if p.StreamID == "256" {
			size := 40_000
			if p.Keyframe {
				size = 200_000
			}
			p.Payload.Bytes = append(p.Payload.Bytes, make([]byte, size)...)
		}
	}
	return streams, packets
}

// muxLoop writes packets forever, retimed each lap so the clock runs on.
type muxLoop struct {
	m       *FormatMuxer
	packets []av.Packet
	i       int
	lap     int64
	span    int64 // 90 kHz ticks per lap
	scratch av.Packet
}

func newMuxLoop(tb testing.TB, options MuxerOptions) *muxLoop {
	streams, packets := benchStreams(tb)
	m := NewFormatMuxer(options)
	if err := m.Open(context.Background(), format.Output{Writer: io.Discard}, streams, format.OpenOptions{}); err != nil {
		tb.Fatal(err)
	}
	first, _ := ticks90k(packets[0].DTS)
	last, _ := ticks90k(packets[len(packets)-1].DTS)
	l := &muxLoop{m: m, packets: packets, span: last - first + 3600}
	for range 2 * len(packets) { // two laps grow the buffers, the wrap between them too
		l.write(tb)
	}
	return l
}

func (l *muxLoop) write(tb testing.TB) int {
	if l.i == len(l.packets) {
		l.i, l.lap = 0, l.lap+1
	}
	l.scratch = l.packets[l.i]
	l.i++
	l.scratch.PTS.Value += l.lap * l.span * l.scratch.PTS.Base.Den / 90000
	l.scratch.DTS.Value += l.lap * l.span * l.scratch.DTS.Base.Den / 90000
	if err := l.m.Write(context.Background(), &l.scratch, nil); err != nil {
		tb.Fatal(err)
	}
	return len(l.scratch.Payload.Bytes)
}

// TestMuxWriteAllocs pins packetizing at zero allocations per packet, at
// a variable and a constant bitrate.
func TestMuxWriteAllocs(t *testing.T) {
	for _, options := range []MuxerOptions{{}, {MuxRate: 80_000_000}} {
		l := newMuxLoop(t, options)
		for range 200 {
			l.write(t)
		}
		if allocs := testing.AllocsPerRun(500, func() { l.write(t) }); allocs != 0 {
			t.Errorf("MuxRate %d: Write allocates %.1f times per packet", options.MuxRate, allocs)
		}
	}
}

// TestDemuxReadIntoAllocs pins demuxing at zero allocations per packet once
// its buffers have grown.
func TestDemuxReadIntoAllocs(t *testing.T) {
	streams, packets := benchStreams(t)
	var stream []byte
	for range 4 {
		stream = append(stream, muxBytes(t, MuxerOptions{}, streams, packets)...)
	}
	d := NewFormatDemuxer()
	ctx := context.Background()
	if err := d.Open(ctx, format.Input{Reader: bytes.NewReader(stream)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 1<<20)}}, Events: make([]av.Event, 0, 1)}
	read := func() {
		result.Reset()
		if err := d.ReadInto(ctx, &result); err != nil {
			t.Fatal(err)
		}
	}
	for range len(packets) + 10 {
		read()
	}
	if allocs := testing.AllocsPerRun(len(packets), read); allocs != 0 {
		t.Errorf("ReadInto allocates %.1f times per packet", allocs)
	}
}

func BenchmarkMuxPacketize(b *testing.B) {
	l := newMuxLoop(b, MuxerOptions{})
	b.ReportAllocs()
	n := 0
	for b.Loop() {
		n += l.write(b)
	}
	b.SetBytes(int64(n / max(b.N, 1)))
}

func BenchmarkMuxPacketizeCBR(b *testing.B) {
	l := newMuxLoop(b, MuxerOptions{MuxRate: 80_000_000})
	b.ReportAllocs()
	n := 0
	for b.Loop() {
		n += l.write(b)
	}
	b.SetBytes(int64(n / max(b.N, 1)))
}

func BenchmarkDemux(b *testing.B) {
	streams, packets := benchStreams(b)
	stream := muxBytes(b, MuxerOptions{}, streams, packets)
	ctx := context.Background()
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 1<<20)}}, Events: make([]av.Event, 0, 1)}
	reader := bytes.NewReader(stream)
	b.SetBytes(int64(len(stream)))
	b.ReportAllocs()
	for b.Loop() {
		reader.Reset(stream)
		d := NewFormatDemuxer()
		if err := d.Open(ctx, format.Input{Reader: reader}, format.OpenOptions{}); err != nil {
			b.Fatal(err)
		}
		for {
			result.Reset()
			if err := d.ReadInto(ctx, &result); err != nil {
				break
			}
		}
	}
}

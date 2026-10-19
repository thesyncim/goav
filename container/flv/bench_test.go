package flv

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// muxBench is an opened muxer and the packets it writes in a loop: the
// H.264 and AAC of the classic fixture, Annex B and raw as goav carries
// them.
func muxBench(tb testing.TB, name string) (*Muxer, []av.Packet) {
	tb.Helper()
	src := demuxBytes(tb, fixture(tb, name))
	m := &Muxer{}
	if err := m.Open(context.Background(), format.Output{Writer: io.Discard}, src.streams, format.OpenOptions{}); err != nil {
		tb.Fatal(err)
	}
	for i := range src.packets {
		if err := m.Write(context.Background(), &src.packets[i], nil); err != nil {
			tb.Fatal(err)
		}
	}
	return m, src.packets
}

// Writing a tag allocates nothing: the tag is built in the muxer's buffer
// and written once, for the legacy and the enhanced headers alike.
func TestMuxWriteAllocatesNothing(t *testing.T) {
	for _, name := range []string{"h264_aac", "hevc_aac", "av1_opus", "vp9_opus", "vp9_flac", "multi"} {
		m, packets := muxBench(t, name)
		ctx := context.Background()
		allocs := testing.AllocsPerRun(20, func() {
			for i := range packets {
				if err := m.Write(ctx, &packets[i], nil); err != nil {
					t.Fatal(err)
				}
			}
		})
		if allocs != 0 {
			t.Errorf("%s: %v allocations per pass", name, allocs)
		}
	}
}

func benchmarkMux(b *testing.B, name string) {
	m, packets := muxBench(b, name)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for i := range packets {
			if err := m.Write(ctx, &packets[i], nil); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkMuxTags writes the classic fixture's H.264 and AAC tags.
func BenchmarkMuxTags(b *testing.B) { benchmarkMux(b, "h264_aac") }

// BenchmarkMuxEnhancedTags writes E-FLV HEVC and AAC tags.
func BenchmarkMuxEnhancedTags(b *testing.B) { benchmarkMux(b, "hevc_aac") }

// BenchmarkMuxMultitrackTags writes multitrack AAC beside H.264.
func BenchmarkMuxMultitrackTags(b *testing.B) { benchmarkMux(b, "multi") }

// BenchmarkDemuxTags opens and reads the classic fixture; the reads after
// Open allocate nothing.
func BenchmarkDemuxTags(b *testing.B) {
	data := fixture(b, "h264_aac")
	ctx := context.Background()
	res := format.ReadResult{Packet: &av.Packet{}, Events: make([]av.Event, 0, 1)}
	b.ReportAllocs()
	for b.Loop() {
		d := &Demuxer{}
		if err := d.Open(ctx, format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
			b.Fatal(err)
		}
		for {
			res.Reset()
			if err := d.ReadInto(ctx, &res); err != nil {
				break
			}
		}
	}
}

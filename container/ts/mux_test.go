package ts

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// TestMuxLiftsZeroBasedTimestamps writes a stream that starts at zero: the
// PCR runs Delay behind the DTS, so every timestamp is lifted by Delay, and
// reads back otherwise unchanged.
func TestMuxLiftsZeroBasedTimestamps(t *testing.T) {
	streams, packets := syntheticAV(t, 3600, 60)
	for _, delay := range []time.Duration{0, 200 * time.Millisecond} {
		muxed := muxBytes(t, MuxerOptions{Delay: delay}, streams, packets)
		report := validateTS(t, muxed)
		if delay == 0 {
			delay = defaultDelay
		}
		shift := delay.Nanoseconds()*90/1_000_000 - 0 // the first DTS is 0
		if first := report.pcrs[0].value; first != 0 {
			t.Fatalf("delay %v: first PCR %d, want 0", delay, first)
		}
		assertSamePackets(t, demuxBytes(t, muxed).packets, packets, shift)
	}
}

// TestMuxPSIAndPCRCadence checks the repetition the muxer promises over a
// sparse stream (5 fps video, no audio): PAT/PMT every 100ms and before
// every keyframe, PCR-only packets filling the 200ms gaps to 40ms.
func TestMuxPSIAndPCRCadence(t *testing.T) {
	streams, packets := syntheticAV(t, 90000*100, 80)
	var video []av.Packet
	for _, p := range packets {
		if p.StreamID == "256" {
			p.DTS.Value = p.DTS.Value * 5
			p.PTS.Value = p.DTS.Value + 18000
			video = append(video, p)
		}
	}
	report := validateTS(t, muxBytes(t, MuxerOptions{}, streams[:1], video))
	// The validator times a PAT by the PCR before it, up to 40ms early.
	for _, gap := range report.psiGaps {
		if gap > 140*27000 {
			t.Fatalf("PAT %v after the last", time.Duration(gap*1000/27))
		}
	}
	span := report.pcrs[len(report.pcrs)-1].value - report.pcrs[0].value
	if want := span / (40 * 27000); int64(len(report.pcrs)) < want {
		t.Fatalf("%d PCRs over %v", len(report.pcrs), time.Duration(span*1000/27))
	}
}

// TestMuxConstantBitrate holds a stream to MuxRate: every packet sits at
// its rate-given place on the PCR clock, null packets fill the gaps, and
// the media reads back unchanged.
func TestMuxConstantBitrate(t *testing.T) {
	streams, packets := syntheticAV(t, 90000*5, 200)
	const rate = 2_000_000
	muxed := muxBytes(t, MuxerOptions{MuxRate: rate}, streams, packets)
	report := validateTS(t, muxed)
	if report.nulls == 0 {
		t.Fatal("no null packets at a constant bitrate")
	}
	first := report.pcrs[0]
	for _, at := range report.pcrs[1:] {
		want := first.value + int64(at.index-first.index)*188*8*27_000_000/rate
		if d := at.value - want; d < -1 || d > 1 {
			t.Fatalf("PCR at packet %d is %d, the rate puts it at %d", at.index, at.value, want)
		}
	}
	elapsed := float64(report.pcrs[len(report.pcrs)-1].value-first.value) / 27e6
	bitrate := float64(report.pcrs[len(report.pcrs)-1].index-first.index) * 188 * 8 / elapsed
	if bitrate < rate*0.999 || bitrate > rate*1.001 {
		t.Fatalf("stream runs at %.0f b/s, want %d", bitrate, rate)
	}
	assertSamePackets(t, demuxBytes(t, muxed).packets, packets, 0)
}

// TestMuxCompletesAccessUnits gives the muxer H.264 without delimiters or
// in-band parameter sets: each access unit gains an AUD, and keyframes the
// SPS and PPS of the stream's avcC.
func TestMuxCompletesAccessUnits(t *testing.T) {
	streams, packets := syntheticAV(t, 90000, 30)
	var video []av.Packet
	var sets []byte
	for _, p := range packets {
		if p.StreamID != "256" {
			continue
		}
		if p.Keyframe {
			at := bytes.Index(p.Payload.Bytes, []byte{0, 0, 1, 0x65})
			sets = p.Payload.Bytes[:at]
			p.Payload.Bytes = append([]byte{0, 0, 0, 1}, p.Payload.Bytes[at+3:]...)
		}
		video = append(video, p)
	}
	if !bytes.HasPrefix(video[0].Payload.Bytes, []byte{0, 0, 0, 1, 0x65}) {
		t.Fatal("fixture keyframe still carries its parameter sets")
	}
	stream := streams[0]
	record, err := avcC(sets)
	if err != nil {
		t.Fatal(err)
	}
	stream.Codec.ExtraData.Bytes = record
	muxed := muxBytes(t, MuxerOptions{}, []av.Stream{stream}, video)
	if n := bytes.Count(muxed, h264AUDUnit); n != len(video) {
		t.Fatalf("%d access unit delimiters for %d access units", n, len(video))
	}
	got := demuxBytes(t, muxed)
	for i, p := range got.packets {
		want := video[i].Payload.Bytes
		if video[i].Keyframe {
			want = append(append([]byte(nil), sets...), want...)
		}
		if !bytes.Equal(p.Payload.Bytes, want) {
			t.Fatalf("access unit %d = % x, want % x", i, p.Payload.Bytes[:12], want[:12])
		}
	}
	if !bytes.Equal(got.streams[0].Codec.ExtraData.Bytes, record) {
		t.Fatal("the demuxed avcC differs from the muxed one")
	}
}

// TestMuxRefusals names the fix for what a transport stream cannot carry.
func TestMuxRefusals(t *testing.T) {
	ctx := context.Background()
	open := func(streams ...av.Stream) error {
		return NewFormatMuxer(MuxerOptions{}).Open(ctx, format.Output{Writer: &bytes.Buffer{}}, streams, format.OpenOptions{})
	}
	for name, tc := range map[string]struct {
		stream av.Stream
		want   string
	}{
		"vp8":       {av.Stream{ID: "v", Codec: av.CodecParameters{ID: av.CodecVP8}}, "carries H.264, HEVC, AAC, and Opus, not \"vp8\""},
		"aac":       {av.Stream{ID: "a", Codec: av.CodecParameters{ID: av.CodecAAC}}, "AudioSpecificConfig"},
		"he-aac v2": {av.Stream{ID: "a", Codec: av.CodecParameters{ID: av.CodecAAC, ExtraData: av.Buffer{Bytes: []byte{0xea, 0x00}}}}, "no ADTS form"},
		"opus 6ch":  {av.Stream{ID: "o", Codec: av.CodecParameters{ID: av.CodecOpus, Channels: 6}}, "needs its OpusHead"},
	} {
		if err := open(tc.stream); !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: %v, want ErrUnsupported naming %q", name, err, tc.want)
		}
	}
	streams, packets := syntheticAV(t, 90000, 2)
	m := NewFormatMuxer(MuxerOptions{})
	if err := m.Open(ctx, format.Output{Writer: &bytes.Buffer{}}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	stray := packets[0]
	stray.StreamID = "9"
	untimed := packets[0]
	untimed.PTS = av.Timestamp{}
	annexB := packets[0]
	annexB.Payload.Bytes = []byte{0, 0, 0, 5, 0x65, 1, 2, 3, 4}
	for name, p := range map[string]*av.Packet{"unknown stream": &stray, "no timestamp": &untimed, "AVCC payload": &annexB, "nil": nil} {
		if err := m.Write(ctx, p, nil); !errors.Is(err, ErrInvalidData) {
			t.Errorf("%s: %v, want ErrInvalidData", name, err)
		}
	}
	m.Close()
	if err := m.Write(ctx, &packets[0], nil); !errors.Is(err, ErrClosed) {
		t.Errorf("after Close: %v", err)
	}
}

func TestProber(t *testing.T) {
	ctx := context.Background()
	data := fixture(t, "h264_aac.ts")
	for name, request := range map[string]format.ProbeRequest{
		"sync bytes": {Header: data[:1024]},
		"mime":       {MIMEType: "video/MP2T"},
		"extension":  {Name: "live/stream.ts"},
		"url":        {Input: format.Input{URI: "https://cdn.example/seg-1.ts?token=x"}},
	} {
		if result, err := (Prober{}).Probe(ctx, request); err != nil || result.Format != av.FormatTS {
			t.Errorf("%s: %+v, %v", name, result, err)
		}
	}
	for name, request := range map[string]format.ProbeRequest{
		"one sync byte": {Header: []byte{0x47, 1, 2, 3}},
		"mp4":           {Name: "a.mp4", Header: []byte("....ftypisom")},
	} {
		if _, err := (Prober{}).Probe(ctx, request); !errors.Is(err, format.ErrNotFound) {
			t.Errorf("%s: %v, want ErrNotFound", name, err)
		}
	}
}

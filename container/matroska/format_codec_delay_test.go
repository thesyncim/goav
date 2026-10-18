package matroska

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// TestFormatOpusCodecDelayRoundTrips pins the CodecDelay contract at the av
// boundary: a packet's PTS is the time of its first decoded sample, so an
// Opus stream's first packet presents a pre-skip before zero. The muxer adds
// the OpusHead's pre-skip back as CodecDelay (blocks start at zero, as the
// spec has players subtract it), and the demuxer subtracts it again.
func TestFormatOpusCodecDelayRoundTrips(t *testing.T) {
	ctx := context.Background()
	head := make([]byte, 19)
	copy(head, "OpusHead")
	head[8], head[9] = 1, 2
	binary.LittleEndian.PutUint16(head[10:], 312)
	binary.LittleEndian.PutUint32(head[12:], 48000)
	stream := av.Stream{ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{
		ID: av.CodecOpus, Type: av.MediaAudio, SampleRate: 48000, Channels: 2,
		ExtraData: av.Buffer{Bytes: head},
	}}
	base := av.TimeBase{Num: 1, Den: 48000}
	ws := &memoryWriteSeeker{}
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: ws}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := range 3 {
		packet := av.Packet{StreamID: "a", Payload: av.Buffer{Bytes: []byte{0xfc, byte(i)}}, PTS: av.Timestamp{Value: int64(i*960 - 312), Base: base}}
		if err := muxer.Write(ctx, &packet, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	raw, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if delay := raw.Tracks()[0].CodecDelayNS; delay != 6_500_000 {
		t.Fatalf("CodecDelay = %d ns, want 6500000 (312 samples)", delay)
	}
	block := Packet{Data: make([]byte, 0, 16)}
	if err := raw.ReadPacket(&block); err != nil {
		t.Fatal(err)
	}
	if block.TimeNS != 0 {
		t.Fatalf("first block at %d ns, want 0: the muxer adds CodecDelay back", block.TimeNS)
	}

	demuxer := &FormatDemuxer{}
	if err := demuxer.Open(ctx, format.Input{Name: "opus.mka", Reader: bytes.NewReader(ws.bytes)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	defer demuxer.Close()
	for i := range 3 {
		result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 16)}}}
		if err := demuxer.ReadInto(ctx, &result); err != nil {
			t.Fatal(err)
		}
		want, _ := av.Timestamp{Value: int64(i*960 - 312), Base: base}.Rescale(av.TimeBase{Num: 1, Den: timeNS})
		if result.Packet.PTS != want {
			t.Fatalf("packet %d PTS = %+v, want %+v", i, result.Packet.PTS, want)
		}
	}
}

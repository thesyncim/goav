package matroska

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// muxSeekFixture writes a cue-indexed Matroska file with one VP8 track and one
// keyframe packet per cluster at 0ms, 2ms, and 4ms.
func muxSeekFixture(t *testing.T) []byte {
	t.Helper()
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 1_000_000})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, timeNS := range []int64{0, 2_000_000, 4_000_000} {
		packet := Packet{TrackID: trackID, TimeNS: timeNS, Keyframe: true, Data: []byte{byte(i + 1)}}
		if err := muxer.WritePacket(packet); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	return ws.bytes
}

func TestFormatDemuxerSeekRepositionsAtOrBefore(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFixture(t)

	demuxer := &FormatDemuxer{}
	if err := demuxer.Open(ctx, format.Input{Name: "seek.mkv", Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	defer demuxer.Close()
	var seeker format.Seeker = demuxer

	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 64)}}}
	// 3ms sits between the 2ms and 4ms cues: at-or-before lands on 2ms.
	if err := seeker.Seek(ctx, 3*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	result.Reset()
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady || result.Packet.PTS.Value != 2_000_000 || !bytes.Equal(result.Packet.Payload.Bytes, []byte{2}) {
		t.Fatalf("packet after seek = %+v, want the 2ms keyframe", result.Packet)
	}

	// Seeking back to zero rewinds to the first packet.
	if err := seeker.Seek(ctx, 0); err != nil {
		t.Fatal(err)
	}
	result.Reset()
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady || result.Packet.PTS.Value != 0 {
		t.Fatalf("packet after rewind = %+v, want the 0ms keyframe", result.Packet)
	}
}

func TestFormatDemuxerSeekNonSeekableReader(t *testing.T) {
	ctx := context.Background()
	data := muxSeekFixture(t)

	demuxer := &FormatDemuxer{}
	// Hide the reader's Seek method: a pure io.Reader cannot reposition.
	reader := struct{ io.Reader }{bytes.NewReader(data)}
	if err := demuxer.Open(ctx, format.Input{Name: "pipe.mkv", Reader: reader}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	defer demuxer.Close()

	err := demuxer.Seek(ctx, time.Millisecond)
	if !errors.Is(err, format.ErrNotSeekable) {
		t.Fatalf("err = %v, want it to wrap format.ErrNotSeekable", err)
	}
	if !errors.Is(err, ErrNonSeekableReader) {
		t.Fatalf("err = %v, want it to keep ErrNonSeekableReader in the chain", err)
	}
}

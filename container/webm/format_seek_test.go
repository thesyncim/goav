package webm

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/matroska"
	"github.com/thesyncim/goav/format"
)

// seekFixtureWriter is the minimal io.WriteSeeker the muxer needs to patch
// cues and the seek head into a finished, seekable WebM file.
type seekFixtureWriter struct {
	bytes []byte
	pos   int64
}

func (w *seekFixtureWriter) Write(p []byte) (int, error) {
	end := w.pos + int64(len(p))
	if end > int64(len(w.bytes)) {
		next := make([]byte, end)
		copy(next, w.bytes)
		w.bytes = next
	}
	copy(w.bytes[w.pos:end], p)
	w.pos = end
	return len(p), nil
}

func (w *seekFixtureWriter) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		w.pos = offset
	case io.SeekCurrent:
		w.pos += offset
	case io.SeekEnd:
		w.pos = int64(len(w.bytes)) + offset
	}
	return w.pos, nil
}

// muxWebMSeekFixture writes a cue-indexed WebM file with one VP8 track and one
// keyframe packet per cluster at 0ms, 2ms, and 4ms.
func muxWebMSeekFixture(t *testing.T) []byte {
	t.Helper()
	ws := &seekFixtureWriter{}
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
	for _, timeNS := range []int64{0, 2_000_000, 4_000_000} {
		if err := muxer.WritePacket(Packet{
			TrackID:  trackID,
			TimeNS:   timeNS,
			Keyframe: true,
			Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
		}); err != nil {
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
	data := muxWebMSeekFixture(t)

	demuxer := &FormatDemuxer{}
	if err := demuxer.Open(ctx, format.Input{Name: "seek.webm", Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	defer demuxer.Close()
	var seeker format.Seeker = demuxer

	// 3ms sits between the 2ms and 4ms cues: at-or-before lands on 2ms.
	if err := seeker.Seek(ctx, 3*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 64)}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady || result.Packet.PTS.Value != 2_000_000 {
		t.Fatalf("packet after seek = %+v, want the 2ms keyframe", result.Packet)
	}

	// Seeking back rewinds: the monotonic-timecode validation must restart at
	// the new position instead of rejecting the rewound timestamps.
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
	data := muxWebMSeekFixture(t)

	demuxer := &FormatDemuxer{}
	// Hide the reader's Seek method: a pure io.Reader cannot reposition.
	reader := struct{ io.Reader }{bytes.NewReader(data)}
	if err := demuxer.Open(ctx, format.Input{Name: "pipe.webm", Reader: reader}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	defer demuxer.Close()

	err := demuxer.Seek(ctx, time.Millisecond)
	if !errors.Is(err, format.ErrNotSeekable) {
		t.Fatalf("err = %v, want it to wrap format.ErrNotSeekable", err)
	}
	if !errors.Is(err, matroska.ErrNonSeekableReader) {
		t.Fatalf("err = %v, want it to keep matroska.ErrNonSeekableReader in the chain", err)
	}
}

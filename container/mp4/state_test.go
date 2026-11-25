package mp4

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

func TestFormatMuxerFailedOpenThenCloseIsNoOp(t *testing.T) {
	var buf bytes.Buffer
	m := &FormatMuxer{}
	streams := []av.Stream{
		{ID: "v", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP9, Type: av.MediaVideo, Width: 64, Height: 48}, TimeBase: av.TimeBase{Num: 1, Den: 90000}},
		{ID: "bad", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecVorbis, Type: av.MediaAudio, SampleRate: 48000, Channels: 2}},
	}
	if err := m.Open(context.Background(), format.Output{Writer: &buf}, streams, format.OpenOptions{}); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("Open() = %v, want ErrUnsupported", err)
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close() after failed Open() = %v, want nil", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("Close() after failed Open() wrote %d bytes, want 0", buf.Len())
	}
}

func TestFormatMuxerFragmentedNonMonotonicDTSAcrossFragments(t *testing.T) {
	var buf bytes.Buffer
	m := &FormatMuxer{}
	streams := []av.Stream{
		{ID: "v", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP9, Type: av.MediaVideo, Width: 64, Height: 48}, TimeBase: av.TimeBase{Num: 1, Den: 90000}},
		{ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio, SampleRate: 48000, Channels: 2, ExtraData: av.Buffer{Bytes: opusHead(312)}}},
	}
	ctx := context.Background()
	if err := m.Open(ctx, format.Output{Writer: &buf}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	ns := av.TimeBase{Num: 1, Den: 1_000_000_000}
	writePkt := func(stream av.StreamID, dts time.Duration, dur time.Duration, key bool, data []byte) error {
		return m.Write(ctx, &av.Packet{
			StreamID: stream,
			Keyframe: key,
			PTS:      av.Timestamp{Value: int64(dts), Base: ns},
			DTS:      av.Timestamp{Value: int64(dts), Base: ns},
			Duration: av.Duration{Value: int64(dur), Base: ns},
			Payload:  av.Buffer{Bytes: data},
		}, nil)
	}

	if err := writePkt("v", 0, 40*time.Millisecond, true, vp9Keyframe()); err != nil {
		t.Fatal(err)
	}
	if err := writePkt("a", 0, 20*time.Millisecond, true, []byte{0xfc, 1}); err != nil {
		t.Fatal(err)
	}
	if err := writePkt("a", 20*time.Millisecond, 20*time.Millisecond, true, []byte{0xfc, 2}); err != nil {
		t.Fatal(err)
	}
	// Second video keyframe flushes fragment 1, emptying audio track's t.samples.
	if err := writePkt("v", 40*time.Millisecond, 40*time.Millisecond, true, vp9Keyframe()); err != nil {
		t.Fatal(err)
	}
	if len(m.byStream["a"].samples) != 0 {
		t.Fatalf("expected audio samples to be flushed, got %d", len(m.byStream["a"].samples))
	}
	// Non-monotonic DTS across the fragment boundary must be rejected.
	if err := writePkt("a", 10*time.Millisecond, 20*time.Millisecond, true, []byte{0xfc, 3}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("Write() with backward DTS across fragment = %v, want ErrInvalidData", err)
	}
}

type failSeekWriteSeeker struct {
	buf      []byte
	pos      int64
	failSeek bool
	seekErr  error
}

func (f *failSeekWriteSeeker) Write(p []byte) (int, error) {
	end := int(f.pos) + len(p)
	if end > len(f.buf) {
		next := make([]byte, end)
		copy(next, f.buf)
		f.buf = next
	}
	copy(f.buf[f.pos:], p)
	f.pos += int64(len(p))
	return len(p), nil
}

func (f *failSeekWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	if f.failSeek {
		return 0, f.seekErr
	}
	switch whence {
	case io.SeekStart:
		f.pos = offset
	case io.SeekCurrent:
		f.pos += offset
	case io.SeekEnd:
		f.pos = int64(len(f.buf)) + offset
	}
	return f.pos, nil
}

func TestFormatMuxerFinishProgressiveStickyError(t *testing.T) {
	ws := &failSeekWriteSeeker{seekErr: errors.New("seek failed")}
	m := &FormatMuxer{}
	streams := []av.Stream{
		{ID: "a", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio, SampleRate: 48000, Channels: 2, ExtraData: av.Buffer{Bytes: opusHead(312)}}},
	}
	ctx := context.Background()
	if err := m.Open(ctx, format.Output{Writer: ws}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	ns := av.TimeBase{Num: 1, Den: 1_000_000_000}
	if err := m.Write(ctx, &av.Packet{
		StreamID: "a",
		Keyframe: true,
		PTS:      av.Timestamp{Value: 0, Base: ns},
		DTS:      av.Timestamp{Value: 0, Base: ns},
		Duration: av.Duration{Value: int64(20 * time.Millisecond), Base: ns},
		Payload:  av.Buffer{Bytes: []byte{0xfc, 1}},
	}, nil); err != nil {
		t.Fatal(err)
	}
	ws.failSeek = true
	if err := m.Close(); !errors.Is(err, ws.seekErr) {
		t.Fatalf("first Close() = %v, want %v", err, ws.seekErr)
	}
	if !errors.Is(m.err, ws.seekErr) {
		t.Fatalf("m.err = %v, want sticky %v", m.err, ws.seekErr)
	}
	// Retry Close must return the sticky error, not nil.
	if err := m.Close(); !errors.Is(err, ws.seekErr) {
		t.Fatalf("second Close() = %v, want sticky %v", err, ws.seekErr)
	}
}

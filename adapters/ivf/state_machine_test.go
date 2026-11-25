package ivf

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

func TestDemuxerClosedStreamsAndHeaderPendingReset(t *testing.T) {
	ctx := context.Background()
	stream := testStream(av.CodecVP8)
	data1 := makeIVFData(t, stream, []ivfFrame{{timestamp: 10, payload: []byte{1, 2, 3, 4}}})
	data2 := makeIVFData(t, stream, []ivfFrame{{timestamp: 20, payload: []byte{9, 8}}})

	d := &Demuxer{}
	if err := d.Open(ctx, format.Input{Reader: bytes.NewReader(data1)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	// Trigger headerPending via ErrPayloadTooSmall.
	small := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 2)}}}
	if err := d.ReadInto(ctx, &small); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	if d.state != ivfDemuxHeaderPending {
		t.Fatalf("state = %d, want ivfDemuxHeaderPending", d.state)
	}

	// Re-open on a new stream must clear headerPending and read data2's frame.
	if err := d.Open(ctx, format.Input{Reader: bytes.NewReader(data2)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if d.state != ivfDemuxOpen {
		t.Fatalf("state after reopen = %d, want ivfDemuxOpen", d.state)
	}
	out := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 8)}}}
	if err := d.ReadInto(ctx, &out); err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if !bytes.Equal(out.Packet.Payload.Bytes, []byte{9, 8}) || out.Packet.PTS.Value != 20 {
		t.Fatalf("got payload=%v pts=%d, want [9 8] pts=20", out.Packet.Payload.Bytes, out.Packet.PTS.Value)
	}

	// Close must make Streams() return nil.
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if streams := d.Streams(); streams != nil {
		t.Fatalf("closed Streams() = %+v, want nil", streams)
	}
}

func TestMuxerCloseTransitionsToClosedEvenIfWriteHeaderFails(t *testing.T) {
	ctx := context.Background()
	writeErr := errors.New("disk full")

	m := &Muxer{}
	if err := m.Open(ctx, format.Output{Writer: errorWriter{err: writeErr}}, []av.Stream{testStream(av.CodecVP8)}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := m.Close(); !errors.Is(err, writeErr) {
		t.Fatalf("Close err = %v, want %v", err, writeErr)
	}
	if m.state != ivfMuxClosed {
		t.Fatalf("state after failed Close = %d, want ivfMuxClosed", m.state)
	}
	// Subsequent Close is a no-op and Write reports closed pipe.
	if err := m.Close(); err != nil {
		t.Fatalf("second Close err = %v, want nil", err)
	}
	if err := m.Write(ctx, &av.Packet{Payload: av.Buffer{Bytes: []byte{1}}}, nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Write after failed Close err = %v, want ErrClosedPipe", err)
	}
}

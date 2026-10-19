package ts

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

// demuxed is everything a demuxer read: the streams Open found, and the
// packets and events in order.
type demuxed struct {
	streams []av.Stream
	packets []av.Packet
	events  []av.Event
}

func demuxBytes(t testing.TB, data []byte) demuxed {
	t.Helper()
	got, err := demux(data)
	if err != nil {
		t.Fatalf("demux: %v", err)
	}
	return got
}

func demux(data []byte) (demuxed, error) {
	d := NewFormatDemuxer()
	defer d.Close()
	ctx := context.Background()
	if err := d.Open(ctx, format.Input{Reader: bytes.NewReader(data)}, format.OpenOptions{}); err != nil {
		return demuxed{}, err
	}
	out := demuxed{streams: d.Streams()}
	result := format.ReadResult{Packet: &av.Packet{}, Events: make([]av.Event, 0, 1)}
	for {
		result.Reset()
		err := d.ReadInto(ctx, &result)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out.events = append(out.events, result.Events...)
		if result.PacketReady {
			p := *result.Packet
			p.Payload.Bytes = bytes.Clone(p.Payload.Bytes)
			out.packets = append(out.packets, p)
		}
	}
}

func muxBytes(t testing.TB, options MuxerOptions, streams []av.Stream, packets []av.Packet) []byte {
	t.Helper()
	var buf bytes.Buffer
	m := NewFormatMuxer(options)
	ctx := context.Background()
	if err := m.Open(ctx, format.Output{Writer: &buf}, streams, format.OpenOptions{}); err != nil {
		t.Fatalf("mux open: %v", err)
	}
	for i := range packets {
		if err := m.Write(ctx, &packets[i], nil); err != nil {
			t.Fatalf("mux packet %d: %v", i, err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

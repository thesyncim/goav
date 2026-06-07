package annexb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

func TestRegisterProvidesMuxerAndProber(t *testing.T) {
	registry := format.NewRegistry()
	Register(registry)

	result, err := registry.Probe(context.Background(), format.ProbeRequest{
		Name: "video.h264",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatAnnexB {
		t.Fatalf("format = %s, want %s", result.Format, av.FormatAnnexB)
	}
	if _, err := registry.MuxerFactory(av.FormatAnnexB); err != nil {
		t.Fatalf("muxer factory: %v", err)
	}
	desc, err := registry.MuxerDescriptor(av.FormatAnnexB)
	if err != nil {
		t.Fatalf("muxer descriptor: %v", err)
	}
	if desc.Format != av.FormatAnnexB ||
		desc.MaxStreams != 1 ||
		len(desc.Codecs) != 1 ||
		desc.Codecs[0] != av.CodecH264 {
		t.Fatalf("descriptor = %+v, want Annex B H264 single-stream capabilities", desc)
	}
}

func TestProberDetectsStartCode(t *testing.T) {
	result, err := (Prober{}).Probe(context.Background(), format.ProbeRequest{
		Header: []byte{0x00, 0x00, 0x00, 0x01, 0x65},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatAnnexB || result.Score != 100 {
		t.Fatalf("result = %+v", result)
	}
}

func TestMuxerWritesPackets(t *testing.T) {
	ctx := context.Background()
	stream := testH264Stream()
	payload := []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0xaa}
	var buffer bytes.Buffer

	muxer := &Muxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: payload},
	}, &format.WriteResult{}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buffer.Bytes(), payload) {
		t.Fatalf("payload = %v, want %v", buffer.Bytes(), payload)
	}
}

func TestMuxerSkipsUnrelatedStreams(t *testing.T) {
	ctx := context.Background()
	stream := testH264Stream()
	var buffer bytes.Buffer

	muxer := &Muxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: "other",
		Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
	}, nil); err != nil {
		t.Fatal(err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("bytes = %d, want 0", buffer.Len())
	}
}

func TestMuxerRejectsUnsupportedStreamLayouts(t *testing.T) {
	ctx := context.Background()
	var buffer bytes.Buffer
	err := (&Muxer{}).Open(ctx, format.Output{Writer: &buffer}, []av.Stream{
		testH264Stream(),
		{ID: "video-2", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo}},
	}, format.OpenOptions{})
	if !errors.Is(err, ErrUnsupportedStream) {
		t.Fatalf("err = %v, want ErrUnsupportedStream", err)
	}
}

func TestClosedAndUnopenedErrors(t *testing.T) {
	ctx := context.Background()
	if err := (&Muxer{}).Write(ctx, &av.Packet{}, nil); !errors.Is(err, ErrNilWriter) {
		t.Fatalf("write err = %v, want ErrNilWriter", err)
	}
	muxer := &Muxer{}
	if err := muxer.Open(ctx, format.Output{Writer: discardWriter{}}, []av.Stream{testH264Stream()}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{}, nil); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("closed err = %v, want io.ErrClosedPipe", err)
	}
}

func TestMuxerWriteAllocs(t *testing.T) {
	ctx := context.Background()
	stream := testH264Stream()
	muxer := &Muxer{}
	if err := muxer.Open(ctx, format.Output{Writer: discardWriter{}}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	packet := av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{0x00, 0x00, 0x00, 0x01, 0x65}},
	}
	result := format.WriteResult{}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := muxer.Write(ctx, &packet, &result); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

type discardWriter struct{}

func (discardWriter) Write(payload []byte) (int, error) {
	return len(payload), nil
}

func testH264Stream() av.Stream {
	return av.Stream{
		ID:       "video",
		Index:    0,
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecH264,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     640,
			Height:    360,
		},
	}
}

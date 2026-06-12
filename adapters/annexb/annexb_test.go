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

func TestRegisterNilRegistryIsNoop(t *testing.T) {
	Register(nil)
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

func TestProberDetectsThreeByteStartCode(t *testing.T) {
	result, err := (Prober{}).Probe(context.Background(), format.ProbeRequest{
		Header: []byte{0x00, 0x00, 0x01, 0x65},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatAnnexB || result.Score != 100 {
		t.Fatalf("result = %+v", result)
	}
}

func TestProberDetectsExtensions(t *testing.T) {
	tests := []struct {
		name    string
		request format.ProbeRequest
	}{
		{name: "request name", request: format.ProbeRequest{Name: "video.264"}},
		{name: "input name", request: format.ProbeRequest{Input: format.Input{Name: "video.ANNEXB"}}},
		{name: "input uri", request: format.ProbeRequest{Input: format.Input{URI: "file:///tmp/video.H264"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := (Prober{}).Probe(context.Background(), tt.request)
			if err != nil {
				t.Fatal(err)
			}
			if result.Format != av.FormatAnnexB || result.Score != 80 {
				t.Fatalf("result = %+v", result)
			}
		})
	}
}

func TestProberRefusesUnknownOrCanceledProbe(t *testing.T) {
	if _, err := (Prober{}).Probe(context.Background(), format.ProbeRequest{
		Header: []byte{0x00, 0x00, 0x02},
		Name:   "video.mp4",
	}); !errors.Is(err, format.ErrNotFound) {
		t.Fatalf("unknown err = %v, want ErrNotFound", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (Prober{}).Probe(ctx, format.ProbeRequest{Name: "video.h264"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v, want context.Canceled", err)
	}
}

func TestMuxerFactoryNewMuxer(t *testing.T) {
	muxer, err := (MuxerFactory{}).NewMuxer(context.Background(), av.FormatAnnexB)
	if err != nil {
		t.Fatal(err)
	}
	if muxer.Format() != av.FormatAnnexB {
		t.Fatalf("format = %s, want %s", muxer.Format(), av.FormatAnnexB)
	}

	muxer, err = (MuxerFactory{}).NewMuxer(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := muxer.(*Muxer); !ok {
		t.Fatalf("muxer = %T, want *Muxer", muxer)
	}

	if _, err := (MuxerFactory{}).NewMuxer(context.Background(), av.FormatWebM); !errors.Is(err, format.ErrNotFound) {
		t.Fatalf("wrong format err = %v, want ErrNotFound", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (MuxerFactory{}).NewMuxer(ctx, av.FormatAnnexB); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v, want context.Canceled", err)
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

func TestMuxerOpenRejectsInvalidInputs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&Muxer{}).Open(ctx, format.Output{Writer: discardWriter{}}, []av.Stream{testH264Stream()}, format.OpenOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v, want context.Canceled", err)
	}

	if err := (&Muxer{}).Open(context.Background(), format.Output{}, []av.Stream{testH264Stream()}, format.OpenOptions{}); !errors.Is(err, ErrNilWriter) {
		t.Fatalf("nil writer err = %v, want ErrNilWriter", err)
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
	tests := []struct {
		name    string
		streams []av.Stream
	}{
		{
			name:    "duplicate h264",
			streams: []av.Stream{testH264Stream(), {ID: "video-2", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo}}},
		},
		{
			name:    "no h264",
			streams: []av.Stream{{ID: "video", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecVP8, Type: av.MediaVideo}}},
		},
		{
			name:    "stream type",
			streams: []av.Stream{{ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo}}},
		},
		{
			name:    "codec type",
			streams: []av.Stream{{ID: "audio", Type: av.MediaVideo, Codec: av.CodecParameters{ID: av.CodecH264, Type: av.MediaAudio}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := (&Muxer{}).Open(ctx, format.Output{Writer: &buffer}, tt.streams, format.OpenOptions{})
			if !errors.Is(err, ErrUnsupportedStream) {
				t.Fatalf("err = %v, want ErrUnsupportedStream", err)
			}
		})
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
	if err := muxer.Close(); err != nil {
		t.Fatalf("second close err = %v", err)
	}
}

func TestMuxerWriteRejectsInvalidInputs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&Muxer{}).Write(ctx, &av.Packet{}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled err = %v, want context.Canceled", err)
	}

	muxer := &Muxer{}
	if err := muxer.Open(context.Background(), format.Output{Writer: discardWriter{}}, []av.Stream{testH264Stream()}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(context.Background(), nil, nil); !errors.Is(err, format.ErrNilPacket) {
		t.Fatalf("nil packet err = %v, want ErrNilPacket", err)
	}
}

func TestMuxerWriteAcceptsUnspecifiedPacketStream(t *testing.T) {
	ctx := context.Background()
	var buffer bytes.Buffer
	muxer := &Muxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{testH264Stream()}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}

	payload := []byte{0x00, 0x00, 0x01, 0x65}
	if err := muxer.Write(ctx, &av.Packet{Payload: av.Buffer{Bytes: payload}}, nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buffer.Bytes(), payload) {
		t.Fatalf("payload = %v, want %v", buffer.Bytes(), payload)
	}
}

func TestWriteFullHandlesPartialAndFailedWriters(t *testing.T) {
	var buffer bytes.Buffer
	writer := chunkWriter{writer: &buffer, max: 2}
	payload := []byte{1, 2, 3, 4, 5}
	if err := writeFull(writer, payload); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buffer.Bytes(), payload) {
		t.Fatalf("payload = %v, want %v", buffer.Bytes(), payload)
	}

	writeErr := errors.New("write failed")
	if err := writeFull(errorWriter{err: writeErr}, payload); !errors.Is(err, writeErr) {
		t.Fatalf("error writer err = %v, want %v", err, writeErr)
	}
	if err := writeFull(zeroWriter{}, payload); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero writer err = %v, want io.ErrShortWrite", err)
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

type chunkWriter struct {
	writer io.Writer
	max    int
}

func (w chunkWriter) Write(payload []byte) (int, error) {
	if len(payload) > w.max {
		payload = payload[:w.max]
	}
	return w.writer.Write(payload)
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) {
	return 0, nil
}

type errorWriter struct {
	err error
}

func (w errorWriter) Write([]byte) (int, error) {
	return 0, w.err
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

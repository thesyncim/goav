//go:build goav_goav1

package goav

import (
	"context"
	"strings"
	"testing"

	"github.com/pion/rtp"
	goav1adapter "github.com/thesyncim/goav/adapters/goav1"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/rtpav"
	av1backend "github.com/thesyncim/goav1"
)

func TestRuntimeBuilderRTPAV1DecodeSink(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.RTPTimeBase(90000),
		Codec: av.CodecParameters{
			ID:          av.CodecAV1,
			Type:        av.MediaVideo,
			ClockRate:   90000,
			Width:       16,
			Height:      16,
			PixelFormat: av.PixelFormatGray8,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEAV1,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 3000},
			Payload: runtimeAV1RTPPayload(),
		}},
		events: make(chan av.Event),
	}
	sink := &runtimeTestSink{name: "frames"}

	builder := New(WithCodecAdapter(goav1adapter.Register)).New().
		RTP(receiver,
			WithRTPName("av1-rtp"),
			WithRTPDepacketizer(rtpav.NewAV1Depacketizer(stream, rtpav.WithMaxVideoFrameSize(128))),
		).
		Decode(SelectVideo()).
		Sink(sink)
	planned, err := builder.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(planned.String(), "av1-rtp -> select-video") ||
		!strings.Contains(planned.String(), "decode-video -> frames") {
		t.Fatalf("planned:\n%s", planned.String())
	}

	task, err := builder.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if planned.String() != task.Describe().String() || planned.Mermaid() != task.Describe().Mermaid() {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", planned.String(), task.Describe().String())
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if sink.frames != 1 ||
		sink.lastFrame.StreamID != "video" ||
		sink.lastFrame.Video == nil ||
		sink.lastFrame.Video.Width != 16 ||
		sink.lastFrame.Video.Height != 16 ||
		sink.lastFrame.Video.PixelFormat != av.PixelFormatGray8 ||
		sink.lastFrame.PTS.Value != 3000 {
		t.Fatalf("frames=%d last=%+v video=%+v", sink.frames, sink.lastFrame, sink.lastFrame.Video)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !receiver.closed || !sink.closed {
		t.Fatalf("closed receiver=%v sink=%v", receiver.closed, sink.closed)
	}
}

func runtimeAV1RTPPayload() []byte {
	seq := runtimeAV1OBUElement(av1backend.OBUSequenceHeader, runtimeAV1SequenceHeaderPayload())
	frame := runtimeAV1OBUElement(av1backend.OBUFrameHeader, runtimeAV1FrameHeaderPayload())
	tile := runtimeAV1OBUElement(av1backend.OBUTileGroup, []byte{0x80})
	payload := []byte{0x38}
	payload = append(payload, byte(len(seq)))
	payload = append(payload, seq...)
	payload = append(payload, byte(len(frame)))
	payload = append(payload, frame...)
	payload = append(payload, tile...)
	return payload
}

func runtimeAV1OBUElement(typ av1backend.OBUType, payload []byte) []byte {
	var header [2]byte
	n, err := av1backend.PutOBUHeader(header[:], av1backend.OBUHeader{Type: typ})
	if err != nil {
		panic(err)
	}
	out := append([]byte{}, header[:n]...)
	return append(out, payload...)
}

func runtimeAV1SequenceHeaderPayload() []byte {
	var w runtimeAV1BitWriter
	w.writeBits(0, 3)
	w.writeBool(true)
	w.writeBool(true)
	w.writeBits(5, 5)
	w.writeBits(7, 4)
	w.writeBits(7, 4)
	w.writeBits(15, 8)
	w.writeBits(15, 8)
	w.writeBool(false)
	w.writeBool(true)
	w.writeBool(true)
	w.writeBool(false)
	w.writeBool(true)
	w.writeBool(false)
	w.writeBool(false)
	w.writeBool(true)
	w.writeBool(false)
	w.writeBool(false)
	w.writeBool(false)
	return w.trailingBits()
}

func runtimeAV1FrameHeaderPayload() []byte {
	var w runtimeAV1BitWriter
	w.writeBool(true)
	w.writeBool(false)
	w.writeBool(false)
	w.writeBits(0, 8)
	w.writeBool(false)
	w.writeBool(false)
	w.writeBool(false)
	w.writeBool(false)
	return w.bytes()
}

type runtimeAV1BitWriter struct {
	buf [128]byte
	bit int
}

func (w *runtimeAV1BitWriter) writeBits(value uint64, n uint8) {
	for i := int(n) - 1; i >= 0; i-- {
		if (value>>uint(i))&1 != 0 {
			w.buf[w.bit>>3] |= 1 << uint(7-(w.bit&7))
		}
		w.bit++
	}
}

func (w *runtimeAV1BitWriter) writeBool(value bool) {
	if value {
		w.writeBits(1, 1)
		return
	}
	w.writeBits(0, 1)
}

func (w *runtimeAV1BitWriter) bytes() []byte {
	return w.buf[:(w.bit+7)>>3]
}

func (w *runtimeAV1BitWriter) trailingBits() []byte {
	w.writeBits(1, 1)
	for w.bit&7 != 0 {
		w.writeBits(0, 1)
	}
	return w.buf[:w.bit>>3]
}

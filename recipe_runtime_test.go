package goav

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/pion/rtp"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/rtpav"
)

func TestRecordRecipeRTPAutoCodecRuns(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	runtime := New(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	))

	task, err := Record(
		RTP(receiver).Name("audio").Codec(Opus()).RTPBuffer(RTPBufferLimits{MaxPackets: 2}),
		FileOutput("recording.ogg", io.Discard),
		UseRuntime(runtime),
	).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 {
		t.Fatalf("muxers=%d, want 1", len(muxers.muxers))
	}
	if muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "audio" {
		t.Fatalf("writes=%d stream=%s", muxers.muxers[0].writes, muxers.muxers[0].lastStream)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultRecordRecipeRTPVP8Runs(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     16,
			Height:    16,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90},
			Payload: []byte{0x10, 0x00, 0x11, 0x22},
		}},
		events: make(chan av.Event),
	}
	var out bytes.Buffer

	task, err := Record(
		RTP(receiver).Name("video").Codec(VP8()).RTPBuffer(RTPBufferLimits{MaxPackets: 2}),
		FileOutput("recording.ivf", &out),
	).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if out.Len() <= 32 {
		t.Fatalf("output bytes=%d, want IVF header and frame", out.Len())
	}
}

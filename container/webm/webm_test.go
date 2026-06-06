package webm

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/matroska"
	"github.com/thesyncim/goav/format"
)

func TestRegisterProvidesFactoriesAndProber(t *testing.T) {
	registry := format.NewRegistry()
	Register(registry)

	result, err := registry.Probe(context.Background(), format.ProbeRequest{Name: "camera.webm"})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatWebM {
		t.Fatalf("format = %s, want webm", result.Format)
	}
	if _, err := registry.DemuxerFactory(av.FormatWebM); err != nil {
		t.Fatalf("demuxer factory: %v", err)
	}
	if _, err := registry.MuxerFactory(av.FormatWebM); err != nil {
		t.Fatalf("muxer factory: %v", err)
	}
}

func TestMuxerEnforcesProfile(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{Type: TrackVideo, Codec: matroska.CodecH264}); !errors.Is(err, ErrUnsupportedWebMCodec) {
		t.Fatalf("err = %v, want ErrUnsupportedWebMCodec", err)
	}
	if _, err := muxer.AddTrack(Track{Type: TrackAudio, Codec: CodecVP8}); !errors.Is(err, ErrUnsupportedWebMCodec) {
		t.Fatalf("err = %v, want ErrUnsupportedWebMCodec", err)
	}
}

func TestMuxerDemuxerRoundTrip(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecAV1,
		Video: VideoConfig{Width: 1280, Height: 720},
	})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: []byte{1, 2, 3}},
		{TrackID: audioID, TimeNS: 20_000_000, Keyframe: true, Data: []byte{4, 5}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 2 || tracks[0].Codec != CodecAV1 || tracks[1].Codec != CodecOpus {
		t.Fatalf("tracks = %+v", tracks)
	}
	got := Packet{Data: make([]byte, 0, 16)}
	for i := range packets {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatal(err)
		}
		if got.TrackID != packets[i].TrackID || got.TimeNS != packets[i].TimeNS || !bytes.Equal(got.Data, packets[i].Data) {
			t.Fatalf("packet = %+v data=%v, want %+v data=%v", got, got.Data, packets[i], packets[i].Data)
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestDemuxerSeekToTime(t *testing.T) {
	file := writeSeekableCompatibilityWebM(t)
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(10_000_000); err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 16)}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 0 {
		t.Fatalf("packet time = %d, want first cue", packet.TimeNS)
	}
}

func TestFormatMuxerDemuxerRoundTrip(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Index:    0,
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			SampleRate: 48000,
			Channels:   2,
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{9, 8, 7}},
		PTS:      av.Timestamp{Value: 960, Base: stream.TimeBase},
		Keyframe: true,
	}, nil); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer := &FormatDemuxer{}
	if err := demuxer.Open(ctx, format.Input{Reader: bytes.NewReader(buffer.Bytes())}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	streams := demuxer.Streams()
	if len(streams) != 1 || streams[0].Codec.ID != av.CodecOpus || streams[0].Codec.SampleRate != 48000 {
		t.Fatalf("streams = %+v", streams)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 8)}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady || result.Packet.StreamID != "1" || result.Packet.PTS.Value != 20_000_000 ||
		!bytes.Equal(result.Packet.Payload.Bytes, []byte{9, 8, 7}) {
		t.Fatalf("result = %+v packet=%+v", result, result.Packet)
	}
}

package matroska

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/format"
)

func TestRegisterProvidesFactoriesAndProber(t *testing.T) {
	registry := format.NewRegistry()
	Register(registry)

	result, err := registry.Probe(context.Background(), format.ProbeRequest{
		Name: "recording.mkv",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Format != av.FormatMatroska {
		t.Fatalf("format = %s, want matroska", result.Format)
	}
	if _, err := registry.DemuxerFactory(av.FormatMatroska); err != nil {
		t.Fatalf("demuxer factory: %v", err)
	}
	if _, err := registry.MuxerFactory(av.FormatMatroska); err != nil {
		t.Fatalf("muxer factory: %v", err)
	}
}

func TestMuxerDemuxerRoundTrip(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{DocType: "webm"})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
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
	want := []Packet{
		{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: []byte{0x11, 0x22, 0x33}},
		{TrackID: audioID, TimeNS: 20_000_000, Keyframe: true, Data: []byte{0x44, 0x55}},
		{TrackID: videoID, TimeNS: 33_000_000, Data: []byte{0x66, 0x77, 0x88, 0x99}},
	}
	for i := range want {
		if err := muxer.WritePacket(want[i]); err != nil {
			t.Fatalf("write packet %d: %v", i, err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buffer.Bytes()[:4], []byte{0x1a, 0x45, 0xdf, 0xa3}) {
		t.Fatalf("missing ebml header magic")
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 2 {
		t.Fatalf("tracks = %d, want 2", len(tracks))
	}
	if tracks[0].Codec != CodecVP8 || tracks[0].Video.Width != 640 || tracks[1].Codec != CodecOpus || tracks[1].Audio.SampleRate != 48000 {
		t.Fatalf("tracks = %+v", tracks)
	}
	got := Packet{Data: make([]byte, 0, 16)}
	for i := range want {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatalf("read packet %d: %v", i, err)
		}
		if got.TrackID != want[i].TrackID || got.TimeNS != want[i].TimeNS || got.Keyframe != want[i].Keyframe ||
			!bytes.Equal(got.Data, want[i].Data) {
			t.Fatalf("packet %d = %+v data=%v, want %+v data=%v", i, got, got.Data, want[i], want[i].Data)
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestFormatMuxerDemuxerRoundTrip(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Index:    0,
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:     av.CodecVP9,
			Type:   av.MediaVideo,
			Width:  320,
			Height: 180,
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		PTS:      av.Timestamp{Value: 1800, Base: stream.TimeBase},
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
	if len(streams) != 1 || streams[0].Codec.ID != av.CodecVP9 || streams[0].Codec.Width != 320 {
		t.Fatalf("streams = %+v", streams)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 8)}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady || result.Packet.StreamID != "1" || result.Packet.PTS.Value != 20_000_000 ||
		!bytes.Equal(result.Packet.Payload.Bytes, []byte{1, 2, 3}) {
		t.Fatalf("result = %+v packet=%+v", result, result.Packet)
	}
}

func TestReadPacketRequiresCapacity(t *testing.T) {
	data := makeMatroskaData(t, 1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 1)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
}

func TestWritePacketAllocs(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{TrackID: id, TimeNS: 0, Keyframe: true, Data: []byte{1, 2, 3, 4}}
	if err := muxer.WritePacket(packet); err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := muxer.WritePacket(packet); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

func TestReadPacketAllocs(t *testing.T) {
	data := makeMatroskaData(t, 1200)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

func FuzzDemuxer(f *testing.F) {
	f.Add(makeMatroskaData(&testing.T{}, 1))
	f.Add([]byte{0x1a, 0x45, 0xdf, 0xa3, 0x80})
	f.Fuzz(func(t *testing.T, data []byte) {
		demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{MaxElementSize: 1 << 20})
		if err != nil {
			return
		}
		packet := Packet{Data: make([]byte, 0, 64)}
		for i := 0; i < 16; i++ {
			err := demuxer.ReadPacket(&packet)
			if err != nil {
				return
			}
		}
	})
}

func BenchmarkWriteSimpleBlock(b *testing.B) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{TrackID: id, TimeNS: 0, Keyframe: true, Data: make([]byte, 1200)}
	if err := muxer.WritePacket(packet); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(packet.Data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packet.TimeNS = int64(i) * 20_000_000
		if err := muxer.WritePacket(packet); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadSimpleBlock(b *testing.B) {
	data := makeMatroskaData(b, b.N+1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	b.ReportAllocs()
	b.SetBytes(4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := demuxer.ReadPacket(&packet); err != nil {
			b.Fatal(err)
		}
	}
}

func makeMatroskaData(tb testing.TB, packets int) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{DocType: "webm"})
	if err != nil {
		tb.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < packets; i++ {
		if err := muxer.WritePacket(Packet{
			TrackID:  id,
			TimeNS:   int64(i) * 20_000_000,
			Keyframe: i == 0,
			Data:     []byte{byte(i), 2, 3, 4},
		}); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

type discardWriter struct{}

func (discardWriter) Write(payload []byte) (int, error) {
	return len(payload), nil
}

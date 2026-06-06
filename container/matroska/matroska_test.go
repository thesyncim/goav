package matroska

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/ebml"
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

func TestMuxerDemuxerPreservesBlockGroupDuration(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{
		TrackID:    trackID,
		TimeNS:     40_000_000,
		DurationNS: 20_000_000,
		Keyframe:   false,
		Data:       []byte{0x11, 0x22, 0x33},
	}
	if err := muxer.WritePacket(packet); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != packet.TrackID || got.TimeNS != packet.TimeNS || got.DurationNS != packet.DurationNS ||
		got.Keyframe || !bytes.Equal(got.Data, packet.Data) {
		t.Fatalf("packet = %+v data=%v, want %+v data=%v", got, got.Data, packet, packet.Data)
	}
}

func TestNanosecondTimecodeScaleRoundTrip(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{TimecodeScaleNS: 1})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := Packet{
		TrackID:    trackID,
		TimeNS:     123_456_789,
		DurationNS: 2_500_001,
		Keyframe:   true,
		Data:       []byte{1, 2, 3},
	}
	if err := muxer.WritePacket(want); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != want.TimeNS || got.DurationNS != want.DurationNS {
		t.Fatalf("time=%d duration=%d, want time=%d duration=%d", got.TimeNS, got.DurationNS, want.TimeNS, want.DurationNS)
	}
}

func TestMuxerSplitsClustersBeforeBlockTimecodeOverflow(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
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
	for _, packet := range []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 33_000_000_000, Keyframe: true, Data: []byte{2}},
	} {
		if err := muxer.WritePacket(packet); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if clusters := countClusters(t, buffer.Bytes()); clusters != 2 {
		t.Fatalf("clusters = %d, want 2", clusters)
	}
}

func TestSeekableMuxerPatchesSegmentClusterAndDuration(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     40_000_000,
		DurationNS: 20_000_000,
		Keyframe:   true,
		Data:       []byte{1, 2, 3},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := ebmlReader(t, ws.bytes)
	header, err := reader.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	if header.ID != idEBML {
		t.Fatalf("first id = 0x%x, want EBML", uint64(header.ID))
	}
	if err := reader.Skip(header.Size.Value); err != nil {
		t.Fatal(err)
	}
	segment, err := reader.ReadHeader()
	if err != nil {
		t.Fatal(err)
	}
	if segment.ID != idSegment || segment.Size.Unknown {
		t.Fatalf("segment = %+v, want known-size segment", segment)
	}
	var sawKnownCluster bool
	var sawPatchedDuration bool
	segmentEnd := segment.DataOffset + int64(segment.Size.Value)
	for reader.Offset() < segmentEnd {
		child, err := reader.ReadHeader()
		if err != nil {
			t.Fatal(err)
		}
		if child.ID == idCluster {
			if child.Size.Unknown {
				t.Fatalf("cluster size is unknown in seekable mode")
			}
			sawKnownCluster = true
			break
		}
		if child.ID == idInfo {
			duration, ok := readInfoDuration(t, ws.bytes[child.DataOffset:child.DataOffset+int64(child.Size.Value)])
			if !ok {
				t.Fatalf("seekable Info did not contain Duration")
			}
			if duration != 60 {
				t.Fatalf("duration = %f, want 60 timestamp-scale ticks", duration)
			}
			sawPatchedDuration = true
		}
		if child.Size.Unknown {
			t.Fatalf("unexpected unknown child before cluster: %+v", child)
		}
		if err := reader.Skip(child.Size.Value); err != nil {
			t.Fatal(err)
		}
	}
	if !sawKnownCluster {
		t.Fatalf("did not find known-size cluster")
	}
	if !sawPatchedDuration {
		t.Fatalf("did not find patched duration")
	}
}

func TestSeekableMuxerWritesSeekHeadAndCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 2_000_000, Keyframe: true, Data: []byte{2}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	positions := collectTopLevelPositions(t, ws.bytes)
	for _, id := range []ebml.ID{idSeekHead, idInfo, idTracks, idCluster, idCues} {
		if _, ok := positions[id]; !ok {
			t.Fatalf("missing top-level element 0x%x in positions %+v", uint64(id), positions)
		}
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	entries := demuxer.SeekEntries()
	assertSeekEntry(t, entries, idInfo, positions[idInfo])
	assertSeekEntry(t, entries, idTracks, positions[idTracks])
	assertSeekEntry(t, entries, idCues, positions[idCues])

	got := Packet{Data: make([]byte, 0, 8)}
	for i := range packets {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatal(err)
		}
		if got.TimeNS != packets[i].TimeNS || !bytes.Equal(got.Data, packets[i].Data) {
			t.Fatalf("packet %d = %+v data=%v", i, got, got.Data)
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
	cues := demuxer.Cues()
	if len(cues) != len(packets) {
		t.Fatalf("cues = %+v, want %d cues", cues, len(packets))
	}
	for i := range cues {
		if cues[i].TrackID != trackID || cues[i].TimeNS != packets[i].TimeNS {
			t.Fatalf("cue %d = %+v, want track=%d time=%d", i, cues[i], trackID, packets[i].TimeNS)
		}
		if cues[i].ClusterPosition == 0 {
			t.Fatalf("cue %d cluster position = 0, positions=%+v", i, positions)
		}
	}
}

func TestDemuxerSeekToTimeUsesCues(t *testing.T) {
	ws := &memoryWriteSeeker{}
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
	packets := []Packet{
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 2_000_000, Keyframe: true, Data: []byte{2}},
		{TrackID: trackID, TimeNS: 4_000_000, Keyframe: true, Data: []byte{3}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(3_000_000); err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[1].TimeNS || !bytes.Equal(got.Data, packets[1].Data) {
		t.Fatalf("packet after seek = %+v data=%v, want %+v data=%v", got, got.Data, packets[1], packets[1].Data)
	}
}

func TestDemuxerSeekToTimeClearsPendingLacedFrames(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 1_000_000})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.writeHeader(); err != nil {
		t.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		t.Fatal(err)
	}
	if err := writeLacedSimpleBlock(muxer.ebml, trackID, simpleBlockLacingXiph, [][]byte{{1}, {2}, {3}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   2_000_000,
		Keyframe: true,
		Data:     []byte{9},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(ws.bytes), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Data, []byte{1}) {
		t.Fatalf("first packet data = %v, want laced first frame", got.Data)
	}
	if err := demuxer.SeekToTime(2_000_000); err != nil {
		t.Fatal(err)
	}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != 2_000_000 || !bytes.Equal(got.Data, []byte{9}) {
		t.Fatalf("packet after seek = %+v data=%v, want time=2000000 data=[9]", got, got.Data)
	}
}

func TestDemuxerSeekToTimeRequiresSeekableReader(t *testing.T) {
	data := makeMatroskaData(t, 1)
	demuxer, err := NewDemuxer(bytes.NewBuffer(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); !errors.Is(err, ErrNonSeekableReader) {
		t.Fatalf("err = %v, want ErrNonSeekableReader", err)
	}
}

func TestDemuxerReadsLacedSimpleBlocks(t *testing.T) {
	tests := []struct {
		name   string
		lacing byte
		frames [][]byte
	}{
		{
			name:   "xiph",
			lacing: simpleBlockLacingXiph,
			frames: [][]byte{{1, 2}, {3, 4, 5}, {6}},
		},
		{
			name:   "fixed",
			lacing: simpleBlockLacingFixed,
			frames: [][]byte{{1, 2}, {3, 4}, {5, 6}},
		},
		{
			name:   "ebml",
			lacing: simpleBlockLacingEBML,
			frames: [][]byte{{1, 2}, {3, 4, 5}, {6}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeLacedMatroskaData(t, tt.lacing, tt.frames, 20_000_000)
			demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, 8)}
			for i := range tt.frames {
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatalf("read frame %d: %v", i, err)
				}
				if packet.TrackID != 1 || packet.TimeNS != int64(i)*20_000_000 ||
					packet.DurationNS != 20_000_000 || !packet.Keyframe ||
					!bytes.Equal(packet.Data, tt.frames[i]) {
					t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, tt.frames[i])
				}
			}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, io.EOF) {
				t.Fatalf("err = %v, want EOF", err)
			}
		})
	}
}

func TestDemuxerRejectsLaceFrameCountOverLimit(t *testing.T) {
	data := makeLacedMatroskaData(t, simpleBlockLacingFixed, [][]byte{{1}, {2}, {3}}, 20_000_000)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{MaxLaceFrames: 2})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrLaceTooLarge) {
		t.Fatalf("err = %v, want ErrLaceTooLarge", err)
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

func TestReadPacketSmallBufferSkipsSimpleBlock(t *testing.T) {
	data := makeMatroskaData(t, 2)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 1)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, 8)
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 20_000_000 || !bytes.Equal(packet.Data, []byte{1, 2, 3, 4}) {
		t.Fatalf("packet = %+v data=%v", packet, packet.Data)
	}
}

func TestReadPacketSmallBufferSkipsBlockGroup(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     0,
		DurationNS: 20_000_000,
		Keyframe:   true,
		Data:       []byte{1, 2, 3},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   40_000_000,
		Keyframe: true,
		Data:     []byte{4, 5},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 1)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, 8)
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 40_000_000 || !bytes.Equal(packet.Data, []byte{4, 5}) {
		t.Fatalf("packet = %+v data=%v", packet, packet.Data)
	}
}

func TestReadPacketSmallBufferRetriesLacedFrame(t *testing.T) {
	frames := [][]byte{{1, 2}, {3}, {4}}
	data := makeLacedMatroskaData(t, simpleBlockLacingXiph, frames, 20_000_000)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 1)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, 8)
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if packet.TimeNS != int64(i)*20_000_000 || !bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet = %+v data=%v", i, packet, packet.Data)
		}
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

func BenchmarkReadXiphLacedSimpleBlock(b *testing.B) {
	data := makeRepeatedLacedMatroskaData(b, simpleBlockLacingXiph, [][]byte{{1, 2}, {3, 4}, {5, 6}}, b.N/3+1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	b.ReportAllocs()
	b.SetBytes(2)
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

func makeRepeatedLacedMatroskaData(tb testing.TB, lacing byte, frames [][]byte, blocks int) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	for i := 0; i < blocks; i++ {
		if err := writeLacedSimpleBlock(muxer.ebml, trackID, lacing, frames); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeLacedMatroskaData(tb testing.TB, lacing byte, frames [][]byte, defaultDurationNS int64) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: defaultDurationNS,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	if err := writeLacedSimpleBlock(muxer.ebml, trackID, lacing, frames); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func writeLacedSimpleBlock(writer *ebml.Writer, trackID uint32, lacing byte, frames [][]byte) error {
	var payload bytes.Buffer
	var scratch [ebml.MaxSizeWidth]byte
	n, err := ebml.EncodeUnsignedVINT(scratch[:], uint64(trackID))
	if err != nil {
		return err
	}
	payload.Write(scratch[:n])
	var blockHeader [3]byte
	binary.BigEndian.PutUint16(blockHeader[:2], 0)
	blockHeader[2] = simpleBlockKeyframe | lacing
	payload.Write(blockHeader[:])
	if len(frames) < 2 || len(frames) > 256 {
		return ErrInvalidData
	}
	payload.WriteByte(byte(len(frames) - 1))
	switch lacing {
	case simpleBlockLacingXiph:
		for i := 0; i < len(frames)-1; i++ {
			size := len(frames[i])
			for size >= 255 {
				payload.WriteByte(255)
				size -= 255
			}
			payload.WriteByte(byte(size))
		}
	case simpleBlockLacingFixed:
		size := len(frames[0])
		for i := range frames {
			if len(frames[i]) != size {
				return ErrInvalidData
			}
		}
	case simpleBlockLacingEBML:
		n, err := ebml.EncodeUnsignedVINT(scratch[:], uint64(len(frames[0])))
		if err != nil {
			return err
		}
		payload.Write(scratch[:n])
	default:
		return ErrUnsupportedLacing
	}
	if lacing == simpleBlockLacingEBML {
		previous := len(frames[0])
		for i := 1; i < len(frames)-1; i++ {
			delta := len(frames[i]) - previous
			n, err := encodeSignedLaceVINT(scratch[:], int64(delta))
			if err != nil {
				return err
			}
			payload.Write(scratch[:n])
			previous = len(frames[i])
		}
	}
	for i := range frames {
		payload.Write(frames[i])
	}
	return writer.WriteElement(idSimpleBlock, payload.Bytes())
}

func encodeSignedLaceVINT(dst []byte, value int64) (int, error) {
	for width := 1; width <= ebml.MaxSizeWidth; width++ {
		bias := int64((uint64(1) << uint(7*width-1)) - 1)
		encoded := value + bias
		if encoded < 0 {
			continue
		}
		if uint64(encoded) <= (uint64(1)<<uint(7*width))-2 {
			return ebml.EncodeUnsignedVINTWidth(dst, uint64(encoded), width)
		}
	}
	return 0, ErrInvalidData
}

func ebmlReader(tb testing.TB, data []byte) *ebml.Reader {
	tb.Helper()
	return ebml.NewReader(bytes.NewReader(data), ebml.ReaderOptions{})
}

func countClusters(tb testing.TB, data []byte) int {
	tb.Helper()
	reader := ebmlReader(tb, data)
	header, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if header.ID != idEBML {
		tb.Fatalf("first id = 0x%x, want EBML", uint64(header.ID))
	}
	if err := reader.Skip(header.Size.Value); err != nil {
		tb.Fatal(err)
	}
	segment, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if segment.ID != idSegment {
		tb.Fatalf("second id = 0x%x, want Segment", uint64(segment.ID))
	}
	clusters := 0
	for {
		child, err := reader.ReadHeader()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			tb.Fatal(err)
		}
		if child.ID == idCluster {
			clusters++
			if child.Size.Unknown {
				continue
			}
		}
		if child.Size.Unknown {
			break
		}
		if err := reader.Skip(child.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return clusters
}

func collectTopLevelPositions(tb testing.TB, data []byte) map[ebml.ID]uint64 {
	tb.Helper()
	reader := ebmlReader(tb, data)
	header, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if header.ID != idEBML {
		tb.Fatalf("first id = 0x%x, want EBML", uint64(header.ID))
	}
	if err := reader.Skip(header.Size.Value); err != nil {
		tb.Fatal(err)
	}
	segment, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if segment.ID != idSegment || segment.Size.Unknown {
		tb.Fatalf("segment = %+v, want known-size Segment", segment)
	}
	positions := make(map[ebml.ID]uint64)
	segmentEnd := segment.DataOffset + int64(segment.Size.Value)
	for reader.Offset() < segmentEnd {
		child, err := reader.ReadHeader()
		if err != nil {
			tb.Fatal(err)
		}
		positions[child.ID] = uint64(child.Offset - segment.DataOffset)
		if child.Size.Unknown {
			tb.Fatalf("unexpected unknown top-level element %+v", child)
		}
		if err := reader.Skip(child.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return positions
}

func assertSeekEntry(tb testing.TB, entries []SeekEntry, id ebml.ID, position uint64) {
	tb.Helper()
	for i := range entries {
		if entries[i].ID == uint64(id) {
			if entries[i].Position != position {
				tb.Fatalf("seek entry 0x%x position = %d, want %d", uint64(id), entries[i].Position, position)
			}
			return
		}
	}
	tb.Fatalf("missing seek entry 0x%x in %+v", uint64(id), entries)
}

func readInfoDuration(tb testing.TB, payload []byte) (float64, bool) {
	tb.Helper()
	reader := ebml.NewReader(bytes.NewReader(payload), ebml.ReaderOptions{})
	for reader.Offset() < int64(len(payload)) {
		header, err := reader.ReadHeader()
		if err != nil {
			tb.Fatal(err)
		}
		if header.ID == idDuration {
			if header.Size.Value != 8 {
				tb.Fatalf("duration size = %d, want 8", header.Size.Value)
			}
			var scratch [8]byte
			if err := reader.ReadFull(scratch[:]); err != nil {
				tb.Fatal(err)
			}
			return math.Float64frombits(binary.BigEndian.Uint64(scratch[:])), true
		}
		if header.Size.Unknown {
			tb.Fatalf("unexpected unknown Info child: %+v", header)
		}
		if err := reader.Skip(header.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return 0, false
}

type discardWriter struct{}

func (discardWriter) Write(payload []byte) (int, error) {
	return len(payload), nil
}

type memoryWriteSeeker struct {
	bytes []byte
	pos   int64
}

func (m *memoryWriteSeeker) Write(p []byte) (int, error) {
	end := m.pos + int64(len(p))
	if end > int64(len(m.bytes)) {
		next := make([]byte, end)
		copy(next, m.bytes)
		m.bytes = next
	}
	copy(m.bytes[m.pos:end], p)
	m.pos = end
	return len(p), nil
}

func (m *memoryWriteSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += m.pos
	case io.SeekEnd:
		offset += int64(len(m.bytes))
	default:
		return 0, errors.New("invalid whence")
	}
	if offset < 0 {
		return 0, errors.New("negative offset")
	}
	m.pos = offset
	return offset, nil
}

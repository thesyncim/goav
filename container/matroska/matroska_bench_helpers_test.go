package matroska

import (
	"bytes"
	"io"
	"testing"
)

func assertLargeWebRTCCorpusSeekIndex(t testing.TB, data []byte, payloads benchmarkWebRTCPayloadSet, hasDirectCues bool, wantPacketIndex bool) {
	t.Helper()
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	for _, cycle := range []int{0, 1, 127, 511, largeWebRTCCorpusRegressionCycles - 1} {
		timeNS := int64(cycle) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadPacketAtTime(timeNS, &packet); err != nil {
			t.Fatalf("read packet at cycle %d: %v", cycle, err)
		}
		if packet.TimeNS != timeNS || packet.DurationNS != benchmarkWebRTCCorpusFrameDurationNS || len(packet.Data) == 0 {
			t.Fatalf("packet at cycle %d = %+v data=%d", cycle, packet, len(packet.Data))
		}
		assertLargeWebRTCCorpusTrackPacket(t, demuxer, CodecOpus, timeNS, payloads.opus, payloads.maxPayload)
		assertLargeWebRTCCorpusTrackPacket(t, demuxer, CodecAV1, timeNS, payloads.av1, payloads.maxPayload)
		assertLargeWebRTCCorpusTrackPacket(t, demuxer, CodecH264, timeNS, payloads.h264, payloads.maxPayload)
		assertLargeWebRTCCorpusTrackPacket(t, demuxer, CodecVP9, timeNS, payloads.vp9, payloads.maxPayload)
		assertLargeWebRTCCorpusTrackPacket(t, demuxer, CodecVP8, timeNS, payloads.vp8, payloads.maxPayload)
	}
	for _, cycle := range []int{0, 127, 511, largeWebRTCCorpusRegressionCycles - 2} {
		targetNS := int64(cycle)*benchmarkWebRTCCorpusFrameDurationNS + benchmarkWebRTCCorpusFrameDurationNS/2
		wantTimeNS := int64(cycle+1) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadPacketAtTime(targetNS, &packet); err != nil {
			t.Fatalf("read packet after midpoint cycle %d: %v", cycle, err)
		}
		if packet.TimeNS != wantTimeNS {
			t.Fatalf("midpoint packet at cycle %d time = %d, want %d", cycle, packet.TimeNS, wantTimeNS)
		}
		if hasDirectCues {
			if err := demuxer.ReadCuedPacketAtTime(targetNS, &packet); err != nil {
				t.Fatalf("read cued packet after midpoint cycle %d: %v", cycle, err)
			}
			if packet.TimeNS != wantTimeNS {
				t.Fatalf("midpoint cued packet at cycle %d time = %d, want %d", cycle, packet.TimeNS, wantTimeNS)
			}
		}
	}
	if wantPacketIndex && !demuxer.packetIndexBuilt {
		t.Fatal("packet index was not built for large cue-free corpus")
	}
}

func assertLargeWebRTCCorpusTrackPacket(t testing.TB, demuxer *Demuxer, codec Codec, timeNS int64, wantData []byte, capacity int) {
	t.Helper()
	trackID := benchmarkTrackIDForCodec(t, demuxer, codec)
	packet := Packet{Data: make([]byte, 0, capacity)}
	if err := demuxer.ReadTrackPacketAtTime(trackID, timeNS, &packet); err != nil {
		t.Fatalf("read %v track packet at %d: %v", codec, timeNS, err)
	}
	if packet.TrackID != trackID ||
		packet.TimeNS != timeNS ||
		packet.DurationNS != benchmarkWebRTCCorpusFrameDurationNS ||
		!packet.Keyframe ||
		!bytes.Equal(packet.Data, wantData) {
		t.Fatalf("%v packet at %d = %+v data=%x, want data=%x", codec, timeNS, packet, packet.Data, wantData)
	}
}

func benchmarkReadPacketAtTimeWebRTCCorpus(b *testing.B, cycles int) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusSeekableMatroskaData(b, cycles, payloads)
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	if err := demuxer.ReadPacketAtTime(0, &packet); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		targetNS := int64(i%cycles) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadPacketAtTime(targetNS, &packet); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkReadPacketAtTimeNoCuesWebRTCCorpus(b *testing.B, cycles int) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusNoCueMatroskaData(b, cycles, payloads)
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		targetNS := int64(i%cycles) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadPacketAtTime(targetNS, &packet); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkReadTrackPacketAtTimeWebRTCCorpus(b *testing.B, cycles int, noCues bool) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusSeekableMatroskaData(b, cycles, payloads)
	if noCues {
		data = makeBenchmarkWebRTCCorpusNoCueMatroskaData(b, cycles, payloads)
	}
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	trackID := benchmarkTrackIDForCodec(b, demuxer, CodecVP8)
	if err := demuxer.SeekToTrackTime(trackID, 0); err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	if err := demuxer.ReadTrackPacketAtTime(trackID, 0, &packet); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		targetNS := int64(i%cycles) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadTrackPacketAtTime(trackID, targetNS, &packet); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkReadCuedPacketAtTimeWebRTCCorpus(b *testing.B, cycles int) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusSeekableMatroskaData(b, cycles, payloads)
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		targetNS := int64(i%cycles) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadCuedPacketAtTime(targetNS, &packet); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkReadClusterOnlyCuedPacketAtTimeWebRTCCorpus(b *testing.B, cycles int) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusClusterOnlyCueMatroskaData(b, cycles, payloads)
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if err := demuxer.SeekToTime(0); err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		targetNS := int64(i%cycles) * benchmarkWebRTCCorpusFrameDurationNS
		if err := demuxer.ReadCuedPacketAtTime(targetNS, &packet); err != nil {
			b.Fatal(err)
		}
	}
}

const benchmarkWebRTCTrackCount = 5

const benchmarkWebRTCCorpusCycles = 256

const benchmarkWebRTCLargeCueCorpusCycles = 4096

const largeWebRTCCorpusRegressionCycles = 1024

const benchmarkWebRTCCorpusFrameDurationNS = 20_000_000

const benchmarkWebRTCCorpusClusterDurationNS = 1_000_000_000

type benchmarkWebRTCTracks struct {
	opus uint32
	av1  uint32
	h264 uint32
	vp9  uint32
	vp8  uint32
}

type benchmarkWebRTCPayloadSet struct {
	opus       []byte
	av1        []byte
	h264       []byte
	vp9        []byte
	vp8        []byte
	totalBytes int64
	maxPayload int
}

func benchmarkWebRTCPayloads() benchmarkWebRTCPayloadSet {
	payloads := benchmarkWebRTCPayloadSet{
		opus: []byte{0xf8, 0xff, 0xfe},
		av1:  av1SequenceHeaderOBU(),
		h264: h264AnnexBAccessUnit(),
		vp9:  repeatedBenchmarkPayload(1200, 0x83),
		vp8:  repeatedBenchmarkPayload(1200, 0x10),
	}
	payloads.vp8[3] = 0x9d
	payloads.vp8[4] = 0x01
	payloads.vp8[5] = 0x2a
	for _, payload := range [][]byte{payloads.opus, payloads.av1, payloads.h264, payloads.vp9, payloads.vp8} {
		payloads.totalBytes += int64(len(payload))
		if len(payload) > payloads.maxPayload {
			payloads.maxPayload = len(payload)
		}
	}
	return payloads
}

func repeatedBenchmarkPayload(size int, seed byte) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = seed + byte(i)
	}
	return payload
}

func benchmarkTrackIDForCodec(tb testing.TB, demuxer *Demuxer, codec Codec) uint32 {
	tb.Helper()
	for i := range demuxer.tracks {
		if demuxer.tracks[i].Codec == codec {
			return demuxer.tracks[i].ID
		}
	}
	tb.Fatalf("benchmark track with codec %v not found", codec)
	return 0
}

func addBenchmarkWebRTCTracks(tb testing.TB, muxer *Muxer) benchmarkWebRTCTracks {
	tb.Helper()
	var tracks benchmarkWebRTCTracks
	var err error
	tracks.opus, err = muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: benchmarkWebRTCCorpusFrameDurationNS,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		tb.Fatal(err)
	}
	tracks.av1, err = muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecAV1,
		CodecPrivate: av1CodecConfig(),
		Video:        VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		tb.Fatal(err)
	}
	tracks.h264, err = muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH264,
		CodecPrivate: h264AVCDecoderConfigWithLengthSize(2),
		Video:        VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		tb.Fatal(err)
	}
	tracks.vp9, err = muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP9,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		tb.Fatal(err)
	}
	tracks.vp8, err = muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return tracks
}

func writeBenchmarkWebRTCCorpus(tb testing.TB, muxer *Muxer, tracks benchmarkWebRTCTracks, payloads benchmarkWebRTCPayloadSet, cycle int) {
	tb.Helper()
	timeNS := int64(cycle) * benchmarkWebRTCCorpusFrameDurationNS
	packets := []Packet{
		{TrackID: tracks.opus, TimeNS: timeNS, DurationNS: benchmarkWebRTCCorpusFrameDurationNS, Keyframe: true, Data: payloads.opus},
		{TrackID: tracks.av1, TimeNS: timeNS, DurationNS: benchmarkWebRTCCorpusFrameDurationNS, Keyframe: true, Data: payloads.av1},
		{TrackID: tracks.h264, TimeNS: timeNS, DurationNS: benchmarkWebRTCCorpusFrameDurationNS, Keyframe: true, Data: payloads.h264},
		{TrackID: tracks.vp9, TimeNS: timeNS, DurationNS: benchmarkWebRTCCorpusFrameDurationNS, Keyframe: true, Data: payloads.vp9},
		{TrackID: tracks.vp8, TimeNS: timeNS, DurationNS: benchmarkWebRTCCorpusFrameDurationNS, Keyframe: true, Data: payloads.vp8},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			tb.Fatalf("write corpus packet %d: %v", i, err)
		}
	}
}

func makeBenchmarkWebRTCCorpusMatroskaData(tb testing.TB, cycles int, payloads benchmarkWebRTCPayloadSet) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	buffer.Grow(benchmarkWebRTCCorpusCapacity(cycles, payloads))
	writeBenchmarkWebRTCCorpusFile(tb, &Muxer{}, &buffer, cycles, payloads)
	return buffer.Bytes()
}

func makeBenchmarkWebRTCCorpusSeekableMatroskaData(tb testing.TB, cycles int, payloads benchmarkWebRTCPayloadSet) []byte {
	tb.Helper()
	writer := memoryWriteSeeker{bytes: make([]byte, 0, benchmarkWebRTCCorpusCapacity(cycles, payloads))}
	writeBenchmarkWebRTCCorpusFile(tb, &Muxer{}, &writer, cycles, payloads)
	return writer.bytes
}

func makeBenchmarkWebRTCCorpusClusterOnlyCueMatroskaData(tb testing.TB, cycles int, payloads benchmarkWebRTCPayloadSet) []byte {
	tb.Helper()
	writer := memoryWriteSeeker{bytes: make([]byte, 0, benchmarkWebRTCCorpusCapacity(cycles, payloads))}
	writeBenchmarkWebRTCCorpusFileWithCueRewrite(tb, &Muxer{}, &writer, cycles, payloads, rewriteClusterOnlyBenchmarkCues)
	return writer.bytes
}

func makeBenchmarkWebRTCCorpusNoCueMatroskaData(tb testing.TB, cycles int, payloads benchmarkWebRTCPayloadSet) []byte {
	tb.Helper()
	writer := memoryWriteSeeker{bytes: make([]byte, 0, benchmarkWebRTCCorpusCapacity(cycles, payloads))}
	writeBenchmarkWebRTCCorpusFileWithOptionsAndCueRewrite(tb, &Muxer{}, &writer, cycles, payloads, MuxerOptions{
		ClusterMaxDurationNS: benchmarkWebRTCCorpusClusterDurationNS,
		CuePolicy:            CuePolicyNone,
	}, nil)
	return writer.bytes
}

func benchmarkWebRTCCorpusCapacity(cycles int, payloads benchmarkWebRTCPayloadSet) int {
	payloadBytes := payloads.totalBytes * int64(cycles)
	metadataBytes := int64(cycles*benchmarkWebRTCTrackCount*256 + 256*1024)
	return int(payloadBytes + metadataBytes)
}

func writeBenchmarkWebRTCCorpusFile(tb testing.TB, muxer *Muxer, writer io.Writer, cycles int, payloads benchmarkWebRTCPayloadSet) {
	tb.Helper()
	writeBenchmarkWebRTCCorpusFileWithCueRewrite(tb, muxer, writer, cycles, payloads, nil)
}

func writeBenchmarkWebRTCCorpusFileWithCueRewrite(tb testing.TB, muxer *Muxer, writer io.Writer, cycles int, payloads benchmarkWebRTCPayloadSet, rewrite func([]CuePoint) []CuePoint) {
	tb.Helper()
	writeBenchmarkWebRTCCorpusFileWithOptionsAndCueRewrite(tb, muxer, writer, cycles, payloads, MuxerOptions{
		ClusterMaxDurationNS: benchmarkWebRTCCorpusClusterDurationNS,
	}, rewrite)
}

func writeBenchmarkWebRTCCorpusFileWithOptionsAndCueRewrite(tb testing.TB, muxer *Muxer, writer io.Writer, cycles int, payloads benchmarkWebRTCPayloadSet, opts MuxerOptions, rewrite func([]CuePoint) []CuePoint) {
	tb.Helper()
	muxer.init(writer, opts)
	tracks := addBenchmarkWebRTCTracks(tb, muxer)
	for i := range cycles {
		writeBenchmarkWebRTCCorpus(tb, muxer, tracks, payloads, i)
	}
	if rewrite != nil {
		muxer.cues = rewrite(muxer.cues)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
}

func rewriteClusterOnlyBenchmarkCues(cues []CuePoint) []CuePoint {
	out := make([]CuePoint, 0, len(cues))
	for i := range cues {
		position, ok := firstCueTrackPosition(cues[i])
		if !ok {
			continue
		}
		cue := CuePoint{
			TrackID:         position.TrackID,
			TimeNS:          cues[i].TimeNS,
			ClusterPosition: position.ClusterPosition,
			Positions: []CueTrackPosition{{
				TrackID:         position.TrackID,
				ClusterPosition: position.ClusterPosition,
				DurationNS:      position.DurationNS,
				DurationSet:     position.DurationSet,
			}},
		}
		applyCuePosition(&cue, cue.Positions[0])
		out = append(out, cue)
	}
	return out
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
	for i := range packets {
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

func makeH264AVCMatroskaData(tb testing.TB, packets int) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH264,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: h264AVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		tb.Fatal(err)
	}
	data := h264AnnexBAccessUnit()
	for i := range packets {
		if err := muxer.WritePacket(Packet{
			TrackID:  id,
			TimeNS:   int64(i) * 20_000_000,
			Keyframe: i == 0,
			Data:     data,
		}); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeH265HEVCMatroskaData(tb testing.TB, packets int) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH265,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: h265HEVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		tb.Fatal(err)
	}
	data := h265AnnexBAccessUnit()
	for i := range packets {
		if err := muxer.WritePacket(Packet{
			TrackID:  id,
			TimeNS:   int64(i) * 20_000_000,
			Keyframe: i == 0,
			Data:     data,
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
	for range blocks {
		if err := writeLacedSimpleBlock(muxer.ebml, trackID, lacing, frames); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeLaceFrames(count int, frame []byte) [][]byte {
	frames := make([][]byte, count)
	for i := range frames {
		frames[i] = frame
	}
	return frames
}

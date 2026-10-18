package matroska

import (
	"bytes"
	"testing"
)

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

func BenchmarkWriteXiphLacedSimpleBlock(b *testing.B) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		b.Fatal(err)
	}
	packet := LacedPacket{
		TrackID:  id,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames: [][]byte{
			make([]byte, 400),
			make([]byte, 400),
			make([]byte, 400),
		},
	}
	if err := muxer.WriteLacedPacket(packet); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(1200)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packet.TimeNS = int64(i) * 60_000_000
		if err := muxer.WriteLacedPacket(packet); err != nil {
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

func BenchmarkReadH264AVCToAnnexB(b *testing.B) {
	data := makeH264AVCMatroskaData(b, b.N+1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(h264AnnexBAccessUnit()))}
	b.ReportAllocs()
	b.SetBytes(int64(len(h264AnnexBAccessUnit())))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := demuxer.ReadPacket(&packet); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadH265HEVCToAnnexB(b *testing.B) {
	data := makeH265HEVCMatroskaData(b, b.N+1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(h265AnnexBAccessUnit()))}
	b.ReportAllocs()
	b.SetBytes(int64(len(h265AnnexBAccessUnit())))
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

func BenchmarkWriteWebRTCCorpus(b *testing.B) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	tracks := addBenchmarkWebRTCTracks(b, muxer)
	payloads := benchmarkWebRTCPayloads()
	writeBenchmarkWebRTCCorpus(b, muxer, tracks, payloads, 0)
	b.ReportAllocs()
	b.SetBytes(payloads.totalBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeBenchmarkWebRTCCorpus(b, muxer, tracks, payloads, i+1)
	}
}

func BenchmarkReadWebRTCCorpus(b *testing.B) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusMatroskaData(b, benchmarkWebRTCCorpusCycles, payloads)
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	cyclesRead := 0
	b.ReportAllocs()
	b.SetBytes(payloads.totalBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cyclesRead == benchmarkWebRTCCorpusCycles {
			reader.Reset(data)
			if err := demuxer.init(&reader, DemuxerOptions{}); err != nil {
				b.Fatal(err)
			}
			cyclesRead = 0
		}
		for range benchmarkWebRTCTrackCount {
			if err := demuxer.ReadPacket(&packet); err != nil {
				b.Fatal(err)
			}
		}
		cyclesRead++
	}
}

func BenchmarkWriteSeekableWebRTCCorpus(b *testing.B) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusSeekableMatroskaData(b, benchmarkWebRTCCorpusCycles, payloads)
	var writer memoryWriteSeeker
	var muxer Muxer
	b.ReportAllocs()
	b.SetBytes(payloads.totalBytes * int64(benchmarkWebRTCCorpusCycles))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cap(writer.bytes) < len(data) {
			writer.bytes = make([]byte, 0, len(data))
		} else {
			writer.bytes = writer.bytes[:0]
		}
		writer.pos = 0
		writeBenchmarkWebRTCCorpusFile(b, &muxer, &writer, benchmarkWebRTCCorpusCycles, payloads)
	}
}

func BenchmarkReadSeekableWebRTCCorpus(b *testing.B) {
	payloads := benchmarkWebRTCPayloads()
	data := makeBenchmarkWebRTCCorpusSeekableMatroskaData(b, benchmarkWebRTCCorpusCycles, payloads)
	var reader bytes.Reader
	reader.Reset(data)
	demuxer, err := NewDemuxer(&reader, DemuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}
	cyclesRead := 0
	b.ReportAllocs()
	b.SetBytes(payloads.totalBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if cyclesRead == benchmarkWebRTCCorpusCycles {
			reader.Reset(data)
			if err := demuxer.init(&reader, DemuxerOptions{}); err != nil {
				b.Fatal(err)
			}
			cyclesRead = 0
		}
		for range benchmarkWebRTCTrackCount {
			if err := demuxer.ReadPacket(&packet); err != nil {
				b.Fatal(err)
			}
		}
		cyclesRead++
	}
}

func BenchmarkReadPacketAtTimeWebRTCCorpus(b *testing.B) {
	benchmarkReadPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCCorpusCycles)
}

func BenchmarkReadPacketAtTimeLargeWebRTCCorpus(b *testing.B) {
	benchmarkReadPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCLargeCueCorpusCycles)
}

func BenchmarkReadPacketAtTimeNoCuesWebRTCCorpus(b *testing.B) {
	benchmarkReadPacketAtTimeNoCuesWebRTCCorpus(b, benchmarkWebRTCCorpusCycles)
}

func BenchmarkReadPacketAtTimeNoCuesLargeWebRTCCorpus(b *testing.B) {
	benchmarkReadPacketAtTimeNoCuesWebRTCCorpus(b, benchmarkWebRTCLargeCueCorpusCycles)
}

func BenchmarkReadTrackPacketAtTimeWebRTCCorpus(b *testing.B) {
	benchmarkReadTrackPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCCorpusCycles, false)
}

func BenchmarkReadTrackPacketAtTimeLargeWebRTCCorpus(b *testing.B) {
	benchmarkReadTrackPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCLargeCueCorpusCycles, false)
}

func BenchmarkReadTrackPacketAtTimeNoCuesWebRTCCorpus(b *testing.B) {
	benchmarkReadTrackPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCCorpusCycles, true)
}

func BenchmarkReadTrackPacketAtTimeNoCuesLargeWebRTCCorpus(b *testing.B) {
	benchmarkReadTrackPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCLargeCueCorpusCycles, true)
}

func BenchmarkReadCuedPacketAtTimeWebRTCCorpus(b *testing.B) {
	benchmarkReadCuedPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCCorpusCycles)
}

func BenchmarkReadCuedPacketAtTimeLargeWebRTCCorpus(b *testing.B) {
	benchmarkReadCuedPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCLargeCueCorpusCycles)
}

func BenchmarkReadClusterOnlyCuedPacketAtTimeWebRTCCorpus(b *testing.B) {
	benchmarkReadClusterOnlyCuedPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCCorpusCycles)
}

func BenchmarkReadClusterOnlyCuedPacketAtTimeLargeWebRTCCorpus(b *testing.B) {
	benchmarkReadClusterOnlyCuedPacketAtTimeWebRTCCorpus(b, benchmarkWebRTCLargeCueCorpusCycles)
}

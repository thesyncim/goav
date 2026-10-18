package matroska

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/container/ebml"
)

func zlibTestPayload(label string) []byte {
	prefix := []byte("zlib content encoding test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*16)
	for range 16 {
		payload = append(payload, prefix...)
	}
	return payload
}

func lzoContentEncodings(t testing.TB) []byte {
	t.Helper()
	return contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoLZO1X, nil))
		}),
	)
}

func lzoTestPayload(label string) []byte {
	prefix := []byte("lzo1x content encoding test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*16)
	for range 16 {
		payload = append(payload, prefix...)
	}
	return payload
}

func chainedContentPayload(label string) []byte {
	prefix := []byte("ordered content encoding test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*16)
	for range 16 {
		payload = append(payload, prefix...)
	}
	return payload
}

func withContentEncodingHeader(header, data []byte) []byte {
	out := make([]byte, 0, len(header)+len(data))
	out = append(out, header...)
	out = append(out, data...)
	return out
}

func chainedHeaderZlibContentEncodings(header []byte) []ContentEncoding {
	return []ContentEncoding{
		{
			Order:          0,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  header,
			},
		},
		{
			Order:          1,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
		},
	}
}

func chainedHeaderLZOContentEncodings(header []byte) []ContentEncoding {
	return []ContentEncoding{
		{
			Order:          0,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  header,
			},
		},
		{
			Order:          1,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoLZO1X},
		},
	}
}

func chainedHeaderBzlibBlockContentEncodings(header []byte) []ContentEncoding {
	return []ContentEncoding{
		{
			Order:          0,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  header,
			},
		},
		{
			Order:          1,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoBzlib},
		},
	}
}

func chainedHeaderBzlibContentEncodings(t testing.TB, header []byte) []byte {
	t.Helper()
	return contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			if err := w.WriteUInt(idContentEncodingOrd, 0); err != nil {
				return err
			}
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoHeaderStripping, header))
		}),
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			if err := w.WriteUInt(idContentEncodingOrd, 1); err != nil {
				return err
			}
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoBzlib, nil))
		}),
	)
}

func bzlibContentEncodings(t testing.TB) []byte {
	t.Helper()
	return contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoBzlib, nil))
		}),
	)
}

func bzlibTestPayload(label string) []byte {
	prefix := []byte("bzip2 content encoding test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*8)
	for range 8 {
		payload = append(payload, prefix...)
	}
	return payload
}

func bzlibCompressedPayload(t testing.TB, label string) []byte {
	t.Helper()
	fixtures := map[string]string{
		"simple block": "425a6839314159265359c57d93a80000339980400010103eafcc302000902984d34069881554d3d4068f51934dc662e1a0e45854762c207236102c2074207a205435171518161d0c0b8f87c282824486e1a0918141b091228207a2a3f177245385090c57d93a80",
		"laced first":  "425a6839314159265359e931c9840000339980400010103fa5dc302000902984d340698815553f54dea6937a8d344f481e0d848fa286048d05c6048b0dc6050b8d0585c48dc205850c8c0919143c10287438091610351fc703210351d0c8f45dc914e14243a4c72610",
		"laced second": "425a6839314159265359c10c71800000339980400010103ea5cc3020009029a6462626205553351a34c9ea34a0f440a0e45c48a8644881617123c18141020587a24586438181a1b8d0ec5c68545448ec6db8b0702e30282064687e2ee48a70a1218218e300",
	}
	value, ok := fixtures[label]
	if !ok {
		t.Fatalf("missing bzlib fixture %q", label)
	}
	out, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func aesCTRContentEncryptionKeyID() []byte {
	return []byte("aes-ctr-key")
}

func aesCTRContentEncryptionKeys(keyID []byte) []ContentEncryptionKey {
	return []ContentEncryptionKey{{
		KeyID: append([]byte(nil), keyID...),
		Key: []byte{
			0x10, 0x11, 0x12, 0x13,
			0x14, 0x15, 0x16, 0x17,
			0x18, 0x19, 0x1a, 0x1b,
			0x1c, 0x1d, 0x1e, 0x1f,
		},
	}}
}

func aesCTRContentEncryptionInitialIV() []byte {
	return []byte{0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef}
}

func aesCTRContentEncryptionTrackEncodings(keyID []byte) []ContentEncoding {
	return []ContentEncoding{{
		Type:          ContentEncodingTypeEncryption,
		EncryptionSet: true,
		Encryption: ContentEncryption{
			Algorithm:      ContentEncAlgoAES,
			KeyID:          append([]byte(nil), keyID...),
			AESSettingsSet: true,
			AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCTR},
		},
	}}
}

func chainedHeaderCompressionAESCTRContentEncodings(header []byte, algorithm uint64, keyID []byte) []ContentEncoding {
	return []ContentEncoding{
		{
			Order:          0,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  header,
			},
		},
		{
			Order:          1,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: algorithm},
		},
		{
			Order:         2,
			Type:          ContentEncodingTypeEncryption,
			EncryptionSet: true,
			Encryption: ContentEncryption{
				Algorithm:      ContentEncAlgoAES,
				KeyID:          append([]byte(nil), keyID...),
				AESSettingsSet: true,
				AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCTR},
			},
		},
	}
}

func aesCTRContentEncryptionContentEncodings(t testing.TB, keyID []byte) []byte {
	t.Helper()
	return contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			if err := w.WriteUInt(idContentEncodingType, ContentEncodingTypeEncryption); err != nil {
				return err
			}
			return w.WriteElement(idContentEncryption, contentEncryptionPayload(t, ContentEncAlgoAES, keyID, ContentEncAESCipherModeCTR))
		}),
	)
}

func encryptedTestPayload(label string) []byte {
	prefix := []byte("aes ctr content encryption test payload: " + label + ":")
	payload := make([]byte, 0, len(prefix)*12)
	for range 12 {
		payload = append(payload, prefix...)
	}
	return payload
}

func partitionedAESCTRContentEncryptionFrame(signal byte, iv []byte, partitions []uint32, payload []byte) []byte {
	frame := append([]byte{signal}, iv...)
	frame = append(frame, byte(len(partitions)))
	var offset [4]byte
	for i := range partitions {
		binary.BigEndian.PutUint32(offset[:], partitions[i])
		frame = append(frame, offset[:]...)
	}
	return append(frame, payload...)
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
		for range 16 {
			err := demuxer.ReadPacket(&packet)
			if err != nil {
				return
			}
		}
	})
}

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

func equalInt64s(left []int64, right []int64) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func equalBlockAdditions(left []BlockAddition, right []BlockAddition) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].ID != right[i].ID || !bytes.Equal(left[i].Data, right[i].Data) {
			return false
		}
	}
	return true
}

func equalBlockAdditionMapping(left BlockAdditionMapping, right BlockAdditionMapping) bool {
	return left.IDValue == right.IDValue &&
		left.Name == right.Name &&
		left.Type == right.Type &&
		bytes.Equal(left.ExtraData, right.ExtraData)
}

func equalTrackTranslate(left TrackTranslate, right TrackTranslate) bool {
	return left.Codec == right.Codec &&
		bytes.Equal(left.TrackID, right.TrackID) &&
		reflect.DeepEqual(left.EditionUIDs, right.EditionUIDs)
}

func equalCuePoint(left CuePoint, right CuePoint) bool {
	if left.TrackID != right.TrackID ||
		left.TimeNS != right.TimeNS ||
		left.ClusterPosition != right.ClusterPosition ||
		left.RelativePosition != right.RelativePosition ||
		left.RelativePositionSet != right.RelativePositionSet ||
		left.DurationNS != right.DurationNS ||
		left.DurationSet != right.DurationSet ||
		left.BlockNumber != right.BlockNumber ||
		left.BlockNumberSet != right.BlockNumberSet ||
		left.CodecStatePosition != right.CodecStatePosition ||
		left.CodecStateSet != right.CodecStateSet ||
		!equalCueReferences(left.References, right.References) ||
		len(left.Positions) != len(right.Positions) {
		return false
	}
	for i := range left.Positions {
		if !equalCueTrackPosition(left.Positions[i], right.Positions[i]) {
			return false
		}
	}
	return true
}

func equalCueTrackPosition(left CueTrackPosition, right CueTrackPosition) bool {
	return left.TrackID == right.TrackID &&
		left.ClusterPosition == right.ClusterPosition &&
		left.RelativePosition == right.RelativePosition &&
		left.RelativePositionSet == right.RelativePositionSet &&
		left.DurationNS == right.DurationNS &&
		left.DurationSet == right.DurationSet &&
		left.BlockNumber == right.BlockNumber &&
		left.BlockNumberSet == right.BlockNumberSet &&
		left.CodecStatePosition == right.CodecStatePosition &&
		left.CodecStateSet == right.CodecStateSet &&
		equalCueReferences(left.References, right.References)
}

func equalCueReferences(left []CueReference, right []CueReference) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func requirePacketTrackIndex(t *testing.T, demuxer *Demuxer, trackID uint32, times []int64) {
	t.Helper()
	entries, ok := demuxer.packetIndexEntriesForTrack(trackID)
	if !ok {
		t.Fatalf("packet track index for track %d was not built", trackID)
	}
	if len(entries) != len(times) {
		t.Fatalf("packet track index for track %d has %d entries, want %d", trackID, len(entries), len(times))
	}
	for i, entryIndex := range entries {
		if entryIndex < 0 || entryIndex >= len(demuxer.packetIndex) {
			t.Fatalf("packet track index %d for track %d points outside packet index: %d", i, trackID, entryIndex)
		}
		entry := demuxer.packetIndex[entryIndex]
		if entry.TrackID != trackID || entry.TimeNS != times[i] {
			t.Fatalf("packet track index %d for track %d = %+v, want time %d", i, trackID, entry, times[i])
		}
	}
}

func expectedOpusHead(channels int, sampleRate int) []byte {
	if channels == 0 {
		channels = 2
	}
	if sampleRate == 0 {
		sampleRate = 48000
	}
	header := make([]byte, 19)
	copy(header, "OpusHead")
	header[8] = 1
	header[9] = byte(channels)
	binary.LittleEndian.PutUint32(header[12:16], uint32(sampleRate))
	return header
}

func expectedOpusHeadWithPreSkip(channels int, sampleRate int, preSkip int) []byte {
	header := expectedOpusHead(channels, sampleRate)
	binary.LittleEndian.PutUint16(header[10:12], uint16(preSkip))
	return header
}

func expectedMSACMWaveFormat(codec Codec, channels int, sampleRate int) []byte {
	tag := waveFormatMuLaw
	if codec == CodecPCMA {
		tag = waveFormatALaw
	}
	return msACMWaveFormatBytes(tag, channels, sampleRate, uint32(sampleRate*channels), channels, 8, nil)
}

func msACMWaveFormatBytes(tag uint16, channels int, sampleRate int, avgBytesPerSec uint32, blockAlign int, bitsPerSample int, extra []byte) []byte {
	private := make([]byte, waveFormatExBaseSize+len(extra))
	binary.LittleEndian.PutUint16(private[0:2], tag)
	binary.LittleEndian.PutUint16(private[2:4], uint16(channels))
	binary.LittleEndian.PutUint32(private[4:8], uint32(sampleRate))
	binary.LittleEndian.PutUint32(private[8:12], avgBytesPerSec)
	binary.LittleEndian.PutUint16(private[12:14], uint16(blockAlign))
	binary.LittleEndian.PutUint16(private[14:16], uint16(bitsPerSample))
	binary.LittleEndian.PutUint16(private[16:18], uint16(len(extra)))
	copy(private[waveFormatExBaseSize:], extra)
	return private
}

func validFLACCodecPrivate() []byte {
	private := make([]byte, 4+4+34)
	copy(private, "fLaC")
	private[4] = 0x80
	private[7] = 34
	streamInfo := private[8:]
	binary.BigEndian.PutUint16(streamInfo[0:2], 4096)
	binary.BigEndian.PutUint16(streamInfo[2:4], 4096)
	writeFLACStreamInfoAudio(streamInfo, 48000, 2, 16)
	return private
}

func flacCodecPrivateWithHeaderByte(offset int, value byte) []byte {
	private := validFLACCodecPrivate()
	private[offset] = value
	return private
}

func flacCodecPrivateWithStreamInfoBytes(offset int, values ...byte) []byte {
	private := validFLACCodecPrivate()
	copy(private[8+offset:], values)
	return private
}

func writeFLACStreamInfoAudio(streamInfo []byte, sampleRate int, channels int, bitsPerSample int) {
	channelsMinusOne := channels - 1
	bitsMinusOne := bitsPerSample - 1
	streamInfo[10] = byte(sampleRate >> 12)
	streamInfo[11] = byte(sampleRate >> 4)
	streamInfo[12] = byte((sampleRate&0x0f)<<4 | (channelsMinusOne&0x07)<<1 | (bitsMinusOne>>4)&0x01)
	streamInfo[13] = byte(bitsMinusOne << 4)
}

func makeAACAudioSpecificConfig(sampleRate int, channels int) []byte {
	index := aacSamplingFrequencyIndex(sampleRate)
	if index < 0 || channels < 1 || channels > 7 {
		panic("invalid AAC test config")
	}
	channelConfig := channels
	if channels == 8 {
		channelConfig = 7
	}
	value := uint16(2<<11 | index<<7 | channelConfig<<3)
	return []byte{byte(value >> 8), byte(value)}
}

func aacSamplingFrequencyIndex(sampleRate int) int {
	for i, rate := range aacSamplingFrequencies {
		if rate == sampleRate {
			return i
		}
	}
	return -1
}

func validVorbisCodecPrivate() []byte {
	identification := make([]byte, 30)
	identification[0] = 1
	copy(identification[1:], "vorbis")
	identification[11] = 2
	binary.LittleEndian.PutUint32(identification[12:16], 48000)
	identification[28] = 0xb6
	identification[29] = 1
	comment := []byte{3, 'v', 'o', 'r', 'b', 'i', 's'}
	setup := []byte{5, 'v', 'o', 'r', 'b', 'i', 's', 0}
	return vorbisCodecPrivateWithHeaders(identification, comment, setup)
}

func vorbisCodecPrivateWithIdentificationByte(offset int, value byte) []byte {
	private := validVorbisCodecPrivate()
	idStart := 3
	private[idStart+offset] = value
	return private
}

func vorbisCodecPrivateWithIdentificationBytes(offset int, values ...byte) []byte {
	private := validVorbisCodecPrivate()
	idStart := 3
	copy(private[idStart+offset:], values)
	return private
}

func vorbisCodecPrivateWithHeaders(headers ...[]byte) []byte {
	if len(headers) == 0 {
		return nil
	}
	private := []byte{byte(len(headers) - 1)}
	for i := 0; i < len(headers)-1; i++ {
		size := len(headers[i])
		for size >= 255 {
			private = append(private, 255)
			size -= 255
		}
		private = append(private, byte(size))
	}
	for i := range headers {
		private = append(private, headers[i]...)
	}
	return private
}

func av1CodecConfig() []byte {
	private := []byte{0x81, 0x05, 0x10, 0x00}
	return append(private, av1SequenceHeaderOBU()...)
}

func av1CodecConfigWithByte(index int, value byte) []byte {
	private := av1CodecConfig()
	private[index] = value
	return private
}

func av1CodecConfigWithPrefixOBU(obu []byte) []byte {
	private := []byte{0x81, 0x05, 0x10, 0x00}
	private = append(private, obu...)
	private = append(private, av1SequenceHeaderOBU()...)
	return private
}

func av1SequenceHeaderOBU() []byte {
	return []byte{0x0a, 0x06, 0x19, 0x5d, 0xc3, 0xc3, 0xda, 0x44}
}

func av1TemporalDelimiterOBU() []byte {
	return []byte{0x12, 0x00}
}

func h264AVCDecoderConfig() []byte {
	sps := h264SPS()
	pps := h264PPS()
	private := make([]byte, 0, 11+len(sps)+len(pps))
	private = append(private, 0x01, sps[1], sps[2], sps[3], 0xff, 0xe1)
	private = binary.BigEndian.AppendUint16(private, uint16(len(sps)))
	private = append(private, sps...)
	private = append(private, 0x01)
	private = binary.BigEndian.AppendUint16(private, uint16(len(pps)))
	private = append(private, pps...)
	return private
}

func h264SPS() []byte {
	return []byte{0x67, 0x42, 0x00, 0x0a, 0xf8, 0x41, 0xa2}
}

func h264PPS() []byte {
	return []byte{0x68, 0xce, 0x0f, 0x2c, 0x80}
}

func h264AVCDecoderConfigWithLengthSize(lengthSize int) []byte {
	private := h264AVCDecoderConfig()
	private[4] = 0xfc | byte(lengthSize-1)
	return private
}

func h264AVCDecoderConfigWithByte(index int, value byte) []byte {
	private := h264AVCDecoderConfig()
	private[index] = value
	return private
}

func h264AnnexBAccessUnit() []byte {
	return []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x99, 0x00, 0x00, 0x00, 0x01, 0x41, 0x9a}
}

func h264AnnexBParameterAccessUnit() []byte {
	var data []byte
	data = append(data, h264StartCode[:]...)
	data = append(data, h264SPS()...)
	data = append(data, h264StartCode[:]...)
	data = append(data, h264PPS()...)
	data = append(data, h264StartCode[:]...)
	data = append(data, 0x65, 0x88, 0x99)
	return data
}

func h264AnnexBInterFrame() []byte {
	return []byte{0x00, 0x00, 0x00, 0x01, 0x41, 0xab, 0xcd}
}

func h264AVCSampleWithLengthSize2() []byte {
	return []byte{0x00, 0x03, 0x65, 0x88, 0x99, 0x00, 0x02, 0x41, 0x9a}
}

func h264AVCSampleWithLengthSize1() []byte {
	return []byte{0x03, 0x65, 0x88, 0x99, 0x02, 0x41, 0x9a}
}

func h264AVCSampleWithParameterSetsLengthSize4() []byte {
	var data []byte
	for _, nalu := range [][]byte{h264SPS(), h264PPS(), []byte{0x65, 0x88, 0x99}} {
		data = binary.BigEndian.AppendUint32(data, uint32(len(nalu)))
		data = append(data, nalu...)
	}
	return data
}

func h264AVCInterFrameWithLengthSize2() []byte {
	return []byte{0x00, 0x03, 0x41, 0xab, 0xcd}
}

func h265HEVCDecoderConfig() []byte {
	vps := h265VPS()
	sps := h265SPS()
	pps := h265PPS()
	private := []byte{
		0x01,
		0x01,
		0x60, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x78,
		0xf0, 0x00,
		0xfc,
		0xfd,
		0xf8,
		0xf8,
		0x00, 0x00,
		0x0f,
		0x03,
	}
	private = appendHEVCConfigArray(private, h265NALUVPS, vps)
	private = appendHEVCConfigArray(private, h265NALUSPS, sps)
	private = appendHEVCConfigArray(private, h265NALUPPS, pps)
	return private
}

func appendHEVCConfigArray(private []byte, naluType int, nalu []byte) []byte {
	private = append(private, 0x80|byte(naluType))
	private = binary.BigEndian.AppendUint16(private, 1)
	private = binary.BigEndian.AppendUint16(private, uint16(len(nalu)))
	return append(private, nalu...)
}

func h265HEVCDecoderConfigWithLengthSize(lengthSize int) []byte {
	private := h265HEVCDecoderConfig()
	private[21] = (private[21] & 0xfc) | byte(lengthSize-1)
	return private
}

func h265HEVCDecoderConfigWithByte(index int, value byte) []byte {
	private := h265HEVCDecoderConfig()
	private[index] = value
	return private
}

func h265VPS() []byte {
	return []byte{0x40, 0x01, 0x0c, 0x01, 0xff}
}

func h265SPS() []byte {
	return []byte{
		0x42, 0x01,
		0x01,
		0x01,
		0x60, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x78,
	}
}

func h265PPS() []byte {
	return []byte{0x44, 0x01, 0xc0}
}

func h265AnnexBAccessUnit() []byte {
	return []byte{0x00, 0x00, 0x00, 0x01, 0x26, 0x01, 0x88, 0x99, 0x00, 0x00, 0x00, 0x01, 0x02, 0x01, 0xab}
}

func h265AnnexBParameterAccessUnit() []byte {
	var data []byte
	for _, nalu := range [][]byte{h265VPS(), h265SPS(), h265PPS(), []byte{0x26, 0x01, 0x88, 0x99}} {
		data = append(data, h264StartCode[:]...)
		data = append(data, nalu...)
	}
	return data
}

func h265AnnexBInterFrame() []byte {
	return []byte{0x00, 0x00, 0x00, 0x01, 0x02, 0x01, 0xcd}
}

func h265LengthPrefixedSample2() []byte {
	return []byte{0x00, 0x04, 0x26, 0x01, 0x88, 0x99, 0x00, 0x03, 0x02, 0x01, 0xab}
}

func h265LengthPrefixedInterFrame2() []byte {
	return []byte{0x00, 0x03, 0x02, 0x01, 0xcd}
}

func h265LengthPrefixedParameterSample4() []byte {
	var data []byte
	for _, nalu := range [][]byte{h265VPS(), h265SPS(), h265PPS(), []byte{0x26, 0x01, 0x88, 0x99}} {
		data = binary.BigEndian.AppendUint32(data, uint32(len(nalu)))
		data = append(data, nalu...)
	}
	return data
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

func makeSpecLacedMatroskaData(tb testing.TB, lacing byte, frames [][]byte, defaultDurationNS int64) []byte {
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
	if err := writeSpecLacedSimpleBlock(muxer.ebml, trackID, lacing, frames); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func writeSpecLacedSimpleBlock(writer *ebml.Writer, trackID uint32, lacing byte, frames [][]byte) error {
	const (
		specSimpleBlockKeyframe = 0x80
		specLacingMask          = 0x06
		specLacingXiph          = 0x02
		specLacingFixed         = 0x04
		specLacingEBML          = 0x06
	)
	var payload bytes.Buffer
	var scratch [ebml.MaxSizeWidth]byte
	n, err := ebml.EncodeUnsignedVINT(scratch[:], uint64(trackID))
	if err != nil {
		return err
	}
	payload.Write(scratch[:n])
	var blockHeader [3]byte
	blockHeader[2] = specSimpleBlockKeyframe | lacing
	payload.Write(blockHeader[:])
	if len(frames) < 2 || len(frames) > 256 {
		return ErrInvalidData
	}
	payload.WriteByte(byte(len(frames) - 1))
	switch lacing & specLacingMask {
	case specLacingXiph:
		for i := 0; i < len(frames)-1; i++ {
			size := len(frames[i])
			for size >= 255 {
				payload.WriteByte(255)
				size -= 255
			}
			payload.WriteByte(byte(size))
		}
	case specLacingFixed:
		size := len(frames[0])
		for i := range frames {
			if len(frames[i]) != size {
				return ErrInvalidData
			}
		}
	case specLacingEBML:
		n, err := ebml.EncodeUnsignedVINT(scratch[:], uint64(len(frames[0])))
		if err != nil {
			return err
		}
		payload.Write(scratch[:n])
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
	default:
		return ErrUnsupportedLacing
	}
	for i := range frames {
		payload.Write(frames[i])
	}
	return writer.WriteElement(idSimpleBlock, payload.Bytes())
}

func makeLacedBlockGroupMatroskaData(tb testing.TB, lacing byte, frames [][]byte) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{TimecodeScaleNS: 1_000_000})
	if err != nil {
		tb.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:               TrackVideo,
		Codec:              CodecVP8,
		MaxBlockAdditionID: 2,
		Video:              VideoConfig{Width: 640, Height: 360},
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
	var group bytes.Buffer
	writer := ebml.NewWriter(&group)
	if err := writeLacedBlockElement(writer, idBlock, trackID, simpleBlockInvisible|lacing, frames); err != nil {
		tb.Fatal(err)
	}
	if err := writeBlockAdditions(writer, []BlockAddition{{ID: 2, Data: []byte{0xaa, 0xbb}}}); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idBlockDuration, 60); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idReferencePriority, 2); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteInt(idReferenceBlk, -20); err != nil {
		tb.Fatal(err)
	}
	if err := writeBinary(writer, idCodecState, []byte{0xcc, 0xdd}); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteInt(idDiscardPad, -3_000_000); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteElement(idBlockGroup, group.Bytes()); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeTrackNumberMatroskaData(tb testing.TB, trackNumber uint64) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := writeTracksWithTrackNumber(muxer.ebml, trackNumber); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeCueTrackNumberMatroskaData(tb testing.TB, cueTrackNumber uint64) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := muxer.writeTracks(); err != nil {
		tb.Fatal(err)
	}
	if err := writeCuesWithTrackNumber(muxer.ebml, cueTrackNumber); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeCueMetadataMatroskaData(tb testing.TB, writeCues func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := muxer.writeTracks(); err != nil {
		tb.Fatal(err)
	}
	if err := writeCues(muxer.ebml); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	if err := writeSimpleBlockWithTrackNumber(muxer.ebml, 1, []byte{1}); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeBlockTrackNumberMatroskaData(tb testing.TB, trackNumber uint64) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	if err := writeSimpleBlockWithTrackNumber(muxer.ebml, trackNumber, []byte{1}); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeBlockAdditionsMatroskaData(tb testing.TB, writeAdditions func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
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
	var group bytes.Buffer
	gw := ebml.NewWriter(&group)
	if err := writeBlockWithTrackNumber(gw, uint64(trackID), []byte{1}); err != nil {
		tb.Fatal(err)
	}
	if err := writeAdditions(gw); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteElement(idBlockGroup, group.Bytes()); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeBlockGroupMetadataMatroskaData(tb testing.TB, writeGroup func(*ebml.Writer, uint32) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:               TrackVideo,
		Codec:              CodecVP8,
		MaxBlockAdditionID: 4,
		Video:              VideoConfig{Width: 16, Height: 16},
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
	var group bytes.Buffer
	gw := ebml.NewWriter(&group)
	if err := writeGroup(gw, trackID); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteElement(idBlockGroup, group.Bytes()); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeContentEncodedBlockMatroskaData(tb testing.TB, encodings []byte, frame []byte) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := writeTracksWithTrackExtra(muxer.ebml, func(ew *ebml.Writer) error {
		return ew.WriteElement(idContentEncodings, encodings)
	}); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	if err := writeSimpleBlockWithTrackNumber(muxer.ebml, 1, frame); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeContentEncodedLacedMatroskaData(tb testing.TB, encodings []byte, frames [][]byte) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := writeTracksWithTrackExtra(muxer.ebml, func(ew *ebml.Writer) error {
		if err := ew.WriteUInt(idDefaultDur, 20_000_000); err != nil {
			return err
		}
		return ew.WriteElement(idContentEncodings, encodings)
	}); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.startCluster(0); err != nil {
		tb.Fatal(err)
	}
	if err := writeLacedSimpleBlock(muxer.ebml, 1, simpleBlockLacingXiph, frames); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func writeBlockMoreFixture(w *ebml.Writer, id uint64, data []byte, writeID bool) error {
	var payload bytes.Buffer
	pw := ebml.NewWriter(&payload)
	if writeID {
		if err := pw.WriteUInt(idBlockAddID, id); err != nil {
			return err
		}
	}
	if data != nil {
		if err := writeBinary(pw, idBlockAdditional, data); err != nil {
			return err
		}
	}
	return w.WriteElement(idBlockMore, payload.Bytes())
}

func makeTrackMetadataMatroskaData(tb testing.TB, writeTracks func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := writeTracks(muxer.ebml); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeInfoMetadataMatroskaData(tb testing.TB, writeInfo func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeEBMLHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
		tb.Fatal(err)
	}
	muxer.segmentData = muxer.ebml.Offset()
	if err := writeInfo(muxer.ebml); err != nil {
		tb.Fatal(err)
	}
	if err := writeTracksWithVideoDimensions(muxer.ebml, 16, 16); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteElement(idCluster, nil); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeTopLevelMetadataMatroskaData(tb testing.TB, writeMetadata func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	writeMatroskaSegmentPrefix(tb, muxer)
	if err := writeTracksWithVideoDimensions(muxer.ebml, 16, 16); err != nil {
		tb.Fatal(err)
	}
	if writeMetadata != nil {
		if err := writeMetadata(muxer.ebml); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.ebml.WriteElement(idCluster, nil); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func writeAttachmentsElement(w *ebml.Writer, attachments ...Attachment) error {
	var payload bytes.Buffer
	aw := ebml.NewWriter(&payload)
	for i := range attachments {
		if err := writeAttachedFile(aw, attachments[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idAttachments, payload.Bytes())
}

func writeAttachmentsWithAttachedFilePayload(w *ebml.Writer, writeFile func(*ebml.Writer) error) error {
	var attachments bytes.Buffer
	aw := ebml.NewWriter(&attachments)
	var file bytes.Buffer
	if err := writeFile(ebml.NewWriter(&file)); err != nil {
		return err
	}
	if err := aw.WriteElement(idAttachedFile, file.Bytes()); err != nil {
		return err
	}
	return w.WriteElement(idAttachments, attachments.Bytes())
}

func writeChaptersElement(w *ebml.Writer, editions ...ChapterEdition) error {
	var payload bytes.Buffer
	cw := ebml.NewWriter(&payload)
	for i := range editions {
		if err := writeEditionEntry(cw, editions[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idChapters, payload.Bytes())
}

func writeChaptersWithEditionPayload(w *ebml.Writer, writeEdition func(*ebml.Writer) error) error {
	var chapters bytes.Buffer
	cw := ebml.NewWriter(&chapters)
	var edition bytes.Buffer
	if err := writeEdition(ebml.NewWriter(&edition)); err != nil {
		return err
	}
	if err := cw.WriteElement(idEditionEntry, edition.Bytes()); err != nil {
		return err
	}
	return w.WriteElement(idChapters, chapters.Bytes())
}

func metadataValidationChapter(uid uint64, startNS int64) Chapter {
	return Chapter{
		UID:        uid,
		StartNS:    startNS,
		Enabled:    true,
		EnabledSet: true,
	}
}

func duplicateChapterTimeStartPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteUInt(idChapterUID, 2); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteUInt(idChapterTimeStart, 0); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteUInt(idChapterTimeStart, 1); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func duplicateChapterDisplayStringChapterPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteUInt(idChapterUID, 2); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteUInt(idChapterTimeStart, 0); err != nil {
		tb.Fatal(err)
	}
	var display bytes.Buffer
	dw := ebml.NewWriter(&display)
	if err := dw.WriteString(idChapString, "Intro"); err != nil {
		tb.Fatal(err)
	}
	if err := dw.WriteString(idChapString, "Again"); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteElement(idChapterDisplay, display.Bytes()); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func writeTagsWithTagPayload(w *ebml.Writer, writeTagPayload func(*ebml.Writer) error) error {
	var tags bytes.Buffer
	tw := ebml.NewWriter(&tags)
	var tag bytes.Buffer
	if err := writeTagPayload(ebml.NewWriter(&tag)); err != nil {
		return err
	}
	if err := tw.WriteElement(idTag, tag.Bytes()); err != nil {
		return err
	}
	return w.WriteElement(idTags, tags.Bytes())
}

func writeTagsElement(w *ebml.Writer, tags ...Tag) error {
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	for i := range tags {
		if err := writeTag(tw, tags[i]); err != nil {
			return err
		}
	}
	return w.WriteElement(idTags, payload.Bytes())
}

func validTagTargetsPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteUInt(idTargetTypeValue, 50); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteString(idTargetType, "MOVIE"); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func duplicateTagTargetTypePayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteUInt(idTargetTypeValue, 50); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteString(idTargetType, "MOVIE"); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteString(idTargetType, "EPISODE"); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func validSimpleTagPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteString(idTagName, "TITLE"); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteString(idTagString, "Scene"); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func duplicateSimpleTagNamePayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if err := w.WriteString(idTagName, "TITLE"); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteString(idTagName, "ALBUM"); err != nil {
		tb.Fatal(err)
	}
	if err := w.WriteString(idTagString, "Scene"); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func writeInfoWithElements(writer *ebml.Writer, writeExtra func(*ebml.Writer) error) error {
	var info bytes.Buffer
	iw := ebml.NewWriter(&info)
	if writeExtra != nil {
		if err := writeExtra(iw); err != nil {
			return err
		}
	}
	if err := iw.WriteUInt(idTimestampScale, uint64(defaultTimecodeScaleNS)); err != nil {
		return err
	}
	if err := iw.WriteString(idMuxingApp, defaultMuxingApp); err != nil {
		return err
	}
	if err := iw.WriteString(idWritingApp, defaultWritingApp); err != nil {
		return err
	}
	return writer.WriteElement(idInfo, info.Bytes())
}

func writeInfoWithCRC32(writer *ebml.Writer, mutate func([]byte)) error {
	var body bytes.Buffer
	bw := ebml.NewWriter(&body)
	if err := bw.WriteString(idTitle, "crc protected"); err != nil {
		return err
	}
	if err := bw.WriteUInt(idTimestampScale, uint64(defaultTimecodeScaleNS)); err != nil {
		return err
	}
	if err := bw.WriteString(idMuxingApp, "crc-mux"); err != nil {
		return err
	}
	if err := bw.WriteString(idWritingApp, "crc-write"); err != nil {
		return err
	}
	return writeMasterWithCRC32(writer, idInfo, body.Bytes(), mutate)
}

func writeMasterWithCRC32(writer *ebml.Writer, id ebml.ID, payload []byte, mutate func([]byte)) error {
	protected := append([]byte(nil), payload...)
	var info bytes.Buffer
	iw := ebml.NewWriter(&info)
	if err := iw.WriteCRC32(protected); err != nil {
		return err
	}
	if mutate != nil {
		mutate(protected)
	}
	if _, err := iw.Write(protected); err != nil {
		return err
	}
	return writer.WriteElement(id, info.Bytes())
}

func makeMetadataCRCMatroskaData(tb testing.TB, checkedID ebml.ID, mutate func([]byte)) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeEBMLHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
		tb.Fatal(err)
	}
	muxer.segmentData = muxer.ebml.Offset()
	if checkedID == idSeekHead {
		if err := writeMasterWithCRC32(muxer.ebml, idSeekHead, seekHeadPayload(tb), mutate); err != nil {
			tb.Fatal(err)
		}
	}
	if err := writeInfoWithElements(muxer.ebml, nil); err != nil {
		tb.Fatal(err)
	}
	if checkedID == idTracks {
		payload := elementPayload(tb, idTracks, func(w *ebml.Writer) error {
			return writeTracksWithVideoDimensions(w, 16, 16)
		})
		if err := writeMasterWithCRC32(muxer.ebml, idTracks, payload, mutate); err != nil {
			tb.Fatal(err)
		}
	} else if err := writeTracksWithVideoDimensions(muxer.ebml, 16, 16); err != nil {
		tb.Fatal(err)
	}
	if checkedID == idCues {
		payload := elementPayload(tb, idCues, func(w *ebml.Writer) error {
			return writeCuesWithTrackNumber(w, 1)
		})
		if err := writeMasterWithCRC32(muxer.ebml, idCues, payload, mutate); err != nil {
			tb.Fatal(err)
		}
	}
	if checkedID == idAttachments {
		var payload bytes.Buffer
		if err := writeAttachedFile(ebml.NewWriter(&payload), Attachment{
			UID:      1,
			Filename: "note.txt",
			MIMEType: "text/plain",
			Data:     []byte("hello"),
		}); err != nil {
			tb.Fatal(err)
		}
		if err := writeMasterWithCRC32(muxer.ebml, idAttachments, payload.Bytes(), mutate); err != nil {
			tb.Fatal(err)
		}
	}
	if checkedID == idChapters {
		if err := writeMasterWithCRC32(muxer.ebml, idChapters, crcChaptersPayload(tb), mutate); err != nil {
			tb.Fatal(err)
		}
	}
	if checkedID == idTags {
		if err := writeMasterWithCRC32(muxer.ebml, idTags, crcTagsPayload(tb), mutate); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.ebml.WriteElement(idCluster, nil); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func makeNestedCRCMatroskaData(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	if err := muxer.writeEBMLHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
		tb.Fatal(err)
	}
	muxer.segmentData = muxer.ebml.Offset()
	if target == "seek" {
		if err := muxer.ebml.WriteElement(idSeekHead, checkedElement(tb, idSeek, seekEntryPayload(tb, idInfo, 0), mutate)); err != nil {
			tb.Fatal(err)
		}
	}
	if err := writeInfoWithElements(muxer.ebml, nil); err != nil {
		tb.Fatal(err)
	}
	switch target {
	case "track", "video", "audio", "colour", "mastering", "projection":
		if err := writeNestedCRCTracks(muxer.ebml, target, mutate); err != nil {
			tb.Fatal(err)
		}
	default:
		if err := writeTracksWithVideoDimensions(muxer.ebml, 16, 16); err != nil {
			tb.Fatal(err)
		}
	}
	switch target {
	case "cuepoint":
		if err := muxer.ebml.WriteElement(idCues, checkedElement(tb, idCuePoint, cuePointPayload(tb, false, nil), mutate)); err != nil {
			tb.Fatal(err)
		}
	case "cuepositions":
		if err := muxer.ebml.WriteElement(idCues, cuePointElement(tb, true, mutate)); err != nil {
			tb.Fatal(err)
		}
	}
	if target == "attachedfile" {
		if err := muxer.ebml.WriteElement(idAttachments, checkedElement(tb, idAttachedFile, attachedFilePayload(tb), mutate)); err != nil {
			tb.Fatal(err)
		}
	}
	switch target {
	case "edition", "chapter", "chaptertrack", "chapterdisplay":
		if err := muxer.ebml.WriteElement(idChapters, nestedCRCChaptersPayload(tb, target, mutate)); err != nil {
			tb.Fatal(err)
		}
	case "tag", "targets", "simpletag", "childsimpletag":
		if err := muxer.ebml.WriteElement(idTags, nestedCRCTagsPayload(tb, target, mutate)); err != nil {
			tb.Fatal(err)
		}
	}
	if err := muxer.ebml.WriteElement(idCluster, nil); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func crcChaptersPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	if err := writeEditionEntry(ebml.NewWriter(&payload), ChapterEdition{
		UID: 1,
		Chapters: []Chapter{{
			UID:        2,
			StartNS:    0,
			Enabled:    true,
			EnabledSet: true,
			TrackUIDs:  []uint64{1},
			Displays: []ChapterDisplay{{
				String:   "Intro",
				Language: "eng",
			}},
		}},
	}); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func nestedCRCChaptersPayload(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	editionPayload := crcEditionPayload(tb, target, mutate)
	var err error
	if target == "edition" {
		_, err = writer.Write(checkedElement(tb, idEditionEntry, editionPayload, mutate))
	} else {
		err = writer.WriteElement(idEditionEntry, editionPayload)
	}
	if err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcEditionPayload(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idEditionUID, 1); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idEditionFlagHidden, 0); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idEditionFlagDefault, 0); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idEditionFlagOrdered, 0); err != nil {
		tb.Fatal(err)
	}
	chapterPayload := crcChapterPayload(tb, target, mutate)
	var err error
	if target == "chapter" {
		_, err = writer.Write(checkedElement(tb, idChapterAtom, chapterPayload, mutate))
	} else {
		err = writer.WriteElement(idChapterAtom, chapterPayload)
	}
	if err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcChapterPayload(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idChapterUID, 2); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idChapterTimeStart, 0); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idChapterFlagHidden, 0); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idChapterFlagEnabled, 1); err != nil {
		tb.Fatal(err)
	}
	trackPayload := crcChapterTrackPayload(tb)
	if target == "chaptertrack" {
		if _, err := writer.Write(checkedElement(tb, idChapterTrack, trackPayload, mutate)); err != nil {
			tb.Fatal(err)
		}
	} else if err := writer.WriteElement(idChapterTrack, trackPayload); err != nil {
		tb.Fatal(err)
	}
	displayPayload := crcChapterDisplayPayload(tb)
	if target == "chapterdisplay" {
		if _, err := writer.Write(checkedElement(tb, idChapterDisplay, displayPayload, mutate)); err != nil {
			tb.Fatal(err)
		}
	} else if err := writer.WriteElement(idChapterDisplay, displayPayload); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcChapterTrackPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idChapterTrackUID, 1); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcChapterDisplayPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteString(idChapString, "Intro"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idChapLanguage, "eng"); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcTagsPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	if err := writeTag(ebml.NewWriter(&payload), Tag{
		Target: TagTarget{
			TypeValue: 50,
			Type:      "MOVIE",
			TrackUIDs: []uint64{1},
		},
		Simple: []SimpleTag{{
			Name:       "TITLE",
			Language:   "und",
			Default:    true,
			DefaultSet: true,
			String:     "CRC",
			StringSet:  true,
		}},
	}); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func nestedCRCTagsPayload(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	tagPayload := crcTagPayload(tb, target, mutate)
	var err error
	if target == "tag" {
		_, err = writer.Write(checkedElement(tb, idTag, tagPayload, mutate))
	} else {
		err = writer.WriteElement(idTag, tagPayload)
	}
	if err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcTagPayload(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	targetsPayload := crcTargetsPayload(tb)
	if target == "targets" {
		if _, err := writer.Write(checkedElement(tb, idTargets, targetsPayload, mutate)); err != nil {
			tb.Fatal(err)
		}
	} else if err := writer.WriteElement(idTargets, targetsPayload); err != nil {
		tb.Fatal(err)
	}
	simplePayload := crcSimpleTagPayload(tb, target, mutate)
	if target == "simpletag" {
		if _, err := writer.Write(checkedElement(tb, idSimpleTag, simplePayload, mutate)); err != nil {
			tb.Fatal(err)
		}
	} else if err := writer.WriteElement(idSimpleTag, simplePayload); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcTargetsPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idTargetTypeValue, 50); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idTargetType, "MOVIE"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idTagTrackUID, 1); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func crcSimpleTagPayload(tb testing.TB, target string, mutate func([]byte)) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteString(idTagName, "TITLE"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idTagLanguage, "und"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idTagDefault, 1); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idTagString, "CRC"); err != nil {
		tb.Fatal(err)
	}
	if target == "childsimpletag" {
		childPayload := crcSimpleTagLeafPayload(tb)
		if _, err := writer.Write(checkedElement(tb, idSimpleTag, childPayload, mutate)); err != nil {
			tb.Fatal(err)
		}
	}
	return payload.Bytes()
}

func crcSimpleTagLeafPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteString(idTagName, "SORT_WITH"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idTagLanguage, "und"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idTagDefault, 1); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idTagString, "crc"); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func writeNestedCRCTracks(writer *ebml.Writer, target string, mutate func([]byte)) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	trackType := uint64(matroskaTrackVideo)
	codecID := codecIDVP8
	if target == "audio" {
		trackType = matroskaTrackAudio
		codecID = codecIDOpus
	}
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, trackType); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecID); err != nil {
		return err
	}
	switch target {
	case "audio":
		if _, err := ew.Write(checkedElement(nil, idAudio, audioPayload(), mutate)); err != nil {
			return err
		}
	case "video", "colour", "mastering", "projection":
		if _, err := ew.Write(videoElementForNestedCRC(target, mutate)); err != nil {
			return err
		}
	default:
		if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
			return err
		}
	}
	if target == "track" {
		if _, err := tw.Write(checkedElement(nil, idTrackEntry, entry.Bytes(), mutate)); err != nil {
			return err
		}
	} else if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func videoElementForNestedCRC(target string, mutate func([]byte)) []byte {
	var video bytes.Buffer
	vw := ebml.NewWriter(&video)
	if err := vw.WriteUInt(idPixelWidth, 16); err != nil {
		panic(err)
	}
	if err := vw.WriteUInt(idPixelHeight, 16); err != nil {
		panic(err)
	}
	switch target {
	case "colour", "mastering":
		if _, err := vw.Write(colourElementForNestedCRC(target, mutate)); err != nil {
			panic(err)
		}
	case "projection":
		if _, err := vw.Write(checkedElement(nil, idProjection, projectionPayload(), mutate)); err != nil {
			panic(err)
		}
	}
	if target == "video" {
		return checkedElement(nil, idVideo, video.Bytes(), mutate)
	}
	return elementBytes(idVideo, video.Bytes())
}

func colourElementForNestedCRC(target string, mutate func([]byte)) []byte {
	var colour bytes.Buffer
	cw := ebml.NewWriter(&colour)
	if err := cw.WriteUInt(idMatrixCoefficients, 1); err != nil {
		panic(err)
	}
	if target == "mastering" {
		if _, err := cw.Write(checkedElement(nil, idMasteringMetadata, masteringPayload(), mutate)); err != nil {
			panic(err)
		}
	}
	if target == "colour" {
		return checkedElement(nil, idColour, colour.Bytes(), mutate)
	}
	return elementBytes(idColour, colour.Bytes())
}

func checkedElement(tb testing.TB, id ebml.ID, payload []byte, mutate func([]byte)) []byte {
	if tb != nil {
		tb.Helper()
	}
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writeMasterWithCRC32(writer, id, payload, mutate); err != nil {
		if tb != nil {
			tb.Fatal(err)
		}
		panic(err)
	}
	return buffer.Bytes()
}

func elementBytes(id ebml.ID, payload []byte) []byte {
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writer.WriteElement(id, payload); err != nil {
		panic(err)
	}
	return buffer.Bytes()
}

func seekEntryPayload(tb testing.TB, id ebml.ID, position uint64) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	var idPayload [ebml.MaxIDWidth]byte
	n, err := ebml.EncodeID(idPayload[:], id)
	if err != nil {
		tb.Fatal(err)
	}
	if err := writeBinary(writer, idSeekID, idPayload[:n]); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idSeekPosition, position); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func cuePointElement(tb testing.TB, checkedPositions bool, mutate func([]byte)) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	payload := cuePointPayload(tb, checkedPositions, mutate)
	if err := writer.WriteElement(idCuePoint, payload); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func cuePointPayload(tb testing.TB, checkedPositions bool, mutate func([]byte)) []byte {
	tb.Helper()
	var point bytes.Buffer
	writer := ebml.NewWriter(&point)
	if err := writer.WriteUInt(idCueTime, 0); err != nil {
		tb.Fatal(err)
	}
	positionsPayload := cueTrackPositionsPayload(tb)
	if checkedPositions {
		if _, err := writer.Write(checkedElement(tb, idCueTrackPositions, positionsPayload, mutate)); err != nil {
			tb.Fatal(err)
		}
	} else if err := writer.WriteElement(idCueTrackPositions, positionsPayload); err != nil {
		tb.Fatal(err)
	}
	return point.Bytes()
}

func cueTrackPositionsPayload(tb testing.TB) []byte {
	tb.Helper()
	var positions bytes.Buffer
	writer := ebml.NewWriter(&positions)
	if err := writer.WriteUInt(idCueTrack, 1); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idCueClusterPosition, 0); err != nil {
		tb.Fatal(err)
	}
	return positions.Bytes()
}

func writeCuePointWithPositionsPayload(w *ebml.Writer, positionsPayload []byte) error {
	var point bytes.Buffer
	pw := ebml.NewWriter(&point)
	if err := pw.WriteUInt(idCueTime, 0); err != nil {
		return err
	}
	if err := pw.WriteElement(idCueTrackPositions, positionsPayload); err != nil {
		return err
	}
	return w.WriteElement(idCuePoint, point.Bytes())
}

func writeCuePointWithReferencePayload(w *ebml.Writer, referencePayload []byte) error {
	var positions bytes.Buffer
	tw := ebml.NewWriter(&positions)
	if err := tw.WriteUInt(idCueTrack, 1); err != nil {
		return err
	}
	if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
		return err
	}
	if err := tw.WriteElement(idCueReference, referencePayload); err != nil {
		return err
	}
	return writeCuePointWithPositionsPayload(w, positions.Bytes())
}

func audioPayload() []byte {
	var audio bytes.Buffer
	writer := ebml.NewWriter(&audio)
	if err := writer.WriteFloat64(idSamplingFreq, 48000); err != nil {
		panic(err)
	}
	if err := writer.WriteUInt(idChannels, 2); err != nil {
		panic(err)
	}
	return audio.Bytes()
}

func masteringPayload() []byte {
	var metadata bytes.Buffer
	writer := ebml.NewWriter(&metadata)
	if err := writer.WriteFloat64(idLuminanceMax, 1000); err != nil {
		panic(err)
	}
	if err := writer.WriteFloat64(idLuminanceMin, 0.01); err != nil {
		panic(err)
	}
	return metadata.Bytes()
}

func projectionPayload() []byte {
	var projection bytes.Buffer
	writer := ebml.NewWriter(&projection)
	if err := writer.WriteUInt(idProjectionType, 0); err != nil {
		panic(err)
	}
	return projection.Bytes()
}

func attachedFilePayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteString(idFileName, "note.txt"); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteString(idFileMediaType, "text/plain"); err != nil {
		tb.Fatal(err)
	}
	if err := writeBinary(writer, idFileData, []byte("hello")); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUInt(idFileUID, 1); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func seekHeadPayload(tb testing.TB) []byte {
	tb.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writeSeekEntry(writer, idInfo, 0); err != nil {
		tb.Fatal(err)
	}
	return payload.Bytes()
}

func elementPayload(tb testing.TB, id ebml.ID, writeElement func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writeElement(writer); err != nil {
		tb.Fatal(err)
	}
	reader := ebml.NewReader(bytes.NewReader(buffer.Bytes()), ebml.ReaderOptions{})
	header, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if header.ID != id || header.Size.Unknown {
		tb.Fatalf("element header = %+v, want id 0x%x", header, uint64(id))
	}
	payload, err := readBinaryPayload(reader, header.Size.Value)
	if err != nil {
		tb.Fatal(err)
	}
	if reader.Offset() != int64(len(buffer.Bytes())) {
		tb.Fatalf("payload length left reader at %d, want %d", reader.Offset(), len(buffer.Bytes()))
	}
	return payload
}

type ebmlHeaderFixture struct {
	EBMLVersion                 uint64
	EBMLReadVersion             uint64
	EBMLMaxIDLength             uint64
	EBMLMaxSizeLength           uint64
	DocType                     string
	DocTypeVersion              uint64
	DocTypeReadVersion          uint64
	EBMLVersionSet              bool
	EBMLReadVersionSet          bool
	EBMLMaxIDLengthSet          bool
	EBMLMaxSizeLengthSet        bool
	DocTypeSet                  bool
	DocTypeVersionSet           bool
	DocTypeReadVersionSet       bool
	DuplicateEBMLVersion        bool
	DuplicateEBMLReadVersion    bool
	DuplicateEBMLMaxIDLength    bool
	DuplicateEBMLMaxSizeLength  bool
	DuplicateDocType            bool
	DuplicateDocTypeVersion     bool
	DuplicateDocTypeReadVersion bool
}

func defaultEBMLHeaderFixture() ebmlHeaderFixture {
	return ebmlHeaderFixture{
		EBMLVersion:           1,
		EBMLReadVersion:       1,
		EBMLMaxIDLength:       ebml.MaxIDWidth,
		EBMLMaxSizeLength:     ebml.MaxSizeWidth,
		DocType:               "matroska",
		DocTypeVersion:        defaultDocTypeVersion,
		DocTypeReadVersion:    defaultDocTypeReadVersion,
		EBMLVersionSet:        true,
		EBMLReadVersionSet:    true,
		EBMLMaxIDLengthSet:    true,
		EBMLMaxSizeLengthSet:  true,
		DocTypeSet:            true,
		DocTypeVersionSet:     true,
		DocTypeReadVersionSet: true,
	}
}

func makeEBMLHeaderMatroskaData(tb testing.TB, header ebmlHeaderFixture) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writeEBMLHeaderFixture(writer, header); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
		tb.Fatal(err)
	}
	if err := writeInfoWithElements(writer, nil); err != nil {
		tb.Fatal(err)
	}
	if err := writeTracksWithVideoDimensions(writer, 16, 16); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteElement(idCluster, nil); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func writeEBMLHeaderFixture(writer *ebml.Writer, header ebmlHeaderFixture) error {
	var payload bytes.Buffer
	w := ebml.NewWriter(&payload)
	if header.EBMLVersionSet {
		if err := w.WriteUInt(idEBMLVersion, header.EBMLVersion); err != nil {
			return err
		}
		if header.DuplicateEBMLVersion {
			if err := w.WriteUInt(idEBMLVersion, header.EBMLVersion); err != nil {
				return err
			}
		}
	}
	if header.EBMLReadVersionSet {
		if err := w.WriteUInt(idEBMLReadVersion, header.EBMLReadVersion); err != nil {
			return err
		}
		if header.DuplicateEBMLReadVersion {
			if err := w.WriteUInt(idEBMLReadVersion, header.EBMLReadVersion); err != nil {
				return err
			}
		}
	}
	if header.EBMLMaxIDLengthSet {
		if err := w.WriteUInt(idEBMLMaxIDLength, header.EBMLMaxIDLength); err != nil {
			return err
		}
		if header.DuplicateEBMLMaxIDLength {
			if err := w.WriteUInt(idEBMLMaxIDLength, header.EBMLMaxIDLength); err != nil {
				return err
			}
		}
	}
	if header.EBMLMaxSizeLengthSet {
		if err := w.WriteUInt(idEBMLMaxSizeLength, header.EBMLMaxSizeLength); err != nil {
			return err
		}
		if header.DuplicateEBMLMaxSizeLength {
			if err := w.WriteUInt(idEBMLMaxSizeLength, header.EBMLMaxSizeLength); err != nil {
				return err
			}
		}
	}
	if header.DocTypeSet {
		if err := w.WriteString(idDocType, header.DocType); err != nil {
			return err
		}
		if header.DuplicateDocType {
			if err := w.WriteString(idDocType, header.DocType); err != nil {
				return err
			}
		}
	}
	if header.DocTypeVersionSet {
		if err := w.WriteUInt(idDocTypeVersion, header.DocTypeVersion); err != nil {
			return err
		}
		if header.DuplicateDocTypeVersion {
			if err := w.WriteUInt(idDocTypeVersion, header.DocTypeVersion); err != nil {
				return err
			}
		}
	}
	if header.DocTypeReadVersionSet {
		if err := w.WriteUInt(idDocTypeReadVersion, header.DocTypeReadVersion); err != nil {
			return err
		}
		if header.DuplicateDocTypeReadVersion {
			if err := w.WriteUInt(idDocTypeReadVersion, header.DocTypeReadVersion); err != nil {
				return err
			}
		}
	}
	return writer.WriteElement(idEBML, payload.Bytes())
}

func makeSeekHeadMetadataMatroskaData(tb testing.TB, writeSeekHead func(*ebml.Writer) error) []byte {
	tb.Helper()
	return makeTopLevelMetadataMatroskaData(tb, func(writer *ebml.Writer) error {
		var payload bytes.Buffer
		w := ebml.NewWriter(&payload)
		if err := writeSeekHead(w); err != nil {
			return err
		}
		return writer.WriteElement(idSeekHead, payload.Bytes())
	})
}

func makeDeferredRequiredMetadataMatroskaData(tb testing.TB, withSeekHead bool) []byte {
	tb.Helper()
	infoElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeInfoWithElements(w, nil)
	})
	tracksElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeTracksWithVideoDimensions(w, 16, 16)
	})
	clusterElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		var payload bytes.Buffer
		cw := ebml.NewWriter(&payload)
		if err := cw.WriteUInt(idTimestamp, 0); err != nil {
			return err
		}
		if err := writeSimpleBlockWithTrackNumber(cw, 1, []byte{0x77}); err != nil {
			return err
		}
		return w.WriteElement(idCluster, payload.Bytes())
	})
	var seekHeadElement []byte
	if withSeekHead {
		seekHeadElement = seekHeadElementForDeferredMetadata(tb, len(clusterElement), len(infoElement))
	}

	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writeEBMLHeaderFixture(writer, defaultEBMLHeaderFixture()); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
		tb.Fatal(err)
	}
	for _, element := range [][]byte{seekHeadElement, clusterElement, infoElement, tracksElement} {
		if len(element) == 0 {
			continue
		}
		if _, err := writer.Write(element); err != nil {
			tb.Fatal(err)
		}
	}
	return buffer.Bytes()
}

func makeDeferredOptionalMetadataMatroskaData(tb testing.TB, requiredMetadataBeforeCluster bool, attachments []Attachment, chapters []ChapterEdition, tags []Tag) []byte {
	tb.Helper()
	infoElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeInfoWithElements(w, nil)
	})
	tracksElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeTracksWithVideoDimensions(w, 16, 16)
	})
	attachmentsElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeAttachmentsElement(w, attachments...)
	})
	chaptersElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeChaptersElement(w, chapters...)
	})
	tagsElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		return writeTagsElement(w, tags...)
	})
	clusterElement := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
		var payload bytes.Buffer
		cw := ebml.NewWriter(&payload)
		if err := cw.WriteUInt(idTimestamp, 0); err != nil {
			return err
		}
		if err := writeSimpleBlockWithTrackNumber(cw, 1, []byte{0x77}); err != nil {
			return err
		}
		return w.WriteElement(idCluster, payload.Bytes())
	})
	lateElements := []struct {
		id   ebml.ID
		data []byte
	}{
		{id: idAttachments, data: attachmentsElement},
		{id: idChapters, data: chaptersElement},
		{id: idTags, data: tagsElement},
	}
	positionPrefix := len(clusterElement)
	if !requiredMetadataBeforeCluster {
		lateElements = append([]struct {
			id   ebml.ID
			data []byte
		}{
			{id: idInfo, data: infoElement},
			{id: idTracks, data: tracksElement},
		}, lateElements...)
	} else {
		positionPrefix += len(infoElement) + len(tracksElement)
	}
	var seekHeadElement []byte
	seekHeadConverged := false
	for range 8 {
		positions := make([]uint64, len(lateElements))
		position := uint64(len(seekHeadElement) + positionPrefix)
		for i := range lateElements {
			positions[i] = position
			position += uint64(len(lateElements[i].data))
		}
		next := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
			var payload bytes.Buffer
			sw := ebml.NewWriter(&payload)
			for i := range lateElements {
				if err := writeSeekEntry(sw, lateElements[i].id, positions[i]); err != nil {
					return err
				}
			}
			return w.WriteElement(idSeekHead, payload.Bytes())
		})
		if len(next) == len(seekHeadElement) {
			seekHeadElement = next
			seekHeadConverged = true
			break
		}
		seekHeadElement = next
	}
	if !seekHeadConverged {
		tb.Fatal("SeekHead element size did not converge")
	}

	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writeEBMLHeaderFixture(writer, defaultEBMLHeaderFixture()); err != nil {
		tb.Fatal(err)
	}
	if err := writer.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
		tb.Fatal(err)
	}
	elements := [][]byte{seekHeadElement}
	if requiredMetadataBeforeCluster {
		elements = append(elements, infoElement, tracksElement, clusterElement)
	} else {
		elements = append(elements, clusterElement, infoElement, tracksElement)
	}
	elements = append(elements, attachmentsElement, chaptersElement, tagsElement)
	for i := range elements {
		if _, err := writer.Write(elements[i]); err != nil {
			tb.Fatal(err)
		}
	}
	return buffer.Bytes()
}

func segmentChildElementBytes(tb testing.TB, writeElement func(*ebml.Writer) error) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writeElement(writer); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func seekHeadElementForDeferredMetadata(tb testing.TB, clusterElementSize int, infoElementSize int) []byte {
	tb.Helper()
	var seekHead []byte
	for range 8 {
		infoPosition := uint64(len(seekHead) + clusterElementSize)
		tracksPosition := infoPosition + uint64(infoElementSize)
		next := segmentChildElementBytes(tb, func(w *ebml.Writer) error {
			var payload bytes.Buffer
			sw := ebml.NewWriter(&payload)
			if err := writeSeekEntry(sw, idInfo, infoPosition); err != nil {
				return err
			}
			if err := writeSeekEntry(sw, idTracks, tracksPosition); err != nil {
				return err
			}
			return w.WriteElement(idSeekHead, payload.Bytes())
		})
		if len(next) == len(seekHead) {
			return next
		}
		seekHead = next
	}
	tb.Fatal("SeekHead element size did not converge")
	return nil
}

func writeSeekIDElement(w *ebml.Writer, id ebml.ID) error {
	var idPayload [ebml.MaxIDWidth]byte
	n, err := ebml.EncodeID(idPayload[:], id)
	if err != nil {
		return err
	}
	return writeBinary(w, idSeekID, idPayload[:n])
}

func writeMatroskaSegmentPrefix(tb testing.TB, muxer *Muxer) {
	tb.Helper()
	if err := muxer.writeEBMLHeader(); err != nil {
		tb.Fatal(err)
	}
	if err := muxer.ebml.WriteUnknownHeader(idSegment, ebml.MaxSizeWidth); err != nil {
		tb.Fatal(err)
	}
	muxer.segmentData = muxer.ebml.Offset()
	if err := muxer.writeInfo(); err != nil {
		tb.Fatal(err)
	}
}

func writeTracksWithTrackNumber(writer *ebml.Writer, trackNumber uint64) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, trackNumber); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithFlagValue(writer *ebml.Writer, flagID ebml.ID, value uint64) error {
	return writeTracksWithTrackMetadata(writer, trackUIntElement{flagID, value})
}

type trackUIntElement struct {
	id    ebml.ID
	value uint64
}

func writeTracksWithTrackMetadata(writer *ebml.Writer, elements ...trackUIntElement) error {
	return writeTracksWithTrackExtra(writer, func(ew *ebml.Writer) error {
		for i := range elements {
			if err := ew.WriteUInt(elements[i].id, elements[i].value); err != nil {
				return err
			}
		}
		return nil
	})
}

func writeTracksWithTrackExtra(writer *ebml.Writer, writeExtra func(*ebml.Writer) error) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if writeExtra != nil {
		if err := writeExtra(ew); err != nil {
			return err
		}
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

type trackEntryFixture struct {
	Number           uint64
	UID              uint64
	Type             uint64
	CodecID          string
	NumberSet        bool
	UIDSet           bool
	TypeSet          bool
	CodecIDSet       bool
	MediaSet         bool
	DuplicateNumber  bool
	DuplicateUID     bool
	DuplicateType    bool
	DuplicateCodecID bool
	DuplicateMedia   bool
}

func defaultTrackEntryFixture() trackEntryFixture {
	return trackEntryFixture{
		Number:     1,
		UID:        1,
		Type:       matroskaTrackVideo,
		CodecID:    codecIDVP8,
		NumberSet:  true,
		UIDSet:     true,
		TypeSet:    true,
		CodecIDSet: true,
		MediaSet:   true,
	}
}

func writeTracksWithTrackEntryFixtures(writer *ebml.Writer, entries ...trackEntryFixture) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	for i := range entries {
		var entry bytes.Buffer
		ew := ebml.NewWriter(&entry)
		if err := writeTrackEntryFixture(ew, entries[i]); err != nil {
			return err
		}
		if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
			return err
		}
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTrackEntryFixture(w *ebml.Writer, entry trackEntryFixture) error {
	if entry.NumberSet {
		if err := w.WriteUInt(idTrackNumber, entry.Number); err != nil {
			return err
		}
		if entry.DuplicateNumber {
			if err := w.WriteUInt(idTrackNumber, entry.Number); err != nil {
				return err
			}
		}
	}
	if entry.UIDSet {
		if err := w.WriteUInt(idTrackUID, entry.UID); err != nil {
			return err
		}
		if entry.DuplicateUID {
			if err := w.WriteUInt(idTrackUID, entry.UID); err != nil {
				return err
			}
		}
	}
	if entry.TypeSet {
		if err := w.WriteUInt(idTrackType, entry.Type); err != nil {
			return err
		}
		if entry.DuplicateType {
			if err := w.WriteUInt(idTrackType, entry.Type); err != nil {
				return err
			}
		}
	}
	if entry.CodecIDSet {
		if err := w.WriteString(idCodecID, entry.CodecID); err != nil {
			return err
		}
		if entry.DuplicateCodecID {
			if err := w.WriteString(idCodecID, entry.CodecID); err != nil {
				return err
			}
		}
	}
	if entry.MediaSet {
		if err := writeTrackEntryFixtureMedia(w, entry.Type); err != nil {
			return err
		}
		if entry.DuplicateMedia {
			if err := writeTrackEntryFixtureMedia(w, entry.Type); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeTrackEntryFixtureMedia(w *ebml.Writer, trackType uint64) error {
	switch trackType {
	case matroskaTrackAudio:
		return writeAudio(w, AudioConfig{SampleRate: 48000, Channels: 2})
	case matroskaTrackVideo:
		return writeVideo(w, VideoConfig{Width: 16, Height: 16})
	default:
		return nil
	}
}

func trackTranslatePayload(t testing.TB, writeID bool, writeCodec bool) []byte {
	t.Helper()
	var payload bytes.Buffer
	tw := ebml.NewWriter(&payload)
	if writeID {
		if err := writeBinary(tw, idTrackTranslateTrack, []byte{0x01}); err != nil {
			t.Fatal(err)
		}
	}
	if writeCodec {
		if err := tw.WriteUInt(idTrackTranslateCodec, 1); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func contentEncodingsPayload(t testing.TB, encodings ...[]byte) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	for i := range encodings {
		if err := writer.WriteElement(idContentEncoding, encodings[i]); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func contentEncodingPayload(t testing.TB, write func(*ebml.Writer) error) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if write != nil {
		if err := write(writer); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func contentCompressionPayload(t testing.TB, algorithm uint64, settings []byte) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idContentCompAlgo, algorithm); err != nil {
		t.Fatal(err)
	}
	if settings != nil {
		if err := writeBinary(writer, idContentCompSettings, settings); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

const contentAESCipherUnset = ^uint64(0)

func contentEncryptionPayload(t testing.TB, algorithm uint64, keyID []byte, cipherMode uint64) []byte {
	t.Helper()
	var payload bytes.Buffer
	writer := ebml.NewWriter(&payload)
	if err := writer.WriteUInt(idContentEncAlgo, algorithm); err != nil {
		t.Fatal(err)
	}
	if keyID != nil {
		if err := writeBinary(writer, idContentEncKeyID, keyID); err != nil {
			t.Fatal(err)
		}
	}
	if cipherMode != contentAESCipherUnset {
		var aesPayload bytes.Buffer
		aesWriter := ebml.NewWriter(&aesPayload)
		if err := aesWriter.WriteUInt(idContentEncAESCipher, cipherMode); err != nil {
			t.Fatal(err)
		}
		if err := writer.WriteElement(idContentEncAES, aesPayload.Bytes()); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := writer.WriteElement(idContentEncAES, nil); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func blockAdditionMappingPayload(t testing.TB, id uint64, writeID bool, typ uint64, extraData []byte) []byte {
	t.Helper()
	var payload bytes.Buffer
	mw := ebml.NewWriter(&payload)
	if writeID {
		if err := mw.WriteUInt(idBlockAddIDValue, id); err != nil {
			t.Fatal(err)
		}
	}
	if err := mw.WriteUInt(idBlockAddIDType, typ); err != nil {
		t.Fatal(err)
	}
	if extraData != nil {
		if err := writeBinary(mw, idBlockAddIDExtraData, extraData); err != nil {
			t.Fatal(err)
		}
	}
	return payload.Bytes()
}

func writeTracksWithVideoDimensions(writer *ebml.Writer, width uint64, height uint64) error {
	return writeTracksWithVideoElements(writer,
		videoUIntElement{idPixelWidth, width},
		videoUIntElement{idPixelHeight, height},
	)
}

type videoUIntElement struct {
	id    ebml.ID
	value uint64
}

func writeTracksWithVideoElements(writer *ebml.Writer, elements ...videoUIntElement) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	var video bytes.Buffer
	vw := ebml.NewWriter(&video)
	for i := range elements {
		if err := vw.WriteUInt(elements[i].id, elements[i].value); err != nil {
			return err
		}
	}
	if err := ew.WriteElement(idVideo, video.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

type colourUIntElement struct {
	id    ebml.ID
	value uint64
}

type colourFloatElement struct {
	id    ebml.ID
	value float64
}

func writeTracksWithVideoColourElements(writer *ebml.Writer, elements ...colourUIntElement) error {
	return writeTracksWithVideoColourMetadata(writer, elements, nil)
}

func writeTracksWithVideoMasteringFloatElements(writer *ebml.Writer, elements ...colourFloatElement) error {
	return writeTracksWithVideoColourMetadata(writer, nil, elements)
}

func writeTracksWithVideoColourMetadata(writer *ebml.Writer, uints []colourUIntElement, floats []colourFloatElement) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	var video bytes.Buffer
	vw := ebml.NewWriter(&video)
	if err := vw.WriteUInt(idPixelWidth, 16); err != nil {
		return err
	}
	if err := vw.WriteUInt(idPixelHeight, 16); err != nil {
		return err
	}
	var colour bytes.Buffer
	cw := ebml.NewWriter(&colour)
	for i := range uints {
		if err := cw.WriteUInt(uints[i].id, uints[i].value); err != nil {
			return err
		}
	}
	if len(floats) != 0 {
		var mastering bytes.Buffer
		mw := ebml.NewWriter(&mastering)
		for i := range floats {
			if err := mw.WriteFloat64(floats[i].id, floats[i].value); err != nil {
				return err
			}
		}
		if err := cw.WriteElement(idMasteringMetadata, mastering.Bytes()); err != nil {
			return err
		}
	}
	if err := vw.WriteElement(idColour, colour.Bytes()); err != nil {
		return err
	}
	if err := ew.WriteElement(idVideo, video.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

type projectionUIntElement struct {
	id    ebml.ID
	value uint64
}

type projectionFloatElement struct {
	id    ebml.ID
	value float64
}

func writeTracksWithVideoProjectionMetadata(writer *ebml.Writer, uints []projectionUIntElement, floats []projectionFloatElement, private []byte) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "V_VP8"); err != nil {
		return err
	}
	var video bytes.Buffer
	vw := ebml.NewWriter(&video)
	if err := vw.WriteUInt(idPixelWidth, 16); err != nil {
		return err
	}
	if err := vw.WriteUInt(idPixelHeight, 16); err != nil {
		return err
	}
	var projection bytes.Buffer
	pw := ebml.NewWriter(&projection)
	for i := range uints {
		if err := pw.WriteUInt(uints[i].id, uints[i].value); err != nil {
			return err
		}
	}
	for i := range floats {
		if err := pw.WriteFloat64(floats[i].id, floats[i].value); err != nil {
			return err
		}
	}
	if len(private) != 0 {
		if err := writeBinary(pw, idProjectionPrivate, private); err != nil {
			return err
		}
	}
	if err := vw.WriteElement(idProjection, projection.Bytes()); err != nil {
		return err
	}
	if err := ew.WriteElement(idVideo, video.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithAudioMetadata(writer *ebml.Writer, sampleRate float64, channels uint64, bitDepth uint64) error {
	return writeTracksWithAudioMetadataValues(writer, sampleRate, 0, false, channels, bitDepth)
}

func writeTracksWithAudioOutputMetadata(writer *ebml.Writer, sampleRate float64, outputSampleRate float64, channels uint64, bitDepth uint64) error {
	return writeTracksWithAudioMetadataValues(writer, sampleRate, outputSampleRate, true, channels, bitDepth)
}

func writeTracksWithAudioMetadataValues(writer *ebml.Writer, sampleRate float64, outputSampleRate float64, writeOutputSampleRate bool, channels uint64, bitDepth uint64) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "A_OPUS"); err != nil {
		return err
	}
	var audio bytes.Buffer
	aw := ebml.NewWriter(&audio)
	if err := aw.WriteFloat64(idSamplingFreq, sampleRate); err != nil {
		return err
	}
	if writeOutputSampleRate {
		if err := aw.WriteFloat64(idOutputFreq, outputSampleRate); err != nil {
			return err
		}
	}
	if err := aw.WriteUInt(idChannels, channels); err != nil {
		return err
	}
	if err := aw.WriteUInt(idBitDepth, bitDepth); err != nil {
		return err
	}
	if err := ew.WriteElement(idAudio, audio.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithAudioElements(writer *ebml.Writer, writeAudioElements func(*ebml.Writer) error) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDOpus); err != nil {
		return err
	}
	var audio bytes.Buffer
	aw := ebml.NewWriter(&audio)
	if writeAudioElements != nil {
		if err := writeAudioElements(aw); err != nil {
			return err
		}
	}
	if err := ew.WriteElement(idAudio, audio.Bytes()); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithOpusPrivate(writer *ebml.Writer, private []byte) error {
	return writeTracksWithOpusPrivateAndTiming(writer, private, 0, 0)
}

func writeTracksWithMSACMPrivate(writer *ebml.Writer, private []byte, audioConfig AudioConfig) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDMS); err != nil {
		return err
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeAudio(ew, audioConfig); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithOpusPrivateAndTiming(writer *ebml.Writer, private []byte, codecDelayNS uint64, seekPreRollNS uint64) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackAudio); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, "A_OPUS"); err != nil {
		return err
	}
	if codecDelayNS != 0 {
		if err := ew.WriteUInt(idCodecDelay, codecDelayNS); err != nil {
			return err
		}
	}
	if seekPreRollNS != 0 {
		if err := ew.WriteUInt(idSeekPreRoll, seekPreRollNS); err != nil {
			return err
		}
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeAudio(ew, AudioConfig{SampleRate: 48000, Channels: 2}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithAV1Private(writer *ebml.Writer, private []byte) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDAV1); err != nil {
		return err
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithH264Private(writer *ebml.Writer, private []byte) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDH264); err != nil {
		return err
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeTracksWithH265Private(writer *ebml.Writer, private []byte) error {
	var tracks bytes.Buffer
	tw := ebml.NewWriter(&tracks)
	var entry bytes.Buffer
	ew := ebml.NewWriter(&entry)
	if err := ew.WriteUInt(idTrackNumber, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackUID, 1); err != nil {
		return err
	}
	if err := ew.WriteUInt(idTrackType, matroskaTrackVideo); err != nil {
		return err
	}
	if err := ew.WriteString(idCodecID, codecIDH265); err != nil {
		return err
	}
	if err := writeBinary(ew, idCodecPrivate, private); err != nil {
		return err
	}
	if err := writeVideo(ew, VideoConfig{Width: 16, Height: 16}); err != nil {
		return err
	}
	if err := tw.WriteElement(idTrackEntry, entry.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idTracks, tracks.Bytes())
}

func writeCuesWithTrackNumber(writer *ebml.Writer, trackNumber uint64) error {
	var cues bytes.Buffer
	cw := ebml.NewWriter(&cues)
	var point bytes.Buffer
	pw := ebml.NewWriter(&point)
	if err := pw.WriteUInt(idCueTime, 0); err != nil {
		return err
	}
	var positions bytes.Buffer
	tw := ebml.NewWriter(&positions)
	if err := tw.WriteUInt(idCueTrack, trackNumber); err != nil {
		return err
	}
	if err := tw.WriteUInt(idCueClusterPosition, 0); err != nil {
		return err
	}
	if err := pw.WriteElement(idCueTrackPositions, positions.Bytes()); err != nil {
		return err
	}
	if err := cw.WriteElement(idCuePoint, point.Bytes()); err != nil {
		return err
	}
	return writer.WriteElement(idCues, cues.Bytes())
}

func writeSimpleBlockWithTrackNumber(writer *ebml.Writer, trackNumber uint64, frame []byte) error {
	payload, err := blockPayloadWithTrackNumber(trackNumber, simpleBlockKeyframe, frame)
	if err != nil {
		return err
	}
	return writer.WriteElement(idSimpleBlock, payload)
}

func writeBlockWithTrackNumber(writer *ebml.Writer, trackNumber uint64, frame []byte) error {
	payload, err := blockPayloadWithTrackNumber(trackNumber, 0, frame)
	if err != nil {
		return err
	}
	return writer.WriteElement(idBlock, payload)
}

func blockPayloadWithTrackNumber(trackNumber uint64, flags byte, frame []byte) ([]byte, error) {
	var payload bytes.Buffer
	var scratch [ebml.MaxSizeWidth]byte
	n, err := ebml.EncodeUnsignedVINT(scratch[:], trackNumber)
	if err != nil {
		return nil, err
	}
	payload.Write(scratch[:n])
	var blockHeader [3]byte
	blockHeader[2] = flags
	payload.Write(blockHeader[:])
	payload.Write(frame)
	return payload.Bytes(), nil
}

func writeLacedSimpleBlock(writer *ebml.Writer, trackID uint32, lacing byte, frames [][]byte) error {
	return writeLacedBlockElement(writer, idSimpleBlock, trackID, simpleBlockKeyframe|lacing, frames)
}

func writeLacedBlockElement(writer *ebml.Writer, id ebml.ID, trackID uint32, flags byte, frames [][]byte) error {
	var payload bytes.Buffer
	var scratch [ebml.MaxSizeWidth]byte
	n, err := ebml.EncodeUnsignedVINT(scratch[:], uint64(trackID))
	if err != nil {
		return err
	}
	payload.Write(scratch[:n])
	var blockHeader [3]byte
	binary.BigEndian.PutUint16(blockHeader[:2], 0)
	blockHeader[2] = flags
	payload.Write(blockHeader[:])
	if len(frames) < 2 || len(frames) > 256 {
		return ErrInvalidData
	}
	payload.WriteByte(byte(len(frames) - 1))
	lacing := flags & simpleBlockLacingMask
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
	return writer.WriteElement(id, payload.Bytes())
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

func assertTopLevelMasterStartsWithCRC32(tb testing.TB, data []byte, id ebml.ID) {
	tb.Helper()
	payload := topLevelElementPayload(tb, data, id)
	reader := ebml.NewReader(bytes.NewReader(payload), ebml.ReaderOptions{})
	child, err := reader.ReadHeader()
	if err != nil {
		tb.Fatal(err)
	}
	if child.ID != idCRC32 || child.Size.Unknown || child.Size.Value != 4 {
		tb.Fatalf("top-level 0x%x first child = %+v, want CRC32", uint64(id), child)
	}
}

func topLevelElementPayload(tb testing.TB, data []byte, id ebml.ID) []byte {
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
		tb.Fatalf("segment = %+v, want Segment", segment)
	}
	var segmentEnd int64
	if !segment.Size.Unknown {
		segmentEnd = segment.DataOffset + int64(segment.Size.Value)
	}
	for segment.Size.Unknown || reader.Offset() < segmentEnd {
		child, err := reader.ReadHeader()
		if err != nil {
			if segment.Size.Unknown && errors.Is(err, io.EOF) {
				break
			}
			tb.Fatal(err)
		}
		if child.ID == id {
			if child.Size.Unknown || child.Size.Value > uint64(math.MaxInt) {
				tb.Fatalf("top-level 0x%x has invalid size %+v", uint64(id), child.Size)
			}
			payload := make([]byte, int(child.Size.Value))
			if err := reader.ReadFull(payload); err != nil {
				tb.Fatal(err)
			}
			return payload
		}
		if child.Size.Unknown {
			tb.Fatalf("unexpected unknown top-level child %+v while looking for 0x%x", child, uint64(id))
		}
		if err := reader.Skip(child.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	tb.Fatalf("missing top-level element 0x%x", uint64(id))
	return nil
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

func firstBlockFlags(tb testing.TB, data []byte, id ebml.ID) byte {
	tb.Helper()
	flags, ok := firstBlockFlagsInPayload(tb, data, id)
	if !ok {
		tb.Fatalf("missing block 0x%x", uint64(id))
	}
	return flags
}

func firstBlockFlagsInPayload(tb testing.TB, payload []byte, id ebml.ID) (byte, bool) {
	tb.Helper()
	reader := ebml.NewReader(bytes.NewReader(payload), ebml.ReaderOptions{})
	for reader.Offset() < int64(len(payload)) {
		header, err := reader.ReadHeader()
		if err != nil {
			tb.Fatal(err)
		}
		if header.ID == id {
			if header.Size.Unknown || header.Size.Value > uint64(len(payload)) {
				tb.Fatalf("invalid block size %+v", header.Size)
			}
			blockPayload := make([]byte, int(header.Size.Value))
			if err := reader.ReadFull(blockPayload); err != nil {
				tb.Fatal(err)
			}
			return rawBlockFlags(tb, blockPayload), true
		}
		if header.Size.Unknown {
			if isBlockSearchMaster(header.ID) {
				flags, ok := firstBlockFlagsInPayload(tb, payload[reader.Offset():], id)
				if ok {
					return flags, true
				}
			}
			return 0, false
		}
		if header.Size.Value > uint64(len(payload)) {
			tb.Fatalf("invalid element size %+v", header.Size)
		}
		if isBlockSearchMaster(header.ID) {
			childPayload := make([]byte, int(header.Size.Value))
			if err := reader.ReadFull(childPayload); err != nil {
				tb.Fatal(err)
			}
			flags, ok := firstBlockFlagsInPayload(tb, childPayload, id)
			if ok {
				return flags, true
			}
			continue
		}
		if err := reader.Skip(header.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return 0, false
}

func isBlockSearchMaster(id ebml.ID) bool {
	switch id {
	case idSegment, idCluster, idBlockGroup:
		return true
	default:
		return false
	}
}

func rawBlockFlags(tb testing.TB, payload []byte) byte {
	tb.Helper()
	reader := bytes.NewReader(payload)
	var scratch [ebml.MaxSizeWidth]byte
	if _, _, err := ebml.ReadUnsignedVINT(reader, &scratch); err != nil {
		tb.Fatal(err)
	}
	var header [3]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		tb.Fatal(err)
	}
	return header[2]
}

func readFirstDiscardPadding(tb testing.TB, data []byte) (int64, bool) {
	tb.Helper()
	reader := ebml.NewReader(bytes.NewReader(data), ebml.ReaderOptions{})
	for {
		header, err := reader.ReadHeader()
		if errors.Is(err, io.EOF) {
			return 0, false
		}
		if err != nil {
			tb.Fatal(err)
		}
		if header.ID == idBlockGroup {
			return readDiscardPaddingFromBlockGroup(tb, data[header.DataOffset:header.DataOffset+int64(header.Size.Value)])
		}
		if header.Size.Unknown {
			continue
		}
		if err := reader.Skip(header.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
}

func readDiscardPaddingFromBlockGroup(tb testing.TB, payload []byte) (int64, bool) {
	tb.Helper()
	reader := ebml.NewReader(bytes.NewReader(payload), ebml.ReaderOptions{})
	for reader.Offset() < int64(len(payload)) {
		header, err := reader.ReadHeader()
		if err != nil {
			tb.Fatal(err)
		}
		if header.ID == idDiscardPad {
			value, err := readIntPayload(reader, header.Size.Value)
			if err != nil {
				tb.Fatal(err)
			}
			return value, true
		}
		if header.Size.Unknown {
			tb.Fatalf("unexpected unknown BlockGroup child: %+v", header)
		}
		if err := reader.Skip(header.Size.Value); err != nil {
			tb.Fatal(err)
		}
	}
	return 0, false
}

func unknownElementBytes(tb testing.TB, id ebml.ID, payload []byte) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writer.WriteElement(id, payload); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func assertUnknownElement(tb testing.TB, name string, elements []UnknownElement, id ebml.ID, raw []byte) {
	tb.Helper()
	if len(elements) != 1 {
		tb.Fatalf("%s unknown elements = %d, want 1", name, len(elements))
	}
	if elements[0].ID != uint64(id) || !bytes.Equal(elements[0].Raw, raw) {
		tb.Fatalf("%s unknown element = %+v raw=%x, want id=0x%x raw=%x", name, elements[0], elements[0].Raw, uint64(id), raw)
	}
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
		oldLen := len(m.bytes)
		endLen := int(end)
		if endLen <= cap(m.bytes) {
			m.bytes = m.bytes[:endLen]
			clear(m.bytes[oldLen:])
		} else {
			nextCap := max(cap(m.bytes)*2, endLen)
			next := make([]byte, endLen, nextCap)
			copy(next, m.bytes)
			m.bytes = next
		}
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

var errFailingWriter = errors.New("failing writer")

type failToggleWriteSeeker struct {
	memoryWriteSeeker
	fail bool
}

func (m *failToggleWriteSeeker) Write(payload []byte) (int, error) {
	if m.fail {
		return 0, errFailingWriter
	}
	return m.memoryWriteSeeker.Write(payload)
}

package matroska

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strconv"
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

func TestMuxerDemuxerSupportsWebRTCCodecs(t *testing.T) {
	tests := []struct {
		name  string
		track Track
		data  []byte
	}{
		{
			name: "opus",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecOpus,
				Audio: AudioConfig{SampleRate: 48000, Channels: 2},
			},
			data: []byte{0x01, 0x02},
		},
		{
			name: "av1",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecAV1,
				Video:        VideoConfig{Width: 640, Height: 360},
				CodecPrivate: av1CodecConfig(),
			},
			data: []byte{0x12, 0x00, 0x0a},
		},
		{
			name: "h264",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecH264,
				Video:        VideoConfig{Width: 640, Height: 360},
				CodecPrivate: h264AVCDecoderConfig(),
			},
			data: h264AnnexBAccessUnit(),
		},
		{
			name: "vp9",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP9,
				Video: VideoConfig{Width: 640, Height: 360},
			},
			data: []byte{0x83, 0x49, 0x83},
		},
		{
			name: "vp8",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 640, Height: 360},
			},
			data: []byte{0x9d, 0x01, 0x2a},
		},
	}

	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackIDs := make([]uint32, len(tests))
	for i := range tests {
		trackID, err := muxer.AddTrack(tests[i].track)
		if err != nil {
			t.Fatalf("%s add track: %v", tests[i].name, err)
		}
		trackIDs[i] = trackID
	}
	for i := range tests {
		if err := muxer.WritePacket(Packet{
			TrackID:  trackIDs[i],
			TimeNS:   int64(i) * 20_000_000,
			Keyframe: true,
			Data:     tests[i].data,
		}); err != nil {
			t.Fatalf("%s write packet: %v", tests[i].name, err)
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
	if len(tracks) != len(tests) {
		t.Fatalf("tracks = %d, want %d", len(tracks), len(tests))
	}
	for i := range tests {
		wantPrivate := tests[i].track.CodecPrivate
		if tests[i].track.Codec == CodecOpus && len(wantPrivate) == 0 {
			wantPrivate = expectedOpusHead(tests[i].track.Audio.Channels, tests[i].track.Audio.SampleRate)
		}
		if tracks[i].Codec != tests[i].track.Codec || tracks[i].Type != tests[i].track.Type ||
			!bytes.Equal(tracks[i].CodecPrivate, wantPrivate) {
			t.Fatalf("%s track = %+v, want %+v", tests[i].name, tracks[i], tests[i].track)
		}
	}
	got := Packet{Data: make([]byte, 0, 16)}
	for i := range tests {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatalf("%s read packet: %v", tests[i].name, err)
		}
		if got.TrackID != trackIDs[i] || got.TimeNS != int64(i)*20_000_000 ||
			!bytes.Equal(got.Data, tests[i].data) {
			t.Fatalf("%s packet = %+v data=%v", tests[i].name, got, got.Data)
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestMuxerDemuxerPreservesTrackUIDAndFlags(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		ID:             7,
		UID:            12345,
		Type:           TrackVideo,
		Codec:          CodecVP8,
		FlagEnabled:    false,
		FlagEnabledSet: true,
		FlagDefault:    false,
		FlagDefaultSet: true,
		FlagForced:     true,
		FlagForcedSet:  true,
		FlagLacing:     false,
		FlagLacingSet:  true,
		Video:          VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	if trackID != 7 {
		t.Fatalf("trackID = %d, want 7", trackID)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
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
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	track := tracks[0]
	if track.ID != 7 || track.UID != 12345 ||
		track.FlagEnabled || !track.FlagEnabledSet ||
		track.FlagDefault || !track.FlagDefaultSet ||
		!track.FlagForced || !track.FlagForcedSet ||
		track.FlagLacing || !track.FlagLacingSet {
		t.Fatalf("track = %+v", track)
	}
}

func TestMuxerDemuxerPreservesLanguageBCP47(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:          TrackAudio,
		Codec:         CodecOpus,
		Language:      "por",
		LanguageBCP47: "pt-BR",
		Audio:         AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0xf8, 0xff, 0xfe},
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
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if tracks[0].Language != "por" || tracks[0].LanguageBCP47 != "pt-BR" {
		t.Fatalf("track language legacy=%q bcp47=%q", tracks[0].Language, tracks[0].LanguageBCP47)
	}
}

func TestFormatDemuxerPrefersLanguageBCP47(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:          TrackAudio,
		Codec:         CodecOpus,
		Language:      "eng",
		LanguageBCP47: "en-US",
		Audio:         AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0xf8, 0xff, 0xfe},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer := &FormatDemuxer{}
	if err := demuxer.Open(context.Background(), format.Input{Reader: bytes.NewReader(buffer.Bytes())}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	streams := demuxer.Streams()
	if len(streams) != 1 {
		t.Fatalf("streams = %d, want 1", len(streams))
	}
	if streams[0].Language != "en-US" {
		t.Fatalf("stream language = %q, want en-US", streams[0].Language)
	}
}

func TestMuxerDefaultsTrackUIDAndFlags(t *testing.T) {
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
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
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
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if tracks[0].UID != uint64(trackID) ||
		!tracks[0].FlagEnabled || !tracks[0].FlagEnabledSet ||
		!tracks[0].FlagDefault || !tracks[0].FlagDefaultSet ||
		tracks[0].FlagForced || !tracks[0].FlagForcedSet ||
		!tracks[0].FlagLacing || !tracks[0].FlagLacingSet {
		t.Fatalf("track = %+v", tracks[0])
	}
}

func TestMuxerRejectsDuplicateTrackUID(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		UID:   42,
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		UID:   42,
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	}); !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
}

func TestDemuxerRejectsInvalidTrackFlags(t *testing.T) {
	tests := []struct {
		name string
		id   ebml.ID
	}{
		{name: "enabled", id: idFlagEnabled},
		{name: "default", id: idFlagDefault},
		{name: "forced", id: idFlagForced},
		{name: "lacing", id: idFlagLacing},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
				return writeTracksWithFlagValue(writer, tt.id, 2)
			})
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsLacedPacketWhenTrackDisablesLacing(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		FlagLacing:        false,
		FlagLacingSet:     true,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   [][]byte{{1}, {2}},
	}); !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
}

func TestDemuxerRejectsLacedBlockWhenTrackDisablesLacing(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		FlagLacing:        false,
		FlagLacingSet:     true,
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
	if err := writeLacedSimpleBlock(muxer.ebml, trackID, simpleBlockLacingXiph, [][]byte{{1}, {2}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 4)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerTracksReturnsCodecPrivateCopies(t *testing.T) {
	data := makeH264AVCMatroskaData(t, 1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 || len(tracks[0].CodecPrivate) == 0 {
		t.Fatalf("tracks = %+v", tracks)
	}
	tracks[0].CodecPrivate[4] = 0xfe

	fresh := demuxer.Tracks()
	if len(fresh) != 1 || !bytes.Equal(fresh[0].CodecPrivate, h264AVCDecoderConfigWithLengthSize(2)) {
		t.Fatalf("fresh tracks = %+v", fresh)
	}
	packet := Packet{Data: make([]byte, 0, len(h264AnnexBAccessUnit()))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packet.Data, h264AnnexBAccessUnit()) {
		t.Fatalf("data = %v, want Annex B", packet.Data)
	}
}

func TestMuxerWritesDefaultOpusHead(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{1, 2, 3},
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
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if tracks[0].Codec != CodecOpus || tracks[0].Audio.Channels != 1 ||
		tracks[0].CodecDelayNS != 0 || tracks[0].SeekPreRollNS != opusDefaultSeekPreRollNS ||
		!bytes.Equal(tracks[0].CodecPrivate, expectedOpusHead(1, 48000)) {
		t.Fatalf("track = %+v private=%v", tracks[0], tracks[0].CodecPrivate)
	}
}

func TestMuxerDerivesOpusCodecTimingFromPrivateData(t *testing.T) {
	private := expectedOpusHeadWithPreSkip(2, 48000, 312)
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackAudio,
		Codec:        CodecOpus,
		Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
		CodecPrivate: private,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0xf8, 0xff, 0xfe},
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
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if tracks[0].CodecDelayNS != 6_500_000 || tracks[0].SeekPreRollNS != opusDefaultSeekPreRollNS {
		t.Fatalf("track timing delay=%d preroll=%d", tracks[0].CodecDelayNS, tracks[0].SeekPreRollNS)
	}
}

func TestDemuxerReadsOpusCodecTiming(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	writeMatroskaSegmentPrefix(t, muxer)
	if err := writeTracksWithOpusPrivateAndTiming(muxer.ebml, expectedOpusHeadWithPreSkip(2, 48000, 960), 1_234, 5_678); err != nil {
		t.Fatal(err)
	}
	if err := muxer.ebml.WriteElement(idCluster, nil); err != nil {
		t.Fatal(err)
	}
	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	if tracks[0].CodecDelayNS != 1_234 || tracks[0].SeekPreRollNS != 5_678 {
		t.Fatalf("track timing delay=%d preroll=%d", tracks[0].CodecDelayNS, tracks[0].SeekPreRollNS)
	}
}

func TestDemuxerRejectsInvalidOpusHead(t *testing.T) {
	tests := []struct {
		name    string
		private []byte
	}{
		{name: "short", private: []byte("OpusHead")},
		{name: "bad magic", private: []byte("OpusHed\x01\x02\x00\x00\x80\xbb\x00\x00\x00\x00\x00")},
		{name: "zero channels", private: []byte("OpusHead\x01\x00\x00\x00\x80\xbb\x00\x00\x00\x00\x00")},
		{name: "family zero surround", private: []byte("OpusHead\x01\x03\x00\x00\x80\xbb\x00\x00\x00\x00\x00")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
				return writeTracksWithOpusPrivate(writer, tt.private)
			})
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestAV1CodecConfigurationRecordValidation(t *testing.T) {
	config, err := parseAV1CodecConfigurationRecord(av1CodecConfig())
	if err != nil {
		t.Fatal(err)
	}
	if config.SeqProfile != 0 || config.SeqLevelIdx0 != 5 || config.SeqTier0 ||
		config.HighBitDepth || config.TwelveBit || !config.Monochrome ||
		config.ChromaSubsamplingX || config.ChromaSubsamplingY ||
		config.ChromaSamplePosition != 0 || config.InitialPresentationDelaySet ||
		config.ConfigOBUCount != 1 || !config.SequenceHeaderOBUPresent {
		t.Fatalf("config = %+v", config)
	}

	tests := []struct {
		name    string
		private []byte
	}{
		{name: "short", private: []byte{0x81, 0x05, 0x10}},
		{name: "bad marker", private: av1CodecConfigWithByte(0, 0x01)},
		{name: "bad version", private: av1CodecConfigWithByte(0, 0x82)},
		{name: "bad profile", private: av1CodecConfigWithByte(1, 0xe5)},
		{name: "twelve bit without high bit depth", private: av1CodecConfigWithByte(2, 0x20)},
		{name: "reserved chroma sample position", private: av1CodecConfigWithByte(2, 0x03)},
		{name: "reserved fixed bits", private: av1CodecConfigWithByte(3, 0xe0)},
		{name: "delay bits without presence", private: av1CodecConfigWithByte(3, 0x01)},
		{name: "obu missing size", private: av1CodecConfigWithByte(4, 0x08)},
		{name: "sequence not first", private: av1CodecConfigWithPrefixOBU([]byte{0x2a, 0x00})},
		{name: "duplicate sequence", private: append(av1CodecConfig(), av1SequenceHeaderOBU()...)},
		{name: "truncated leb128", private: append([]byte{0x81, 0x05, 0x10, 0x00, 0x0a}, 0x80)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseAV1CodecConfigurationRecord(tt.private); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidAV1CodecPrivate(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecAV1,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: []byte{0x81, 0x05, 0x10, 0x00, 0x0a},
	})
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
}

func TestDemuxerRejectsInvalidAV1CodecPrivate(t *testing.T) {
	data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
		return writeTracksWithAV1Private(writer, []byte{0x81, 0x05, 0x10, 0x00, 0x0a})
	})
	if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestAV1CodecConfigurationRecordFromSequenceHeader(t *testing.T) {
	private, err := av1CodecConfigurationRecordFromFrames([][]byte{av1SequenceHeaderOBU()})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(private, av1CodecConfig()) {
		t.Fatalf("private = %v, want %v", private, av1CodecConfig())
	}
	if _, err := parseAV1CodecConfigurationRecord(private); err != nil {
		t.Fatal(err)
	}

	_, err = av1CodecConfigurationRecordFromFrames([][]byte{av1TemporalDelimiterOBU()})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestMuxerGeneratesAV1CodecPrivateFromFirstPacket(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecAV1,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	data := av1SequenceHeaderOBU()
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 || !bytes.Equal(tracks[0].CodecPrivate, av1CodecConfig()) {
		t.Fatalf("tracks = %+v", tracks)
	}
	packet := Packet{Data: make([]byte, 0, len(data))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TrackID != trackID || packet.TimeNS != 0 || !packet.Keyframe || !bytes.Equal(packet.Data, data) {
		t.Fatalf("packet = %+v data=%v, want %v", packet, packet.Data, data)
	}
}

func TestMuxerGeneratesAV1CodecPrivateFromFirstLacedPacket(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecAV1,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{av1SequenceHeaderOBU(), av1TemporalDelimiterOBU()}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   frames,
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
	tracks := demuxer.Tracks()
	if len(tracks) != 1 || !bytes.Equal(tracks[0].CodecPrivate, av1CodecConfig()) {
		t.Fatalf("tracks = %+v", tracks)
	}
	packet := Packet{Data: make([]byte, 0, len(frames[0]))}
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("frame %d read: %v", i, err)
		}
		if packet.TrackID != trackID || packet.TimeNS != int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || !packet.Keyframe ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, frames[i])
		}
	}
}

func TestMuxerRejectsHeaderWhenAV1CodecPrivateIsMissing(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecAV1,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	audioTrack, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = muxer.WritePacket(Packet{TrackID: audioTrack, TimeNS: 0, Keyframe: true, Data: []byte{1, 2, 3}})
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing AV1 private data", buffer.Len())
	}
}

func TestMuxerCloseRejectsMissingAV1CodecPrivate(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecAV1,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing AV1 private data", buffer.Len())
	}
}

func TestAVCDecoderConfigurationRecordValidation(t *testing.T) {
	config, err := parseAVCDecoderConfigurationRecord(h264AVCDecoderConfig())
	if err != nil {
		t.Fatal(err)
	}
	if config.ProfileIDC != 0x42 || config.ProfileCompatibility != 0x00 || config.LevelIDC != 0x0a ||
		config.NALULengthSize != 4 || config.SPSCount != 1 || config.PPSCount != 1 {
		t.Fatalf("config = %+v", config)
	}

	tests := []struct {
		name    string
		private []byte
	}{
		{name: "short", private: []byte{0x01, 0x42, 0x00, 0x0a, 0xff, 0xe1}},
		{name: "bad version", private: h264AVCDecoderConfigWithByte(0, 0x00)},
		{name: "bad reserved length bits", private: h264AVCDecoderConfigWithByte(4, 0x03)},
		{name: "invalid length size", private: h264AVCDecoderConfigWithByte(4, 0xfe)},
		{name: "no sps", private: h264AVCDecoderConfigWithByte(5, 0xe0)},
		{name: "wrong sps type", private: h264AVCDecoderConfigWithByte(8, 0x68)},
		{name: "missing pps", private: h264AVCDecoderConfig()[:15]},
		{name: "wrong pps type", private: h264AVCDecoderConfigWithByte(18, 0x67)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseAVCDecoderConfigurationRecord(tt.private); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidH264CodecPrivate(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH264,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: []byte{0x01, 0x64, 0x00, 0x1f},
	})
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
}

func TestDemuxerRejectsInvalidH264CodecPrivate(t *testing.T) {
	data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
		return writeTracksWithH264Private(writer, []byte{0x01, 0x64, 0x00, 0x1f})
	})
	if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestH264AVCDecoderConfigurationRecordFromAnnexB(t *testing.T) {
	private, err := h264AVCDecoderConfigurationRecordFromAnnexBFrames([][]byte{h264AnnexBParameterAccessUnit()})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(private, h264AVCDecoderConfig()) {
		t.Fatalf("private = %v, want %v", private, h264AVCDecoderConfig())
	}
	if _, err := parseAVCDecoderConfigurationRecord(private); err != nil {
		t.Fatal(err)
	}

	_, err = h264AVCDecoderConfigurationRecordFromAnnexBFrames([][]byte{h264AnnexBAccessUnit()})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestMuxerGeneratesH264CodecPrivateFromFirstAnnexBPacket(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH264,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	annexB := h264AnnexBParameterAccessUnit()
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: annexB}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	avc := h264AVCSampleWithParameterSetsLengthSize4()
	if !bytes.Contains(buffer.Bytes(), h264AVCDecoderConfig()) {
		t.Fatalf("muxed data does not contain generated AVC config")
	}
	if !bytes.Contains(buffer.Bytes(), avc) {
		t.Fatalf("muxed data does not contain AVC sample %v", avc)
	}
	if bytes.Contains(buffer.Bytes(), annexB) {
		t.Fatalf("muxed data still contains Annex B access unit")
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 || !bytes.Equal(tracks[0].CodecPrivate, h264AVCDecoderConfig()) {
		t.Fatalf("tracks = %+v", tracks)
	}
	packet := Packet{Data: make([]byte, 0, len(annexB))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TrackID != trackID || packet.TimeNS != 0 || !packet.Keyframe || !bytes.Equal(packet.Data, annexB) {
		t.Fatalf("packet = %+v data=%v, want Annex B %v", packet, packet.Data, annexB)
	}
}

func TestMuxerGeneratesH264CodecPrivateFromFirstLacedAnnexBPacket(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecH264,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{h264AnnexBParameterAccessUnit(), h264AnnexBInterFrame()}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   frames,
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
	tracks := demuxer.Tracks()
	if len(tracks) != 1 || !bytes.Equal(tracks[0].CodecPrivate, h264AVCDecoderConfig()) {
		t.Fatalf("tracks = %+v", tracks)
	}
	packet := Packet{Data: make([]byte, 0, len(frames[0]))}
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("frame %d read: %v", i, err)
		}
		if packet.TrackID != trackID || packet.TimeNS != int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || !packet.Keyframe ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, frames[i])
		}
	}
}

func TestMuxerRejectsHeaderWhenH264CodecPrivateIsMissing(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH264,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	audioTrack, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = muxer.WritePacket(Packet{TrackID: audioTrack, TimeNS: 0, Keyframe: true, Data: []byte{1, 2, 3}})
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing H.264 private data", buffer.Len())
	}
}

func TestMuxerCloseRejectsMissingH264CodecPrivate(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH264,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing H.264 private data", buffer.Len())
	}
}

func TestMuxerWritesH264AnnexBPacketsAsAVCSamples(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH264,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: h264AVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	annexB := h264AnnexBAccessUnit()
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: annexB}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	avc := h264AVCSampleWithLengthSize2()
	if !bytes.Contains(buffer.Bytes(), avc) {
		t.Fatalf("muxed data does not contain AVC sample %v", avc)
	}
	if bytes.Contains(buffer.Bytes(), annexB) {
		t.Fatalf("muxed data still contains Annex B access unit")
	}

	small, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(avc))}
	if err := small.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet.Data = make([]byte, 0, len(annexB))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TrackID != trackID || packet.TimeNS != 0 || !packet.Keyframe || !bytes.Equal(packet.Data, annexB) {
		t.Fatalf("packet = %+v data=%v, want Annex B %v", packet, packet.Data, annexB)
	}
}

func TestMuxerWritesLacedH264AnnexBFramesAsAVCSamples(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecH264,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
		CodecPrivate:      h264AVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{h264AnnexBAccessUnit(), h264AnnexBInterFrame()}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   frames,
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	avc := append(h264AVCSampleWithLengthSize2(), h264AVCInterFrameWithLengthSize2()...)
	if !bytes.Contains(buffer.Bytes(), avc) {
		t.Fatalf("muxed data does not contain laced AVC samples %v", avc)
	}
	for i := range frames {
		if bytes.Contains(buffer.Bytes(), frames[i]) {
			t.Fatalf("muxed data still contains Annex B frame %d", i)
		}
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 16)}
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("frame %d read: %v", i, err)
		}
		if packet.TrackID != trackID || packet.TimeNS != int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || !packet.Keyframe ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, frames[i])
		}
	}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, io.EOF) {
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
		got.Keyframe || len(got.ReferenceBlockTimeNS) != 1 || got.ReferenceBlockTimeNS[0] != 0 ||
		!bytes.Equal(got.Data, packet.Data) {
		t.Fatalf("packet = %+v data=%v, want %+v data=%v", got, got.Data, packet, packet.Data)
	}
}

func TestMuxerDemuxerPreservesBlockGroupReferences(t *testing.T) {
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
		TrackID:              trackID,
		TimeNS:               40_000_000,
		DurationNS:           20_000_000,
		ReferenceBlockTimeNS: []int64{-20_000_000, 40_000_000},
		Keyframe:             false,
		Data:                 []byte{0x11, 0x22, 0x33},
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
	got := Packet{
		Data:                 make([]byte, 0, 8),
		ReferenceBlockTimeNS: make([]int64, 0, 2),
	}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != packet.TrackID || got.TimeNS != packet.TimeNS || got.DurationNS != packet.DurationNS ||
		got.Keyframe || !equalInt64s(got.ReferenceBlockTimeNS, packet.ReferenceBlockTimeNS) ||
		!bytes.Equal(got.Data, packet.Data) {
		t.Fatalf("packet = %+v data=%v, want %+v data=%v", got, got.Data, packet, packet.Data)
	}
}

func TestMuxerWritesReferenceOnlyBlockGroup(t *testing.T) {
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
		TrackID:              trackID,
		TimeNS:               40_000_000,
		ReferenceBlockTimeNS: []int64{-20_000_000},
		Data:                 []byte{0x11, 0x22, 0x33},
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
	got := Packet{
		Data:                 make([]byte, 0, 8),
		ReferenceBlockTimeNS: make([]int64, 0, 1),
	}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != packet.TrackID || got.TimeNS != packet.TimeNS || got.DurationNS != 0 ||
		got.Keyframe || !equalInt64s(got.ReferenceBlockTimeNS, packet.ReferenceBlockTimeNS) ||
		!bytes.Equal(got.Data, packet.Data) {
		t.Fatalf("packet = %+v data=%v, want %+v data=%v", got, got.Data, packet, packet.Data)
	}
}

func TestMuxerDemuxerPreservesBlockGroupDiscardPadding(t *testing.T) {
	tests := []struct {
		name      string
		paddingNS int64
	}{
		{name: "end", paddingNS: 13_000_000},
		{name: "beginning", paddingNS: -7_000_000},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{TimecodeScaleNS: 1_000_000})
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
			packet := Packet{
				TrackID:          trackID,
				TimeNS:           40_000_000,
				DiscardPaddingNS: tt.paddingNS,
				Keyframe:         true,
				Data:             []byte{0xf8, 0xff, 0xfe},
			}
			if err := muxer.WritePacket(packet); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if paddingNS, ok := readFirstDiscardPadding(t, buffer.Bytes()); !ok || paddingNS != tt.paddingNS {
				t.Fatalf("raw DiscardPadding = %d ok=%v, want %d", paddingNS, ok, tt.paddingNS)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			got := Packet{Data: make([]byte, 0, 8)}
			if err := demuxer.ReadPacket(&got); err != nil {
				t.Fatal(err)
			}
			if got.TrackID != packet.TrackID || got.TimeNS != packet.TimeNS ||
				got.DurationNS != 0 || got.DiscardPaddingNS != packet.DiscardPaddingNS ||
				!got.Keyframe || !bytes.Equal(got.Data, packet.Data) {
				t.Fatalf("packet = %+v data=%v, want %+v data=%v", got, got.Data, packet, packet.Data)
			}
		})
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

func TestMuxerRejectsInvalidPacketDuration(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
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
	tests := []struct {
		name   string
		packet Packet
	}{
		{
			name: "negative",
			packet: Packet{
				TrackID:    trackID,
				TimeNS:     0,
				DurationNS: -1,
				Keyframe:   true,
				Data:       []byte{1},
			},
		},
		{
			name: "overflow",
			packet: Packet{
				TrackID:    trackID,
				TimeNS:     math.MaxInt64,
				DurationNS: 1,
				Keyframe:   true,
				Data:       []byte{1},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := muxer.WritePacket(tt.packet); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsKeyframeReferences(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
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
	err = muxer.WritePacket(Packet{
		TrackID:              trackID,
		TimeNS:               0,
		ReferenceBlockTimeNS: []int64{-20_000_000},
		Keyframe:             true,
		Data:                 []byte{1},
	})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestMuxerRejectsInvalidTrackMetadata(t *testing.T) {
	tests := []struct {
		name  string
		track Track
	}{
		{
			name: "audio sample rate",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecOpus,
				Audio: AudioConfig{SampleRate: -1, Channels: 2},
			},
		},
		{
			name: "audio channels",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecOpus,
				Audio: AudioConfig{SampleRate: 48000, Channels: -1},
			},
		},
		{
			name: "audio bit depth",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecPCMU,
				Audio: AudioConfig{SampleRate: 8000, Channels: 1, BitDepth: -1},
			},
		},
		{
			name: "opus surround without private data",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecOpus,
				Audio: AudioConfig{SampleRate: 48000, Channels: 6},
			},
		},
		{
			name: "video width",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: -1, Height: 16},
			},
		},
		{
			name: "default duration",
			track: Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: -1,
				Video:             VideoConfig{Width: 16, Height: 16},
			},
		},
		{
			name: "codec delay",
			track: Track{
				Type:         TrackAudio,
				Codec:        CodecOpus,
				CodecDelayNS: -1,
				Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
			},
		},
		{
			name: "seek preroll",
			track: Track{
				Type:          TrackAudio,
				Codec:         CodecOpus,
				SeekPreRollNS: -1,
				Audio:         AudioConfig{SampleRate: 48000, Channels: 2},
			},
		},
		{
			name: "timebase",
			track: Track{
				Type:        TrackAudio,
				Codec:       CodecOpus,
				TimebaseNum: -1,
				TimebaseDen: 48000,
				Audio:       AudioConfig{SampleRate: 48000, Channels: 2},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := muxer.AddTrack(tt.track); !errors.Is(err, ErrInvalidTrack) {
				t.Fatalf("err = %v, want ErrInvalidTrack", err)
			}
		})
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
		if !cues[i].RelativePositionSet {
			t.Fatalf("cue %d missing relative position: %+v", i, cues[i])
		}
	}
}

func TestDemuxerSeekToTimeUsesCueRelativePosition(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
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
		{TrackID: trackID, TimeNS: 20_000_000, DurationNS: 10_000_000, Keyframe: true, Data: []byte{2}},
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
	if err := demuxer.SeekToTime(20_000_000); err != nil {
		t.Fatal(err)
	}
	cues := demuxer.Cues()
	if len(cues) != len(packets) {
		t.Fatalf("cues = %+v, want %d cues", cues, len(packets))
	}
	if cues[0].ClusterPosition != cues[1].ClusterPosition {
		t.Fatalf("cues are not in the same cluster: %+v", cues)
	}
	if !cues[1].RelativePositionSet || cues[1].RelativePosition <= cues[0].RelativePosition {
		t.Fatalf("cues did not preserve relative block positions: %+v", cues)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[1].TimeNS || got.DurationNS != packets[1].DurationNS || !bytes.Equal(got.Data, packets[1].Data) {
		t.Fatalf("packet after seek = %+v data=%v, want %+v data=%v", got, got.Data, packets[1], packets[1].Data)
	}
}

func TestMuxerFailedPacketDoesNotAdvanceCuesOrDuration(t *testing.T) {
	ws := &failToggleWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{})
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
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     0,
		DurationNS: 10_000_000,
		Keyframe:   true,
		Data:       []byte{1},
	}); err != nil {
		t.Fatal(err)
	}
	if len(muxer.cues) != 1 || muxer.maxTimeNS != 10_000_000 {
		t.Fatalf("cues=%+v maxTimeNS=%d after first packet", muxer.cues, muxer.maxTimeNS)
	}
	ws.fail = true
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     20_000_000,
		DurationNS: 10_000_000,
		Keyframe:   true,
		Data:       []byte{2},
	}); !errors.Is(err, errFailingWriter) {
		t.Fatalf("err = %v, want errFailingWriter", err)
	}
	if len(muxer.cues) != 1 {
		t.Fatalf("cues = %+v, want only the successfully written packet", muxer.cues)
	}
	if muxer.maxTimeNS != 10_000_000 {
		t.Fatalf("maxTimeNS = %d, want 10000000", muxer.maxTimeNS)
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

func TestDemuxerReadPacketAtTimeUsesCues(t *testing.T) {
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
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacketAtTime(3_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[2].TimeNS || !bytes.Equal(got.Data, packets[2].Data) {
		t.Fatalf("packet at time = %+v data=%v, want %+v data=%v", got, got.Data, packets[2], packets[2].Data)
	}
}

func TestDemuxerReadPacketAtTimeFindsLacedFrame(t *testing.T) {
	ws := &memoryWriteSeeker{}
	muxer, err := NewMuxer(ws, MuxerOptions{})
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
	frames := [][]byte{{1}, {2}, {3}}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   frames,
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
	if err := demuxer.ReadPacketAtTime(20_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != 20_000_000 || got.DurationNS != 20_000_000 || !bytes.Equal(got.Data, frames[1]) {
		t.Fatalf("packet at time = %+v data=%v, want second laced frame", got, got.Data)
	}
}

func TestDemuxerReadPacketAtTimeRejectsNilPacket(t *testing.T) {
	data := makeMatroskaData(t, 1)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := demuxer.ReadPacketAtTime(0, nil); !errors.Is(err, ErrNilPacket) {
		t.Fatalf("err = %v, want ErrNilPacket", err)
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

func TestMuxerWritesLacedSimpleBlocks(t *testing.T) {
	tests := []struct {
		name   string
		lacing LacingMode
		frames [][]byte
	}{
		{
			name:   "xiph",
			lacing: LacingXiph,
			frames: [][]byte{{1}, {2, 3}, {4, 5, 6}},
		},
		{
			name:   "fixed",
			lacing: LacingFixed,
			frames: [][]byte{{1, 2}, {3, 4}, {5, 6}},
		},
		{
			name:   "ebml",
			lacing: LacingEBML,
			frames: [][]byte{{1, 2, 3}, {4}, {5, 6}},
		},
		{
			name:   "auto",
			lacing: LacingAuto,
			frames: [][]byte{{1, 2, 3}, {4, 5}, {6, 7, 8, 9}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
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
			if err := muxer.WriteLacedPacket(LacedPacket{
				TrackID:     trackID,
				TimeNS:      40_000_000,
				Keyframe:    true,
				Invisible:   true,
				Discardable: true,
				Lacing:      tt.lacing,
				Frames:      tt.frames,
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
			packet := Packet{Data: make([]byte, 0, 16)}
			for i := range tt.frames {
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatalf("read frame %d: %v", i, err)
				}
				if packet.TrackID != trackID || packet.TimeNS != 40_000_000+int64(i)*20_000_000 ||
					packet.DurationNS != 20_000_000 || !packet.Keyframe || !packet.Invisible ||
					!packet.Discardable || !bytes.Equal(packet.Data, tt.frames[i]) {
					t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, tt.frames[i])
				}
			}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, io.EOF) {
				t.Fatalf("err = %v, want EOF", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidLacedPackets(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
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

	tests := []struct {
		name   string
		packet LacedPacket
		want   error
	}{
		{
			name: "unknown track",
			packet: LacedPacket{
				TrackID: trackID + 1,
				Frames:  [][]byte{{1}, {2}},
			},
			want: ErrUnknownTrack,
		},
		{
			name: "single frame",
			packet: LacedPacket{
				TrackID: trackID,
				Frames:  [][]byte{{1}},
			},
			want: ErrInvalidData,
		},
		{
			name: "too many frames",
			packet: LacedPacket{
				TrackID: trackID,
				Frames:  makeLaceFrames(257, []byte{1}),
			},
			want: ErrInvalidData,
		},
		{
			name: "fixed unequal sizes",
			packet: LacedPacket{
				TrackID: trackID,
				Lacing:  LacingFixed,
				Frames:  [][]byte{{1}, {2, 3}},
			},
			want: ErrInvalidData,
		},
		{
			name: "unsupported lacing",
			packet: LacedPacket{
				TrackID: trackID,
				Lacing:  LacingMode(99),
				Frames:  [][]byte{{1}, {2}},
			},
			want: ErrUnsupportedLacing,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := muxer.WriteLacedPacket(tt.packet); !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
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

func TestDemuxerRejectsOversizedTrackIDs(t *testing.T) {
	oversized := maxTrackID + 1
	t.Run("track entry", func(t *testing.T) {
		data := makeTrackNumberMatroskaData(t, oversized)
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("cue track", func(t *testing.T) {
		data := makeCueTrackNumberMatroskaData(t, oversized)
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("block track", func(t *testing.T) {
		data := makeBlockTrackNumberMatroskaData(t, oversized)
		demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
		if err != nil {
			t.Fatal(err)
		}
		packet := Packet{Data: make([]byte, 0, 8)}
		if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
}

func TestDemuxerRejectsBlockForUnknownTrack(t *testing.T) {
	data := makeBlockTrackNumberMatroskaData(t, 2)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrUnknownTrack) {
		t.Fatalf("err = %v, want ErrUnknownTrack", err)
	}
}

func TestDemuxerRejectsInvalidTrackMetadata(t *testing.T) {
	t.Run("video dimension overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithVideoDimensions(writer, maxIntValue+1, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("audio channels overflow", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithAudioMetadata(writer, 48000, maxIntValue+1, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("audio negative sample rate", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithAudioMetadata(writer, -1, 2, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
	t.Run("audio nan sample rate", func(t *testing.T) {
		data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
			return writeTracksWithAudioMetadata(writer, math.NaN(), 2, 16)
		})
		if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
			t.Fatalf("err = %v, want ErrInvalidData", err)
		}
	})
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

func TestFormatDemuxerStreamsReturnsExtraDataCopies(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Index:    0,
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: timeNS},
		Codec: av.CodecParameters{
			ID:        av.CodecH264,
			Type:      av.MediaVideo,
			Width:     16,
			Height:    16,
			ExtraData: av.Buffer{Bytes: h264AVCDecoderConfigWithLengthSize(2)},
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: h264AnnexBAccessUnit()},
		PTS:      av.Timestamp{Value: 0, Base: stream.TimeBase},
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
	if len(streams) != 1 || len(streams[0].Codec.ExtraData.Bytes) == 0 {
		t.Fatalf("streams = %+v", streams)
	}
	streams[0].Codec.ExtraData.Bytes[4] = 0xfe

	fresh := demuxer.Streams()
	if len(fresh) != 1 || !bytes.Equal(fresh[0].Codec.ExtraData.Bytes, h264AVCDecoderConfigWithLengthSize(2)) {
		t.Fatalf("fresh streams = %+v", fresh)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, len(h264AnnexBAccessUnit()))}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.Packet.Payload.Bytes, h264AnnexBAccessUnit()) {
		t.Fatalf("payload = %v, want Annex B", result.Packet.Payload.Bytes)
	}
}

func TestFormatMuxerDemuxerSupportsWebRTCCodecs(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{
		{
			ID:       "opus",
			Index:    0,
			Type:     av.MediaAudio,
			TimeBase: av.TimeBase{Num: 1, Den: 1000},
			Codec: av.CodecParameters{
				ID:         av.CodecOpus,
				Type:       av.MediaAudio,
				SampleRate: 48000,
				Channels:   2,
			},
		},
		{
			ID:       "av1",
			Index:    1,
			Type:     av.MediaVideo,
			TimeBase: av.TimeBase{Num: 1, Den: 1000},
			Codec: av.CodecParameters{
				ID:        av.CodecAV1,
				Type:      av.MediaVideo,
				Width:     640,
				Height:    360,
				ExtraData: av.Buffer{Bytes: av1CodecConfig()},
			},
		},
		{
			ID:       "h264",
			Index:    2,
			Type:     av.MediaVideo,
			TimeBase: av.TimeBase{Num: 1, Den: 1000},
			Codec: av.CodecParameters{
				ID:        av.CodecH264,
				Type:      av.MediaVideo,
				Width:     640,
				Height:    360,
				ExtraData: av.Buffer{Bytes: h264AVCDecoderConfig()},
			},
		},
		{
			ID:       "vp9",
			Index:    3,
			Type:     av.MediaVideo,
			TimeBase: av.TimeBase{Num: 1, Den: 1000},
			Codec: av.CodecParameters{
				ID:     av.CodecVP9,
				Type:   av.MediaVideo,
				Width:  640,
				Height: 360,
			},
		},
		{
			ID:       "vp8",
			Index:    4,
			Type:     av.MediaVideo,
			TimeBase: av.TimeBase{Num: 1, Den: 1000},
			Codec: av.CodecParameters{
				ID:     av.CodecVP8,
				Type:   av.MediaVideo,
				Width:  640,
				Height: 360,
			},
		},
	}
	payloads := [][]byte{
		{0x01, 0x02},
		{0x12, 0x00, 0x0a},
		h264AnnexBAccessUnit(),
		{0x83, 0x49, 0x83},
		{0x9d, 0x01, 0x2a},
	}

	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, streams, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	for i := range streams {
		if err := muxer.Write(ctx, &av.Packet{
			StreamID: streams[i].ID,
			Payload:  av.Buffer{Bytes: payloads[i]},
			PTS:      av.Timestamp{Value: int64(i) * 20, Base: streams[i].TimeBase},
			Keyframe: true,
		}, nil); err != nil {
			t.Fatalf("%s write packet: %v", streams[i].ID, err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer := &FormatDemuxer{}
	if err := demuxer.Open(ctx, format.Input{Reader: bytes.NewReader(buffer.Bytes())}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	gotStreams := demuxer.Streams()
	if len(gotStreams) != len(streams) {
		t.Fatalf("streams = %d, want %d", len(gotStreams), len(streams))
	}
	for i := range streams {
		wantPrivate := streams[i].Codec.ExtraData.Bytes
		if streams[i].Codec.ID == av.CodecOpus && len(wantPrivate) == 0 {
			wantPrivate = expectedOpusHead(streams[i].Codec.Channels, streams[i].Codec.SampleRate)
		}
		if gotStreams[i].Codec.ID != streams[i].Codec.ID || gotStreams[i].Type != streams[i].Type ||
			!bytes.Equal(gotStreams[i].Codec.ExtraData.Bytes, wantPrivate) {
			t.Fatalf("%s stream = %+v, want %+v", streams[i].ID, gotStreams[i], streams[i])
		}
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 16)}}}
	for i := range streams {
		if err := demuxer.ReadInto(ctx, &result); err != nil {
			t.Fatalf("%s read packet: %v", streams[i].ID, err)
		}
		if !result.PacketReady || result.Packet.StreamID != av.StreamID(strconv.Itoa(i+1)) ||
			result.Packet.PTS.Value != int64(i)*20_000_000 ||
			!bytes.Equal(result.Packet.Payload.Bytes, payloads[i]) {
			t.Fatalf("%s result = %+v packet=%+v", streams[i].ID, result, result.Packet)
		}
	}
	if err := demuxer.ReadInto(ctx, &result); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestFormatMuxerRejectsNegativeDuration(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Index:    0,
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: timeNS},
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
	err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{1}},
		PTS:      av.Timestamp{Value: 0, Base: stream.TimeBase},
		Duration: av.Duration{Value: -1, Base: stream.TimeBase},
		Keyframe: true,
	}, nil)
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
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

func TestWriteLacedPacketAllocs(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := LacedPacket{
		TrackID:  id,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   [][]byte{{1, 2}, {3, 4}, {5, 6}},
	}
	if err := muxer.WriteLacedPacket(packet); err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := muxer.WriteLacedPacket(packet); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

func TestWriteH264LacedPacketAllocs(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecH264,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
		CodecPrivate:      h264AVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := LacedPacket{
		TrackID:  id,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   [][]byte{h264AnnexBAccessUnit(), h264AnnexBInterFrame()},
	}
	if err := muxer.WriteLacedPacket(packet); err != nil {
		t.Fatal(err)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := muxer.WriteLacedPacket(packet); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

func TestH264AVCToAnnexBInPlaceAllocs(t *testing.T) {
	tests := []struct {
		name       string
		lengthSize int
		input      []byte
	}{
		{name: "length1", lengthSize: 1, input: h264AVCSampleWithLengthSize1()},
		{name: "length2", lengthSize: 2, input: h264AVCSampleWithLengthSize2()},
	}
	want := h264AnnexBAccessUnit()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buffer := make([]byte, len(want))
			allocs := testing.AllocsPerRun(1000, func() {
				data := buffer[:len(tt.input)]
				copy(data, tt.input)
				out, err := h264AVCToAnnexBInPlace(data, len(want), tt.lengthSize)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(out, want) {
					t.Fatalf("out = %v, want %v", out, want)
				}
			})
			if allocs != 0 {
				t.Fatalf("allocs = %f, want 0", allocs)
			}
		})
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

func TestReadH264AVCToAnnexBAllocs(t *testing.T) {
	data := makeH264AVCMatroskaData(t, 1200)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(h264AnnexBAccessUnit()))}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(packet.Data, h264AnnexBAccessUnit()) {
			t.Fatalf("data = %v, want Annex B", packet.Data)
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
	for i := 0; i < packets; i++ {
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
	if err := ew.WriteUInt(flagID, value); err != nil {
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

func writeTracksWithVideoDimensions(writer *ebml.Writer, width uint64, height uint64) error {
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
	if err := vw.WriteUInt(idPixelWidth, width); err != nil {
		return err
	}
	if err := vw.WriteUInt(idPixelHeight, height); err != nil {
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

func writeTracksWithOpusPrivate(writer *ebml.Writer, private []byte) error {
	return writeTracksWithOpusPrivateAndTiming(writer, private, 0, 0)
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
	var payload bytes.Buffer
	var scratch [ebml.MaxSizeWidth]byte
	n, err := ebml.EncodeUnsignedVINT(scratch[:], trackNumber)
	if err != nil {
		return err
	}
	payload.Write(scratch[:n])
	var blockHeader [3]byte
	blockHeader[2] = simpleBlockKeyframe
	payload.Write(blockHeader[:])
	payload.Write(frame)
	return writer.WriteElement(idSimpleBlock, payload.Bytes())
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

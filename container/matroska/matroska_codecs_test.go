package matroska

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/container/ebml"
	"github.com/thesyncim/goav/format"
)

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

func TestMuxerDemuxerSupportsTextUTF8Subtitles(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackSubtitle,
		Codec:             CodecTextUTF8,
		Name:              "English captions",
		Language:          "eng",
		LanguageBCP47:     "en-US",
		FlagDefault:       false,
		FlagDefaultSet:    true,
		FlagForced:        false,
		FlagForcedSet:     true,
		DefaultDurationNS: 2_000_000_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantText := []byte("Hello from a UTF-8 subtitle.")
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     1_000_000_000,
		DurationNS: 2_000_000_000,
		Keyframe:   true,
		Data:       wantText,
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
	if tracks[0].Type != TrackSubtitle ||
		tracks[0].Codec != CodecTextUTF8 ||
		tracks[0].Name != "English captions" ||
		tracks[0].Language != "eng" ||
		tracks[0].LanguageBCP47 != "en-US" ||
		len(tracks[0].CodecPrivate) != 0 {
		t.Fatalf("track = %+v", tracks[0])
	}
	got := Packet{Data: make([]byte, 0, 64)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != trackID ||
		got.TimeNS != 1_000_000_000 ||
		got.DurationNS != 2_000_000_000 ||
		!got.Keyframe ||
		!bytes.Equal(got.Data, wantText) {
		t.Fatalf("packet = %+v data=%q", got, got.Data)
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestMuxerDemuxerPreservesMSACMG711CodecPrivate(t *testing.T) {
	tests := []struct {
		name  string
		codec Codec
		data  []byte
	}{
		{name: "pcmu", codec: CodecPCMU, data: []byte{0xff, 0x7f}},
		{name: "pcma", codec: CodecPCMA, data: []byte{0xd5, 0x2a}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(Track{
				Type:  TrackAudio,
				Codec: tt.codec,
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := muxer.WritePacket(Packet{
				TrackID:  trackID,
				TimeNS:   0,
				Keyframe: true,
				Data:     tt.data,
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
			wantPrivate := expectedMSACMWaveFormat(tt.codec, 1, 8000)
			if tracks[0].Codec != tt.codec ||
				tracks[0].Audio.SampleRate != 8000 ||
				tracks[0].Audio.Channels != 1 ||
				tracks[0].Audio.BitDepth != 8 ||
				!bytes.Equal(tracks[0].CodecPrivate, wantPrivate) {
				t.Fatalf("track = %+v private=%x, want codec %v audio 8000/1/8 private=%x", tracks[0], tracks[0].CodecPrivate, tt.codec, wantPrivate)
			}
			got := Packet{Data: make([]byte, 0, 8)}
			if err := demuxer.ReadPacket(&got); err != nil {
				t.Fatal(err)
			}
			if got.TrackID != trackID || !bytes.Equal(got.Data, tt.data) {
				t.Fatalf("packet = %+v data=%x, want track %d data=%x", got, got.Data, trackID, tt.data)
			}
		})
	}
}

func TestMuxerDemuxerPreservesVorbisCodecPrivate(t *testing.T) {
	var buffer bytes.Buffer
	private := validVorbisCodecPrivate()
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackAudio,
		Codec:        CodecVorbis,
		Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
		CodecPrivate: private,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x11, 0x22, 0x33}
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     20_000_000,
		DurationNS: 20_000_000,
		Keyframe:   true,
		Data:       want,
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
	if tracks[0].Codec != CodecVorbis ||
		tracks[0].Type != TrackAudio ||
		tracks[0].Audio.SampleRate != 48000 ||
		tracks[0].Audio.Channels != 2 ||
		!bytes.Equal(tracks[0].CodecPrivate, private) {
		t.Fatalf("track = %+v private=%x", tracks[0], tracks[0].CodecPrivate)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != trackID ||
		got.TimeNS != 20_000_000 ||
		got.DurationNS != 20_000_000 ||
		!bytes.Equal(got.Data, want) {
		t.Fatalf("packet = %+v data=%x, want track %d data=%x", got, got.Data, trackID, want)
	}
}

func TestMuxerRejectsInvalidVorbisCodecPrivate(t *testing.T) {
	tests := []struct {
		name    string
		private []byte
	}{
		{name: "missing", private: nil},
		{name: "wrong packet count", private: []byte{1, 7, 7, 1, 'v', 'o', 'r', 'b', 'i', 's', 3, 'v', 'o', 'r', 'b', 'i', 's', 5, 'v', 'o', 'r', 'b', 'i', 's'}},
		{name: "truncated xiph lacing", private: []byte{2, 255}},
		{name: "empty identification", private: []byte{2, 0, 7, 3, 'v', 'o', 'r', 'b', 'i', 's', 5, 'v', 'o', 'r', 'b', 'i', 's'}},
		{name: "wrong header order", private: vorbisCodecPrivateWithHeaders([]byte{3, 'v', 'o', 'r', 'b', 'i', 's'}, []byte{1, 'v', 'o', 'r', 'b', 'i', 's'}, []byte{5, 'v', 'o', 'r', 'b', 'i', 's'})},
		{name: "zero channels", private: vorbisCodecPrivateWithIdentificationByte(11, 0)},
		{name: "zero sample rate", private: vorbisCodecPrivateWithIdentificationBytes(12, 0, 0, 0, 0)},
		{name: "missing framing bit", private: vorbisCodecPrivateWithIdentificationByte(29, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := muxer.AddTrack(Track{
				Type:         TrackAudio,
				Codec:        CodecVorbis,
				Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
				CodecPrivate: tt.private,
			}); !errors.Is(err, ErrInvalidTrack) {
				t.Fatalf("err = %v, want ErrInvalidTrack", err)
			}
		})
	}
}

func TestMuxerDemuxerPreservesFLACCodecPrivate(t *testing.T) {
	var buffer bytes.Buffer
	private := validFLACCodecPrivate()
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackAudio,
		Codec:        CodecFLAC,
		Audio:        AudioConfig{SampleRate: 48000, Channels: 2, BitDepth: 16},
		CodecPrivate: private,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0xff, 0xf8, 0x69, 0x18}
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     20_000_000,
		DurationNS: 20_000_000,
		Keyframe:   true,
		Data:       want,
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
	if tracks[0].Codec != CodecFLAC ||
		tracks[0].Type != TrackAudio ||
		tracks[0].Audio.SampleRate != 48000 ||
		tracks[0].Audio.Channels != 2 ||
		tracks[0].Audio.BitDepth != 16 ||
		!bytes.Equal(tracks[0].CodecPrivate, private) {
		t.Fatalf("track = %+v private=%x", tracks[0], tracks[0].CodecPrivate)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != trackID ||
		got.TimeNS != 20_000_000 ||
		got.DurationNS != 20_000_000 ||
		!bytes.Equal(got.Data, want) {
		t.Fatalf("packet = %+v data=%x, want track %d data=%x", got, got.Data, trackID, want)
	}
}

func TestMuxerRejectsInvalidFLACCodecPrivate(t *testing.T) {
	tests := []struct {
		name    string
		private []byte
	}{
		{name: "missing", private: nil},
		{name: "short", private: []byte("fLaC")},
		{name: "wrong marker", private: append([]byte("bad!"), validFLACCodecPrivate()[4:]...)},
		{name: "wrong metadata type", private: flacCodecPrivateWithHeaderByte(4, 1)},
		{name: "wrong streaminfo length", private: flacCodecPrivateWithHeaderByte(7, 33)},
		{name: "zero min block size", private: flacCodecPrivateWithStreamInfoBytes(0, 0, 0)},
		{name: "reversed block sizes", private: flacCodecPrivateWithStreamInfoBytes(0, 0x20, 0x00, 0x10, 0x00)},
		{name: "zero sample rate", private: flacCodecPrivateWithStreamInfoBytes(10, 0, 0, 0, 0)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := muxer.AddTrack(Track{
				Type:         TrackAudio,
				Codec:        CodecFLAC,
				Audio:        AudioConfig{SampleRate: 48000, Channels: 2, BitDepth: 16},
				CodecPrivate: tt.private,
			}); !errors.Is(err, ErrInvalidTrack) {
				t.Fatalf("err = %v, want ErrInvalidTrack", err)
			}
		})
	}
}

func TestMuxerDemuxerPreservesAACCodecPrivate(t *testing.T) {
	var buffer bytes.Buffer
	private := makeAACAudioSpecificConfig(48000, 2)
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackAudio,
		Codec:        CodecAAC,
		Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
		CodecPrivate: private,
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x21, 0x10, 0x56, 0xe5}
	if err := muxer.WritePacket(Packet{
		TrackID:    trackID,
		TimeNS:     20_000_000,
		DurationNS: 20_000_000,
		Keyframe:   true,
		Data:       want,
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
	if tracks[0].Codec != CodecAAC ||
		tracks[0].Type != TrackAudio ||
		tracks[0].Audio.SampleRate != 48000 ||
		tracks[0].Audio.Channels != 2 ||
		!bytes.Equal(tracks[0].CodecPrivate, private) {
		t.Fatalf("track = %+v private=%x", tracks[0], tracks[0].CodecPrivate)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != trackID ||
		got.TimeNS != 20_000_000 ||
		got.DurationNS != 20_000_000 ||
		!bytes.Equal(got.Data, want) {
		t.Fatalf("packet = %+v data=%x, want track %d data=%x", got, got.Data, trackID, want)
	}
}

func TestMuxerRejectsInvalidAACCodecPrivate(t *testing.T) {
	tests := []struct {
		name    string
		private []byte
	}{
		{name: "missing", private: nil},
		{name: "short", private: []byte{0x11}},
		{name: "zero object type", private: []byte{0x01, 0x90}},
		{name: "reserved sample rate", private: []byte{0x17, 0x90}},
		{name: "zero explicit sample rate", private: []byte{0x17, 0x80, 0x00, 0x00, 0x00}},
		{name: "program config channels", private: []byte{0x11, 0x80}},
		{name: "reserved channel config", private: []byte{0x11, 0xf8}},
		{name: "sample rate mismatch", private: makeAACAudioSpecificConfig(44100, 2)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := muxer.AddTrack(Track{
				Type:         TrackAudio,
				Codec:        CodecAAC,
				Audio:        AudioConfig{SampleRate: 48000, Channels: 2},
				CodecPrivate: tt.private,
			}); !errors.Is(err, ErrInvalidTrack) {
				t.Fatalf("err = %v, want ErrInvalidTrack", err)
			}
		})
	}
}

func TestMuxerDemuxerPreservesCodecName(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:      TrackVideo,
		Codec:     CodecVP8,
		Name:      "camera-main",
		CodecName: "libvpx VP8",
		Video:     VideoConfig{Width: 640, Height: 360},
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
	if tracks[0].Name != "camera-main" || tracks[0].CodecName != "libvpx VP8" {
		t.Fatalf("track name=%q codec name=%q", tracks[0].Name, tracks[0].CodecName)
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

func TestMSACMWaveFormatValidation(t *testing.T) {
	format, err := parseMSACMWaveFormat(expectedMSACMWaveFormat(CodecPCMU, 2, 16000))
	if err != nil {
		t.Fatal(err)
	}
	if format.FormatTag != waveFormatMuLaw ||
		format.Channels != 2 ||
		format.SampleRate != 16000 ||
		format.AvgBytesPerSec != 32000 ||
		format.BlockAlign != 2 ||
		format.BitsPerSample != 8 ||
		format.ExtraSize != 0 {
		t.Fatalf("format = %+v", format)
	}

	tests := []struct {
		name    string
		private []byte
	}{
		{name: "short", private: expectedMSACMWaveFormat(CodecPCMU, 1, 8000)[:17]},
		{name: "trailing bytes without cbSize", private: append(expectedMSACMWaveFormat(CodecPCMU, 1, 8000), 0)},
		{name: "zero channels", private: msACMWaveFormatBytes(waveFormatMuLaw, 0, 8000, 8000, 1, 8, nil)},
		{name: "zero sample rate", private: msACMWaveFormatBytes(waveFormatMuLaw, 1, 0, 8000, 1, 8, nil)},
		{name: "zero average bytes", private: msACMWaveFormatBytes(waveFormatMuLaw, 1, 8000, 0, 1, 8, nil)},
		{name: "zero block align", private: msACMWaveFormatBytes(waveFormatMuLaw, 1, 8000, 8000, 0, 8, nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseMSACMWaveFormat(tt.private); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidMSACMG711CodecPrivate(t *testing.T) {
	avgMismatch := expectedMSACMWaveFormat(CodecPCMU, 1, 8000)
	binary.LittleEndian.PutUint32(avgMismatch[8:12], 1)
	blockAlignMismatch := expectedMSACMWaveFormat(CodecPCMU, 1, 8000)
	binary.LittleEndian.PutUint16(blockAlignMismatch[12:14], 2)

	tests := []struct {
		name  string
		track Track
	}{
		{
			name: "short",
			track: Track{
				Type:         TrackAudio,
				Codec:        CodecPCMU,
				Audio:        AudioConfig{SampleRate: 8000, Channels: 1},
				CodecPrivate: []byte{0x07, 0x00},
			},
		},
		{
			name: "wrong tag",
			track: Track{
				Type:         TrackAudio,
				Codec:        CodecPCMU,
				Audio:        AudioConfig{SampleRate: 8000, Channels: 1},
				CodecPrivate: expectedMSACMWaveFormat(CodecPCMA, 1, 8000),
			},
		},
		{
			name: "average bytes mismatch",
			track: Track{
				Type:         TrackAudio,
				Codec:        CodecPCMU,
				Audio:        AudioConfig{SampleRate: 8000, Channels: 1},
				CodecPrivate: avgMismatch,
			},
		},
		{
			name: "block align mismatch",
			track: Track{
				Type:         TrackAudio,
				Codec:        CodecPCMU,
				Audio:        AudioConfig{SampleRate: 8000, Channels: 1},
				CodecPrivate: blockAlignMismatch,
			},
		},
		{
			name: "audio sample rate mismatch",
			track: Track{
				Type:         TrackAudio,
				Codec:        CodecPCMU,
				Audio:        AudioConfig{SampleRate: 16000, Channels: 1},
				CodecPrivate: expectedMSACMWaveFormat(CodecPCMU, 1, 8000),
			},
		},
		{
			name: "default channel overflow",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecPCMU,
				Audio: AudioConfig{SampleRate: 8000, Channels: int(uint64(^uint16(0)) + 1)},
			},
		},
		{
			name: "default bit depth mismatch",
			track: Track{
				Type:  TrackAudio,
				Codec: CodecPCMU,
				Audio: AudioConfig{SampleRate: 8000, Channels: 1, BitDepth: 16},
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

func TestDemuxerRejectsInvalidMSACMG711CodecPrivate(t *testing.T) {
	extraMismatch := expectedMSACMWaveFormat(CodecPCMU, 1, 8000)
	extraMismatch = append(extraMismatch, 0)
	avgMismatch := expectedMSACMWaveFormat(CodecPCMU, 1, 8000)
	binary.LittleEndian.PutUint32(avgMismatch[8:12], 1)
	blockAlignMismatch := expectedMSACMWaveFormat(CodecPCMU, 1, 8000)
	binary.LittleEndian.PutUint16(blockAlignMismatch[12:14], 2)
	bitDepthMismatch := expectedMSACMWaveFormat(CodecPCMU, 1, 8000)
	binary.LittleEndian.PutUint16(bitDepthMismatch[14:16], 16)

	tests := []struct {
		name    string
		private []byte
	}{
		{name: "short", private: []byte{0x07, 0x00}},
		{name: "extra size mismatch", private: extraMismatch},
		{name: "zero sample rate", private: msACMWaveFormatBytes(waveFormatMuLaw, 1, 0, 8000, 1, 8, nil)},
		{name: "average bytes mismatch", private: avgMismatch},
		{name: "block align mismatch", private: blockAlignMismatch},
		{name: "bit depth mismatch", private: bitDepthMismatch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
				return writeTracksWithMSACMPrivate(writer, tt.private, AudioConfig{SampleRate: 8000, Channels: 1, BitDepth: 8})
			})
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestDemuxerReadsUnsupportedMSACMAsUnknown(t *testing.T) {
	private := msACMWaveFormatBytes(0x0055, 2, 44100, 16000, 1, 0, nil)
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	writeMatroskaSegmentPrefix(t, muxer)
	if err := writeTracksWithMSACMPrivate(muxer.ebml, private, AudioConfig{SampleRate: 44100, Channels: 2}); err != nil {
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
	if tracks[0].Codec != CodecUnknown ||
		tracks[0].Audio.SampleRate != 44100 ||
		tracks[0].Audio.Channels != 2 ||
		tracks[0].Audio.BitDepth != 0 ||
		!bytes.Equal(tracks[0].CodecPrivate, private) {
		t.Fatalf("track = %+v private=%x", tracks[0], tracks[0].CodecPrivate)
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
	if _, err := parseAV1CodecConfigurationRecord(av1CodecConfig()); err != nil {
		t.Fatal(err)
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

func TestMuxerDefersHeaderWhenAV1CodecPrivateIsMissing(t *testing.T) {
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
	if err != nil {
		t.Fatal(err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before AV1 private data was available", buffer.Len())
	}
	err = muxer.Close()
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing AV1 private data", buffer.Len())
	}
}

func TestMuxerDefersHeaderUntilGeneratedCodecPrivateTracksReady(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	h264ID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH264,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	av1ID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecAV1,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	opusID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}

	h264Data := h264AnnexBParameterAccessUnit()
	if err := muxer.WritePacket(Packet{TrackID: h264ID, TimeNS: 0, Keyframe: true, Data: h264Data}); err != nil {
		t.Fatal(err)
	}
	h264Data[0] = 0xff
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before all generated private data was available", buffer.Len())
	}
	if _, err := muxer.AddTrack(Track{Type: TrackAudio, Codec: CodecOpus, Audio: AudioConfig{SampleRate: 48000, Channels: 1}}); !errors.Is(err, ErrTrackAfterWrite) {
		t.Fatalf("err = %v, want ErrTrackAfterWrite", err)
	}

	opusData := []byte{0xf8, 0xff, 0xfe}
	if err := muxer.WritePacket(Packet{TrackID: opusID, TimeNS: 20_000_000, Keyframe: true, Data: opusData}); err != nil {
		t.Fatal(err)
	}
	opusData[0] = 0
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before all generated private data was available", buffer.Len())
	}

	av1Data := av1SequenceHeaderOBU()
	if err := muxer.WritePacket(Packet{TrackID: av1ID, TimeNS: 40_000_000, Keyframe: true, Data: av1Data}); err != nil {
		t.Fatal(err)
	}
	av1Data[0] = 0
	if buffer.Len() == 0 {
		t.Fatalf("header was not written after generated private data became available")
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 3 {
		t.Fatalf("tracks = %d, want 3", len(tracks))
	}
	if !bytes.Equal(tracks[0].CodecPrivate, h264AVCDecoderConfig()) {
		t.Fatalf("h264 private = %x, want %x", tracks[0].CodecPrivate, h264AVCDecoderConfig())
	}
	if !bytes.Equal(tracks[1].CodecPrivate, av1CodecConfig()) {
		t.Fatalf("av1 private = %x, want %x", tracks[1].CodecPrivate, av1CodecConfig())
	}

	wantPackets := []Packet{
		{TrackID: h264ID, TimeNS: 0, Keyframe: true, Data: h264AnnexBParameterAccessUnit()},
		{TrackID: opusID, TimeNS: 20_000_000, Keyframe: true, Data: []byte{0xf8, 0xff, 0xfe}},
		{TrackID: av1ID, TimeNS: 40_000_000, Keyframe: true, Data: av1SequenceHeaderOBU()},
	}
	got := Packet{Data: make([]byte, 0, len(h264AnnexBParameterAccessUnit()))}
	for i := range wantPackets {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatalf("packet %d read: %v", i, err)
		}
		if got.TrackID != wantPackets[i].TrackID || got.TimeNS != wantPackets[i].TimeNS ||
			got.Keyframe != wantPackets[i].Keyframe || !bytes.Equal(got.Data, wantPackets[i].Data) {
			t.Fatalf("packet %d = %+v data=%x, want %+v data=%x", i, got, got.Data, wantPackets[i], wantPackets[i].Data)
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestMuxerDefersLacedHeaderPacketUntilGeneratedCodecPrivateTracksReady(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	av1ID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecAV1,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	h264ID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH264,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}

	frames := [][]byte{av1SequenceHeaderOBU(), av1TemporalDelimiterOBU()}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  av1ID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   frames,
	}); err != nil {
		t.Fatal(err)
	}
	frames[0][0] = 0
	frames[1][0] = 0
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before all generated private data was available", buffer.Len())
	}
	if err := muxer.WritePacket(Packet{TrackID: h264ID, TimeNS: 40_000_000, Keyframe: true, Data: h264AnnexBParameterAccessUnit()}); err != nil {
		t.Fatal(err)
	}
	if buffer.Len() == 0 {
		t.Fatalf("header was not written after generated private data became available")
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 2 {
		t.Fatalf("tracks = %d, want 2", len(tracks))
	}
	if !bytes.Equal(tracks[0].CodecPrivate, av1CodecConfig()) {
		t.Fatalf("av1 private = %x, want %x", tracks[0].CodecPrivate, av1CodecConfig())
	}
	if !bytes.Equal(tracks[1].CodecPrivate, h264AVCDecoderConfig()) {
		t.Fatalf("h264 private = %x, want %x", tracks[1].CodecPrivate, h264AVCDecoderConfig())
	}

	wantPackets := []Packet{
		{TrackID: av1ID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: av1SequenceHeaderOBU()},
		{TrackID: av1ID, TimeNS: 20_000_000, DurationNS: 20_000_000, Keyframe: true, Data: av1TemporalDelimiterOBU()},
		{TrackID: h264ID, TimeNS: 40_000_000, Keyframe: true, Data: h264AnnexBParameterAccessUnit()},
	}
	got := Packet{Data: make([]byte, 0, len(h264AnnexBParameterAccessUnit()))}
	for i := range wantPackets {
		if err := demuxer.ReadPacket(&got); err != nil {
			t.Fatalf("packet %d read: %v", i, err)
		}
		if got.TrackID != wantPackets[i].TrackID || got.TimeNS != wantPackets[i].TimeNS ||
			got.DurationNS != wantPackets[i].DurationNS || got.Keyframe != wantPackets[i].Keyframe ||
			!bytes.Equal(got.Data, wantPackets[i].Data) {
			t.Fatalf("packet %d = %+v data=%x, want %+v data=%x", i, got, got.Data, wantPackets[i], wantPackets[i].Data)
		}
	}
	if err := demuxer.ReadPacket(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
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

func TestMuxerDefersHeaderWhenH264CodecPrivateIsMissing(t *testing.T) {
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
	if err != nil {
		t.Fatal(err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before H.264 private data was available", buffer.Len())
	}
	err = muxer.Close()
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

func TestHEVCDecoderConfigurationRecordValidation(t *testing.T) {
	config, err := parseHEVCDecoderConfigurationRecord(h265HEVCDecoderConfig())
	if err != nil {
		t.Fatal(err)
	}
	if config.GeneralProfileSpace != 0 || config.GeneralTierFlag ||
		config.GeneralProfileIDC != 1 || config.GeneralProfileCompatibilityFlags != 0x60000000 ||
		config.GeneralConstraintIndicatorFlags != 0 || config.GeneralLevelIDC != 120 ||
		config.ChromaFormat != 1 || config.NALULengthSize != 4 ||
		config.VPSCount != 1 || config.SPSCount != 1 || config.PPSCount != 1 ||
		config.NumTemporalLayers != 1 || !config.TemporalIDNested {
		t.Fatalf("config = %+v", config)
	}

	tests := []struct {
		name    string
		private []byte
	}{
		{name: "short", private: []byte{0x01, 0x01}},
		{name: "bad version", private: h265HEVCDecoderConfigWithByte(0, 0x00)},
		{name: "bad min spatial reserved", private: h265HEVCDecoderConfigWithByte(13, 0x0f)},
		{name: "bad parallelism reserved", private: h265HEVCDecoderConfigWithByte(15, 0x03)},
		{name: "bad chroma reserved", private: h265HEVCDecoderConfigWithByte(16, 0x03)},
		{name: "bad luma reserved", private: h265HEVCDecoderConfigWithByte(17, 0x07)},
		{name: "bad chroma depth reserved", private: h265HEVCDecoderConfigWithByte(18, 0x07)},
		{name: "invalid length size", private: h265HEVCDecoderConfigWithLengthSize(3)},
		{name: "no arrays", private: h265HEVCDecoderConfigWithByte(22, 0)},
		{name: "bad array reserved", private: h265HEVCDecoderConfigWithByte(23, 0xe0)},
		{name: "wrong nalu type", private: h265HEVCDecoderConfigWithByte(28, 0x42)},
		{name: "missing pps", private: h265HEVCDecoderConfig()[:len(h265HEVCDecoderConfig())-len(h265PPS())-2]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := parseHEVCDecoderConfigurationRecord(tt.private); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsInvalidH265CodecPrivate(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH265,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: []byte{0x01, 0x01},
	})
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
}

func TestDemuxerRejectsInvalidH265CodecPrivate(t *testing.T) {
	data := makeTrackMetadataMatroskaData(t, func(writer *ebml.Writer) error {
		return writeTracksWithH265Private(writer, []byte{0x01, 0x01})
	})
	if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestH265HEVCDecoderConfigurationRecordFromAnnexB(t *testing.T) {
	private, err := h265HEVCDecoderConfigurationRecordFromAnnexBFrames([][]byte{h265AnnexBParameterAccessUnit()})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(private, h265HEVCDecoderConfig()) {
		t.Fatalf("private = %v, want %v", private, h265HEVCDecoderConfig())
	}
	if _, err := parseHEVCDecoderConfigurationRecord(private); err != nil {
		t.Fatal(err)
	}

	_, err = h265HEVCDecoderConfigurationRecordFromAnnexBFrames([][]byte{h265AnnexBAccessUnit()})
	if !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestMuxerGeneratesH265CodecPrivateFromFirstAnnexBPacket(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH265,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	annexB := h265AnnexBParameterAccessUnit()
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: annexB}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	hvcc := h265LengthPrefixedParameterSample4()
	if !bytes.Contains(buffer.Bytes(), h265HEVCDecoderConfig()) {
		t.Fatalf("muxed data does not contain generated HEVC config")
	}
	if !bytes.Contains(buffer.Bytes(), hvcc) {
		t.Fatalf("muxed data does not contain HEVC sample %v", hvcc)
	}
	if bytes.Contains(buffer.Bytes(), annexB) {
		t.Fatalf("muxed data still contains Annex B access unit")
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracks := demuxer.Tracks()
	if len(tracks) != 1 || !bytes.Equal(tracks[0].CodecPrivate, h265HEVCDecoderConfig()) {
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

func TestMuxerGeneratesH265CodecPrivateFromFirstLacedAnnexBPacket(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecH265,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{h265AnnexBParameterAccessUnit(), h265AnnexBInterFrame()}
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
	if len(tracks) != 1 || !bytes.Equal(tracks[0].CodecPrivate, h265HEVCDecoderConfig()) {
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

func TestMuxerDefersHeaderWhenH265CodecPrivateIsMissing(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH265,
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
	if err != nil {
		t.Fatal(err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before H.265 private data was available", buffer.Len())
	}
	err = muxer.Close()
	if !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing H.265 private data", buffer.Len())
	}
}

func TestMuxerCloseRejectsMissingH265CodecPrivate(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecH265,
		Video: VideoConfig{Width: 16, Height: 16},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); !errors.Is(err, ErrInvalidTrack) {
		t.Fatalf("err = %v, want ErrInvalidTrack", err)
	}
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before rejecting missing H.265 private data", buffer.Len())
	}
}

func TestMuxerWritesH265AnnexBPacketsAsHEVCSamples(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH265,
		Video:        VideoConfig{Width: 16, Height: 16},
		CodecPrivate: h265HEVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	annexB := h265AnnexBAccessUnit()
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: annexB}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}

	hvcc := h265LengthPrefixedSample2()
	if !bytes.Contains(buffer.Bytes(), hvcc) {
		t.Fatalf("muxed data does not contain HEVC sample %v", hvcc)
	}
	if bytes.Contains(buffer.Bytes(), annexB) {
		t.Fatalf("muxed data still contains Annex B access unit")
	}

	small, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(hvcc))}
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

func TestMuxerWritesLacedH265AnnexBFramesAsHEVCSamples(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecH265,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
		CodecPrivate:      h265HEVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	frames := [][]byte{h265AnnexBAccessUnit(), h265AnnexBInterFrame()}
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

	hvcc := append(h265LengthPrefixedSample2(), h265LengthPrefixedInterFrame2()...)
	if !bytes.Contains(buffer.Bytes(), hvcc) {
		t.Fatalf("muxed data does not contain laced HEVC samples %v", hvcc)
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
	packet := Packet{Data: make([]byte, 0, 32)}
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

func TestFormatMuxerDemuxerSupportsVorbis(t *testing.T) {
	ctx := context.Background()
	private := validVorbisCodecPrivate()
	stream := av.Stream{
		ID:       "audio",
		Index:    0,
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecVorbis,
			Type:       av.MediaAudio,
			SampleRate: 48000,
			Channels:   2,
			ExtraData:  av.Buffer{Bytes: private},
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{0xaa, 0xbb}},
		PTS:      av.Timestamp{Value: 960, Base: stream.TimeBase},
		Duration: av.Duration{Value: 960, Base: stream.TimeBase},
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
	if len(streams) != 1 ||
		streams[0].Codec.ID != av.CodecVorbis ||
		streams[0].Codec.SampleRate != 48000 ||
		streams[0].Codec.Channels != 2 ||
		!bytes.Equal(streams[0].Codec.ExtraData.Bytes, private) {
		t.Fatalf("streams = %+v", streams)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 8)}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady ||
		result.Packet.StreamID != "1" ||
		result.Packet.PTS.Value != 20_000_000 ||
		result.Packet.Duration.Value != 20_000_000 ||
		!bytes.Equal(result.Packet.Payload.Bytes, []byte{0xaa, 0xbb}) {
		t.Fatalf("result = %+v packet=%+v", result, result.Packet)
	}
}

func TestFormatMuxerDemuxerSupportsFLAC(t *testing.T) {
	ctx := context.Background()
	private := validFLACCodecPrivate()
	stream := av.Stream{
		ID:       "audio",
		Index:    0,
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecFLAC,
			Type:       av.MediaAudio,
			SampleRate: 48000,
			Channels:   2,
			ExtraData:  av.Buffer{Bytes: private},
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{0xff, 0xf8, 0x69, 0x18}},
		PTS:      av.Timestamp{Value: 960, Base: stream.TimeBase},
		Duration: av.Duration{Value: 960, Base: stream.TimeBase},
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
	if len(streams) != 1 ||
		streams[0].Codec.ID != av.CodecFLAC ||
		streams[0].Codec.SampleRate != 48000 ||
		streams[0].Codec.Channels != 2 ||
		!bytes.Equal(streams[0].Codec.ExtraData.Bytes, private) {
		t.Fatalf("streams = %+v", streams)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 8)}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady ||
		result.Packet.StreamID != "1" ||
		result.Packet.PTS.Value != 20_000_000 ||
		result.Packet.Duration.Value != 20_000_000 ||
		!bytes.Equal(result.Packet.Payload.Bytes, []byte{0xff, 0xf8, 0x69, 0x18}) {
		t.Fatalf("result = %+v packet=%+v", result, result.Packet)
	}
}

func TestFormatMuxerDemuxerSupportsAAC(t *testing.T) {
	ctx := context.Background()
	private := makeAACAudioSpecificConfig(48000, 2)
	stream := av.Stream{
		ID:       "audio",
		Index:    0,
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecAAC,
			Type:       av.MediaAudio,
			SampleRate: 48000,
			Channels:   2,
			ExtraData:  av.Buffer{Bytes: private},
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{0x21, 0x10, 0x56, 0xe5}},
		PTS:      av.Timestamp{Value: 960, Base: stream.TimeBase},
		Duration: av.Duration{Value: 960, Base: stream.TimeBase},
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
	if len(streams) != 1 ||
		streams[0].Codec.ID != av.CodecAAC ||
		streams[0].Codec.SampleRate != 48000 ||
		streams[0].Codec.Channels != 2 ||
		!bytes.Equal(streams[0].Codec.ExtraData.Bytes, private) {
		t.Fatalf("streams = %+v", streams)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 8)}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady ||
		result.Packet.StreamID != "1" ||
		result.Packet.PTS.Value != 20_000_000 ||
		result.Packet.Duration.Value != 20_000_000 ||
		!bytes.Equal(result.Packet.Payload.Bytes, []byte{0x21, 0x10, 0x56, 0xe5}) {
		t.Fatalf("result = %+v packet=%+v", result, result.Packet)
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

func TestWriteH265LacedPacketAllocs(t *testing.T) {
	muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	id, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecH265,
		DefaultDurationNS: 20_000_000,
		Video:             VideoConfig{Width: 16, Height: 16},
		CodecPrivate:      h265HEVCDecoderConfigWithLengthSize(2),
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := LacedPacket{
		TrackID:  id,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingEBML,
		Frames:   [][]byte{h265AnnexBAccessUnit(), h265AnnexBInterFrame()},
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

func TestH265LengthPrefixedToAnnexBInPlaceAllocs(t *testing.T) {
	tests := []struct {
		name       string
		lengthSize int
		input      []byte
	}{
		{name: "length2", lengthSize: 2, input: h265LengthPrefixedSample2()},
	}
	want := h265AnnexBAccessUnit()
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

func TestReadH265HEVCToAnnexBAllocs(t *testing.T) {
	data := makeH265HEVCMatroskaData(t, 1200)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(h265AnnexBAccessUnit()))}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(packet.Data, h265AnnexBAccessUnit()) {
			t.Fatalf("data = %v, want Annex B", packet.Data)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

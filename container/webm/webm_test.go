package webm

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"
	"time"

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

func TestMuxerRejectsInvalidTrackMetadata(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{SampleRate: -1, Channels: 2},
	}); !errors.Is(err, matroska.ErrInvalidTrack) {
		t.Fatalf("err = %v, want matroska.ErrInvalidTrack", err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{
			Width:      16,
			Height:     16,
			Projection: VideoProjectionConfig{Set: true, PoseYaw: 180.1},
		},
	}); !errors.Is(err, matroska.ErrInvalidTrack) {
		t.Fatalf("err = %v, want matroska.ErrInvalidTrack", err)
	}
}

func TestMuxerRejectsInvalidSegmentInfoMetadata(t *testing.T) {
	if _, err := NewMuxer(io.Discard, MuxerOptions{
		Info: SegmentInfo{SegmentUUID: []byte{1, 2, 3}},
	}); !errors.Is(err, matroska.ErrInvalidData) {
		t.Fatalf("err = %v, want matroska.ErrInvalidData", err)
	}
}

func TestMuxerDemuxerPreservesSegmentInfoMetadata(t *testing.T) {
	created := time.Date(2026, 6, 7, 12, 34, 56, 789, time.FixedZone("test", 3600))
	wantInfo := SegmentInfo{
		SegmentUUID:     []byte{0x10, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f},
		SegmentFilename: "camera-a.webm",
		Title:           "camera webm",
		DateUTC:         created.UTC(),
		DateUTCSet:      true,
		MuxingApp:       "goav-webm-test-mux",
		WritingApp:      "goav-webm-test-write",
	}
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{
		MuxingApp:  wantInfo.MuxingApp,
		WritingApp: wantInfo.WritingApp,
		Info: SegmentInfo{
			SegmentUUID:     append([]byte(nil), wantInfo.SegmentUUID...),
			SegmentFilename: wantInfo.SegmentFilename,
			Title:           wantInfo.Title,
			DateUTC:         created,
			DateUTCSet:      true,
		},
	})
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
	if got := demuxer.Info(); !reflect.DeepEqual(got, wantInfo) {
		t.Fatalf("info = %+v, want %+v", got, wantInfo)
	}
}

func TestMuxerDemuxerPreservesAudioOutputSampleRate(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackAudio,
		Codec: CodecOpus,
		Audio: AudioConfig{
			SampleRate:       44100,
			OutputSampleRate: 48000,
			Channels:         2,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0x01, 0x02},
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
	if tracks[0].Audio.SampleRate != 44100 || tracks[0].Audio.OutputSampleRate != 48000 {
		t.Fatalf("audio = %+v", tracks[0].Audio)
	}
}

func TestMuxerDemuxerPreservesVideoDisplayMetadata(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantVideo := VideoConfig{
		Width:           1920,
		Height:          1080,
		PixelCropBottom: 2,
		PixelCropTop:    4,
		PixelCropLeft:   6,
		PixelCropRight:  8,
		DisplayWidth:    16,
		DisplayHeight:   9,
		DisplayUnit:     3,
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: wantVideo,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0x9d, 0x01, 0x2a},
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
	if !reflect.DeepEqual(tracks[0].Video, wantVideo) {
		t.Fatalf("video = %+v, want %+v", tracks[0].Video, wantVideo)
	}
}

func TestMuxerDemuxerPreservesVideoModeMetadata(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantVideo := VideoConfig{
		Width:         640,
		Height:        360,
		StereoMode:    11,
		StereoModeSet: true,
		AlphaMode:     0,
		AlphaModeSet:  true,
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: wantVideo,
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
	if !reflect.DeepEqual(tracks[0].Video, wantVideo) {
		t.Fatalf("video = %+v, want %+v", tracks[0].Video, wantVideo)
	}
}

func TestMuxerDemuxerPreservesVideoColourMetadata(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantVideo := VideoConfig{
		Width:  640,
		Height: 360,
		Colour: VideoColourConfig{
			MatrixCoefficients:         9,
			MatrixCoefficientsSet:      true,
			BitsPerChannel:             0,
			BitsPerChannelSet:          true,
			ChromaSubsamplingHorz:      1,
			ChromaSubsamplingHorzSet:   true,
			ChromaSubsamplingVert:      1,
			ChromaSubsamplingVertSet:   true,
			CbSubsamplingHorz:          1,
			CbSubsamplingHorzSet:       true,
			CbSubsamplingVert:          1,
			CbSubsamplingVertSet:       true,
			ChromaSitingHorz:           0,
			ChromaSitingHorzSet:        true,
			ChromaSitingVert:           0,
			ChromaSitingVertSet:        true,
			Range:                      1,
			RangeSet:                   true,
			TransferCharacteristics:    16,
			TransferCharacteristicsSet: true,
			Primaries:                  9,
			PrimariesSet:               true,
			MaxCLL:                     1000,
			MaxCLLSet:                  true,
			MaxFALL:                    400,
			MaxFALLSet:                 true,
			MasteringMetadata: VideoMasteringMetadataConfig{
				PrimaryRChromaticityX:      0.708,
				PrimaryRChromaticityXSet:   true,
				PrimaryRChromaticityY:      0.292,
				PrimaryRChromaticityYSet:   true,
				PrimaryGChromaticityX:      0.17,
				PrimaryGChromaticityXSet:   true,
				PrimaryGChromaticityY:      0.797,
				PrimaryGChromaticityYSet:   true,
				PrimaryBChromaticityX:      0.131,
				PrimaryBChromaticityXSet:   true,
				PrimaryBChromaticityY:      0.046,
				PrimaryBChromaticityYSet:   true,
				WhitePointChromaticityX:    0.3127,
				WhitePointChromaticityXSet: true,
				WhitePointChromaticityY:    0.329,
				WhitePointChromaticityYSet: true,
				LuminanceMax:               1000,
				LuminanceMaxSet:            true,
				LuminanceMin:               0.005,
				LuminanceMinSet:            true,
			},
		},
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: wantVideo,
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
	if !reflect.DeepEqual(tracks[0].Video, wantVideo) {
		t.Fatalf("video = %+v, want %+v", tracks[0].Video, wantVideo)
	}
}

func TestMuxerDemuxerPreservesVideoProjectionMetadata(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantVideo := VideoConfig{
		Width:  640,
		Height: 360,
		Projection: VideoProjectionConfig{
			Set:       true,
			Type:      1,
			Private:   []byte{0, 0, 0, 0},
			PoseYaw:   45,
			PosePitch: -10.5,
			PoseRoll:  180,
		},
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: wantVideo,
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
	if !reflect.DeepEqual(tracks[0].Video, wantVideo) {
		t.Fatalf("video = %+v, want %+v", tracks[0].Video, wantVideo)
	}
}

func TestMuxerDemuxerPreservesTrackSelectionFlags(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:                    TrackAudio,
		Codec:                   CodecOpus,
		FlagHearingImpaired:     true,
		FlagHearingImpairedSet:  true,
		FlagVisualImpaired:      false,
		FlagVisualImpairedSet:   true,
		FlagTextDescriptions:    true,
		FlagTextDescriptionsSet: true,
		FlagOriginal:            true,
		FlagOriginalSet:         true,
		FlagCommentary:          false,
		FlagCommentarySet:       true,
		Audio:                   AudioConfig{SampleRate: 48000, Channels: 2},
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
	track := tracks[0]
	if !track.FlagHearingImpaired || !track.FlagHearingImpairedSet ||
		track.FlagVisualImpaired || !track.FlagVisualImpairedSet ||
		!track.FlagTextDescriptions || !track.FlagTextDescriptionsSet ||
		!track.FlagOriginal || !track.FlagOriginalSet ||
		track.FlagCommentary || !track.FlagCommentarySet {
		t.Fatalf("track = %+v", track)
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

func TestMuxerDemuxerPreservesDecodedFieldDuration(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:                          TrackVideo,
		Codec:                         CodecVP8,
		DefaultDurationNS:             20_000_000,
		DefaultDecodedFieldDurationNS: 10_000_000,
		Video:                         VideoConfig{Width: 640, Height: 360},
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
	if tracks[0].DefaultDurationNS != 20_000_000 || tracks[0].DefaultDecodedFieldDurationNS != 10_000_000 {
		t.Fatalf("track = %+v", tracks[0])
	}
}

func TestDemuxerRejectsMatroskaDocType(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := matroska.NewMuxer(&buffer, matroska.MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(matroska.Track{
		Type:  matroska.TrackVideo,
		Codec: matroska.CodecVP8,
		Video: matroska.VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(matroska.Packet{
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
	if _, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{}); !errors.Is(err, ErrUnsupportedWebMDocType) {
		t.Fatalf("err = %v, want ErrUnsupportedWebMDocType", err)
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
		{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: webmAV1SequenceHeaderOBU()},
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

func TestMuxerDemuxerSupportsWebMCodecs(t *testing.T) {
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
			name: "vp8",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 640, Height: 360},
			},
			data: []byte{0x9d, 0x01, 0x2a},
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
			name: "av1",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecAV1,
				Video:        VideoConfig{Width: 640, Height: 360},
				CodecPrivate: webmAV1CodecConfig(),
			},
			data: []byte{0x12, 0x00, 0x0a},
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
		if tracks[i].Codec != tests[i].track.Codec || tracks[i].Type != tests[i].track.Type {
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

func webmAV1CodecConfig() []byte {
	private := []byte{0x81, 0x05, 0x10, 0x00}
	return append(private, webmAV1SequenceHeaderOBU()...)
}

func webmAV1SequenceHeaderOBU() []byte {
	return []byte{0x0a, 0x06, 0x19, 0x5d, 0xc3, 0xc3, 0xda, 0x44}
}

func TestMuxerWritesLacedPackets(t *testing.T) {
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
	frames := [][]byte{{1}, {2, 3}, {4, 5, 6}}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   40_000_000,
		Keyframe: true,
		Lacing:   LacingXiph,
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
	packet := Packet{Data: make([]byte, 0, 8)}
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if packet.TrackID != trackID || packet.TimeNS != 40_000_000+int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || !packet.Keyframe ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%v want data=%v", i, packet, packet.Data, frames[i])
		}
	}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, io.EOF) {
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

func TestDemuxerReadPacketAtTime(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "seek-exact-*.webm")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	muxer, err := NewMuxer(file, MuxerOptions{ClusterMaxDurationNS: 1_000_000})
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
		{TrackID: trackID, TimeNS: 20_000_000, Keyframe: true, Data: []byte{2}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatal(err)
		}
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	demuxer, err := NewDemuxer(file, DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacketAtTime(10_000_000, &packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != packets[1].TimeNS || !bytes.Equal(packet.Data, packets[1].Data) {
		t.Fatalf("packet at time = %+v data=%v, want %+v data=%v", packet, packet.Data, packets[1], packets[1].Data)
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
	if len(streams) != 1 || streams[0].Codec.ID != av.CodecOpus || streams[0].Codec.SampleRate != 48000 {
		t.Fatalf("streams = %+v", streams)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, 8)}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !result.PacketReady || result.Packet.StreamID != "1" || result.Packet.PTS.Value != 20_000_000 ||
		result.Packet.Duration.Value != 20_000_000 ||
		!bytes.Equal(result.Packet.Payload.Bytes, []byte{9, 8, 7}) {
		t.Fatalf("result = %+v packet=%+v", result, result.Packet)
	}
}

func TestFormatDemuxerStreamsReturnsExtraDataCopies(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Index:    0,
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 1000},
		Codec: av.CodecParameters{
			ID:        av.CodecAV1,
			Type:      av.MediaVideo,
			Width:     16,
			Height:    16,
			ExtraData: av.Buffer{Bytes: webmAV1CodecConfig()},
		},
	}
	var buffer bytes.Buffer
	muxer := &FormatMuxer{}
	if err := muxer.Open(ctx, format.Output{Writer: &buffer}, []av.Stream{stream}, format.OpenOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: webmAV1SequenceHeaderOBU()},
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
	streams[0].Codec.ExtraData.Bytes[0] = 0

	fresh := demuxer.Streams()
	if len(fresh) != 1 || !bytes.Equal(fresh[0].Codec.ExtraData.Bytes, webmAV1CodecConfig()) {
		t.Fatalf("fresh streams = %+v", fresh)
	}
	result := format.ReadResult{Packet: &av.Packet{Payload: av.Buffer{Bytes: make([]byte, 0, len(webmAV1SequenceHeaderOBU()))}}}
	if err := demuxer.ReadInto(ctx, &result); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.Packet.Payload.Bytes, webmAV1SequenceHeaderOBU()) {
		t.Fatalf("payload = %v, want AV1 sequence header", result.Packet.Payload.Bytes)
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
		LanguageBCP47: "en-GB",
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
	if streams[0].Language != "en-GB" {
		t.Fatalf("stream language = %q, want en-GB", streams[0].Language)
	}
}

func TestFormatMuxerRejectsNegativeDuration(t *testing.T) {
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
	err := muxer.Write(ctx, &av.Packet{
		StreamID: stream.ID,
		Payload:  av.Buffer{Bytes: []byte{1}},
		PTS:      av.Timestamp{Value: 0, Base: stream.TimeBase},
		Duration: av.Duration{Value: -1, Base: stream.TimeBase},
		Keyframe: true,
	}, nil)
	if !errors.Is(err, matroska.ErrInvalidData) {
		t.Fatalf("err = %v, want matroska.ErrInvalidData", err)
	}
}

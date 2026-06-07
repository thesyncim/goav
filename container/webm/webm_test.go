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
	"github.com/thesyncim/goav/container/ebml"
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

func TestMuxerRejectsUnsupportedContentEncodings(t *testing.T) {
	tests := []struct {
		name     string
		encoding ContentEncoding
	}{
		{
			name: "compression",
			encoding: ContentEncoding{
				Type:           ContentEncodingTypeCompression,
				CompressionSet: true,
				Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
			},
		},
		{
			name: "private scope encryption",
			encoding: ContentEncoding{
				Scope:         ContentEncodingScopePrivate,
				Type:          ContentEncodingTypeEncryption,
				EncryptionSet: true,
				Encryption:    validWebMContentEncryption([]byte("scope-key")),
			},
		},
		{
			name: "missing aes settings",
			encoding: ContentEncoding{
				Type:          ContentEncodingTypeEncryption,
				EncryptionSet: true,
				Encryption: ContentEncryption{
					Algorithm: ContentEncAlgoAES,
					KeyID:     []byte("missing-aes"),
				},
			},
		},
		{
			name: "cbc encryption",
			encoding: ContentEncoding{
				Type:          ContentEncodingTypeEncryption,
				EncryptionSet: true,
				Encryption: ContentEncryption{
					Algorithm:      ContentEncAlgoAES,
					KeyID:          []byte("cbc-key"),
					AESSettingsSet: true,
					AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCBC},
				},
			},
		},
		{
			name: "signature metadata",
			encoding: ContentEncoding{
				Type:          ContentEncodingTypeEncryption,
				EncryptionSet: true,
				Encryption: ContentEncryption{
					Algorithm:          ContentEncAlgoAES,
					KeyID:              []byte("signature-key"),
					AESSettingsSet:     true,
					AESSettings:        ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCTR},
					Signature:          []byte{1},
					SignatureAlgorithm: ContentSigAlgoRSA,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := muxer.AddTrack(Track{
				Type:             TrackVideo,
				Codec:            CodecVP8,
				ContentEncodings: []ContentEncoding{tt.encoding},
				Video:            VideoConfig{Width: 16, Height: 16},
			}); !errors.Is(err, ErrUnsupportedWebMContentEncoding) {
				t.Fatalf("err = %v, want ErrUnsupportedWebMContentEncoding", err)
			}
		})
	}
}

func TestMuxerRejectsUnsupportedCodecPrivate(t *testing.T) {
	tests := []struct {
		name    string
		track   Track
		wantErr error
	}{
		{
			name: "vp8 codec private",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecVP8,
				CodecPrivate: []byte{1},
				Video:        VideoConfig{Width: 16, Height: 16},
			},
			wantErr: ErrUnsupportedWebMCodecPrivate,
		},
		{
			name: "vp9 private reserved bit",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecVP9,
				CodecPrivate: []byte{0x81, 1, 0},
				Video:        VideoConfig{Width: 16, Height: 16},
			},
			wantErr: ErrUnsupportedWebMCodecPrivate,
		},
		{
			name: "vp9 private unknown feature",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecVP9,
				CodecPrivate: []byte{5, 1, 0},
				Video:        VideoConfig{Width: 16, Height: 16},
			},
			wantErr: ErrUnsupportedWebMCodecPrivate,
		},
		{
			name: "vp9 private truncated feature",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecVP9,
				CodecPrivate: []byte{1, 2, 0},
				Video:        VideoConfig{Width: 16, Height: 16},
			},
			wantErr: ErrUnsupportedWebMCodecPrivate,
		},
		{
			name: "vp9 private duplicate feature",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecVP9,
				CodecPrivate: []byte{1, 1, 0, 1, 1, 1},
				Video:        VideoConfig{Width: 16, Height: 16},
			},
			wantErr: ErrUnsupportedWebMCodecPrivate,
		},
		{
			name: "vp9 private invalid profile",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecVP9,
				CodecPrivate: []byte{1, 1, 4},
				Video:        VideoConfig{Width: 16, Height: 16},
			},
			wantErr: ErrUnsupportedWebMCodecPrivate,
		},
		{
			name: "vp9 private invalid level",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecVP9,
				CodecPrivate: []byte{2, 1, 22},
				Video:        VideoConfig{Width: 16, Height: 16},
			},
			wantErr: ErrUnsupportedWebMCodecPrivate,
		},
		{
			name: "vp9 private invalid bit depth",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecVP9,
				CodecPrivate: []byte{3, 1, 9},
				Video:        VideoConfig{Width: 16, Height: 16},
			},
			wantErr: ErrUnsupportedWebMCodecPrivate,
		},
		{
			name: "vp9 private invalid chroma subsampling",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecVP9,
				CodecPrivate: []byte{4, 1, 4},
				Video:        VideoConfig{Width: 16, Height: 16},
			},
			wantErr: ErrUnsupportedWebMCodecPrivate,
		},
		{
			name: "valid vp9 private",
			track: Track{
				Type:         TrackVideo,
				Codec:        CodecVP9,
				CodecPrivate: validVP9CodecPrivate(),
				Video:        VideoConfig{Width: 16, Height: 16},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = muxer.AddTrack(tt.track)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("err = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMuxerRejectsUnsupportedTrackMetadata(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{
			Width:       16,
			Height:      16,
			DisplayUnit: 3,
		},
	}); !errors.Is(err, ErrUnsupportedWebMTrackMetadata) {
		t.Fatalf("err = %v, want ErrUnsupportedWebMTrackMetadata", err)
	}
}

func TestMuxerRejectsNonMonotonicTimecodes(t *testing.T) {
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
		TimeNS:   20_000_000,
		Keyframe: true,
		Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  trackID,
		TimeNS:   10_000_000,
		Keyframe: true,
		Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
	}); !errors.Is(err, ErrNonMonotonicWebMTimecode) {
		t.Fatalf("err = %v, want ErrNonMonotonicWebMTimecode", err)
	}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:         trackID,
		TimeNS:          10_000_000,
		FrameDurationNS: 10_000_000,
		Keyframe:        true,
		Lacing:          LacingXiph,
		Frames: [][]byte{
			{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
			{0x11, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
		},
	}); !errors.Is(err, ErrNonMonotonicWebMTimecode) {
		t.Fatalf("laced err = %v, want ErrNonMonotonicWebMTimecode", err)
	}
}

func TestMuxerRejectsUnscaledReferenceBlockTimes(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{TimecodeScaleNS: 1_000_000})
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
	err = muxer.WritePacket(Packet{
		TrackID:              trackID,
		TimeNS:               20_000_000,
		ReferenceBlockTimeNS: []int64{-500_000},
		Data:                 []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
	})
	if !errors.Is(err, matroska.ErrInvalidData) {
		t.Fatalf("err = %v, want matroska.ErrInvalidData", err)
	}
}

func TestMuxerAllowsCrossTrackInterleavedTimecodes(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  videoID,
		TimeNS:   40_000_000,
		Keyframe: true,
		Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:    audioID,
		TimeNS:     20_000_000,
		DurationNS: 20_000_000,
		Data:       []byte{0xf8, 0xff, 0xfe},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{
		TrackID:  videoID,
		TimeNS:   30_000_000,
		Keyframe: true,
		Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
	}); !errors.Is(err, ErrNonMonotonicWebMTimecode) {
		t.Fatalf("err = %v, want ErrNonMonotonicWebMTimecode", err)
	}
}

func TestDemuxerRejectsUnsupportedContentEncodings(t *testing.T) {
	data := makeCompressedDocTypeWebMData(t)
	if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrUnsupportedWebMContentEncoding) {
		t.Fatalf("err = %v, want ErrUnsupportedWebMContentEncoding", err)
	}
}

func TestDemuxerRejectsUnsupportedCodecPrivate(t *testing.T) {
	tests := []struct {
		name  string
		track matroska.Track
	}{
		{
			name: "vp8 codec private",
			track: matroska.Track{
				Type:         matroska.TrackVideo,
				Codec:        matroska.CodecVP8,
				CodecPrivate: []byte{1},
				Video:        matroska.VideoConfig{Width: 16, Height: 16},
			},
		},
		{
			name: "vp9 invalid codec private",
			track: matroska.Track{
				Type:         matroska.TrackVideo,
				Codec:        matroska.CodecVP9,
				CodecPrivate: []byte{1, 1, 4},
				Video:        matroska.VideoConfig{Width: 16, Height: 16},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeDocTypeWebMData(t, tt.track)
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrUnsupportedWebMCodecPrivate) {
				t.Fatalf("err = %v, want ErrUnsupportedWebMCodecPrivate", err)
			}
		})
	}
}

func TestDemuxerRejectsUnsupportedTrackMetadata(t *testing.T) {
	data := makeDocTypeWebMData(t, matroska.Track{
		Type:  matroska.TrackVideo,
		Codec: matroska.CodecVP8,
		Video: matroska.VideoConfig{
			Width:       16,
			Height:      16,
			DisplayUnit: 3,
		},
	})
	if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrUnsupportedWebMTrackMetadata) {
		t.Fatalf("err = %v, want ErrUnsupportedWebMTrackMetadata", err)
	}
}

func TestDemuxerRejectsUnsupportedMetadata(t *testing.T) {
	tests := []struct {
		name string
		opts matroska.MuxerOptions
	}{
		{
			name: "attachments",
			opts: matroska.MuxerOptions{
				Attachments: []matroska.Attachment{{
					UID:      1,
					Filename: "cover.png",
					MIMEType: "image/png",
					Data:     []byte{0x89, 0x50, 0x4e, 0x47},
				}},
			},
		},
		{
			name: "tag edition target",
			opts: matroska.MuxerOptions{
				Tags: []matroska.Tag{{
					Target: matroska.TagTarget{EditionUIDs: []uint64{1}},
					Simple: []matroska.SimpleTag{{
						Name:      "TITLE",
						String:    "bad target",
						StringSet: true,
					}},
				}},
			},
		},
		{
			name: "tag chapter target",
			opts: matroska.MuxerOptions{
				Tags: []matroska.Tag{{
					Target: matroska.TagTarget{ChapterUIDs: []uint64{1}},
					Simple: []matroska.SimpleTag{{
						Name:      "TITLE",
						String:    "bad target",
						StringSet: true,
					}},
				}},
			},
		},
		{
			name: "tag attachment target",
			opts: matroska.MuxerOptions{
				Tags: []matroska.Tag{{
					Target: matroska.TagTarget{AttachmentUIDs: []uint64{1}},
					Simple: []matroska.SimpleTag{{
						Name:      "TITLE",
						String:    "bad target",
						StringSet: true,
					}},
				}},
			},
		},
		{
			name: "nested simple tag",
			opts: matroska.MuxerOptions{
				Tags: []matroska.Tag{{
					Simple: []matroska.SimpleTag{{
						Name:      "TITLE",
						String:    "bad nesting",
						StringSet: true,
						Children: []matroska.SimpleTag{{
							Name:      "SORT_WITH",
							String:    "bad",
							StringSet: true,
						}},
					}},
				}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeDocTypeWebMDataWithOptions(t, tt.opts)
			if _, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{}); !errors.Is(err, ErrUnsupportedWebMMetadata) {
				t.Fatalf("err = %v, want ErrUnsupportedWebMMetadata", err)
			}
		})
	}
}

func TestDemuxerRejectsNonMonotonicTimecodes(t *testing.T) {
	data := makeDocTypeWebMPacketsData(t,
		matroska.Track{
			Type:  matroska.TrackVideo,
			Codec: matroska.CodecVP8,
			Video: matroska.VideoConfig{Width: 16, Height: 16},
		},
		[]matroska.Packet{
			{
				TimeNS:   20_000_000,
				Keyframe: true,
				Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
			},
			{
				TimeNS:   10_000_000,
				Keyframe: true,
				Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
			},
		},
	)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 16)}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrNonMonotonicWebMTimecode) {
		t.Fatalf("err = %v, want ErrNonMonotonicWebMTimecode", err)
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
		DurationNS:      123_000_000,
		DurationSet:     true,
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
			DurationNS:      wantInfo.DurationNS,
			DurationSet:     wantInfo.DurationSet,
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

func TestMuxerDemuxerPreservesTags(t *testing.T) {
	wantTags := []Tag{{
		Target: TagTarget{
			TypeValue: 50,
			Type:      "MOVIE",
			TrackUIDs: []uint64{11},
		},
		Simple: []SimpleTag{{
			Name:       "TITLE",
			Language:   "und",
			Default:    true,
			DefaultSet: true,
			String:     "WebM Camera Roll",
			StringSet:  true,
		}},
	}}
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{Tags: wantTags})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		UID:   11,
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	wantTags[0].Simple[0].String = "mutated after muxer construction"
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

	wantTags[0].Simple[0].String = "WebM Camera Roll"
	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gotTags := demuxer.Tags()
	if !reflect.DeepEqual(gotTags, wantTags) {
		t.Fatalf("tags = %+v, want %+v", gotTags, wantTags)
	}
	gotTags[0].Simple[0].String = "mutated demuxer result"
	if got := demuxer.Tags(); !reflect.DeepEqual(got, wantTags) {
		t.Fatalf("fresh tags = %+v, want %+v", got, wantTags)
	}
}

func TestMuxerDemuxerPreservesChapters(t *testing.T) {
	wantChapters := []ChapterEdition{{
		Chapters: []Chapter{{
			UID:       7,
			StringUID: "intro",
			StartNS:   0,
			EndNS:     1_000_000_000,
			EndSet:    true,
			Enabled:   true,
			Displays: []ChapterDisplay{{
				String:        "Intro",
				Language:      "eng",
				LanguageBCP47: "en-US",
				Country:       "us",
			}},
			Children: []Chapter{{
				UID:     8,
				StartNS: 500_000_000,
				Enabled: true,
				Displays: []ChapterDisplay{{
					String:   "Halfway",
					Language: "eng",
				}},
			}},
		}},
	}}
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{Chapters: wantChapters})
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
	wantChapters[0].Chapters[0].Displays[0].String = "mutated after muxer construction"
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

	wantChapters[0].Chapters[0].Displays[0].String = "Intro"
	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	gotChapters := demuxer.Chapters()
	if !reflect.DeepEqual(gotChapters, wantChapters) {
		t.Fatalf("chapters = %+v, want %+v", gotChapters, wantChapters)
	}
	gotChapters[0].Chapters[0].Displays[0].String = "mutated demuxer result"
	if got := demuxer.Chapters(); !reflect.DeepEqual(got, wantChapters) {
		t.Fatalf("fresh chapters = %+v, want %+v", got, wantChapters)
	}
}

func TestMuxerRejectsUnsupportedChapterMetadata(t *testing.T) {
	baseChapter := func() Chapter {
		return Chapter{
			UID:     7,
			StartNS: 0,
			Displays: []ChapterDisplay{{
				String:   "Intro",
				Language: "eng",
			}},
		}
	}
	baseEdition := func() ChapterEdition {
		return ChapterEdition{Chapters: []Chapter{baseChapter()}}
	}
	tests := []struct {
		name    string
		edition ChapterEdition
	}{
		{
			name: "edition uid",
			edition: ChapterEdition{
				UID:      1,
				Chapters: []Chapter{baseChapter()},
			},
		},
		{
			name: "edition hidden",
			edition: func() ChapterEdition {
				edition := baseEdition()
				edition.Hidden = true
				return edition
			}(),
		},
		{
			name: "edition default",
			edition: func() ChapterEdition {
				edition := baseEdition()
				edition.Default = true
				return edition
			}(),
		},
		{
			name: "edition ordered",
			edition: func() ChapterEdition {
				edition := baseEdition()
				edition.Ordered = true
				return edition
			}(),
		},
		{
			name: "edition unknown",
			edition: func() ChapterEdition {
				edition := baseEdition()
				edition.UnknownElements = []UnknownElement{{Raw: unknownWebMElementBytes(t, 0x4ff6, []byte{1})}}
				return edition
			}(),
		},
		{
			name: "chapter hidden",
			edition: func() ChapterEdition {
				edition := baseEdition()
				edition.Chapters[0].Hidden = true
				return edition
			}(),
		},
		{
			name: "chapter enabled flag",
			edition: func() ChapterEdition {
				edition := baseEdition()
				edition.Chapters[0].Enabled = true
				edition.Chapters[0].EnabledSet = true
				return edition
			}(),
		},
		{
			name: "chapter track",
			edition: func() ChapterEdition {
				edition := baseEdition()
				edition.Chapters[0].TrackUIDs = []uint64{11}
				return edition
			}(),
		},
		{
			name: "chapter unknown",
			edition: func() ChapterEdition {
				edition := baseEdition()
				edition.Chapters[0].UnknownElements = []UnknownElement{{Raw: unknownWebMElementBytes(t, 0x4ff5, []byte{1})}}
				return edition
			}(),
		},
		{
			name: "chapter display unknown",
			edition: func() ChapterEdition {
				edition := baseEdition()
				edition.Chapters[0].Displays[0].UnknownElements = []UnknownElement{{Raw: unknownWebMElementBytes(t, 0x4ff4, []byte{1})}}
				return edition
			}(),
		},
		{
			name: "child chapter enabled flag",
			edition: func() ChapterEdition {
				edition := baseEdition()
				edition.Chapters[0].Children = []Chapter{baseChapter()}
				edition.Chapters[0].Children[0].UID = 8
				edition.Chapters[0].Children[0].EnabledSet = true
				return edition
			}(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewMuxer(io.Discard, MuxerOptions{Chapters: []ChapterEdition{tt.edition}}); !errors.Is(err, ErrUnsupportedWebMMetadata) {
				t.Fatalf("err = %v, want ErrUnsupportedWebMMetadata", err)
			}
		})
	}
}

func TestMuxerRejectsUnsupportedTagMetadata(t *testing.T) {
	baseSimple := []SimpleTag{{
		Name:       "TITLE",
		Language:   "und",
		Default:    true,
		DefaultSet: true,
		String:     "x",
		StringSet:  true,
	}}
	tests := []struct {
		name string
		tag  Tag
	}{
		{
			name: "edition target",
			tag: Tag{
				Target: TagTarget{EditionUIDs: []uint64{1}},
				Simple: baseSimple,
			},
		},
		{
			name: "chapter target",
			tag: Tag{
				Target: TagTarget{ChapterUIDs: []uint64{1}},
				Simple: baseSimple,
			},
		},
		{
			name: "attachment target",
			tag: Tag{
				Target: TagTarget{AttachmentUIDs: []uint64{1}},
				Simple: baseSimple,
			},
		},
		{
			name: "nested simple tag",
			tag: Tag{
				Simple: []SimpleTag{{
					Name:       "TITLE",
					Language:   "und",
					Default:    true,
					DefaultSet: true,
					String:     "x",
					StringSet:  true,
					Children: []SimpleTag{{
						Name:      "SORT_WITH",
						String:    "x",
						StringSet: true,
					}},
				}},
			},
		},
		{
			name: "unknown tag child",
			tag: Tag{
				Simple:          baseSimple,
				UnknownElements: []UnknownElement{{Raw: unknownWebMElementBytes(t, 0x4ff7, []byte{1})}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := NewMuxer(io.Discard, MuxerOptions{Tags: []Tag{tt.tag}}); !errors.Is(err, ErrUnsupportedWebMMetadata) {
				t.Fatalf("err = %v, want ErrUnsupportedWebMMetadata", err)
			}
		})
	}
}

func TestDemuxerReadCuedPacketAtTime(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "cued-*.webm")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	muxer, err := NewMuxer(file, MuxerOptions{})
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
		{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00}},
		{TrackID: trackID, TimeNS: 20_000_000, Data: []byte{0x11, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00}},
		{TrackID: trackID, TimeNS: 40_000_000, Keyframe: true, Data: []byte{0x12, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00}},
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
	got := Packet{Data: make([]byte, 0, 16)}
	if err := demuxer.ReadCuedPacketAtTime(10_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TimeNS != packets[2].TimeNS || !bytes.Equal(got.Data, packets[2].Data) {
		t.Fatalf("cued packet = %+v data=%v, want %+v data=%v", got, got.Data, packets[2], packets[2].Data)
	}
	if err := demuxer.ReadCuedTrackPacketAtTime(trackID, 10_000_000, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != trackID || got.TimeNS != packets[2].TimeNS || !bytes.Equal(got.Data, packets[2].Data) {
		t.Fatalf("cued track packet = %+v data=%v, want track %d packet %+v data=%v", got, got.Data, trackID, packets[2], packets[2].Data)
	}
}

func TestMuxerDemuxerPreservesUnknownSegmentElements(t *testing.T) {
	unknownID := ebml.ID(0x4ffc)
	raw := unknownWebMElementBytes(t, unknownID, []byte{0x44, 0x55})
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{
		UnknownSegmentElements: []UnknownElement{{Raw: raw}},
	})
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
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00}}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(buffer.Bytes(), raw) {
		t.Fatalf("muxed data does not contain raw unknown element %x", raw)
	}
	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	elements := demuxer.UnknownSegmentElements()
	if len(elements) != 1 || elements[0].ID != uint64(unknownID) || !bytes.Equal(elements[0].Raw, raw) {
		t.Fatalf("unknown elements = %+v, want id=0x%x raw=%x", elements, uint64(unknownID), raw)
	}
}

func TestMuxerDemuxerPreservesNestedUnknownElements(t *testing.T) {
	infoUnknown := unknownWebMElementBytes(t, 0x4ffb, []byte{0x31, 0x32})
	trackUnknown := unknownWebMElementBytes(t, 0x4ffa, []byte{0x41, 0x42, 0x43})
	tracksUnknown := unknownWebMElementBytes(t, 0x4ff9, []byte{0x51})
	clusterUnknown := unknownWebMElementBytes(t, 0x4ff8, []byte{0x61, 0x62})
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{
		Info: SegmentInfo{
			UnknownElements: []UnknownElement{{Raw: append([]byte(nil), infoUnknown...)}},
		},
		UnknownTracksElements: []UnknownElement{{Raw: append([]byte(nil), tracksUnknown...)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	track := Track{
		Type:            TrackVideo,
		Codec:           CodecVP8,
		Video:           VideoConfig{Width: 16, Height: 16},
		UnknownElements: []UnknownElement{{Raw: append([]byte(nil), trackUnknown...)}},
	}
	trackID, err := muxer.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	track.UnknownElements[0].Raw[0] = 0
	if err := muxer.WritePacket(Packet{
		TrackID:                trackID,
		TimeNS:                 0,
		Keyframe:               true,
		Data:                   []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
		UnknownClusterElements: []UnknownElement{{Raw: append([]byte(nil), clusterUnknown...)}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(buffer.Bytes(), infoUnknown) {
		t.Fatalf("muxed data does not contain raw Info unknown element %x", infoUnknown)
	}
	if !bytes.Contains(buffer.Bytes(), trackUnknown) {
		t.Fatalf("muxed data does not contain raw TrackEntry unknown element %x", trackUnknown)
	}
	if !bytes.Contains(buffer.Bytes(), tracksUnknown) {
		t.Fatalf("muxed data does not contain raw Tracks unknown element %x", tracksUnknown)
	}
	if !bytes.Contains(buffer.Bytes(), clusterUnknown) {
		t.Fatalf("muxed data does not contain raw Cluster unknown element %x", clusterUnknown)
	}
	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	assertWebMUnknownElement(t, "info", demuxer.Info().UnknownElements, 0x4ffb, infoUnknown)
	tracks := demuxer.Tracks()
	if len(tracks) != 1 {
		t.Fatalf("tracks = %d, want 1", len(tracks))
	}
	assertWebMUnknownElement(t, "track", tracks[0].UnknownElements, 0x4ffa, trackUnknown)
	assertWebMUnknownElement(t, "tracks master", demuxer.UnknownTracksElements(), 0x4ff9, tracksUnknown)
	packet := Packet{Data: make([]byte, 0, 16)}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	assertWebMUnknownElement(t, "packet cluster", packet.UnknownClusterElements, 0x4ff8, clusterUnknown)
	tracks[0].UnknownElements[0].Raw[0] = 0
	assertWebMUnknownElement(t, "fresh track", demuxer.Tracks()[0].UnknownElements, 0x4ffa, trackUnknown)
	elements := demuxer.UnknownTracksElements()
	elements[0].Raw[0] = 0
	assertWebMUnknownElement(t, "fresh tracks master", demuxer.UnknownTracksElements(), 0x4ff9, tracksUnknown)
}

func TestMuxerDemuxerAppliesAESCTRContentEncryption(t *testing.T) {
	keyID := []byte("webm-aes-key")
	key := []byte{
		0x20, 0x21, 0x22, 0x23,
		0x24, 0x25, 0x26, 0x27,
		0x28, 0x29, 0x2a, 0x2b,
		0x2c, 0x2d, 0x2e, 0x2f,
	}
	keys := []ContentEncryptionKey{{KeyID: keyID, Key: key}}
	initialIV := []byte{0x10, 0x32, 0x54, 0x76, 0x98, 0xba, 0xdc, 0xfe}
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{
		ContentEncryptionKeys:      keys,
		ContentEncryptionInitialIV: initialIV,
	})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		ContentEncodings: []ContentEncoding{{
			Type:          ContentEncodingTypeEncryption,
			EncryptionSet: true,
			Encryption: ContentEncryption{
				Algorithm:      ContentEncAlgoAES,
				KeyID:          keyID,
				AESSettingsSet: true,
				AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCTR},
			},
		}},
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []byte("encrypted webm vp8 packet payload encrypted webm vp8 packet payload")
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: want}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(buffer.Bytes(), want) {
		t.Fatalf("file still contains unencrypted frame %q", want)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{ContentEncryptionKeys: keys})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(want))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packet.Data, want) {
		t.Fatalf("packet data = %q, want %q", packet.Data, want)
	}
}

func TestMuxerDemuxerAppliesPartitionedAESCTRContentEncryption(t *testing.T) {
	keyID := []byte("webm-aes-partitioned-key")
	key := []byte{
		0x30, 0x31, 0x32, 0x33,
		0x34, 0x35, 0x36, 0x37,
		0x38, 0x39, 0x3a, 0x3b,
		0x3c, 0x3d, 0x3e, 0x3f,
	}
	keys := []ContentEncryptionKey{{KeyID: keyID, Key: key}}
	initialIV := []byte{0x20, 0x42, 0x64, 0x86, 0xa8, 0xca, 0xec, 0x0e}
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{
		ContentEncryptionKeys:      keys,
		ContentEncryptionInitialIV: initialIV,
	})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		ContentEncodings: []ContentEncoding{{
			Type:          ContentEncodingTypeEncryption,
			EncryptionSet: true,
			Encryption: ContentEncryption{
				Algorithm:      ContentEncAlgoAES,
				KeyID:          keyID,
				AESSettingsSet: true,
				AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCTR},
			},
		}},
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	clearPrefix := []byte("webm partition clear prefix:")
	secret := []byte("webm partition encrypted middle webm partition encrypted middle")
	clearSuffix := []byte(":webm partition clear suffix")
	want := append(append(append([]byte(nil), clearPrefix...), secret...), clearSuffix...)
	partitions := []uint32{uint32(len(clearPrefix)), uint32(len(clearPrefix) + len(secret))}
	if err := muxer.WritePacket(Packet{
		TrackID:                     trackID,
		TimeNS:                      0,
		Keyframe:                    true,
		Data:                        want,
		ContentEncryptionPartitions: partitions,
	}); err != nil {
		t.Fatal(err)
	}
	if err := muxer.Close(); err != nil {
		t.Fatal(err)
	}
	encoded := buffer.Bytes()
	if !bytes.Contains(encoded, clearPrefix) {
		t.Fatalf("file does not contain clear prefix")
	}
	if !bytes.Contains(encoded, clearSuffix) {
		t.Fatalf("file does not contain clear suffix")
	}
	if bytes.Contains(encoded, secret) {
		t.Fatalf("file still contains encrypted partition %q", secret)
	}

	demuxer, err := NewDemuxer(bytes.NewReader(encoded), DemuxerOptions{ContentEncryptionKeys: keys})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(want))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packet.Data, want) {
		t.Fatalf("packet data = %q, want %q", packet.Data, want)
	}
}

func TestMuxerDemuxerAppliesAESCTRContentEncryptionToLacedFrames(t *testing.T) {
	keyID := []byte("webm-aes-laced-key")
	key := []byte{
		0x40, 0x41, 0x42, 0x43,
		0x44, 0x45, 0x46, 0x47,
		0x48, 0x49, 0x4a, 0x4b,
		0x4c, 0x4d, 0x4e, 0x4f,
	}
	keys := []ContentEncryptionKey{{KeyID: keyID, Key: key}}
	initialIV := []byte{0x30, 0x52, 0x74, 0x96, 0xb8, 0xda, 0xfc, 0x1e}
	frames := [][]byte{
		[]byte("encrypted webm vp8 laced frame one encrypted webm vp8 laced frame one"),
		[]byte("encrypted webm vp8 laced frame two encrypted webm vp8 laced frame two"),
	}
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{
		ContentEncryptionKeys:      keys,
		ContentEncryptionInitialIV: initialIV,
	})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecVP8,
		DefaultDurationNS: 20_000_000,
		ContentEncodings: []ContentEncoding{{
			Type:          ContentEncodingTypeEncryption,
			EncryptionSet: true,
			Encryption: ContentEncryption{
				Algorithm:      ContentEncAlgoAES,
				KeyID:          keyID,
				AESSettingsSet: true,
				AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCTR},
			},
		}},
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
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
	encoded := buffer.Bytes()
	for i := range frames {
		if bytes.Contains(encoded, frames[i]) {
			t.Fatalf("file still contains unencrypted laced frame %d", i)
		}
	}

	demuxer, err := NewDemuxer(bytes.NewReader(encoded), DemuxerOptions{ContentEncryptionKeys: keys})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(frames[0]))}
	for i := range frames {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if packet.TrackID != trackID || packet.TimeNS != int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || !packet.Keyframe ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%q", i, packet, packet.Data)
		}
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

func TestMuxerDefersHeaderUntilMultipleAV1CodecPrivateTracksReady(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	firstID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecAV1,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	secondID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecAV1,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}

	firstData := webmAV1SequenceHeaderOBU()
	if err := muxer.WritePacket(Packet{TrackID: firstID, TimeNS: 0, Keyframe: true, Data: firstData}); err != nil {
		t.Fatal(err)
	}
	firstData[0] = 0
	if buffer.Len() != 0 {
		t.Fatalf("wrote %d bytes before all AV1 private data was available", buffer.Len())
	}
	secondData := webmAV1SequenceHeaderOBU()
	if err := muxer.WritePacket(Packet{TrackID: secondID, TimeNS: 10_000_000, Keyframe: true, Data: secondData}); err != nil {
		t.Fatal(err)
	}
	secondData[0] = 0
	if buffer.Len() == 0 {
		t.Fatalf("header was not written after all AV1 private data was available")
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
	for i := range tracks {
		if tracks[i].Codec != CodecAV1 || !bytes.Equal(tracks[i].CodecPrivate, webmAV1CodecConfig()) {
			t.Fatalf("track %d = %+v private=%x", i, tracks[i], tracks[i].CodecPrivate)
		}
	}

	wantPackets := []Packet{
		{TrackID: firstID, TimeNS: 0, Keyframe: true, Data: webmAV1SequenceHeaderOBU()},
		{TrackID: secondID, TimeNS: 10_000_000, Keyframe: true, Data: webmAV1SequenceHeaderOBU()},
	}
	got := Packet{Data: make([]byte, 0, len(webmAV1SequenceHeaderOBU()))}
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

func TestDemuxerReadTrackPacketAtTime(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "seek-track-*.webm")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	muxer, err := NewMuxer(file, MuxerOptions{ClusterMaxDurationNS: 60_000_000})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: audioID, TimeNS: 0, DurationNS: 20_000_000, Data: []byte{0xa0}},
		{TrackID: videoID, TimeNS: 0, Keyframe: true, Data: []byte{0xb0}},
		{TrackID: audioID, TimeNS: 20_000_000, DurationNS: 20_000_000, Data: []byte{0xa1}},
		{TrackID: videoID, TimeNS: 20_000_000, Data: []byte{0xb1}},
		{TrackID: audioID, TimeNS: 40_000_000, DurationNS: 20_000_000, Data: []byte{0xa2}},
		{TrackID: videoID, TimeNS: 40_000_000, Keyframe: true, Data: []byte{0xb2}},
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
	if err := demuxer.ReadTrackPacketAtTime(videoID, 30_000_000, &packet); err != nil {
		t.Fatal(err)
	}
	if packet.TrackID != videoID || packet.TimeNS != 40_000_000 || !bytes.Equal(packet.Data, []byte{0xb2}) {
		t.Fatalf("video packet at time = %+v data=%v, want video at 40000000", packet, packet.Data)
	}
	if err := demuxer.ReadTrackPacketAtTime(audioID, 30_000_000, &packet); err != nil {
		t.Fatal(err)
	}
	if packet.TrackID != audioID || packet.TimeNS != 40_000_000 || !bytes.Equal(packet.Data, []byte{0xa2}) {
		t.Fatalf("audio packet at time = %+v data=%v, want audio at 40000000", packet, packet.Data)
	}
}

func TestMuxerDefaultCuePolicyUsesVideoKeyframes(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "default-cues-*.webm")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	muxer, err := NewMuxer(file, MuxerOptions{ClusterMaxDurationNS: 1_000_000})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: audioID, TimeNS: 0, DurationNS: 20_000_000, Data: []byte{0xf8, 0xff, 0xfe}},
		{TrackID: videoID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: []byte{0x9d, 0x01, 0x2a}},
		{TrackID: audioID, TimeNS: 20_000_000, DurationNS: 20_000_000, Data: []byte{0xf8, 0xff, 0xfe}},
		{TrackID: videoID, TimeNS: 20_000_000, DurationNS: 20_000_000, Data: []byte{0x00}},
		{TrackID: audioID, TimeNS: 40_000_000, DurationNS: 20_000_000, Data: []byte{0xf8, 0xff, 0xfe}},
		{TrackID: videoID, TimeNS: 40_000_000, DurationNS: 20_000_000, Keyframe: true, Data: []byte{0x9d, 0x01, 0x2a}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatalf("write packet %d: %v", i, err)
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
	if err := demuxer.SeekToTime(0); err != nil {
		t.Fatal(err)
	}
	cues := demuxer.Cues()
	if len(cues) != 2 {
		t.Fatalf("cues = %+v, want 2 video keyframe cues", cues)
	}
	for i, wantTime := range []int64{0, 40_000_000} {
		if cues[i].TrackID != videoID || cues[i].TimeNS != wantTime {
			t.Fatalf("cue %d = %+v, want video track %d at %d", i, cues[i], videoID, wantTime)
		}
	}
}

func TestMuxerCuePolicyAndDemuxerCues(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "dense-cues-*.webm")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	muxer, err := NewMuxer(file, MuxerOptions{
		ClusterMaxDurationNS: 1_000_000,
		CuePolicy:            CuePolicyAllPackets,
	})
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
		{TrackID: trackID, TimeNS: 0, Data: []byte{1}},
		{TrackID: trackID, TimeNS: 20_000_000, Data: []byte{2}},
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
	if err := demuxer.SeekToTime(0); err != nil {
		t.Fatal(err)
	}
	cues := demuxer.Cues()
	if len(cues) != len(packets) {
		t.Fatalf("cues = %+v, want %d", cues, len(packets))
	}
	if len(demuxer.SeekEntries()) == 0 {
		t.Fatalf("missing seek entries")
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacketAtTime(10_000_000, &packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != packets[1].TimeNS || !bytes.Equal(packet.Data, packets[1].Data) {
		t.Fatalf("packet at time = %+v data=%v, want %+v data=%v", packet, packet.Data, packets[1], packets[1].Data)
	}
}

func TestMuxerCuePolicyAllPacketsWritesCuesSortedByTime(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "sorted-cues-*.webm")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	muxer, err := NewMuxer(file, MuxerOptions{CuePolicy: CuePolicyAllPackets})
	if err != nil {
		t.Fatal(err)
	}
	audioID, err := muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	videoID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 16, Height: 16},
	})
	if err != nil {
		t.Fatal(err)
	}
	packets := []Packet{
		{TrackID: audioID, TimeNS: 40_000_000, DurationNS: 20_000_000, Data: []byte{0xf8, 0xff, 0xfe}},
		{TrackID: videoID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: []byte{0x9d, 0x01, 0x2a}},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			t.Fatalf("write packet %d: %v", i, err)
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
	if err := demuxer.SeekToTime(0); err != nil {
		t.Fatal(err)
	}
	cues := demuxer.Cues()
	if len(cues) != 2 {
		t.Fatalf("cues = %+v, want 2 sorted cues", cues)
	}
	if cues[0].TrackID != videoID || cues[0].TimeNS != 0 ||
		cues[1].TrackID != audioID || cues[1].TimeNS != 40_000_000 {
		t.Fatalf("cues = %+v, want video at 0 then audio at 40000000", cues)
	}
	got := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadCuedPacketAtTime(0, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != videoID || got.TimeNS != 0 || !bytes.Equal(got.Data, []byte{0x9d, 0x01, 0x2a}) {
		t.Fatalf("cued packet = track %d time %d data %v, want video 0", got.TrackID, got.TimeNS, got.Data)
	}
	if err := demuxer.ReadCuedTrackPacketAtTime(audioID, 0, &got); err != nil {
		t.Fatal(err)
	}
	if got.TrackID != audioID || got.TimeNS != 40_000_000 || !bytes.Equal(got.Data, []byte{0xf8, 0xff, 0xfe}) {
		t.Fatalf("audio cued packet = track %d time %d data %v, want audio 40000000", got.TrackID, got.TimeNS, got.Data)
	}
}

func TestMuxerWriteCRC32OptionRoundTrips(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "crc-*.webm")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	muxer, err := NewMuxer(file, MuxerOptions{WriteCRC32: true})
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
		Data:     []byte{0x9d, 0x01, 0x2a},
	}); err != nil {
		t.Fatal(err)
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
	if err := demuxer.SeekToTime(0); err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 8)}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TrackID != trackID || packet.TimeNS != 0 || !bytes.Equal(packet.Data, []byte{0x9d, 0x01, 0x2a}) {
		t.Fatalf("packet = track %d time %d data %v, want crc-protected round trip", packet.TrackID, packet.TimeNS, packet.Data)
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

func TestWritePacketAllocs(t *testing.T) {
	muxer, err := NewMuxer(io.Discard, MuxerOptions{})
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
	packet := Packet{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
	}
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
	muxer, err := NewMuxer(io.Discard, MuxerOptions{})
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
	packet := LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   [][]byte{{0xf8, 0xff, 0xfe}, {0xf8, 0xff, 0xfe}},
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

func TestReadPacketAllocs(t *testing.T) {
	payloads := benchmarkWebMPayloads()
	data := makeBenchmarkWebMCorpusData(t, 300, payloads)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, payloads.maxPayload)}

	allocs := testing.AllocsPerRun(1000, func() {
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatal(err)
		}
	})
	if allocs != 0 {
		t.Fatalf("allocs = %f, want 0", allocs)
	}
}

func BenchmarkWriteWebMCorpus(b *testing.B) {
	muxer, err := NewMuxer(io.Discard, MuxerOptions{})
	if err != nil {
		b.Fatal(err)
	}
	tracks := addBenchmarkWebMTracks(b, muxer)
	payloads := benchmarkWebMPayloads()
	writeBenchmarkWebMCorpus(b, muxer, tracks, payloads, 0)
	b.ReportAllocs()
	b.SetBytes(payloads.totalBytes)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writeBenchmarkWebMCorpus(b, muxer, tracks, payloads, i+1)
	}
}

func BenchmarkReadWebMCorpus(b *testing.B) {
	payloads := benchmarkWebMPayloads()
	data := makeBenchmarkWebMCorpusData(b, benchmarkWebMCorpusCycles, payloads)
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
		if cyclesRead == benchmarkWebMCorpusCycles {
			reader.Reset(data)
			demuxer, err = NewDemuxer(&reader, DemuxerOptions{})
			if err != nil {
				b.Fatal(err)
			}
			cyclesRead = 0
		}
		for frame := 0; frame < benchmarkWebMTrackCount; frame++ {
			if err := demuxer.ReadPacket(&packet); err != nil {
				b.Fatal(err)
			}
		}
		cyclesRead++
	}
}

const benchmarkWebMTrackCount = 4
const benchmarkWebMCorpusCycles = 4096

type benchmarkWebMTracks struct {
	vp8  uint32
	vp9  uint32
	av1  uint32
	opus uint32
}

type benchmarkWebMPayloadSet struct {
	vp8        []byte
	vp9        []byte
	av1        []byte
	opus       []byte
	totalBytes int64
	maxPayload int
}

func benchmarkWebMPayloads() benchmarkWebMPayloadSet {
	payloads := benchmarkWebMPayloadSet{
		vp8:  repeatedWebMBenchmarkPayload(1200, 0x10),
		vp9:  repeatedWebMBenchmarkPayload(1200, 0x83),
		av1:  webmAV1SequenceHeaderOBU(),
		opus: []byte{0xf8, 0xff, 0xfe},
	}
	payloads.vp8[3] = 0x9d
	payloads.vp8[4] = 0x01
	payloads.vp8[5] = 0x2a
	for _, payload := range [][]byte{payloads.vp8, payloads.vp9, payloads.av1, payloads.opus} {
		payloads.totalBytes += int64(len(payload))
		if len(payload) > payloads.maxPayload {
			payloads.maxPayload = len(payload)
		}
	}
	return payloads
}

func repeatedWebMBenchmarkPayload(size int, seed byte) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = seed + byte(i)
	}
	return payload
}

func addBenchmarkWebMTracks(tb testing.TB, muxer *Muxer) benchmarkWebMTracks {
	tb.Helper()
	var tracks benchmarkWebMTracks
	var err error
	tracks.vp8, err = muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		Video: VideoConfig{Width: 640, Height: 360},
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
	tracks.av1, err = muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecAV1,
		CodecPrivate: webmAV1CodecConfig(),
		Video:        VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		tb.Fatal(err)
	}
	tracks.opus, err = muxer.AddTrack(Track{
		Type:              TrackAudio,
		Codec:             CodecOpus,
		DefaultDurationNS: 20_000_000,
		Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return tracks
}

func writeBenchmarkWebMCorpus(tb testing.TB, muxer *Muxer, tracks benchmarkWebMTracks, payloads benchmarkWebMPayloadSet, cycle int) {
	tb.Helper()
	timeNS := int64(cycle) * 20_000_000
	packets := []Packet{
		{TrackID: tracks.vp8, TimeNS: timeNS, DurationNS: 20_000_000, Keyframe: true, Data: payloads.vp8},
		{TrackID: tracks.vp9, TimeNS: timeNS, DurationNS: 20_000_000, Keyframe: true, Data: payloads.vp9},
		{TrackID: tracks.av1, TimeNS: timeNS, DurationNS: 20_000_000, Keyframe: true, Data: payloads.av1},
		{TrackID: tracks.opus, TimeNS: timeNS, DurationNS: 20_000_000, Keyframe: true, Data: payloads.opus},
	}
	for i := range packets {
		if err := muxer.WritePacket(packets[i]); err != nil {
			tb.Fatalf("write corpus packet %d: %v", i, err)
		}
	}
}

func makeBenchmarkWebMCorpusData(tb testing.TB, cycles int, payloads benchmarkWebMPayloadSet) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	buffer.Grow(benchmarkWebMCorpusCapacity(cycles, payloads))
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	tracks := addBenchmarkWebMTracks(tb, muxer)
	for i := 0; i < cycles; i++ {
		writeBenchmarkWebMCorpus(tb, muxer, tracks, payloads, i)
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func benchmarkWebMCorpusCapacity(cycles int, payloads benchmarkWebMPayloadSet) int {
	payloadBytes := payloads.totalBytes * int64(cycles)
	metadataBytes := int64(cycles*benchmarkWebMTrackCount*128 + 64*1024)
	return int(payloadBytes + metadataBytes)
}

func validWebMContentEncryption(keyID []byte) ContentEncryption {
	return ContentEncryption{
		Algorithm:      ContentEncAlgoAES,
		KeyID:          append([]byte(nil), keyID...),
		AESSettingsSet: true,
		AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCTR},
	}
}

func validVP9CodecPrivate() []byte {
	return []byte{
		1, 1, 0,
		2, 1, 10,
		3, 1, 8,
		4, 1, 1,
	}
}

func makeCompressedDocTypeWebMData(tb testing.TB) []byte {
	tb.Helper()
	return makeDocTypeWebMData(tb, matroska.Track{
		Type:  matroska.TrackVideo,
		Codec: matroska.CodecVP8,
		ContentEncodings: []matroska.ContentEncoding{{
			Type:           matroska.ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    matroska.ContentCompression{Algorithm: matroska.ContentCompAlgoZlib},
		}},
		Video: matroska.VideoConfig{Width: 16, Height: 16},
	})
}

func makeDocTypeWebMData(tb testing.TB, track matroska.Track) []byte {
	tb.Helper()
	return makeDocTypeWebMPacketsData(tb, track, []matroska.Packet{{
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
	}})
}

func makeDocTypeWebMPacketsData(tb testing.TB, track matroska.Track, packets []matroska.Packet) []byte {
	tb.Helper()
	return makeDocTypeWebMPacketsDataWithOptions(tb, matroska.MuxerOptions{}, track, packets)
}

func makeDocTypeWebMDataWithOptions(tb testing.TB, opts matroska.MuxerOptions) []byte {
	tb.Helper()
	return makeDocTypeWebMPacketsDataWithOptions(tb, opts, matroska.Track{
		Type:  matroska.TrackVideo,
		Codec: matroska.CodecVP8,
		Video: matroska.VideoConfig{Width: 16, Height: 16},
	}, []matroska.Packet{{
		TimeNS:   0,
		Keyframe: true,
		Data:     []byte{0x10, 0x00, 0x9d, 0x01, 0x2a, 0x10, 0x00, 0x10, 0x00},
	}})
}

func makeDocTypeWebMPacketsDataWithOptions(tb testing.TB, opts matroska.MuxerOptions, track matroska.Track, packets []matroska.Packet) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	opts.DocType = "webm"
	muxer, err := matroska.NewMuxer(&buffer, opts)
	if err != nil {
		tb.Fatal(err)
	}
	trackID, err := muxer.AddTrack(track)
	if err != nil {
		tb.Fatal(err)
	}
	for i := range packets {
		packet := packets[i]
		if packet.TrackID == 0 {
			packet.TrackID = trackID
		}
		if err := muxer.WritePacket(packet); err != nil {
			tb.Fatalf("write packet %d: %v", i, err)
		}
	}
	if err := muxer.Close(); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func unknownWebMElementBytes(tb testing.TB, id ebml.ID, payload []byte) []byte {
	tb.Helper()
	var buffer bytes.Buffer
	writer := ebml.NewWriter(&buffer)
	if err := writer.WriteElement(id, payload); err != nil {
		tb.Fatal(err)
	}
	return buffer.Bytes()
}

func assertWebMUnknownElement(tb testing.TB, name string, elements []UnknownElement, id ebml.ID, raw []byte) {
	tb.Helper()
	if len(elements) != 1 {
		tb.Fatalf("%s unknown elements = %d, want 1", name, len(elements))
	}
	if elements[0].ID != uint64(id) || !bytes.Equal(elements[0].Raw, raw) {
		tb.Fatalf("%s unknown element = %+v raw=%x, want id=0x%x raw=%x", name, elements[0], elements[0].Raw, uint64(id), raw)
	}
}

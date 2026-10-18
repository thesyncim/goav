package matroska

import (
	"bytes"
	"compress/zlib"
	"errors"
	"reflect"
	"testing"

	"github.com/thesyncim/goav/container/ebml"
)

func TestMuxerDemuxerPreservesContentEncodings(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wantEncodings := []ContentEncoding{
		{
			Order:          1,
			Scope:          ContentEncodingScopePrivate,
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  []byte{0x00, 0x00, 0x01},
			},
		},
		{
			Order:         0,
			Scope:         ContentEncodingScopeNext,
			Type:          ContentEncodingTypeEncryption,
			EncryptionSet: true,
			Encryption: ContentEncryption{
				Algorithm:      ContentEncAlgoAES,
				KeyID:          []byte{0x10, 0x20},
				AESSettingsSet: true,
				AESSettings: ContentEncAESSettings{
					CipherMode: ContentEncAESCipherModeCTR,
				},
				Signature:              []byte{0xaa, 0xbb},
				SignatureKeyID:         []byte{0x30, 0x40},
				SignatureAlgorithm:     ContentSigAlgoRSA,
				SignatureHashAlgorithm: ContentSigHashAlgoSHA1,
			},
		},
	}
	track := Track{
		Type:             TrackVideo,
		Codec:            CodecVP8,
		ContentEncodings: cloneContentEncodings(wantEncodings),
		Video:            VideoConfig{Width: 640, Height: 360},
	}
	trackID, err := muxer.AddTrack(track)
	if err != nil {
		t.Fatal(err)
	}
	track.ContentEncodings[0].Compression.Settings[0] = 0xff
	track.ContentEncodings[1].Encryption.KeyID[0] = 0xff
	track.ContentEncodings[1].Encryption.Signature[0] = 0xff
	track.ContentEncodings[1].Encryption.SignatureKeyID[0] = 0xff
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
	if !reflect.DeepEqual(tracks[0].ContentEncodings, wantEncodings) {
		t.Fatalf("encodings = %+v, want %+v", tracks[0].ContentEncodings, wantEncodings)
	}
	tracks[0].ContentEncodings[0].Compression.Settings[0] = 0xee
	tracks[0].ContentEncodings[1].Encryption.KeyID[0] = 0xee
	tracks[0].ContentEncodings[1].Encryption.Signature[0] = 0xee
	tracks[0].ContentEncodings[1].Encryption.SignatureKeyID[0] = 0xee
	fresh := demuxer.Tracks()
	if !reflect.DeepEqual(fresh[0].ContentEncodings, wantEncodings) {
		t.Fatalf("content encoding alias was not protected: %+v", fresh[0].ContentEncodings)
	}
}

func TestMuxerDemuxerAppliesHeaderStrippingContentEncoding(t *testing.T) {
	settings := []byte("HS:")
	tests := []struct {
		name  string
		write func(*Muxer, uint32, []byte) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "simple block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, 32)}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "block group",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, 32)}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
		{
			name: "laced block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WriteLacedPacket(LacedPacket{
					TrackID:  trackID,
					TimeNS:   0,
					Keyframe: true,
					Lacing:   LacingXiph,
					Frames: [][]byte{
						data,
						append(append([]byte(nil), settings...), []byte("second")...),
					},
				})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, 32)}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("first packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, []byte("HS:second")) {
					t.Fatalf("second packet = %+v data=%q", packet, packet.Data)
				}
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
			want := append(append([]byte(nil), settings...), []byte(tt.name)...)
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings: []ContentEncoding{{
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression: ContentCompression{
						Algorithm: ContentCompAlgoHeaderStripping,
						Settings:  settings,
					},
				}},
				Video: VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := tt.write(muxer, trackID, want); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) {
				t.Fatalf("file still contains unstripped frame %q", want)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)
		})
	}
}

func TestMuxerDemuxerAppliesZlibContentEncoding(t *testing.T) {
	tests := []struct {
		name  string
		write func(*Muxer, uint32, []byte) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "simple block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "block group",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
		{
			name: "laced block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WriteLacedPacket(LacedPacket{
					TrackID:  trackID,
					TimeNS:   0,
					Keyframe: true,
					Lacing:   LacingXiph,
					Frames: [][]byte{
						data,
						zlibTestPayload("second laced block"),
					},
				})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("first packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
				second := zlibTestPayload("second laced block")
				packet.Data = make([]byte, 0, len(second))
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
					t.Fatalf("second packet = %+v data=%q, want %q", packet, packet.Data, second)
				}
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
			want := zlibTestPayload(tt.name)
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings: []ContentEncoding{{
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression: ContentCompression{
						Algorithm: ContentCompAlgoZlib,
					},
				}},
				Video: VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := tt.write(muxer, trackID, want); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) {
				t.Fatalf("file still contains uncompressed frame %q", want)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)
		})
	}
}

func TestMuxerDemuxerAppliesChainedHeaderStrippingAndZlibContentEncoding(t *testing.T) {
	settings := []byte("HS:")
	tests := []struct {
		name  string
		track Track
		write func(*Muxer, uint32, []byte) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "vp8 simple block",
			track: Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				Video:             VideoConfig{Width: 640, Height: 360},
			},
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "vp9 block group",
			track: Track{
				Type:              TrackVideo,
				Codec:             CodecVP9,
				DefaultDurationNS: 20_000_000,
				Video:             VideoConfig{Width: 1280, Height: 720},
			},
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
		{
			name: "av1 laced block",
			track: Track{
				Type:              TrackVideo,
				Codec:             CodecAV1,
				CodecPrivate:      av1CodecConfig(),
				DefaultDurationNS: 20_000_000,
				Video:             VideoConfig{Width: 1920, Height: 1080},
			},
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WriteLacedPacket(LacedPacket{
					TrackID:  trackID,
					TimeNS:   0,
					Keyframe: true,
					Lacing:   LacingXiph,
					Frames: [][]byte{
						data,
						withContentEncodingHeader(settings, chainedContentPayload("av1 second laced block")),
					},
				})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("first packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
				second := withContentEncodingHeader(settings, chainedContentPayload("av1 second laced block"))
				packet.Data = make([]byte, 0, len(second))
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
					t.Fatalf("second packet = %+v data=%q, want %q", packet, packet.Data, second)
				}
			},
		},
		{
			name: "opus simple block",
			track: Track{
				Type:              TrackAudio,
				Codec:             CodecOpus,
				DefaultDurationNS: 20_000_000,
				Audio:             AudioConfig{SampleRate: 48000, Channels: 2},
			},
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
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
			track := tt.track
			track.ContentEncodings = chainedHeaderZlibContentEncodings(settings)
			trackID, err := muxer.AddTrack(track)
			if err != nil {
				t.Fatal(err)
			}
			want := withContentEncodingHeader(settings, chainedContentPayload(tt.name))
			if err := tt.write(muxer, trackID, want); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) || bytes.Contains(buffer.Bytes(), want[len(settings):]) {
				t.Fatalf("file still contains unencoded frame payload")
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)
		})
	}
}

func TestDemuxerRetriesZlibLacedFrameAfterSmallBuffer(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecVP8,
		DefaultDurationNS: 20_000_000,
		ContentEncodings: []ContentEncoding{{
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
		}},
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := zlibTestPayload("first retry frame")
	second := zlibTestPayload("second retry frame")
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   [][]byte{first, second},
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
	packet := Packet{Data: make([]byte, 0, 4)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, len(first))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 0 || !bytes.Equal(packet.Data, first) {
		t.Fatalf("first retry packet = %+v data=%q", packet, packet.Data)
	}
	packet.Data = make([]byte, 0, len(second))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
		t.Fatalf("second packet = %+v data=%q", packet, packet.Data)
	}
}

func TestDemuxerRetriesChainedZlibLacedFrameAfterSmallBuffer(t *testing.T) {
	settings := []byte("HS:")
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecVP8,
		DefaultDurationNS: 20_000_000,
		ContentEncodings:  chainedHeaderZlibContentEncodings(settings),
		Video:             VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	first := withContentEncodingHeader(settings, chainedContentPayload("first retry frame"))
	second := withContentEncodingHeader(settings, chainedContentPayload("second retry frame"))
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   [][]byte{first, second},
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
	packet := Packet{Data: make([]byte, 0, 4)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, len(first))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 0 || !bytes.Equal(packet.Data, first) {
		t.Fatalf("first retry packet = %+v data=%q", packet, packet.Data)
	}
	packet.Data = make([]byte, 0, len(second))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
		t.Fatalf("second packet = %+v data=%q", packet, packet.Data)
	}
}

func TestMuxerDemuxerAppliesZlibContentEncodingToWebRTCCodecs(t *testing.T) {
	tests := []struct {
		name  string
		track Track
		data  []byte
	}{
		{
			name:  "opus",
			track: Track{Type: TrackAudio, Codec: CodecOpus, Audio: AudioConfig{SampleRate: 48000, Channels: 2}},
			data:  zlibTestPayload("opus"),
		},
		{
			name:  "av1",
			track: Track{Type: TrackVideo, Codec: CodecAV1, CodecPrivate: av1CodecConfig(), Video: VideoConfig{Width: 640, Height: 360}},
			data:  zlibTestPayload("av1"),
		},
		{
			name:  "h264",
			track: Track{Type: TrackVideo, Codec: CodecH264, CodecPrivate: h264AVCDecoderConfigWithLengthSize(2), Video: VideoConfig{Width: 640, Height: 360}},
			data:  h264AnnexBAccessUnit(),
		},
		{
			name:  "vp9",
			track: Track{Type: TrackVideo, Codec: CodecVP9, Video: VideoConfig{Width: 640, Height: 360}},
			data:  zlibTestPayload("vp9"),
		},
		{
			name:  "vp8",
			track: Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 640, Height: 360}},
			data:  zlibTestPayload("vp8"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			track := tt.track
			track.ContentEncodings = []ContentEncoding{{
				Type:           ContentEncodingTypeCompression,
				CompressionSet: true,
				Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
			}}
			trackID, err := muxer.AddTrack(track)
			if err != nil {
				t.Fatal(err)
			}
			if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: tt.data}); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), tt.data) {
				t.Fatalf("file still contains uncompressed %s payload", tt.name)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, len(tt.data))}
			if err := demuxer.ReadPacket(&packet); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(packet.Data, tt.data) {
				t.Fatalf("packet data = %v, want %v", packet.Data, tt.data)
			}
		})
	}
}

func TestMuxerDemuxerAppliesAESCTRContentEncryption(t *testing.T) {
	tests := []struct {
		name  string
		write func(*Muxer, uint32, []byte) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "simple block",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "block group",
			write: func(muxer *Muxer, trackID uint32, data []byte) error {
				return muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, DurationNS: 20_000_000, Keyframe: true, Data: data})
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyID := aesCTRContentEncryptionKeyID()
			initialIV := aesCTRContentEncryptionInitialIV()
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{
				ContentEncryptionKeys:      aesCTRContentEncryptionKeys(keyID),
				ContentEncryptionInitialIV: initialIV,
			})
			if err != nil {
				t.Fatal(err)
			}
			want := encryptedTestPayload(tt.name)
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings:  aesCTRContentEncryptionTrackEncodings(keyID),
				Video:             VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			if err := tt.write(muxer, trackID, want); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) {
				t.Fatalf("file still contains unencrypted frame %q", want)
			}
			if !bytes.Contains(buffer.Bytes(), append([]byte{contentEncryptionSignalEncrypted}, initialIV...)) {
				t.Fatalf("file does not contain initial encrypted-frame signal and IV")
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{
				ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
			})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)

			missingKeyDemuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, len(want))}
			if err := missingKeyDemuxer.ReadPacket(&packet); !errors.Is(err, ErrUnsupportedContentEncoding) {
				t.Fatalf("missing key err = %v, want ErrUnsupportedContentEncoding", err)
			}
		})
	}
}

func TestMuxerDemuxerAppliesPartitionedAESCTRContentEncryption(t *testing.T) {
	tests := []struct {
		name  string
		write func(*Muxer, uint32, Packet) error
		read  func(*Demuxer, []byte)
	}{
		{
			name: "simple block",
			write: func(muxer *Muxer, trackID uint32, packet Packet) error {
				packet.TrackID = trackID
				packet.TimeNS = 0
				packet.Keyframe = true
				return muxer.WritePacket(packet)
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "block group",
			write: func(muxer *Muxer, trackID uint32, packet Packet) error {
				packet.TrackID = trackID
				packet.TimeNS = 0
				packet.DurationNS = 20_000_000
				packet.Keyframe = true
				return muxer.WritePacket(packet)
			},
			read: func(demuxer *Demuxer, want []byte) {
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet = %+v data=%q, want data=%q duration 20000000", packet, packet.Data, want)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyID := aesCTRContentEncryptionKeyID()
			initialIV := aesCTRContentEncryptionInitialIV()
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{
				ContentEncryptionKeys:      aesCTRContentEncryptionKeys(keyID),
				ContentEncryptionInitialIV: initialIV,
			})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings:  aesCTRContentEncryptionTrackEncodings(keyID),
				Video:             VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			clearPrefix := []byte("partitioned aes ctr clear prefix:")
			secret := encryptedTestPayload("partitioned secret " + tt.name)
			clearSuffix := []byte(":partitioned aes ctr clear suffix")
			want := append(append(append([]byte(nil), clearPrefix...), secret...), clearSuffix...)
			partitions := []uint32{uint32(len(clearPrefix)), uint32(len(clearPrefix) + len(secret))}
			if err := tt.write(muxer, trackID, Packet{
				Data:                        want,
				ContentEncryptionPartitions: partitions,
			}); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			encoded := buffer.Bytes()
			if !bytes.Contains(encoded, append([]byte{contentEncryptionSignalEncrypted | contentEncryptionSignalPartitioned}, initialIV...)) {
				t.Fatalf("file does not contain partitioned encrypted-frame signal and IV")
			}
			if !bytes.Contains(encoded, clearPrefix) {
				t.Fatalf("file does not contain clear prefix")
			}
			if !bytes.Contains(encoded, clearSuffix) {
				t.Fatalf("file does not contain clear suffix")
			}
			if bytes.Contains(encoded, secret) {
				t.Fatalf("file still contains encrypted partition %q", secret)
			}

			demuxer, err := NewDemuxer(bytes.NewReader(encoded), DemuxerOptions{
				ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
			})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer, want)

			missingKeyDemuxer, err := NewDemuxer(bytes.NewReader(encoded), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, len(want))}
			if err := missingKeyDemuxer.ReadPacket(&packet); !errors.Is(err, ErrUnsupportedContentEncoding) {
				t.Fatalf("missing key err = %v, want ErrUnsupportedContentEncoding", err)
			}
		})
	}
}

func TestMuxerDemuxerAppliesAESCTRContentEncryptionToWebRTCCodecs(t *testing.T) {
	tests := []struct {
		name  string
		track Track
		data  []byte
	}{
		{
			name:  "opus",
			track: Track{Type: TrackAudio, Codec: CodecOpus, Audio: AudioConfig{SampleRate: 48000, Channels: 2}},
			data:  encryptedTestPayload("opus"),
		},
		{
			name:  "av1",
			track: Track{Type: TrackVideo, Codec: CodecAV1, CodecPrivate: av1CodecConfig(), Video: VideoConfig{Width: 640, Height: 360}},
			data:  encryptedTestPayload("av1"),
		},
		{
			name:  "h264",
			track: Track{Type: TrackVideo, Codec: CodecH264, CodecPrivate: h264AVCDecoderConfigWithLengthSize(2), Video: VideoConfig{Width: 640, Height: 360}},
			data:  h264AnnexBAccessUnit(),
		},
		{
			name:  "vp9",
			track: Track{Type: TrackVideo, Codec: CodecVP9, Video: VideoConfig{Width: 640, Height: 360}},
			data:  encryptedTestPayload("vp9"),
		},
		{
			name:  "vp8",
			track: Track{Type: TrackVideo, Codec: CodecVP8, Video: VideoConfig{Width: 640, Height: 360}},
			data:  encryptedTestPayload("vp8"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyID := aesCTRContentEncryptionKeyID()
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{
				ContentEncryptionKeys:      aesCTRContentEncryptionKeys(keyID),
				ContentEncryptionInitialIV: aesCTRContentEncryptionInitialIV(),
			})
			if err != nil {
				t.Fatal(err)
			}
			track := tt.track
			track.ContentEncodings = aesCTRContentEncryptionTrackEncodings(keyID)
			trackID, err := muxer.AddTrack(track)
			if err != nil {
				t.Fatal(err)
			}
			if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: tt.data}); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), tt.data) {
				t.Fatalf("file still contains unencrypted %s payload", tt.name)
			}
			if tt.name == "h264" && bytes.Contains(buffer.Bytes(), h264AVCSampleWithLengthSize2()) {
				t.Fatalf("file still contains unencrypted h264 avc sample")
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{
				ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
			})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, len(tt.data))}
			if err := demuxer.ReadPacket(&packet); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(packet.Data, tt.data) {
				t.Fatalf("packet data = %v, want %v", packet.Data, tt.data)
			}
		})
	}
}

func TestMuxerDemuxerAppliesChainedCompressionAndAESCTRContentEncryption(t *testing.T) {
	settings := []byte("HS:")
	tests := []struct {
		name      string
		algorithm uint64
	}{
		{name: "zlib", algorithm: ContentCompAlgoZlib},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keyID := aesCTRContentEncryptionKeyID()
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{
				ContentEncryptionKeys:      aesCTRContentEncryptionKeys(keyID),
				ContentEncryptionInitialIV: aesCTRContentEncryptionInitialIV(),
			})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(Track{
				Type:              TrackVideo,
				Codec:             CodecVP8,
				DefaultDurationNS: 20_000_000,
				ContentEncodings:  chainedHeaderCompressionAESCTRContentEncodings(settings, tt.algorithm, keyID),
				Video:             VideoConfig{Width: 640, Height: 360},
			})
			if err != nil {
				t.Fatal(err)
			}
			want := withContentEncodingHeader(settings, chainedContentPayload("encrypted "+tt.name))
			if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: want}); err != nil {
				t.Fatal(err)
			}
			if err := muxer.Close(); err != nil {
				t.Fatal(err)
			}
			if bytes.Contains(buffer.Bytes(), want) || bytes.Contains(buffer.Bytes(), want[len(settings):]) {
				t.Fatalf("file still contains unencoded frame payload")
			}

			demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{
				ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
			})
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
		})
	}
}

func TestMuxerRejectsPartitionedAESCTRContentEncryptionInvalidPartitions(t *testing.T) {
	keyID := aesCTRContentEncryptionKeyID()
	manyPartitions := make([]uint32, 256)
	for i := range manyPartitions {
		manyPartitions[i] = uint32(i)
	}
	tests := []struct {
		name       string
		track      Track
		keys       []ContentEncryptionKey
		data       []byte
		partitions []uint32
	}{
		{
			name: "track without encryption",
			track: Track{
				Type:  TrackVideo,
				Codec: CodecVP8,
				Video: VideoConfig{Width: 640, Height: 360},
			},
			data:       []byte("clear payload"),
			partitions: []uint32{0},
		},
		{
			name: "offset past payload",
			track: Track{
				Type:             TrackVideo,
				Codec:            CodecVP8,
				ContentEncodings: aesCTRContentEncryptionTrackEncodings(keyID),
				Video:            VideoConfig{Width: 640, Height: 360},
			},
			keys:       aesCTRContentEncryptionKeys(keyID),
			data:       []byte("payload"),
			partitions: []uint32{8},
		},
		{
			name: "duplicate offset",
			track: Track{
				Type:             TrackVideo,
				Codec:            CodecVP8,
				ContentEncodings: aesCTRContentEncryptionTrackEncodings(keyID),
				Video:            VideoConfig{Width: 640, Height: 360},
			},
			keys:       aesCTRContentEncryptionKeys(keyID),
			data:       []byte("payload"),
			partitions: []uint32{2, 2},
		},
		{
			name: "too many partitions",
			track: Track{
				Type:             TrackVideo,
				Codec:            CodecVP8,
				ContentEncodings: aesCTRContentEncryptionTrackEncodings(keyID),
				Video:            VideoConfig{Width: 640, Height: 360},
			},
			keys:       aesCTRContentEncryptionKeys(keyID),
			data:       make([]byte, 300),
			partitions: manyPartitions,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buffer bytes.Buffer
			muxer, err := NewMuxer(&buffer, MuxerOptions{
				ContentEncryptionKeys:      tt.keys,
				ContentEncryptionInitialIV: aesCTRContentEncryptionInitialIV(),
			})
			if err != nil {
				t.Fatal(err)
			}
			trackID, err := muxer.AddTrack(tt.track)
			if err != nil {
				t.Fatal(err)
			}
			err = muxer.WritePacket(Packet{
				TrackID:                     trackID,
				TimeNS:                      0,
				Keyframe:                    true,
				Data:                        tt.data,
				ContentEncryptionPartitions: tt.partitions,
			})
			if !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerRejectsAESCTRContentEncryptionWithoutKey(t *testing.T) {
	keyID := aesCTRContentEncryptionKeyID()
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{
		ContentEncryptionInitialIV: aesCTRContentEncryptionInitialIV(),
	})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:             TrackVideo,
		Codec:            CodecVP8,
		ContentEncodings: aesCTRContentEncryptionTrackEncodings(keyID),
		Video:            VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: encryptedTestPayload("missing key")}); !errors.Is(err, ErrUnsupportedContentEncoding) {
		t.Fatalf("err = %v, want ErrUnsupportedContentEncoding", err)
	}
}

func TestMuxerDemuxerAppliesAESCTRContentEncryptionToLacedFrames(t *testing.T) {
	keyID := aesCTRContentEncryptionKeyID()
	frames := [][]byte{
		encryptedTestPayload("first laced"),
		encryptedTestPayload("second laced"),
		encryptedTestPayload("third laced"),
	}
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{
		ContentEncryptionKeys:      aesCTRContentEncryptionKeys(keyID),
		ContentEncryptionInitialIV: aesCTRContentEncryptionInitialIV(),
	})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:              TrackVideo,
		Codec:             CodecVP8,
		DefaultDurationNS: 20_000_000,
		ContentEncodings:  aesCTRContentEncryptionTrackEncodings(keyID),
		Video:             VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:         trackID,
		TimeNS:          0,
		FrameDurationNS: 20_000_000,
		Keyframe:        true,
		Lacing:          LacingEBML,
		Frames:          frames,
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

	demuxer, err := NewDemuxer(bytes.NewReader(encoded), DemuxerOptions{
		ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
	})
	if err != nil {
		t.Fatal(err)
	}
	maxFrameSize := 0
	for i := range frames {
		if len(frames[i]) > maxFrameSize {
			maxFrameSize = len(frames[i])
		}
	}
	packet := Packet{Data: make([]byte, 0, maxFrameSize)}
	for i := range frames {
		if i == 1 {
			packet.Data = make([]byte, 0, len(frames[i])-1)
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
				t.Fatalf("small buffer err = %v, want ErrPayloadTooSmall", err)
			}
			packet.Data = make([]byte, 0, maxFrameSize)
		}
		if err := demuxer.ReadPacket(&packet); err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if packet.TrackID != trackID || packet.TimeNS != int64(i)*20_000_000 ||
			packet.DurationNS != 20_000_000 || !packet.Keyframe ||
			!bytes.Equal(packet.Data, frames[i]) {
			t.Fatalf("frame %d packet=%+v data=%q", i, packet, packet.Data)
		}
	}

	missingKeyDemuxer, err := NewDemuxer(bytes.NewReader(encoded), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := missingKeyDemuxer.ReadPacket(&packet); !errors.Is(err, ErrUnsupportedContentEncoding) {
		t.Fatalf("missing key err = %v, want ErrUnsupportedContentEncoding", err)
	}
}

func TestDemuxerReadsUnencryptedFrameInEncryptedTrack(t *testing.T) {
	keyID := aesCTRContentEncryptionKeyID()
	want := encryptedTestPayload("clear signal")
	frame := append([]byte{0}, want...)
	data := makeContentEncodedBlockMatroskaData(t, aesCTRContentEncryptionContentEncodings(t, keyID), frame)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
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

func TestDemuxerRejectsUnsupportedAESCTRContentEncryptionSignal(t *testing.T) {
	keyID := aesCTRContentEncryptionKeyID()
	frame := []byte{contentEncryptionSignalExtension}
	frame = append(frame, aesCTRContentEncryptionInitialIV()...)
	frame = append(frame, encryptedTestPayload("extension")...)
	data := makeContentEncodedBlockMatroskaData(t, aesCTRContentEncryptionContentEncodings(t, keyID), frame)
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{
		ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
	})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 64)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrUnsupportedContentEncoding) {
		t.Fatalf("err = %v, want ErrUnsupportedContentEncoding", err)
	}
}

func TestDemuxerRejectsInvalidPartitionedAESCTRContentEncryption(t *testing.T) {
	keyID := aesCTRContentEncryptionKeyID()
	iv := aesCTRContentEncryptionInitialIV()
	signal := contentEncryptionSignalEncrypted | contentEncryptionSignalPartitioned
	tests := []struct {
		name  string
		frame []byte
	}{
		{
			name:  "partitioned without encrypted flag",
			frame: []byte{contentEncryptionSignalPartitioned, 0x01, 0x02},
		},
		{
			name:  "missing iv",
			frame: []byte{signal, 0x01, 0x02},
		},
		{
			name:  "missing partition count",
			frame: append([]byte{signal}, iv...),
		},
		{
			name:  "truncated partition offsets",
			frame: append(append([]byte{signal}, iv...), 1, 0, 1),
		},
		{
			name:  "offset past payload",
			frame: partitionedAESCTRContentEncryptionFrame(signal, iv, []uint32{4}, []byte("abc")),
		},
		{
			name:  "unsorted offsets",
			frame: partitionedAESCTRContentEncryptionFrame(signal, iv, []uint32{3, 2}, []byte("abcd")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := makeContentEncodedBlockMatroskaData(t, aesCTRContentEncryptionContentEncodings(t, keyID), tt.frame)
			demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{
				ContentEncryptionKeys: aesCTRContentEncryptionKeys(keyID),
			})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, 64)}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
				t.Fatalf("err = %v, want ErrInvalidData", err)
			}
		})
	}
}

func TestMuxerDemuxerAppliesZlibContentEncodingToH264(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:         TrackVideo,
		Codec:        CodecH264,
		CodecPrivate: h264AVCDecoderConfigWithLengthSize(2),
		ContentEncodings: []ContentEncoding{{
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
		}},
		Video: VideoConfig{Width: 640, Height: 360},
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
	if bytes.Contains(buffer.Bytes(), annexB) {
		t.Fatalf("file still contains the Annex B access unit")
	}
	// Assert the zlib stream itself rather than the absence of raw bytes:
	// deflate may emit a stored block for tiny inputs (Go 1.27 does for this
	// 9-byte sample), so raw sample bytes inside the stream are legitimate.
	var want bytes.Buffer
	zw := zlib.NewWriter(&want)
	if _, err := zw.Write(h264AVCSampleWithLengthSize2()); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(buffer.Bytes(), want.Bytes()) {
		t.Fatalf("file does not carry the zlib-encoded AVC sample")
	}

	demuxer, err := NewDemuxer(bytes.NewReader(buffer.Bytes()), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(annexB))}
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(packet.Data, annexB) {
		t.Fatalf("packet data = %v, want %v", packet.Data, annexB)
	}
}

func TestMuxerRejectsHeaderStrippingPacketWithoutPrefix(t *testing.T) {
	var buffer bytes.Buffer
	muxer, err := NewMuxer(&buffer, MuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	trackID, err := muxer.AddTrack(Track{
		Type:  TrackVideo,
		Codec: CodecVP8,
		ContentEncodings: []ContentEncoding{{
			Type:           ContentEncodingTypeCompression,
			CompressionSet: true,
			Compression: ContentCompression{
				Algorithm: ContentCompAlgoHeaderStripping,
				Settings:  []byte("HS:"),
			},
		}},
		Video: VideoConfig{Width: 640, Height: 360},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := muxer.WritePacket(Packet{TrackID: trackID, TimeNS: 0, Keyframe: true, Data: []byte("missing")}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
	if err := muxer.WriteLacedPacket(LacedPacket{
		TrackID:  trackID,
		TimeNS:   0,
		Keyframe: true,
		Lacing:   LacingXiph,
		Frames:   [][]byte{[]byte("HS:first"), []byte("missing")},
	}); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("laced err = %v, want ErrInvalidData", err)
	}
}

func TestMuxerRejectsUnsupportedBlockContentEncoding(t *testing.T) {
	tests := []struct {
		name      string
		encodings []ContentEncoding
		wantError error
	}{
		{
			name: "bzlib compression settings",
			encodings: []ContentEncoding{{
				Type:           ContentEncodingTypeCompression,
				CompressionSet: true,
				Compression:    ContentCompression{Algorithm: ContentCompAlgoBzlib, Settings: []byte{1}},
			}},
			wantError: ErrUnsupportedContentEncoding,
		},
		{
			name: "lzo compression",
			encodings: []ContentEncoding{{
				Type:           ContentEncodingTypeCompression,
				CompressionSet: true,
				Compression:    ContentCompression{Algorithm: ContentCompAlgoLZO1X},
			}},
			wantError: ErrLZOContentEncoding,
		},
		{
			name:      "chained header stripping and lzo compression",
			encodings: chainedHeaderLZOContentEncodings([]byte("HS:")),
			wantError: ErrLZOContentEncoding,
		},
		{
			name: "bzlib compression",
			encodings: []ContentEncoding{{
				Type:           ContentEncodingTypeCompression,
				CompressionSet: true,
				Compression:    ContentCompression{Algorithm: ContentCompAlgoBzlib},
			}},
			wantError: ErrBzip2ContentEncodingWrite,
		},
		{
			name:      "chained header stripping and bzlib compression",
			encodings: chainedHeaderBzlibBlockContentEncodings([]byte("HS:")),
			wantError: ErrBzip2ContentEncodingWrite,
		},
		{
			name: "lzo compression settings",
			encodings: []ContentEncoding{{
				Type:           ContentEncodingTypeCompression,
				CompressionSet: true,
				Compression:    ContentCompression{Algorithm: ContentCompAlgoLZO1X, Settings: []byte{1}},
			}},
			wantError: ErrLZOContentEncoding,
		},
		{
			name: "cbc block encryption",
			encodings: []ContentEncoding{{
				Type:          ContentEncodingTypeEncryption,
				EncryptionSet: true,
				Encryption: ContentEncryption{
					Algorithm:      ContentEncAlgoAES,
					AESSettingsSet: true,
					AESSettings:    ContentEncAESSettings{CipherMode: ContentEncAESCipherModeCBC},
				},
			}},
			wantError: ErrUnsupportedContentEncoding,
		},
		{
			name: "multiple header stripping encodings",
			encodings: []ContentEncoding{
				{
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoHeaderStripping, Settings: []byte("A")},
				},
				{
					Order:          1,
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoHeaderStripping, Settings: []byte("B")},
				},
			},
			wantError: ErrUnsupportedContentEncoding,
		},
		{
			name: "multiple block compressions",
			encodings: []ContentEncoding{
				{
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
				},
				{
					Order:          1,
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoBzlib},
				},
			},
			wantError: ErrUnsupportedContentEncoding,
		},
		{
			name: "header stripping after zlib",
			encodings: []ContentEncoding{
				{
					Order:          0,
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoZlib},
				},
				{
					Order:          1,
					Type:           ContentEncodingTypeCompression,
					CompressionSet: true,
					Compression:    ContentCompression{Algorithm: ContentCompAlgoHeaderStripping, Settings: []byte("HS:")},
				},
			},
			wantError: ErrUnsupportedContentEncoding,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			muxer, err := NewMuxer(discardWriter{}, MuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			_, err = muxer.AddTrack(Track{
				Type:             TrackVideo,
				Codec:            CodecVP8,
				ContentEncodings: tt.encodings,
				Video:            VideoConfig{Width: 16, Height: 16},
			})
			if !errors.Is(err, tt.wantError) {
				t.Fatalf("err = %v, want %v", err, tt.wantError)
			}
		})
	}
}

func TestDemuxerRejectsUnsupportedBlockContentEncoding(t *testing.T) {
	tests := []struct {
		name      string
		encodings []byte
		frame     []byte
	}{
		{
			name: "block encryption",
			encodings: contentEncodingsPayload(t,
				contentEncodingPayload(t, func(w *ebml.Writer) error {
					if err := w.WriteUInt(idContentEncodingType, ContentEncodingTypeEncryption); err != nil {
						return err
					}
					return w.WriteElement(idContentEncryption, contentEncryptionPayload(t, ContentEncAlgoAES, nil, ContentEncAESCipherModeCTR))
				}),
			),
			frame: append(append([]byte{contentEncryptionSignalEncrypted}, aesCTRContentEncryptionInitialIV()...), []byte("compressed")...),
		},
		{
			name: "header stripping after zlib",
			encodings: contentEncodingsPayload(t,
				contentEncodingPayload(t, func(w *ebml.Writer) error {
					if err := w.WriteUInt(idContentEncodingOrd, 0); err != nil {
						return err
					}
					return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoZlib, nil))
				}),
				contentEncodingPayload(t, func(w *ebml.Writer) error {
					if err := w.WriteUInt(idContentEncodingOrd, 1); err != nil {
						return err
					}
					return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoHeaderStripping, []byte("HS:")))
				}),
			),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := tt.frame
			if frame == nil {
				frame = []byte("compressed")
			}
			data := makeContentEncodedBlockMatroskaData(t, tt.encodings, frame)
			demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, 32)}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrUnsupportedContentEncoding) {
				t.Fatalf("err = %v, want ErrUnsupportedContentEncoding", err)
			}
		})
	}
}

func TestDemuxerRejectsLZOContentEncoding(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "simple block",
			data: makeContentEncodedBlockMatroskaData(t, lzoContentEncodings(t), lzoTestPayload("simple block")),
		},
		{
			name: "laced block",
			data: makeContentEncodedLacedMatroskaData(t, lzoContentEncodings(t), [][]byte{
				lzoTestPayload("laced first"),
				lzoTestPayload("laced second"),
			}),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			demuxer, err := NewDemuxer(bytes.NewReader(tt.data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			packet := Packet{Data: make([]byte, 0, 1024)}
			if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrLZOContentEncoding) {
				t.Fatalf("err = %v, want ErrLZOContentEncoding", err)
			}
		})
	}
}

func TestDemuxerReadsBzlibContentEncoding(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		read func(*Demuxer)
	}{
		{
			name: "simple block",
			data: makeContentEncodedBlockMatroskaData(t, bzlibContentEncodings(t), bzlibCompressedPayload(t, "simple block")),
			read: func(demuxer *Demuxer) {
				want := bzlibTestPayload("simple block")
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "laced block",
			data: makeContentEncodedLacedMatroskaData(t, bzlibContentEncodings(t), [][]byte{
				bzlibCompressedPayload(t, "laced first"),
				bzlibCompressedPayload(t, "laced second"),
			}),
			read: func(demuxer *Demuxer) {
				first := bzlibTestPayload("laced first")
				packet := Packet{Data: make([]byte, 0, len(first))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, first) {
					t.Fatalf("first packet = %+v data=%q, want %q", packet, packet.Data, first)
				}
				second := bzlibTestPayload("laced second")
				packet.Data = make([]byte, 0, len(second))
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
					t.Fatalf("second packet = %+v data=%q, want %q", packet, packet.Data, second)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			demuxer, err := NewDemuxer(bytes.NewReader(tt.data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer)
		})
	}
}

func TestDemuxerReadsChainedHeaderStrippingAndBzlibContentEncoding(t *testing.T) {
	settings := []byte("HS:")
	tests := []struct {
		name string
		data []byte
		read func(*Demuxer)
	}{
		{
			name: "simple block",
			data: makeContentEncodedBlockMatroskaData(t, chainedHeaderBzlibContentEncodings(t, settings), bzlibCompressedPayload(t, "simple block")),
			read: func(demuxer *Demuxer) {
				want := withContentEncodingHeader(settings, bzlibTestPayload("simple block"))
				packet := Packet{Data: make([]byte, 0, len(want))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(packet.Data, want) {
					t.Fatalf("packet data = %q, want %q", packet.Data, want)
				}
			},
		},
		{
			name: "laced block",
			data: makeContentEncodedLacedMatroskaData(t, chainedHeaderBzlibContentEncodings(t, settings), [][]byte{
				bzlibCompressedPayload(t, "laced first"),
				bzlibCompressedPayload(t, "laced second"),
			}),
			read: func(demuxer *Demuxer) {
				first := withContentEncodingHeader(settings, bzlibTestPayload("laced first"))
				packet := Packet{Data: make([]byte, 0, len(first))}
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 0 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, first) {
					t.Fatalf("first packet = %+v data=%q, want %q", packet, packet.Data, first)
				}
				second := withContentEncodingHeader(settings, bzlibTestPayload("laced second"))
				packet.Data = make([]byte, 0, len(second))
				if err := demuxer.ReadPacket(&packet); err != nil {
					t.Fatal(err)
				}
				if packet.TimeNS != 20_000_000 || packet.DurationNS != 20_000_000 || !bytes.Equal(packet.Data, second) {
					t.Fatalf("second packet = %+v data=%q, want %q", packet, packet.Data, second)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			demuxer, err := NewDemuxer(bytes.NewReader(tt.data), DemuxerOptions{})
			if err != nil {
				t.Fatal(err)
			}
			tt.read(demuxer)
		})
	}
}

func TestDemuxerRetriesBzlibLacedFrameAfterSmallBuffer(t *testing.T) {
	data := makeContentEncodedLacedMatroskaData(t, bzlibContentEncodings(t), [][]byte{
		bzlibCompressedPayload(t, "laced first"),
		bzlibCompressedPayload(t, "laced second"),
	})
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	first := bzlibTestPayload("laced first")
	packet := Packet{Data: make([]byte, 0, 4)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrPayloadTooSmall) {
		t.Fatalf("err = %v, want ErrPayloadTooSmall", err)
	}
	packet.Data = make([]byte, 0, len(first))
	if err := demuxer.ReadPacket(&packet); err != nil {
		t.Fatal(err)
	}
	if packet.TimeNS != 0 || !bytes.Equal(packet.Data, first) {
		t.Fatalf("retry packet = %+v data=%q, want %q", packet, packet.Data, first)
	}
}

func TestDemuxerRejectsInvalidBzlibContentEncodingPayload(t *testing.T) {
	data := makeContentEncodedBlockMatroskaData(t, bzlibContentEncodings(t), []byte("not a bzip2 stream"))
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 32)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerRejectsInvalidZlibContentEncodingPayload(t *testing.T) {
	data := makeContentEncodedBlockMatroskaData(t, contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoZlib, nil))
		}),
	), []byte("not a zlib stream"))
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, 32)}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

func TestDemuxerRejectsTruncatedZlibContentEncodingPayload(t *testing.T) {
	var compressed bytes.Buffer
	zw := zlib.NewWriter(&compressed)
	if _, err := zw.Write(zlibTestPayload("truncated stream")); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	payload := compressed.Bytes()
	data := makeContentEncodedBlockMatroskaData(t, contentEncodingsPayload(t,
		contentEncodingPayload(t, func(w *ebml.Writer) error {
			return w.WriteElement(idContentCompression, contentCompressionPayload(t, ContentCompAlgoZlib, nil))
		}),
	), payload[:len(payload)-1])
	demuxer, err := NewDemuxer(bytes.NewReader(data), DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	packet := Packet{Data: make([]byte, 0, len(zlibTestPayload("truncated stream")))}
	if err := demuxer.ReadPacket(&packet); !errors.Is(err, ErrInvalidData) {
		t.Fatalf("err = %v, want ErrInvalidData", err)
	}
}

package native_test

import (
	"context"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/native"
	"github.com/thesyncim/goav/runconfig"
	goh264 "github.com/thesyncim/goh264"
)

const benchWidth, benchHeight = 1280, 720

var benchBase = av.TimeBase{Num: 1, Den: 90000}

func benchFrame(n int) *av.Frame {
	w, h := benchWidth, benchHeight
	y, u, v := make([]byte, w*h), make([]byte, w*h/4), make([]byte, w*h/4)
	for r := range h {
		for c := range w {
			y[r*w+c] = byte((c*3 + r*2 + n*4) % 200)
		}
	}
	for i := range u {
		u[i], v[i] = byte(64+i%64), byte(96+i%32)
	}
	return &av.Frame{
		Type:     av.MediaVideo,
		PTS:      av.Timestamp{Value: int64(n) * 3000, Base: benchBase},
		Duration: av.Duration{Value: 3000, Base: benchBase},
		Video:    &av.VideoFrame{Width: w, Height: h, PixelFormat: av.PixelFormatI420},
		Planes:   []av.Plane{{Buffer: av.Buffer{Bytes: y}, Stride: w}, {Buffer: av.Buffer{Bytes: u}, Stride: w / 2}, {Buffer: av.Buffer{Bytes: v}, Stride: w / 2}},
	}
}

func benchEncoder(b *testing.B, options runconfig.Option, id av.CodecID, backend string) codec.Encoder {
	b.Helper()
	config, err := runconfig.NewConfig(options)
	if err != nil {
		b.Fatal(err)
	}
	choice, err := config.Codecs.Select(codec.Need{ID: id, Mode: codec.ModeEncode, Backend: backend})
	if err != nil {
		b.Skipf("%s/%s: %v", id, backend, err)
	}
	encoder, err := choice.Encoder.NewEncoder(context.Background(), codec.EncodeConfig{
		Stream:     av.Stream{Type: av.MediaVideo, TimeBase: benchBase, Codec: av.CodecParameters{Type: av.MediaVideo, Width: benchWidth, Height: benchHeight, PixelFormat: av.PixelFormatI420}},
		Parameters: av.CodecParameters{ID: id, Type: av.MediaVideo, Width: benchWidth, Height: benchHeight, ClockRate: 90000},
		Settings:   codec.CodecSettings{Bitrate: 4_000_000, KeyframeInterval: 60, Backend: backend},
	})
	if err != nil {
		b.Skipf("%s/%s: %v", id, backend, err)
	}
	return encoder
}

// BenchmarkEncode720p measures encode frames per second for each native
// backend against the pure-Go encoders the bundle ships (goav has no
// pure-Go H.264 encoder, so VP8 and AV1 stand in).
func BenchmarkEncode720p(b *testing.B) {
	for _, c := range []struct {
		name    string
		options runconfig.Option
		id      av.CodecID
		backend string
	}{
		{"h264/videotoolbox", native.VideoToolbox(), av.CodecH264, "videotoolbox"},
		{"hevc/videotoolbox", native.VideoToolbox(), "hevc", "videotoolbox"},
		{"h264/openh264", native.OpenH264(), av.CodecH264, "openh264"},
		{"vp8/govpx", bundle.Codecs(), av.CodecVP8, "govpx"},
		{"av1/goav1", bundle.Codecs(), av.CodecAV1, "goav1"},
	} {
		b.Run(c.name, func(b *testing.B) {
			encoder := benchEncoder(b, c.options, c.id, c.backend)
			defer encoder.Close()
			frames := []*av.Frame{benchFrame(0), benchFrame(1), benchFrame(2), benchFrame(3)}
			// A result like the runtime's: pure-Go encoders write into its
			// payload capacity.
			result := codec.EncodeResult{Packets: []av.Packet{{Payload: av.Buffer{Bytes: make([]byte, 0, 4<<20)}}}[:0]}
			start := time.Now()
			n := 0
			for b.Loop() {
				frame := *frames[n%len(frames)]
				frame.PTS.Value = int64(n) * 3000
				if err := encoder.EncodeInto(context.Background(), &frame, &result); err != nil {
					b.Fatal(err)
				}
				for i := range result.Packets {
					if result.Packets[i].Payload.Owner != nil {
						result.Packets[i].Payload.Release()
					}
				}
				result.Reset()
				n++
			}
			b.ReportMetric(float64(n)/time.Since(start).Seconds(), "fps")
		})
	}
}

// BenchmarkDecode720p measures H.264 decode frames per second for each
// native backend against goh264, on one stream OpenH264 or VideoToolbox
// encoded.
func BenchmarkDecode720p(b *testing.B) {
	var stream [][]byte
	for _, options := range []runconfig.Option{native.OpenH264(), native.VideoToolbox()} {
		config, _ := runconfig.NewConfig(options)
		if _, err := config.Codecs.Select(codec.Need{ID: av.CodecH264, Mode: codec.ModeEncode}); err == nil {
			encoder := benchEncoder(b, options, av.CodecH264, "")
			result := codec.EncodeResult{}
			for n := range 60 {
				encoder.EncodeInto(context.Background(), benchFrame(n), &result)
				encoder.FlushInto(context.Background(), &result)
			}
			for i := range result.Packets {
				stream = append(stream, append([]byte(nil), result.Packets[i].Payload.Bytes...))
			}
			encoder.Close()
			break
		}
	}
	if len(stream) == 0 {
		b.Skip("no native H.264 encoder to make the stream")
	}
	b.Run("h264/goh264", func(b *testing.B) {
		start, n := time.Now(), 0
		for b.Loop() {
			decoder := goh264.NewDecoder()
			for _, unit := range stream {
				frames, err := decoder.DecodeFrames(unit)
				if err != nil {
					b.Fatal(err)
				}
				n += len(frames)
			}
			frames, _ := decoder.FlushDelayedFrames()
			n += len(frames)
		}
		b.ReportMetric(float64(n)/time.Since(start).Seconds(), "fps")
	})
	for _, c := range []struct {
		name    string
		options runconfig.Option
		backend string
	}{
		{"h264/openh264", native.OpenH264(), "openh264"},
		{"h264/videotoolbox", native.VideoToolbox(), "videotoolbox"},
	} {
		b.Run(c.name, func(b *testing.B) {
			config, err := runconfig.NewConfig(c.options)
			if err != nil {
				b.Fatal(err)
			}
			choice, err := config.Codecs.Select(codec.Need{ID: av.CodecH264, Mode: codec.ModeDecode, Backend: c.backend})
			if err != nil {
				b.Skip(err)
			}
			start, n := time.Now(), 0
			for b.Loop() {
				decoder, err := choice.Decoder.NewDecoder(context.Background(), codec.DecodeConfig{Stream: av.Stream{Type: av.MediaVideo, TimeBase: benchBase, Codec: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo}}})
				if err != nil {
					b.Skip(err)
				}
				result := codec.DecodeResult{}
				for i, unit := range stream {
					packet := av.Packet{Payload: av.Buffer{Bytes: unit}, PTS: av.Timestamp{Value: int64(i) * 3000, Base: benchBase}, Keyframe: i%60 == 0}
					if err := decoder.DecodeInto(context.Background(), &packet, &result); err != nil {
						b.Fatal(err)
					}
					n += len(result.Frames)
					for j := range result.Frames {
						result.Frames[j].Release()
					}
					result.Reset()
				}
				decoder.FlushInto(context.Background(), &result)
				n += len(result.Frames)
				for j := range result.Frames {
					result.Frames[j].Release()
				}
				result.Reset()
				decoder.Close()
			}
			b.ReportMetric(float64(n)/time.Since(start).Seconds(), "fps")
		})
	}
}

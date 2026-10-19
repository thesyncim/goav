package native_test

import (
	"context"
	"math"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/native"
	"github.com/thesyncim/goav/native/kit"
	"github.com/thesyncim/goav/runconfig"
	goh264 "github.com/thesyncim/goh264"
)

func TestOpenH264SmokeRoundTrip(t *testing.T) {
	if err := kit.Probe(native.OpenH264()); err != nil {
		t.Skip(err)
	}
	config, err := runconfig.NewConfig(native.OpenH264())
	if err != nil {
		t.Fatal(err)
	}
	choice, err := config.Codecs.Select(codec.Need{ID: av.CodecH264, Mode: codec.ModeEncode})
	if err != nil {
		t.Fatal(err)
	}
	const w, h = 320, 240
	base := av.TimeBase{Num: 1, Den: 90000}
	enc, err := choice.Encoder.NewEncoder(context.Background(), codec.EncodeConfig{
		Stream:     av.Stream{Type: av.MediaVideo, TimeBase: base, Codec: av.CodecParameters{ID: av.CodecH264, Width: w, Height: h}},
		Parameters: av.CodecParameters{ID: av.CodecH264, Type: av.MediaVideo, Width: w, Height: h},
		Settings:   codec.CodecSettings{Bitrate: 1_000_000},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	dec := goh264.NewDecoder()
	var psnrSum float64
	var decoded int
	for i := range 30 {
		frame := pattern(w, h, i)
		frame.PTS = av.Timestamp{Value: int64(i) * 3000, Base: base}
		var out codec.EncodeResult
		if err := enc.EncodeInto(context.Background(), frame, &out); err != nil {
			t.Fatal(err)
		}
		for _, p := range out.Packets {
			pics, err := dec.DecodeFrames(p.Payload.Bytes)
			if err != nil {
				t.Fatal(err)
			}
			for _, pic := range pics {
				psnrSum += psnr(frame.Planes[0].Buffer.Bytes, pic.Y, w, h, pic.YStride)
				decoded++
			}
			p.Release()
		}
	}
	pics, _ := dec.FlushDelayedFrames()
	decoded += len(pics)
	if decoded != 30 {
		t.Fatalf("decoded %d frames, want 30", decoded)
	}
	t.Logf("mean luma PSNR %.1f dB", psnrSum/float64(decoded))
}

func pattern(w, h, n int) *av.Frame {
	y := make([]byte, w*h)
	for r := range h {
		for c := range w {
			y[r*w+c] = byte((c + r + n*3) % 256)
		}
	}
	u := make([]byte, w*h/4)
	v := make([]byte, w*h/4)
	for i := range u {
		u[i], v[i] = 100, 150
	}
	return &av.Frame{Type: av.MediaVideo, Video: &av.VideoFrame{Width: w, Height: h, PixelFormat: av.PixelFormatI420},
		Planes: []av.Plane{{Buffer: av.Buffer{Bytes: y}, Stride: w}, {Buffer: av.Buffer{Bytes: u}, Stride: w / 2}, {Buffer: av.Buffer{Bytes: v}, Stride: w / 2}}}
}

func psnr(a, b []byte, w, h, stride int) float64 {
	var mse float64
	for r := range h {
		for c := range w {
			d := float64(a[r*w+c]) - float64(b[r*stride+c])
			mse += d * d
		}
	}
	mse /= float64(w * h)
	if mse == 0 {
		return 99
	}
	return 10 * math.Log10(255*255/mse)
}

package goav_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// solidVideo is an I420 source of frames whose luma is one value, so a pixel
// of the composited canvas names the arm it came from.
func solidVideo(name string, width, height, frames int, luma byte) goav.InputSpec {
	return goav.Source(name, shape.Frame(av.MediaVideo, shape.Video(width, height, av.PixelFormatI420)),
		func(_ context.Context, push source.Push) error {
			base := av.TimeBase{Num: 1, Den: 30}
			for n := range frames {
				cw, ch := (width+1)/2, (height+1)/2
				y, u, v := bytes.Repeat([]byte{luma}, width*height), bytes.Repeat([]byte{128}, cw*ch), bytes.Repeat([]byte{128}, cw*ch)
				if _, err := push.Frame(&av.Frame{
					Type: av.MediaVideo, PTS: av.Timestamp{Value: int64(n), Base: base}, Duration: av.Duration{Value: 1, Base: base},
					Video:  &av.VideoFrame{Width: width, Height: height, PixelFormat: av.PixelFormatI420},
					Planes: []av.Plane{{Buffer: av.Buffer{Bytes: y}, Stride: width}, {Buffer: av.Buffer{Bytes: u}, Stride: cw}, {Buffer: av.Buffer{Bytes: v}, Stride: cw}},
				}); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// TestCompositePictureInPicture pins picture-in-picture: an arm resized on
// its own chain is drawn at its new size where At places it, on a canvas the
// size of the main picture, from frame sources and from files whose size is
// known only when they open; Describe plans the same resize node the build
// runs.
func TestCompositePictureInPicture(t *testing.T) {
	encode := func(in goav.InputSpec) []byte {
		var out bytes.Buffer
		if err := offlineRuntime().Run(context.Background(), in.Video().Encode(codec.VP8()).To(goav.Write("v.webm", &out))); err != nil {
			t.Fatal(err)
		}
		return out.Bytes()
	}
	camFile, screenFile := encode(solidVideo("cam", 96, 64, 3, 60)), encode(solidVideo("screen", 96, 64, 3, 200))
	for _, tc := range []struct {
		name        string
		cam, screen goav.Stream
	}{
		{"frames", solidVideo("cam", 96, 64, 3, 60).Video(), solidVideo("screen", 96, 64, 3, 200).Video()},
		{"files", goav.FileInput("cam.webm", bytes.NewReader(camFile)).Video().Decode(), goav.FileInput("screen.webm", bytes.NewReader(screenFile)).Video().Decode()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pip := goav.Composite(tc.cam, goav.At(tc.screen.Resize(32, 24), 56, 32))
			out := goavtest.NewCollector()
			spec, err := offlineRuntime().Describe(pip.To(out.Sink()))
			if err != nil {
				t.Fatalf("describe: %v", err)
			}
			if got := countNodesWithPrefix(spec, "composite-resize-"); got != 1 {
				t.Fatalf("planned %d arm resize nodes in %v, want 1", got, specNodeNames(spec))
			}
			if err := offlineRuntime().Run(context.Background(), pip.To(out.Sink())); err != nil {
				t.Fatalf("run: %v", err)
			}
			frames := out.Frames()
			if len(frames) == 0 {
				t.Fatal("no composited frames")
			}
			frame := frames[len(frames)-1]
			if frame.Video.Width != 96 || frame.Video.Height != 64 {
				t.Fatalf("canvas %dx%d, want the main picture's 96x64", frame.Video.Width, frame.Video.Height)
			}
			luma := func(x, y int) byte { return frame.Planes[0].Buffer.Bytes[y*frame.Planes[0].Stride+x] }
			if near(luma(70, 44), 200) && near(luma(10, 10), 60) && near(luma(50, 44), 60) {
				return
			}
			t.Fatalf("luma inside the inset %d, outside %d and %d; want about 200, 60, 60", luma(70, 44), luma(10, 10), luma(50, 44))
		})
	}
}

func near(got, want byte) bool { return got >= want-12 && got <= want+12 }

// TestCompositeArmRefusesWorkItCannotLower pins the arm grammar's edges: a
// transform before the arm decodes, a resample on a video arm, and an encode
// on an arm are refused with the fix.
func TestCompositeArmRefusesWorkItCannotLower(t *testing.T) {
	file := goav.FileInput("cam.webm", bytes.NewReader(nil))
	for _, tc := range []struct {
		arm  goav.Stream
		want string
	}{
		{file.Video().Resize(32, 24), "decode the arm before transforming it"},
		{solidVideo("a", 16, 16, 1, 1).Video().Resample(48000, 1), "transform the arm's own media"},
		{solidVideo("b", 16, 16, 1, 1).Video().Encode(codec.VP8()), "encode, or run stages on, the JOINED stream"},
	} {
		_, err := offlineRuntime().Describe(goav.Composite(solidVideo("main", 16, 16, 1, 1).Video(), tc.arm).To(goavtest.NewCollector().Sink()))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("err = %v, want %q", err, tc.want)
		}
	}
}

// TestCompositeArmTransformNeedsItsAdapter pins that an arm's resize is
// checked like a chain's: a runtime without the resize filter refuses it at
// plan time, naming the adapter to register.
func TestCompositeArmTransformNeedsItsAdapter(t *testing.T) {
	_, err := mustRuntime().Describe(goav.Composite(solidVideo("main", 16, 16, 1, 1).Video(), solidVideo("inset", 16, 16, 1, 1).Video().Resize(8, 8)).To(goavtest.NewCollector().Sink()))
	requireBuildError(t, err, errcode.Code("transform_adapter_missing"), "build composite", "inset", "bundle.Filters()")
}

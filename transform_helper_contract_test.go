package goav

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/shape"
)

func TestTransformSpecIsOpaque(t *testing.T) {
	typ := reflect.TypeOf(TransformSpec{})
	for i := 0; i < typ.NumField(); i++ {
		if typ.Field(i).IsExported() {
			t.Fatalf("TransformSpec field %s is exported; use constructors instead", typ.Field(i).Name)
		}
	}
}

func TestTransformSpecConstructorsApplyOptions(t *testing.T) {
	resize := Resize(640, 360, nil, func(config *filter.ResizeConfig) {
		config.PixelFormat = av.PixelFormatI420
		config.Mode = filter.ResizeFit
	})
	if resize.resize == nil ||
		resize.resize.Width != 640 ||
		resize.resize.Height != 360 ||
		resize.resize.PixelFormat != av.PixelFormatI420 ||
		resize.resize.Mode != filter.ResizeFit ||
		resize.resample != nil {
		t.Fatalf("Resize spec = %+v", resize)
	}

	resample := Resample(48_000, codec.Stereo, nil, func(config *filter.ResampleConfig) {
		config.ChannelLayout = "stereo"
		config.SampleFormat = av.SampleFormatF32
	})
	if resample.resample == nil ||
		resample.resample.SampleRate != 48_000 ||
		resample.resample.Channels != codec.Stereo ||
		resample.resample.ChannelLayout != "stereo" ||
		resample.resample.SampleFormat != av.SampleFormatF32 ||
		resample.resize != nil {
		t.Fatalf("Resample spec = %+v", resample)
	}

	if got := resizeModeWithDefault(""); got != filter.ResizeExact {
		t.Fatalf("default resize mode = %s, want exact", got)
	}
	if got := resizeModeWithDefault(filter.ResizeFill); got != filter.ResizeFill {
		t.Fatalf("explicit resize mode = %s, want fill", got)
	}
}

func TestStreamTransformContracts(t *testing.T) {
	resize, err := streamTransform("", av.StreamSelector{ID: "cam", Type: av.MediaVideo}, Resize(320, 180), 1)
	if err != nil {
		t.Fatal(err)
	}
	if resize.name != "resize-cam-2" ||
		resize.factory != filter.FactoryResize ||
		resize.video == nil ||
		resize.video.Width != 320 ||
		resize.video.Height != 180 {
		t.Fatalf("resize transform = %+v", resize)
	}

	resample, err := streamTransform("voice", av.StreamSelector{Type: av.MediaAudio}, Resample(16_000, codec.Mono), 0)
	if err != nil {
		t.Fatal(err)
	}
	if resample.name != "resample-voice" ||
		resample.factory != filter.FactoryResample ||
		resample.audio == nil ||
		resample.audio.SampleRate != 16_000 ||
		resample.audio.Channels != codec.Mono {
		t.Fatalf("resample transform = %+v", resample)
	}

	assertTransformBuildError := func(name string, spec TransformSpec, selector av.StreamSelector, code errcode.Code, fragment string) {
		t.Helper()
		_, err := streamTransform(name, selector, spec, 0)
		var buildErr *BuildError
		if !errors.As(err, &buildErr) || buildErr.Code != code {
			t.Fatalf("err = %v, want %s", err, code)
		}
		if fragment != "" && !strings.Contains(buildErr.Reason, fragment) {
			t.Fatalf("reason = %q, want %q", buildErr.Reason, fragment)
		}
	}
	assertTransformBuildError("", TransformSpec{}, av.StreamSelector{Type: av.MediaAudio}, errcode.TransformInvalid, "empty stream transform")
	assertTransformBuildError("both", TransformSpec{
		resize:   &filter.ResizeConfig{Width: 320, Height: 180},
		resample: &filter.ResampleConfig{SampleRate: 48_000, Channels: codec.Stereo},
	}, av.StreamSelector{Type: av.MediaAudio}, errcode.TransformInvalid, "cannot be both")
	assertTransformBuildError("audio", Resize(320, 180), av.StreamSelector{Type: av.MediaAudio}, errcode.TransformMediaMismatch, "resize applies to video")
	assertTransformBuildError("video", Resample(48_000, codec.Stereo), av.StreamSelector{Type: av.MediaVideo}, errcode.TransformMediaMismatch, "resample applies to audio")
}

func TestTransformHelperErrorContracts(t *testing.T) {
	if err := validateTransformSpec("build stream", "both", TransformSpec{
		resize:   &filter.ResizeConfig{Width: 320, Height: 180},
		resample: &filter.ResampleConfig{SampleRate: 48_000, Channels: codec.Stereo},
	}); err == nil {
		t.Fatal("combined transform validation succeeded, want transform_invalid")
	}
	if err := validateTransformSpec("build stream", "empty", TransformSpec{}); err != nil {
		t.Fatalf("empty transform validation = %v, want streamTransform to reject later", err)
	}

	if in, out := transformAdapterExpectedMedia("custom-filter"); in != "" || out != "" {
		t.Fatalf("custom transform expected media = %s/%s, want empty", in, out)
	}
	if got := transformMethodName("custom-filter"); got != "Do" {
		t.Fatalf("custom transform method = %q, want Do", got)
	}

	cause := errors.New("registry unavailable")
	stream := streamIntent{Name: "voice", Select: streamSelectFromAV(av.StreamSelector{Type: av.MediaAudio})}
	if err := recipeTransformAdapterError("build stream", stream, filter.FactoryResample, cause); !errors.Is(err, cause) {
		t.Fatalf("non-not-found error = %v, want original cause", err)
	}
	err := recipeTransformAdapterError("build stream", stream, filter.FactoryResample, filter.ErrNotFound)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != errcode.TransformAdapterMissing ||
		buildErr.Node != "voice" ||
		!strings.Contains(buildErr.Reason, "no resample filter adapter is registered") ||
		!strings.Contains(strings.Join(buildErr.FixLines(), "\n"), ".Resample") {
		t.Fatalf("missing adapter error = %#v", err)
	}

	mediaErr := transformMediaError("voice", "resize", av.MediaVideo, av.MediaAudio)
	if !strings.Contains(mediaErr.Error(), shape.Frame(av.MediaVideo).String()) ||
		!strings.Contains(mediaErr.Error(), shape.Frame(av.MediaAudio).String()) {
		t.Fatalf("media error = %v", mediaErr)
	}
}

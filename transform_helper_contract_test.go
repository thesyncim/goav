package goav

import (
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/recipeir"
	"github.com/thesyncim/goav/shape"
)

func TestTransformSpecConstructorsApplyOptions(t *testing.T) {
	resize := resizeSpec(640, 360, nil, func(config *filter.ResizeConfig) {
		config.PixelFormat = av.PixelFormatI420
		config.Mode = filter.ResizeFit
	})
	if resize.Kind != recipeir.TransformResize ||
		resize.Resize.Width != 640 ||
		resize.Resize.Height != 360 ||
		resize.Resize.PixelFormat != av.PixelFormatI420 ||
		resize.Resize.Mode != filter.ResizeFit {
		t.Fatalf("Resize spec = %+v", resize)
	}

	resample := resampleSpec(48_000, codec.Stereo, nil, func(config *filter.ResampleConfig) {
		config.ChannelLayout = "stereo"
		config.SampleFormat = av.SampleFormatF32
	})
	if resample.Kind != recipeir.TransformResample ||
		resample.Resample.SampleRate != 48_000 ||
		resample.Resample.Channels != codec.Stereo ||
		resample.Resample.ChannelLayout != "stereo" ||
		resample.Resample.SampleFormat != av.SampleFormatF32 {
		t.Fatalf("Resample spec = %+v", resample)
	}

	if got := resizeModeWithDefault(""); got != filter.ResizeExact {
		t.Fatalf("default resize mode = %s, want exact", got)
	}
	if got := resizeModeWithDefault(filter.ResizeFill); got != filter.ResizeFill {
		t.Fatalf("explicit resize mode = %s, want fill", got)
	}
}

func TestTransformHelperErrorContracts(t *testing.T) {
	err := validateTransformSpec("build stream", "empty", recipeir.Transform{})
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != transformInvalidCode ||
		!strings.Contains(buildErr.Reason, "empty stream transform") {
		t.Fatalf("empty transform validation = %v, want transform_invalid", err)
	}

	if in, out := transformAdapterExpectedMedia("custom-filter"); in != "" || out != "" {
		t.Fatalf("custom transform expected media = %s/%s, want empty", in, out)
	}
	if got := transformMethodName("custom-filter"); got != "Do" {
		t.Fatalf("custom transform method = %q, want Do", got)
	}

	cause := errors.New("registry unavailable")
	stream := recipeir.Stream{Name: "voice", Selector: streamSelectFromAV(av.StreamSelector{Type: av.MediaAudio})}
	if err := recipeTransformAdapterError("build stream", stream, filter.FactoryResample, cause); !errors.Is(err, cause) {
		t.Fatalf("non-not-found error = %v, want original cause", err)
	}
	err = recipeTransformAdapterError("build stream", stream, filter.FactoryResample, filter.ErrNotFound)
	var missingErr *BuildError
	if !errors.As(err, &missingErr) ||
		missingErr.Code != transformAdapterMissingCode ||
		missingErr.Node != "voice" ||
		!strings.Contains(missingErr.Reason, "no resample filter adapter is registered") ||
		!strings.Contains(strings.Join(missingErr.FixLines(), "\n"), ".Resample") {
		t.Fatalf("missing adapter error = %#v", err)
	}

	mediaErr := transformMediaError("voice", "resize", av.MediaVideo, av.MediaAudio)
	if !strings.Contains(mediaErr.Error(), shape.Frame(av.MediaVideo).String()) ||
		!strings.Contains(mediaErr.Error(), shape.Frame(av.MediaAudio).String()) {
		t.Fatalf("media error = %v", mediaErr)
	}
}

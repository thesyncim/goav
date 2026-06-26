package goav

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/thesyncim/goav/component"
)

func TestStreamRecipeNamesCodecChangePolicy(t *testing.T) {
	sink := component.SinkFunc("frames", func(context.Context, component.Message) error {
		return nil
	})
	policy := defaultCodecChangePolicy()
	job := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		OnCodecChange(policy).
		To(Sink(sink))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "codec-change=rebind-compatible,request-keyframe,drop-until-sync,fail-different-codec") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.plan()
	if len(intent.Streams) != 1 || intent.Streams[0].CodecChange != policy {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestStreamRecipeRejectsUnsupportedCodecChangePolicy(t *testing.T) {
	sink := component.SinkFunc("frames", func(context.Context, component.Message) error {
		return nil
	})
	_, err := From(FileInput("input.ogg", strings.NewReader(""))).
		Audio().
		Decode().
		OnCodecChange(codecChangePolicy{RebindCompatible: true}).
		To(Sink(sink)).
		Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "codec_change_policy_unsupported" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want codec_change_policy_unsupported wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "default live receive behavior") ||
		!strings.Contains(err.Error(), "different decoder codec") {
		t.Fatalf("err = %v, want codec-change policy guidance", err)
	}
}

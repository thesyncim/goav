package expect_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/goavtest/expect"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

func TestBuildErrorChecksStructuredFields(t *testing.T) {
	_, err := goav.From(goav.Source("bytes",
		shape.New(shape.Domain(shape.MediaDomain("bytes")), shape.Media(av.MediaAudio)),
		func(context.Context, source.Push) error { return nil },
	)).
		Audio().
		Decode().
		To(goavtest.NewCollector().Sink()).
		Describe()
	err = fmt.Errorf("wrapped: %w", err)

	buildErr := expect.BuildError(t, err, errcode.Code("source_shape_unsupported"),
		expect.Operation("build input"),
		expect.Node("bytes"),
		expect.ReasonContains("packet-domain"),
		expect.DetailContains("actual_shape="),
		expect.SuggestionContains("shape.Packet"),
	)
	expect.Equal(t, "code", buildErr.Code, errcode.Code("source_shape_unsupported"))
}

func TestValueAndGoldenHelpers(t *testing.T) {
	dir := t.TempDir()
	golden := filepath.Join(dir, "expected.txt")
	expect.NoError(t, os.WriteFile(golden, []byte("frames: [[1 2]]\n"), 0o600))

	expect.Equal(t, "accepted", 2, 2)
	expect.DeepEqual(t, "frames", [][]int16{{1, 2}}, [][]int16{{1, 2}})
	expect.Contains(t, "message", "goav: cannot build input", "build input")
	expect.StringSliceContains(t, "suggestions", []string{"use goav.Input(provider)"}, "provider")
	expect.GoldenString(t, golden, "frames: [[1 2]]\n")
}

func TestS16ChecksCollectorOutput(t *testing.T) {
	ctx := context.Background()
	out := goavtest.NewCollector()
	err := goav.From(goavtest.Audio(48000, 1, []int16{1, 2}, []int16{3})).
		Audio().
		To(out.Sink()).
		UseRuntime(goavtest.Runtime()).
		Run(ctx)
	expect.NoError(t, err)
	expect.S16(t, out, [][]int16{{1, 2}, {3}})
}

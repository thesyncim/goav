package expect_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/goavtest/expect"
)

func TestBuildErrorChecksStructuredFields(t *testing.T) {
	cause := errors.New("sentinel")
	err := fmt.Errorf("wrapped: %w", &goav.BuildError{
		Code:        errcode.InputInvalid,
		Operation:   "build input",
		Node:        "input",
		Reason:      "nil source provider",
		Details:     []string{"input=0"},
		Suggestions: []string{"pass a non-nil provider to goav.Input(provider)"},
		Cause:       cause,
	})

	buildErr := expect.BuildError(t, err, errcode.InputInvalid,
		expect.Operation("build input"),
		expect.Node("input"),
		expect.Cause(cause),
		expect.ReasonContains("nil source"),
		expect.DetailContains("input=0"),
		expect.SuggestionContains("goav.Input(provider)"),
	)
	expect.Equal(t, "code", buildErr.Code, errcode.InputInvalid)
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

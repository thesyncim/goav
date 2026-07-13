package goav_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// These golden tests prove Describe and Explain are machine-consumable: their
// output marshals to stable JSON with typed metadata (component, media kind,
// codec, format, shape, adapter, config). Detail strings are a display
// fallback, not the machine surface. Regenerate the goldens with
// GOAV_UPDATE_GOLDEN=1 go test -run 'GoldenJSON'.

func describeJSONRecipe() *goav.Job {
	return goav.From(goav.FileInput("input.webm", strings.NewReader(""))).
		Video().
		Decode().
		Resize(1280, 720).
		Encode(codec.VP9(codec.Bitrate(2_000_000))).
		To(goav.Write("preview.webm", new(bytes.Buffer)))
}

func explainJSONRecipe() *goav.Job {
	sink := goav.Sink(component.SinkFunc("packets", func(context.Context, component.Message) error {
		return nil
	}))
	input := goav.Source("audio-1", shape.Frame(av.MediaAudio, shape.Audio(44_100, 1, av.SampleFormatS16)), func(context.Context, source.Push) error {
		return nil
	})
	return goav.From(input).
		UseRuntime(bundle.MustNew()).
		Audio().
		Resample(48_000, 2).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(sink)
}

func TestDescribeGoldenJSON(t *testing.T) {
	spec, err := describeJSONRecipe().Describe()
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	got := marshalGolden(t, spec)

	// The typed graph topology is present and machine-readable.
	if !json.Valid(got) {
		t.Fatalf("Describe JSON is not valid:\n%s", got)
	}
	if !bytes.Contains(got, []byte(`"kind": "source"`)) ||
		!bytes.Contains(got, []byte(`"nodes"`)) ||
		!bytes.Contains(got, []byte(`"edges"`)) {
		t.Fatalf("Describe JSON missing typed topology:\n%s", got)
	}
	assertGolden(t, "describe_video_transcode.json", got)
}

func TestExplainGoldenJSON(t *testing.T) {
	report, err := explainJSONRecipe().Explain(context.Background())
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	got := marshalGolden(t, report)

	if !json.Valid(got) {
		t.Fatalf("Explain JSON is not valid:\n%s", got)
	}
	// Typed metadata the work item requires must be present without parsing
	// Detail: an operation kind, a codec adapter, encoder config, and a shape
	// carrying a codec.
	for _, want := range []string{
		`"kind": "encode"`,
		`"adapter": "opus"`,
		`"config"`,
		`"bitrate": "96000"`,
		`"shape"`,
		`"requiredAdapters"`,
	} {
		if !bytes.Contains(got, []byte(want)) {
			t.Fatalf("Explain JSON missing typed metadata %q:\n%s", want, got)
		}
	}
	// Transform and encode operations carry typed config consistently whether
	// they sit on a stream or a branch (a substring check would miss a branch
	// operation silently losing its config).
	assertOperationsHaveConfig(t, report.Streams, "stream")
	for _, b := range report.Branches {
		assertOperationConfig(t, b.Operations, "branch")
	}
	assertGolden(t, "explain_audio_transcode.json", got)
}

func assertOperationsHaveConfig(t *testing.T, streams []plan.Stream, where string) {
	t.Helper()
	for _, s := range streams {
		assertOperationConfig(t, s.Operations, where)
	}
}

func assertOperationConfig(t *testing.T, operations []plan.Operation, where string) {
	t.Helper()
	for _, op := range operations {
		switch op.Kind {
		case plan.OpTransform, plan.OpEncode:
			if len(op.Config) == 0 {
				t.Fatalf("%s %s operation has no typed config (machine consumers expect it)", where, op.Kind)
			}
		}
	}
}

// TestDescribeExplainJSONIsStable proves the encodings are deterministic, so a
// machine consumer (or a golden) can rely on byte-stable output.
func TestDescribeExplainJSONIsStable(t *testing.T) {
	spec, err := describeJSONRecipe().Describe()
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if a, b := marshalGolden(t, spec), marshalGolden(t, spec); !bytes.Equal(a, b) {
		t.Fatalf("Describe JSON is not stable across marshals")
	}
	report, err := explainJSONRecipe().Explain(context.Background())
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	if a, b := marshalGolden(t, report), marshalGolden(t, report); !bytes.Equal(a, b) {
		t.Fatalf("Explain JSON is not stable across marshals")
	}
}

func marshalGolden(t *testing.T, v any) []byte {
	t.Helper()
	out, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return append(out, '\n')
}

func assertGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", "golden", name)
	if os.Getenv("GOAV_UPDATE_GOLDEN") != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("mkdir golden: %v", err)
		}
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden %s (regenerate with GOAV_UPDATE_GOLDEN=1): %v", path, err)
	}
	if !bytes.Equal(want, got) {
		t.Fatalf("golden %s mismatch (regenerate with GOAV_UPDATE_GOLDEN=1):\n--- got ---\n%s", name, got)
	}
}

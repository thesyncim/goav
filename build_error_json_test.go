package goav

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
)

// commonBuildError is one named representative refusal for the machine-readable
// and golden pins. Each closure triggers a deterministic build-time refusal
// through the public grammar.
type commonBuildError struct {
	name    string
	trigger func() error
}

func commonBuildErrorCases() []commonBuildError {
	reader := func() io.Reader { return strings.NewReader("") }
	out := func() io.Writer { return new(bytes.Buffer) }
	sink := func() Destination {
		return Sink(component.SinkFunc("sink", func(context.Context, component.Message) error { return nil }))
	}
	describe := func(j *Job) func() error {
		return func() error { _, err := j.Describe(); return err }
	}
	build := func(j *Job) func() error {
		return func() error { _, err := j.Build(context.Background()); return err }
	}
	return []commonBuildError{
		{"unconstructed-job", build(&Job{})},
		{"input-missing", describe(From().Copy().To(Write("out.ivf", out())))},
		{"encode-missing", describe(From(FileInput("in.ivf", reader())).Video().Decode().To(Write("out.mp4", out())))},
		{"copy-after-decode", describe(From(FileInput("in.ivf", reader())).Video().Decode().Copy().To(Write("out.ivf", out())))},
		{"resize-on-copy", describe(From(FileInput("in.ivf", reader())).Video().Copy().Resize(1280, 720).To(sink()))},
		{"packet-tap-at-frame", describe(From(FileInput("in.ivf", reader())).Video().Decode().Tap(PacketTap("frames")).To(sink()))},
		{"runtime-missing", build(From(FileInput("in.ogg", reader())).Audio().Decode().Encode(codec.Opus(codec.Bitrate(96_000))).To(Write("out.ogg", out())))},
	}
}

// collectCommonBuildErrors triggers each representative refusal and returns the
// resulting *BuildError values, in declaration order. It fails if a case stops
// producing a structured refusal, so the golden set never silently shrinks.
func collectCommonBuildErrors(t *testing.T) []*BuildError {
	t.Helper()
	cases := commonBuildErrorCases()
	errs := make([]*BuildError, 0, len(cases))
	for _, c := range cases {
		err := c.trigger()
		var buildErr *BuildError
		if !errors.As(err, &buildErr) {
			t.Fatalf("case %q produced %v, want a *BuildError", c.name, err)
		}
		errs = append(errs, buildErr)
	}
	return errs
}

// TestErrorDetailsAreMachineReadable proves a refusal is consumable without
// parsing rendered text: every error sets the typed contract fields, marshals
// to JSON carrying them, and exposes each keyed detail through Detail(key).
func TestErrorDetailsAreMachineReadable(t *testing.T) {
	for _, buildErr := range collectCommonBuildErrors(t) {
		if buildErr.Phase == "" || buildErr.Family == "" || buildErr.Code == "" || buildErr.Reason == "" {
			t.Fatalf("error missing typed fields: %+v", buildErr)
		}
		raw, err := json.Marshal(buildErr)
		if err != nil {
			t.Fatalf("marshal %s: %v", buildErr.Code, err)
		}
		var doc struct {
			Phase   string `json:"phase"`
			Family  string `json:"family"`
			Code    string `json:"code"`
			Reason  string `json:"reason"`
			Details []struct {
				Key   string `json:"key"`
				Value any    `json:"value"`
			} `json:"details"`
		}
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("unmarshal %s: %v", buildErr.Code, err)
		}
		if doc.Phase != buildErr.Phase || doc.Family != string(buildErr.Family) ||
			doc.Code != string(buildErr.Code) || doc.Reason != buildErr.Reason {
			t.Fatalf("JSON lost typed fields for %s:\n%s", buildErr.Code, raw)
		}
		// Every keyed detail in the JSON must be reachable through Detail.
		for _, d := range doc.Details {
			if d.Key == "" {
				continue
			}
			if _, ok := buildErr.Detail(d.Key); !ok {
				t.Fatalf("detail key %q for %s is in JSON but not readable via Detail()", d.Key, buildErr.Code)
			}
		}
	}
}

// TestCommonErrorsGoldenJSON pins the JSON encoding of the representative
// refusals, proving Describe/Build errors are stable machine documents.
// Regenerate with GOAV_UPDATE_GOLDEN=1.
func TestCommonErrorsGoldenJSON(t *testing.T) {
	errs := collectCommonBuildErrors(t)
	type named struct {
		Name  string      `json:"name"`
		Error *BuildError `json:"error"`
	}
	doc := make([]named, 0, len(errs))
	for i, c := range commonBuildErrorCases() {
		doc = append(doc, named{Name: c.name, Error: errs[i]})
	}
	got, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got = append(got, '\n')

	path := filepath.Join("testdata", "golden", "common_errors.json")
	if os.Getenv("GOAV_UPDATE_GOLDEN") != "" {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden (regenerate with GOAV_UPDATE_GOLDEN=1): %v", err)
	}
	if !bytes.Equal(want, got) {
		t.Fatalf("common errors golden mismatch (regenerate with GOAV_UPDATE_GOLDEN=1):\n%s", got)
	}
}

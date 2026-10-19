package goav_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/runconfig"
)

// hardwareVP8 registers a second VP8 encoder that declares itself hardware,
// so the default preference picks it over the bundled pure-Go govpx.
func hardwareVP8() runconfig.Option {
	return runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		desc := codec.Descriptor{
			ID:   av.CodecVP8,
			Type: av.MediaVideo,
			Capabilities: codec.Capabilities{
				MaxWidth:  640,
				MaxHeight: 480,
			},
			Backend: codec.BackendInfo{Name: "fakehw", Kind: codec.KindHardware},
		}
		passthrough, _ := runconfig.NewConfig(goavtest.Codec(av.CodecVP8))
		choice, _ := passthrough.Codecs.Select(codec.Need{ID: av.CodecVP8, Mode: codec.ModeEncode})
		registry.RegisterEncoder(desc, choice.Encoder)
	})
}

func encodeDetail(t *testing.T, rt *goav.Runtime, out goav.Output) string {
	t.Helper()
	spec, err := rt.Describe(out)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	for _, node := range spec.Nodes {
		if strings.HasPrefix(node.Name, "encode") {
			return node.Detail
		}
	}
	t.Fatalf("no encode node in %+v", spec.Nodes)
	return ""
}

// TestCodecBackendSelectionIsVisible pins the multi-backend registry through
// the front door: the preferred backend serves, Describe names it, Explain
// says why and lists the alternatives, and codec.Backend overrides the order.
func TestCodecBackendSelectionIsVisible(t *testing.T) {
	rt := bundle.MustNew(hardwareVP8())
	sink := goavtest.NewCollector().Sink()
	out := goavtest.Video(320, 240, 2).Video().Encode(codec.VP8()).To(sink)
	if got := encodeDetail(t, rt, out); got != "frames -> packets, codec=vp8, backend=fakehw (hardware)" {
		t.Fatalf("Describe encode detail = %q", got)
	}
	report, err := rt.Explain(context.Background(), out)
	if err != nil {
		t.Fatalf("Explain: %v", err)
	}
	i := slices.IndexFunc(report.RequiredAdapters, func(r plan.AdapterRequirement) bool { return r.Kind == "encoder" })
	if i < 0 {
		t.Fatalf("no encoder requirement in %+v", report.RequiredAdapters)
	}
	encoder := report.RequiredAdapters[i]
	if encoder.Backend != "fakehw (hardware)" || !strings.Contains(encoder.BackendReason, "hardware > lib > go") || !slices.Equal(encoder.Alternatives, []string{"govpx (go)"}) {
		t.Fatalf("encoder requirement = %+v", encoder)
	}

	named := goavtest.Video(320, 240, 2).Video().Encode(codec.VP8(codec.Backend("govpx"))).To(sink)
	if got := encodeDetail(t, rt, named); got != "frames -> packets, codec=vp8, backend=govpx (go)" {
		t.Fatalf("named backend detail = %q", got)
	}
	preferGo := bundle.MustNew(hardwareVP8(), runconfig.WithCodecPreference("", "", string(codec.KindGo)))
	if got := encodeDetail(t, preferGo, out); got != "frames -> packets, codec=vp8, backend=govpx (go)" {
		t.Fatalf("preference detail = %q", got)
	}
	big := goavtest.Video(320, 240, 2).Video().Resize(1280, 720).Encode(codec.VP8()).To(sink)
	if got := encodeDetail(t, rt, big); got != "frames -> packets, codec=vp8, backend=govpx (go)" {
		t.Fatalf("capability filter detail = %q, want govpx past fakehw's 640x480 maximum", got)
	}
}

// TestNamedCodecBackendRefusalNamesTheAlternatives pins that a named backend
// is never substituted: a missing or unfit one is a typed refusal that lists
// the backends that would serve the request.
func TestNamedCodecBackendRefusalNamesTheAlternatives(t *testing.T) {
	rt := bundle.MustNew(hardwareVP8())
	sink := goavtest.NewCollector().Sink()
	for name, tc := range map[string]struct {
		spec  codec.Spec
		code  string
		parts []string
	}{
		"missing": {codec.VP8(codec.Backend("x264")), "encode_adapter_missing", []string{`no vp8 encoder backend named "x264"`, "satisfying=fakehw (hardware), govpx (go)"}},
		"unfit":   {codec.VP8(codec.Backend("fakehw"), codec.BitDepth(10)), "encode_adapter_incompatible", []string{"vp8 encoder backend fakehw cannot serve the request: bit depth 10 not supported (8)"}},
	} {
		task, err := rt.Build(context.Background(), goavtest.Video(320, 240, 2).Video().Encode(tc.spec).To(sink))
		if err == nil {
			task.Close()
		}
		var buildErr *goav.BuildError
		if !errors.As(err, &buildErr) || string(buildErr.Code) != tc.code {
			t.Fatalf("%s: err = %v, want %s", name, err, tc.code)
		}
		for _, part := range tc.parts {
			if !strings.Contains(err.Error(), part) {
				t.Fatalf("%s: refusal %q lacks %q", name, err, part)
			}
		}
	}
}

// TestLaunchSelectsCodecBackend pins encode backend= and decode backend= in
// programs.
func TestLaunchSelectsCodecBackend(t *testing.T) {
	rt := bundle.MustNew(hardwareVP8())
	outputs, err := launch.Compile("testsrc video size=320x240 duration=100ms ! encode vp8 backend=govpx ! file out.webm", launch.Files(tempFiles(t)))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	spec, err := rt.Describe(outputs...)
	if err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if !slices.ContainsFunc(spec.Nodes, func(node pipeline.NodeSpec) bool { return strings.HasSuffix(node.Detail, "backend=govpx (go)") }) {
		t.Fatalf("no govpx encode node in %+v", spec.Nodes)
	}
}

func TestLaunchDecodeTakesABackend(t *testing.T) {
	outputs, err := launch.Compile("file in.webm ! video ! decode backend=govpx ! encode vp8 ! file out.webm", launch.Files(tempFiles(t)))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if _, err := bundle.Describe(outputs...); err != nil {
		t.Fatalf("Describe: %v", err)
	}
	if _, err := launch.Compile("file in.webm ! video ! decode backnd=govpx ! file out.webm", launch.Files(tempFiles(t))); err == nil || !strings.Contains(err.Error(), "backend") {
		t.Fatalf("misspelled decode setting = %v, want a backend suggestion", err)
	}
}

func tempFiles(t *testing.T) launch.FileSystem {
	t.Helper()
	fs, err := launch.Dir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return fs
}

// TestMisspelledKnobScopeIsRefused pins that a setting scoped to a backend
// the runtime does not hold is refused with the nearest name, not dropped.
func TestMisspelledKnobScopeIsRefused(t *testing.T) {
	rt := bundle.MustNew(hardwareVP8())
	task, err := rt.Build(context.Background(), goavtest.Video(320, 240, 2).Video().Encode(codec.VP8(codec.Setting("govp.speed", "4"))).To(goavtest.NewCollector().Sink()))
	if err == nil {
		task.Close()
	}
	if err == nil || !strings.Contains(err.Error(), "scoped to govp, which is no vp8 backend here") || !strings.Contains(err.Error(), "did you mean govpx?") {
		t.Fatalf("err = %v, want the scope refused with govpx suggested", err)
	}
}

// TestMisspelledKnobNamesEachBackendsReason pins that a setting no backend
// takes, on a request naming none, is refused with each backend and why,
// and the nearest setting, not as an unsupported codec.
func TestMisspelledKnobNamesEachBackendsReason(t *testing.T) {
	strict := runconfig.WithCodecAdapter(func(registry *codec.SimpleRegistry) {
		passthrough, _ := runconfig.NewConfig(goavtest.Codec(av.CodecH264))
		choice, _ := passthrough.Codecs.Select(codec.Need{ID: av.CodecH264, Mode: codec.ModeEncode})
		for _, backend := range []codec.BackendInfo{{Name: "fakehw", Kind: codec.KindHardware}, {Name: "fakelib", Kind: codec.KindLib}} {
			registry.RegisterEncoder(codec.Descriptor{ID: av.CodecH264, Type: av.MediaVideo, Backend: backend, Knobs: []codec.Knob{codec.KnobBitrate}}, choice.Encoder)
		}
	})
	rt, err := goav.New(strict)
	if err != nil {
		t.Fatal(err)
	}
	task, err := rt.Build(context.Background(), goavtest.Video(64, 48, 2).Video().Encode(codec.H264(codec.Setting("bitrat", "1000"))).To(goavtest.NewCollector().Sink()))
	if err == nil {
		task.Close()
	}
	for _, want := range []string{"no h264 encoder backend takes the request", "fakehw takes no setting bitrat", "fakelib takes no setting bitrat", "did you mean bitrate?"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
	if !errors.Is(err, codec.ErrInvalidSetting) {
		t.Fatalf("err = %v, want codec.ErrInvalidSetting", err)
	}
}

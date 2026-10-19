package extbackend_test

import (
	"context"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/libcodec/codectest"
	"github.com/thesyncim/goav/libcodec/kit"
	"github.com/thesyncim/goav/libcodec/proof/extbackend"
	"github.com/thesyncim/goav/plan"
)

func available(t *testing.T) {
	t.Helper()
	if err := kit.Probe(extbackend.Option()); err != nil {
		t.Skip(err)
	}
}

// TestConformance is the backend's whole conformance suite, in one line.
func TestConformance(t *testing.T) { codectest.Run(t, extbackend.Option()) }

// TestProgramSelectsTheBackendByName runs a goav program that names the
// backend and its knob, and decodes the result back losslessly.
func TestProgramSelectsTheBackendByName(t *testing.T) {
	available(t)
	rt := bundle.MustNew(extbackend.Option())
	encoded := goavtest.NewCollector()
	collect := launch.Sink("collect", "keep packets for the test", func(struct{}) (goav.Destination, error) { return encoded.Sink(), nil })
	outputs, err := launch.Compile("testsrc video size=64x48 frames=10 ! encode zlib media=video backend=zlib level=9 ! collect", launch.Use(collect))
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Run(context.Background(), outputs...); err != nil {
		t.Fatal(err)
	}
	packets := make([]av.Packet, 0, 10)
	for _, p := range encoded.Packets() {
		packets = append(packets, *p)
	}
	decoded := goavtest.NewCollector()
	if err := rt.Run(context.Background(), goavtest.Packets(extbackend.Codec, packets...).Video().Decode(codec.Backend("zlib")).To(decoded.Sink())); err != nil {
		t.Fatal(err)
	}
	if frames := decoded.Frames(); len(packets) != 10 || len(frames) != 10 || frames[0].Video.Width != 64 {
		t.Fatalf("%d packets decoded to %d frames", len(packets), len(frames))
	}
}

// TestExplainNamesTheBackend pins that the runtime reports the backend, why
// it was chosen, and its knobs, like a built-in one.
func TestExplainNamesTheBackend(t *testing.T) {
	available(t)
	out := goavtest.Video(64, 48, 2).Video().Encode(codec.Codec(extbackend.Codec, "video")).To(goavtest.NewCollector().Sink())
	report, err := bundle.MustNew(extbackend.Option()).Explain(context.Background(), out)
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(report.RequiredAdapters, func(r plan.AdapterRequirement) bool { return r.Kind == "encoder" })
	if i < 0 || report.RequiredAdapters[i].Backend != "zlib (lib)" || !slices.Contains(report.RequiredAdapters[i].Knobs, "level=0..9 (default 6): deflate level") {
		t.Fatalf("Explain = %+v", report.RequiredAdapters)
	}
}

// TestImportsOnlyPublicPackages pins that nothing here reaches into goav's
// internal packages: what this backend does, any module can.
func TestImportsOnlyPublicPackages(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", `{{join .Imports "\n"}}`, ".").Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range strings.Fields(string(out)) {
		if strings.Contains(path, "/internal") {
			t.Errorf("imports %s", path)
		}
	}
}

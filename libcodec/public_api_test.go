package libcodec_test

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/thesyncim/goav/libcodec"
	"github.com/thesyncim/goav/libcodec/kit"
)

// TestBackendsUseOnlyPublicAPI pins that the in-tree backends are written
// against the same public packages a backend in any other module uses (the
// kit, codec, av, runconfig): none imports an internal package, goav's or
// another backend's, so libcodec/proof/extbackend's reach is theirs.
func TestBackendsUseOnlyPublicAPI(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", `{{.ImportPath}}{{range .Imports}} {{.}}{{end}}`, "./internal/...", "./kit/...", "./codectest/...").Output()
	if err != nil {
		t.Fatal(err)
	}
	backends := 0
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		fields := strings.Fields(line)
		if strings.Contains(fields[0], "/internal/") {
			backends++
		}
		for _, path := range fields[1:] {
			if strings.Contains(path, "/internal") {
				t.Errorf("%s imports %s", fields[0], path)
			}
		}
	}
	if backends < 3 {
		t.Fatalf("checked %d backends, want the three in-tree ones", backends)
	}
}

// TestReleasesReachInstallers pins that a backend's published builds are
// readable through the options that register it, as an installer reads them.
func TestReleasesReachInstallers(t *testing.T) {
	releases, err := kit.Releases(libcodec.OpenH264())
	if err != nil {
		t.Fatal(err)
	}
	cisco := releases["h264/openh264"]
	if len(cisco) == 0 || cisco[0].Version != "2.6.0" || len(cisco[0].Files) != 6 {
		t.Fatalf("OpenH264 releases = %+v", cisco)
	}
	for _, file := range cisco[0].Files {
		if len(file.SHA256) != 64 || !strings.HasPrefix(file.URL, "http://ciscobinary.openh264.org/") {
			t.Errorf("release file %+v", file)
		}
	}
}

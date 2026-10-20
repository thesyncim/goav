package libcodec_test

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/thesyncim/goav/libcodec"
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

// TestDeclarationsArePlainValues pins what installers, lockfiles, and
// license pages read from the declarations, without loading a library:
// OpenH264's Cisco releases (HTTPS, SHA-256, inside its ABI window), the
// notice and terms Cisco's patent license asks for, and the license class of
// every backend.
func TestDeclarationsArePlainValues(t *testing.T) {
	for _, c := range libcodec.Declarations() {
		if c.License.Class == "" {
			t.Errorf("%s/%s declares no license class", c.ID, c.Backend)
		}
		if c.Backend != "openh264" {
			continue
		}
		if c.License.Notice != "OpenH264 Video Codec provided by Cisco Systems, Inc." || !strings.Contains(c.License.Terms, "must reproduce all of the above text") {
			t.Errorf("OpenH264 license = %+v", c.License)
		}
		if len(c.Releases) == 0 || len(c.Releases[0].Files) != 6 || !c.ABI.Contains(c.Releases[0].Version) {
			t.Fatalf("OpenH264 releases %+v, window %v", c.Releases, c.ABI)
		}
		for _, file := range c.Releases[0].Files {
			if len(file.SHA256) != 64 || !strings.HasPrefix(file.URL, "https://ciscobinary.openh264.org/") {
				t.Errorf("release file %+v", file)
			}
		}
	}
}

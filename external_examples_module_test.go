package goav_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var externalExampleModules = []string{
	"examples/custom-source",
	"examples/custom-destination",
	"examples/custom-filter",
	"examples/transactional-writer",
	"examples/custom-codec",
	"examples/custom-join",
	"examples/provider-source",
}

func TestExternalExampleModulesAreCopyable(t *testing.T) {
	for _, dir := range externalExampleModules {
		t.Run(dir, func(t *testing.T) {
			for _, name := range []string{"go.mod", "go.sum", "README.md", "main.go", "main_test.go", "testdata/expected.txt"} {
				if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
					t.Fatalf("%s missing %s: %v", dir, name, err)
				}
			}

			readme := readExternalExampleFile(t, filepath.Join(dir, "README.md"))
			for _, phrase := range []string{"Expected output:", "testdata/expected.txt", "Failure example:"} {
				if !strings.Contains(readme, phrase) {
					t.Fatalf("%s README missing %q", dir, phrase)
				}
			}

			mod := readExternalExampleFile(t, filepath.Join(dir, "go.mod"))
			if want := "module github.com/thesyncim/goav/" + dir; !strings.Contains(mod, want) {
				t.Fatalf("%s go.mod missing %q", dir, want)
			}
			if !strings.Contains(mod, "replace github.com/thesyncim/goav => ../..") {
				t.Fatalf("%s go.mod should replace root module for local copyability", dir)
			}

			err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				if entry.IsDir() || filepath.Ext(path) != ".go" {
					return nil
				}
				text := readExternalExampleFile(t, path)
				for _, forbidden := range []string{
					`"github.com/thesyncim/goav/internal/`,
					`"github.com/thesyncim/goav/adapterproof/`,
				} {
					if strings.Contains(text, forbidden) {
						t.Fatalf("%s imports non-public package prefix %s", path, forbidden)
					}
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestExternalExampleModulesAreLinkedFromFrontDoorDocs(t *testing.T) {
	for _, file := range []string{"README.md", "docs/ADAPTER_AUTHORING.md"} {
		body := readExternalExampleFile(t, file)
		for _, dir := range externalExampleModules {
			if !strings.Contains(body, dir) {
				t.Fatalf("%s should link %s", file, dir)
			}
		}
	}
}

func TestControlPlaneHostExampleIsStandaloneModule(t *testing.T) {
	dir := "examples/control-plane-host"
	for _, name := range []string{"go.mod", "README.md", "main.go", "main_test.go"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("%s missing %s: %v", dir, name, err)
		}
	}
	mod := readExternalExampleFile(t, filepath.Join(dir, "go.mod"))
	for _, required := range []string{
		"module github.com/thesyncim/goav/examples/control-plane-host",
		"require github.com/thesyncim/goav v0.0.0",
		"replace github.com/thesyncim/goav => ../..",
	} {
		if !strings.Contains(mod, required) {
			t.Fatalf("%s/go.mod missing %q", dir, required)
		}
	}
	readme := readExternalExampleFile(t, filepath.Join(dir, "README.md"))
	for _, phrase := range []string{
		"go run . --control unix:///tmp/goav-control-plane-host.sock",
		"custom branch steps",
		"runtime-registered custom encoder",
		"ctl.ValidateCapabilities",
	} {
		if !strings.Contains(readme, phrase) {
			t.Fatalf("%s README missing %q", dir, phrase)
		}
	}
	for _, file := range []string{"README.md", "docs/EXTENSION_COOKBOOK.md", "docs/CONTROL_PLANE.md", "docs/API_SURFACE.md"} {
		body := readExternalExampleFile(t, file)
		if !strings.Contains(body, dir) {
			t.Fatalf("%s should link %s", file, dir)
		}
	}
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		text := readExternalExampleFile(t, path)
		for _, forbidden := range []string{
			`"github.com/thesyncim/goav/internal/`,
			`"github.com/thesyncim/goav/adapterproof/`,
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s imports non-public package prefix %s", path, forbidden)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func readExternalExampleFile(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

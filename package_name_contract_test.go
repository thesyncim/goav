package goav

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeAndControlHostPackageNamesStayClear(t *testing.T) {
	modulePath := "github.com/thesyncim/goav"
	forbidImport := map[string]string{
		modulePath + "/runtime": "use github.com/thesyncim/goav/runconfig",
		modulePath + "/ctl":     "use github.com/thesyncim/goav/ctlserver",
	}
	forbidPackage := map[string]string{
		"runtime": "use package runconfig for runtime options",
		"ctl":     "use package ctlserver for the socket control-plane host",
	}

	fset := token.NewFileSet()
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".idea", ".claude", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		if fix, ok := forbidPackage[file.Name.Name]; ok {
			t.Errorf("%s declares package %q; %s", path, file.Name.Name, fix)
		}
		for _, imported := range file.Imports {
			pathValue := strings.Trim(imported.Path.Value, "\"")
			if fix, ok := forbidImport[pathValue]; ok {
				t.Errorf("%s imports %s; %s", path, pathValue, fix)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

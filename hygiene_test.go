package goav_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNoCGOImports(t *testing.T) {
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".idea":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(data)
		if strings.Contains(text, "import \"C\"") || strings.Contains(text, "\"C\"\n") {
			t.Fatalf("%s imports C", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

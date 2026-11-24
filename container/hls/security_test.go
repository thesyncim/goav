package hls

import (
	"testing"
)

func TestDirStoreRejectsPathTraversal(t *testing.T) {
	store := Dir(t.TempDir())
	for _, bad := range []string{"", "../escape.m3u8", "sub/../../escape.m3u8", "/etc/passwd", `\outside.m3u8`} {
		if f, err := store.Create(bad); err == nil {
			_ = f.Close()
			t.Errorf("Create(%q) succeeded, want error", bad)
		}
		if err := store.Remove(bad); err == nil {
			t.Errorf("Remove(%q) succeeded, want error", bad)
		}
	}
	f, err := store.Create("v0/index.m3u8")
	if err != nil {
		t.Fatalf("Create valid nested path: %v", err)
	}
	if _, err := f.Write([]byte("#EXTM3U\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Remove("v0/index.m3u8"); err != nil {
		t.Fatalf("Remove valid nested path: %v", err)
	}
}

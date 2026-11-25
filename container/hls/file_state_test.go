package hls

import "testing"

func TestServerFileAndPublishingStoreFileState(t *testing.T) {
	srv := NewServer()
	w, err := srv.Create("seg.m4s")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("abc")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// Second Close and abort after Close must be no-ops.
	if err := w.Close(); err != nil {
		t.Fatalf("second Close() = %v", err)
	}
	w.(*serverFile).abort()

	dir := Dir(t.TempDir())
	pw, err := dir.Create("out.m4s")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pw.Write([]byte("xyz")); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := pw.Close(); err != nil {
		t.Fatalf("second publishing Close() = %v", err)
	}
	pw.(*publishing).abort()
}

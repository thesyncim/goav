package launch_test

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/launch"
)

// renamingFS is a memFS that renames and removes, as launch.OS and
// launch.Dir do, and records every name it creates.
type renamingFS struct {
	*memFS
	mu      sync.Mutex
	created []string
}

func (f *renamingFS) Create(name string) (io.Writer, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, name)
	return f.memFS.Create(name)
}

func (f *renamingFS) Rename(oldname, newname string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.outputs[newname] = f.outputs[oldname]
	delete(f.outputs, oldname)
	return nil
}

func (f *renamingFS) Remove(name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.outputs, name)
	return nil
}

// TestHLSElement packages a program as HLS through its files: every file is
// written under a temporary name and renamed into place, and one path used
// by two pipelines is one packager.
func TestHLSElement(t *testing.T) {
	fs := &renamingFS{memFS: newMemFS()}
	outs, err := launch.Compile(`testsrc video size=64x48 fps=25 frames=50 ! encode vp8 keyframe_interval=25 ! hls out/live.m3u8 segment=1s
testsrc audio duration=2s ! encode opus ! hls out/live.m3u8 segment=1s`, launch.Files(fs))
	if err != nil {
		t.Fatal(err)
	}
	if err := bundle.Run(context.Background(), outs...); err != nil {
		t.Fatal(err)
	}
	var names []string
	for name := range fs.outputs {
		names = append(names, name)
	}
	for _, want := range []string{"out/live.m3u8", "out/live-init.mp4", "out/live-00000.m4s", "out/live-00001.m4s"} {
		if _, ok := fs.outputs[want]; !ok {
			t.Fatalf("no %s among %v", want, names)
		}
	}
	for _, name := range fs.created {
		if !strings.HasSuffix(name, ".tmp") {
			t.Fatalf("%s was written in place, not published by rename", name)
		}
	}
	if !strings.Contains(fs.outputs["out/live.m3u8"].String(), "#EXT-X-ENDLIST") {
		t.Fatalf("playlist:\n%s", fs.outputs["out/live.m3u8"])
	}
}

func TestHLSElementRefusals(t *testing.T) {
	for program, want := range map[string]string{
		`testsrc video ! encode vp8 ! hls live.m3u8`:           "may not write files",
		`testsrc video ! encode vp8 ! hls live.m3u8 window=-1`: "lists no segments",
	} {
		var options []launch.Option
		if strings.Contains(program, "window") {
			options = append(options, launch.Files(newMemFS()))
		}
		if _, err := launch.Compile(program, options...); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: err = %v, want %q", program, err, want)
		}
	}
}

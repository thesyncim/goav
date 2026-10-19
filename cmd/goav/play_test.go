//go:build !goav_noplay

package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/launch"
)

// writeClip encodes a testsrc clip through the CLI's own run command.
func writeClip(t *testing.T, program string) string {
	t.Helper()
	dir := t.TempDir()
	fs, err := launch.Dir(dir)
	if err != nil {
		t.Fatal(err)
	}
	outs, err := launch.Compile(program, launch.Files(fs))
	if err != nil {
		t.Fatal(err)
	}
	if err := bundle.Run(context.Background(), outs...); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "clip.webm")
}

func TestPlayElementIsListedAndDescribes(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"elements"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "play [title=") {
		t.Fatalf("elements does not list play: code=%d\n%s%s", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	if code := run([]string{"describe", "testsrc video size=64x48 ! play; testsrc audio ! play"}, &stdout, &stderr); code != 0 {
		t.Fatalf("describe with play: code=%d %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "player") {
		t.Fatalf("described graph has no player sink:\n%s", stdout.String())
	}
}

func TestPlayRefusesRuntimesWithTheirOwnClock(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"run", "--runtime", "demo", "testsrc video frames=1 ! play"}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "play runs on the speaker's clock") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestPlayFileProgramPicksTheStreamsTheFileHas(t *testing.T) {
	both := writeClip(t, `testsrc video size=32x24 frames=3 ! encode vp8 ! file clip.webm
		testsrc audio duration=60ms ! encode opus ! file clip.webm`)
	program, err := fileProgram(both)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(program, "! video !") || !strings.Contains(program, "! audio !") {
		t.Fatalf("audio+video clip program = %q", program)
	}
	videoOnly := writeClip(t, `testsrc video size=32x24 frames=3 ! encode vp8 ! file clip.webm`)
	if program, err = fileProgram(videoOnly); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(program, "! video !") || strings.Contains(program, "! audio !") {
		t.Fatalf("video-only clip program = %q", program)
	}
	if _, err := fileProgram(filepath.Join(t.TempDir(), "missing.webm")); err == nil {
		t.Fatal("missing file produced a program")
	}
}

func TestPlayQuotesPaths(t *testing.T) {
	path := `dir with "quotes"\clip.webm`
	outs, err := launch.Compile("file "+quote(path)+" ! null", launch.Files(&probeFiles{}))
	if err != nil || len(outs) != 1 {
		t.Fatalf("quoted path did not compile: %v", err)
	}
}

func TestPlayHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"play", "--help"}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "goav play [flags] FILE") {
		t.Fatalf("code=%d %s", code, stdout.String())
	}
}

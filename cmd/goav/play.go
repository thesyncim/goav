package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/runconfig"
)

const playUsage = `goav play [flags] FILE
goav play [flags] '<program>'

Plays a file's video and audio in a window: shorthand for goav run with the
play destination. A program plays what it routes to play.

keys: space pause, ←/→ seek 5s, ↓/↑ seek 30s, [ ] rate, f full screen,
      i overlay, q quit

examples:
  goav play clip.webm
  goav play 'testsrc video size=1280x720 live ! play; testsrc audio live ! play'
`

// playRuntime is the runtime a program that plays runs on: the bundled
// adapters, realtime, on the window's speaker clock, so the task timeline —
// pacing, Pause, Rate, Seek — runs in the time the audio is heard.
func playRuntime(name string, win window) (*goav.Runtime, error) {
	if name != "default" && name != "bundle" {
		return nil, fmt.Errorf("play runs on the speaker's clock; --runtime %s has its own clock: drop --runtime, or route to file or null instead of play", name)
	}
	return bundle.New(runconfig.WithRealtime(true), runconfig.WithClock(win.Clock()))
}

// playStats is the summary line of a program that played: the evidence the
// picture followed the sound.
type playStats struct {
	PositionMS    float64 `json:"position_ms"`
	Presented     uint64  `json:"presented"`
	Dropped       uint64  `json:"dropped"`
	Underruns     uint64  `json:"underruns"`
	MaxAVOffsetMS float64 `json:"max_av_offset_ms"`
}

// window is the play destination the CLI registers: the player module's
// window, or a stub in a goav_noplay build.
type window interface {
	Element() launch.Element
	Used() bool
	Clock() av.Clock
	Run(context.Context, goav.LiveTask) error
	summary() *playStats
}

func ms(seconds float64) float64 { return float64(int64(seconds*10000+0.5)) / 10 }

func playCommand(argv []string, stdout, stderr io.Writer) int {
	args, help, err := parseProgramArgs("play", argv)
	if err != nil {
		printErr(stderr, err)
		return 2
	}
	if help {
		fmt.Fprint(stdout, playUsage)
		return 0
	}
	program := args.program
	if info, err := os.Stat(program); err == nil && !info.IsDir() {
		if program, err = fileProgram(program); err != nil {
			printErr(stderr, err)
			return 1
		}
	}
	rest := []string{"--runtime", args.runtime}
	if args.control != "" {
		rest = append(rest, "--control", args.control)
	}
	return runCommand(append(rest, program), stdout, stderr)
}

// fileProgram is the program that plays a file: its video and its audio,
// whichever it carries, decoded to play. The streams are found by building
// the candidates, which reads the container header but plays nothing.
func fileProgram(path string) (string, error) {
	in := quote(path)
	title := quote(filepath.Base(path))
	video := "file " + in + " ! video ! decode ! play title=" + title
	audio := "file " + in + " ! audio ! decode ! resample 48000 2 ! play title=" + title
	var errs []error
	for _, program := range []string{video + "\n" + audio, video, audio} {
		err := buildsOn(program)
		if err == nil {
			return program, nil
		}
		errs = append(errs, err)
	}
	return "", fmt.Errorf("goav play: %s has no playable video or audio: %w", path, errs[0])
}

func buildsOn(program string) error {
	files := &probeFiles{}
	defer files.close()
	outs, err := launch.Compile(program, launch.Files(files), launch.Use(newWindow().Element()))
	if err != nil {
		return err
	}
	rt, err := bundle.New()
	if err != nil {
		return err
	}
	task, err := rt.Build(context.Background(), outs...)
	if err != nil {
		return err
	}
	return task.Close()
}

// probeFiles opens inputs for a probe build and closes them after.
type probeFiles struct {
	mu     sync.Mutex
	opened []*os.File
}

func (f *probeFiles) Open(name string) (io.Reader, error) {
	file, err := os.Open(name)
	if err == nil {
		f.mu.Lock()
		f.opened = append(f.opened, file)
		f.mu.Unlock()
	}
	return file, err
}

func (*probeFiles) Create(name string) (io.Writer, error) {
	return nil, errors.New("goav play: a probe build creates no files")
}

func (f *probeFiles) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, file := range f.opened {
		_ = file.Close()
	}
}

// quote writes s as a launch quoted value.
func quote(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(s) + `"`
}

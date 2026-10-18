package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/container/hls"
	"github.com/thesyncim/goav/ctlserver"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/runconfig"
)

// programArgs are the flags run, describe, and explain share, and the
// program they act on: inline arguments, or a file with -f.
type programArgs struct {
	runtime string
	control string
	serve   string
	format  string
	program string
}

func parseProgramArgs(verb string, argv []string) (programArgs, bool, error) {
	args := programArgs{runtime: "default", format: "text"}
	var file string
	var parts []string
	value := func(i *int, flag string) (string, error) {
		if *i+1 >= len(argv) {
			return "", fmt.Errorf("goav %s: %s needs a value", verb, flag)
		}
		*i++
		return argv[*i], nil
	}
	for i := 0; i < len(argv); i++ {
		arg := argv[i]
		name, inline, hasInline := strings.Cut(arg, "=")
		take := func() (string, error) {
			if hasInline {
				return inline, nil
			}
			return value(&i, name)
		}
		var err error
		switch {
		case arg == "-h" || arg == "--help" || arg == "help":
			return args, true, nil
		case name == "--runtime":
			args.runtime, err = take()
		case name == "--control" && verb == "run":
			args.control, err = take()
		case name == "--serve" && verb == "run":
			args.serve, err = take()
		case name == "--format" && verb == "describe":
			args.format, err = take()
		case name == "-f":
			file, err = take()
		case strings.HasPrefix(arg, "-") && arg != "-":
			return args, false, fmt.Errorf("goav %s: unknown flag %s", verb, arg)
		default:
			parts = append(parts, arg)
		}
		if err != nil {
			return args, false, err
		}
	}
	switch {
	case file != "" && len(parts) > 0:
		return args, false, fmt.Errorf("goav %s: pass a program inline or with -f, not both", verb)
	case file != "":
		data, err := os.ReadFile(file)
		if err != nil {
			return args, false, err
		}
		args.program = string(data)
	default:
		args.program = strings.Join(parts, " ")
	}
	if strings.TrimSpace(args.program) == "" {
		return args, true, nil
	}
	return args, false, nil
}

func runtimeNamed(name string) (*goav.Runtime, error) {
	switch name {
	case "default", "bundle":
		return bundle.New()
	case "demo":
		// The bundled adapters on a deterministic clock: live sources run as
		// fast as the graph allows, for reproducible demos and tests.
		return bundle.New(runconfig.WithClock(goavtest.NewClock()))
	case "test":
		return goavtest.Runtime(), nil
	}
	return nil, fmt.Errorf("unknown runtime %q: want default, demo, or test", name)
}

// cliFiles is the process's files for the CLI: outputs get their parent
// directories, and the files written are recorded for the summary.
type cliFiles struct {
	mu      sync.Mutex
	written []string
}

func (*cliFiles) Open(name string) (io.Reader, error) { return os.Open(name) }

func (f *cliFiles) Create(name string) (io.Writer, error) {
	if dir := filepath.Dir(name); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	file, err := os.Create(name)
	if err == nil {
		f.mu.Lock()
		f.written = append(f.written, name)
		f.mu.Unlock()
	}
	return file, err
}

// Rename publishes a file written under a temporary name, as HLS does with
// its playlist; the summary lists the final name.
func (f *cliFiles) Rename(oldname, newname string) error {
	if err := os.Rename(oldname, newname); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written = slices.DeleteFunc(f.written, func(n string) bool { return n == oldname || n == newname })
	f.written = append(f.written, newname)
	return nil
}

// Remove deletes a file, as a live HLS playlist does with old segments.
func (f *cliFiles) Remove(name string) error {
	if err := os.Remove(name); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.written = slices.DeleteFunc(f.written, func(n string) bool { return n == name })
	return nil
}

const programUsage = `goav %[1]s [flags] '<program>'
goav %[1]s [flags] -f program.goav

A program is goav's pipeline language: steps joined by !, one pipeline per
line, name := ... to reuse a stream. 'goav elements' lists the vocabulary;
docs/CLI.md has the grammar.

examples:
  goav %[1]s 'file in.webm ! video ! decode ! resize 1280x720 ! encode vp9 bitrate=2M ! file out.webm'
  goav %[1]s 'testsrc video size=1280x720 duration=2s ! encode vp9 ! file clip.webm
              testsrc audio duration=2s ! encode opus ! file clip.webm'
`

const runFlags = `
flags:
  --runtime default|demo|test   adapters to run with (default: the bundle)
  --control unix://PATH         serve the running task to goav ctl
  --serve ADDR                  serve the program's hls outputs over HTTP (LL-HLS too)
  -f FILE                       read the program from FILE
`

func runCommand(argv []string, stdout, stderr io.Writer) int {
	args, help, err := parseProgramArgs("run", argv)
	if err != nil {
		printErr(stderr, err)
		return 2
	}
	if help {
		fmt.Fprintf(stdout, programUsage+runFlags, "run")
		return 0
	}
	files := &cliFiles{}
	options := []launch.Option{launch.Files(files)}
	var served *http.Server
	if args.serve != "" {
		// The program's hls outputs package into a server, served while the
		// program runs: an LL-HLS origin in one command.
		srv := hls.NewServer()
		l, err := net.Listen("tcp", args.serve)
		if err != nil {
			printErr(stderr, err)
			return 2
		}
		served = &http.Server{Handler: srv, ReadHeaderTimeout: 10 * time.Second}
		go served.Serve(l)
		defer served.Close()
		fmt.Fprintf(stderr, "serving HLS at http://%s/\n", l.Addr())
		options = append(options, launch.HLS(srv))
	}
	outs, err := launch.Compile(args.program, options...)
	if err != nil {
		printErr(stderr, err)
		return 2
	}
	rt, err := runtimeNamed(args.runtime)
	if err != nil {
		printErr(stderr, err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	task, err := rt.Build(ctx, outs...)
	if err != nil {
		printErr(stderr, err)
		return 1
	}
	defer task.Close()
	if args.control != "" {
		err = runServed(ctx, task, args.control, files)
	} else {
		err = task.Run(ctx)
	}
	// Close finalizes the destinations: a muxer writes its index, HLS its
	// last segment and playlist. The summary reports the finished files.
	if closeErr := task.Close(); err == nil || errors.Is(err, context.Canceled) {
		err = cmp.Or(closeErr, err)
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		printErr(stderr, err)
		return 1
	}
	sort.Strings(files.written)
	summary := struct {
		Runtime string   `json:"runtime"`
		Wrote   []string `json:"wrote"`
		Control string   `json:"control,omitempty"`
		Stopped bool     `json:"stopped,omitempty"`
	}{args.runtime, files.written, args.control, errors.Is(err, context.Canceled)}
	if err := json.NewEncoder(stdout).Encode(summary); err != nil {
		printErr(stderr, err)
		return 1
	}
	return 0
}

// runServed runs the task while a control socket serves it; the first to
// stop stops both. The CLI's own user is the client, so attached programs
// use the user's files, and the summary lists what they write.
func runServed(ctx context.Context, task goav.LiveTask, control string, files launch.FileSystem) error {
	server, err := ctlserver.New(task, ctlserver.Files(files))
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errC := make(chan error, 2)
	go func() { errC <- task.Run(runCtx) }()
	go func() { errC <- server.ListenAndServe(runCtx, control) }()
	var first error
	for range 2 {
		err := <-errC
		cancel()
		if err != nil && first == nil && !expectedShutdown(err) {
			first = err
			_ = task.Close()
		}
	}
	return first
}

func expectedShutdown(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, goav.ErrClosed) || errors.Is(err, pipeline.ErrClosed)
}

func describeCommand(argv []string, stdout, stderr io.Writer) int {
	args, help, err := parseProgramArgs("describe", argv)
	if err != nil {
		printErr(stderr, err)
		return 2
	}
	if help {
		fmt.Fprintf(stdout, programUsage+"\nflags:\n  --runtime default|demo|test\n  --format text|dot|mermaid|json   (default text)\n  -f FILE\n", "describe")
		return 0
	}
	outs, rt, code := compileFor(args, stderr)
	if code != 0 {
		return code
	}
	spec, err := rt.Describe(outs...)
	if err != nil {
		printErr(stderr, err)
		return 1
	}
	if args.format == "json" {
		return encodeJSON(stdout, stderr, spec)
	}
	target := map[string]string{"text": "goav:graph", "dot": "goav:graph?format=dot", "mermaid": "goav:graph?format=mermaid"}[args.format]
	if target == "" {
		printErr(stderr, fmt.Errorf("goav describe: --format is text, dot, mermaid, or json, not %q", args.format))
		return 2
	}
	text, err := graphrender.RenderURI(spec, target)
	if err != nil {
		printErr(stderr, err)
		return 1
	}
	fmt.Fprint(stdout, text)
	return 0
}

func explainCommand(argv []string, stdout, stderr io.Writer) int {
	args, help, err := parseProgramArgs("explain", argv)
	if err != nil {
		printErr(stderr, err)
		return 2
	}
	if help {
		fmt.Fprintf(stdout, programUsage+"\nflags:\n  --runtime default|demo|test\n  -f FILE\n", "explain")
		return 0
	}
	outs, rt, code := compileFor(args, stderr)
	if code != 0 {
		return code
	}
	report, err := rt.Explain(context.Background(), outs...)
	if err != nil {
		printErr(stderr, err)
		return 1
	}
	return encodeJSON(stdout, stderr, report)
}

// compileFor compiles a program for describe or explain: files are named
// but never opened or created.
func compileFor(args programArgs, stderr io.Writer) ([]goav.Output, *goav.Runtime, int) {
	outs, err := launch.Compile(args.program, launch.Files(&cliFiles{}))
	if err != nil {
		printErr(stderr, err)
		return nil, nil, 2
	}
	rt, err := runtimeNamed(args.runtime)
	if err != nil {
		printErr(stderr, err)
		return nil, nil, 2
	}
	return outs, rt, 0
}

func elementsCommand(stdout io.Writer) int {
	kind := ""
	for _, el := range launch.Elements() {
		if el.Kind != kind {
			if kind != "" {
				fmt.Fprintln(stdout)
			}
			kind = el.Kind
			fmt.Fprintf(stdout, "%ss\n", kind)
		}
		fmt.Fprintf(stdout, "  %s\n      %s\n", el.Usage, el.Summary)
	}
	return 0
}

func encodeJSON(stdout, stderr io.Writer, v any) int {
	enc := json.NewEncoder(stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		printErr(stderr, err)
		return 1
	}
	return 0
}

package launch

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/hls"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/shape"
)

// Element is one name a program can use: a source that starts a pipeline, an
// operation on a stream, a destination that ends one, or a join called with
// streams. The built-ins cover goav's grammar; Use adds your own.
type Element struct{ el *element }

type element struct {
	name    string
	summary string
	kind    string
	schema  argSchema
	err     error
	// selects marks input-to-stream selection (video, audio, stream).
	selects bool
	start   func(c *compiler, args reflect.Value) (value, error)
	apply   func(c *compiler, args reflect.Value, in value) (value, error)
	dest    func(c *compiler, args reflect.Value) (value, error)
	join    func(c *compiler, args reflect.Value, inputs []value, call *termNode) (value, error)
}

func (e *element) bind(t *termNode) (reflect.Value, error) {
	v, err := e.schema.bind(e.name, t.args)
	if err != nil {
		if err.Pos < 0 {
			err.Pos, err.End = t.pos, t.end
		}
		return reflect.Value{}, err
	}
	return v, nil
}

func argsOf[T any](v reflect.Value) T {
	if !v.IsValid() {
		var zero T
		return zero
	}
	return v.Interface().(T)
}

func define[T any](name, summary, kind string) *element {
	el := &element{name: name, summary: summary, kind: kind}
	typ := reflect.TypeFor[T]()
	if typ == reflect.TypeFor[struct{}]() {
		typ = nil
	}
	el.schema, el.err = schemaOf(typ)
	return el
}

// Source defines an element that starts a pipeline with an input. Its
// settings are T's fields tagged `launch:"name"` (see Size for the value
// forms); open runs once, when the program compiles.
func Source[T any](name, summary string, open func(T) (goav.InputSpec, error)) Element {
	el := define[T](name, summary, "source")
	el.start = func(_ *compiler, args reflect.Value) (value, error) {
		in, err := open(argsOf[T](args))
		return value{kind: kindInput, input: in}, err
	}
	return Element{el}
}

// Stage defines an operation that runs a custom stage on a stream (s.Do).
func Stage[T any](name, summary string, build func(T) (pipeline.Stage, error)) Element {
	el := define[T](name, summary, "operation")
	el.apply = func(_ *compiler, args reflect.Value, in value) (value, error) {
		stage, err := build(argsOf[T](args))
		if err != nil {
			return value{}, err
		}
		return value{kind: kindStream, stream: in.stream.Do(stage)}, nil
	}
	return Element{el}
}

// Encoder defines an operation that encodes a stream with a codec of your own
// spelling: acme bitrate=128k → s.Encode(spec).
func Encoder[T any](name, summary string, build func(T) (codec.Spec, error)) Element {
	el := define[T](name, summary, "operation")
	el.apply = func(_ *compiler, args reflect.Value, in value) (value, error) {
		spec, err := build(argsOf[T](args))
		if err != nil {
			return value{}, err
		}
		return value{kind: kindStream, stream: in.stream.Encode(spec)}, nil
	}
	return Element{el}
}

// Sink defines a destination that ends a pipeline.
func Sink[T any](name, summary string, open func(T) (goav.Destination, error)) Element {
	el := define[T](name, summary, "destination")
	el.dest = func(_ *compiler, args reflect.Value) (value, error) {
		d, err := open(argsOf[T](args))
		return value{kind: kindDest, dest: d}, err
	}
	return Element{el}
}

// Join defines a join called with streams, name(a, b, key=value), that
// converges them through a custom stage: goav.Join(name, stage, streams...).
func Join[T any](name, summary string, build func(T) (pipeline.Stage, error)) Element {
	el := define[T](name, summary, "join")
	el.join = func(_ *compiler, args reflect.Value, inputs []value, call *termNode) (value, error) {
		stage, err := build(argsOf[T](args))
		if err != nil {
			return value{}, err
		}
		streams, err := joinStreams(inputs, call)
		if err != nil {
			return value{}, err
		}
		return value{kind: kindStream, stream: goav.Join(name, stage, streams...)}, nil
	}
	return Element{el}
}

// Option configures Compile.
type Option func(*config)

type config struct {
	files FileSystem
	// uris is the schemes URIs grants; nil grants none and "" grants all.
	uris     map[string]bool
	task     goav.LiveTask
	hls      *hls.Server
	elements map[string]*element
	err      error
}

func newConfig(options []Option) *config {
	cfg := &config{elements: make(map[string]*element, len(builtins))}
	for name, el := range builtins {
		cfg.elements[name] = el
	}
	for _, opt := range options {
		if opt != nil {
			opt(cfg)
		}
	}
	return cfg
}

// Files lets a program read and write files through fs: file in.webm opens
// fs.Open("in.webm") when the run starts, and file out.webm creates its
// output then. Without Files a program touches no files.
func Files(fs FileSystem) Option {
	return func(c *config) { c.files = fs }
}

// URIs lets a program read and write URLs of the named schemes, resolved
// through the schemes of the runtime it runs on: uri "https://cdn/in.mp4",
// or the URL bare. With no names it grants every scheme the runtime
// registers. A file URL is a file whatever URIs grants: it opens through the
// FileSystem given to Files, so a program compiled with Dir stays in its
// directory. Without URIs a program opens no URLs.
func URIs(schemes ...string) Option {
	return func(c *config) {
		if c.uris == nil {
			c.uris = map[string]bool{}
		}
		for _, scheme := range schemes {
			c.uris[strings.ToLower(scheme)] = true
		}
		if len(schemes) == 0 {
			c.uris[""] = true
		}
	}
}

func (c *config) grants(scheme string) bool {
	return c.uris != nil && (c.uris[""] || c.uris[scheme])
}

// Task compiles a program against a running task, for task.Attach: a
// pipeline may start at one of its taps, tap live ! encode vp8 ! file p.webm.
func Task(task goav.LiveTask) Option {
	return func(c *config) { c.task = task }
}

// Use adds elements to the program's vocabulary; an element named like a
// built-in replaces it.
func Use(elements ...Element) Option {
	return func(c *config) {
		for _, e := range elements {
			if e.el == nil {
				continue
			}
			if e.el.err != nil && c.err == nil {
				c.err = e.el.err
			}
			c.elements[e.el.name] = e.el
		}
	}
}

// ElementInfo describes one element for help and capability listings.
type ElementInfo struct {
	Name    string `json:"name"`
	Kind    string `json:"kind"` // source, operation, destination, or join
	Summary string `json:"summary"`
	Usage   string `json:"usage"`
}

// Elements lists the vocabulary a program compiled with options can use,
// by kind, then name.
func Elements(options ...Option) []ElementInfo {
	cfg := newConfig(options)
	infos := make([]ElementInfo, 0, len(cfg.elements))
	for _, el := range cfg.elements {
		usage := el.schema.usage(el.name)
		if el.join != nil {
			usage = el.name + "(streams...)"
		}
		infos = append(infos, ElementInfo{Name: el.name, Kind: el.kind, Summary: el.summary, Usage: usage})
	}
	order := map[string]int{"source": 0, "operation": 1, "join": 2, "destination": 3}
	sort.Slice(infos, func(i, j int) bool {
		if infos[i].Kind != infos[j].Kind {
			return order[infos[i].Kind] < order[infos[j].Kind]
		}
		return infos[i].Name < infos[j].Name
	})
	return infos
}

// FileSystem opens the files a program reads and creates the files it writes.
type FileSystem interface {
	Open(name string) (io.Reader, error)
	Create(name string) (io.Writer, error)
}

// OS is the process's files, as os.Open and os.Create see them.
func OS() FileSystem { return osFiles{} }

type osFiles struct{}

func (osFiles) Open(name string) (io.Reader, error) { return os.Open(name) }
func (osFiles) Create(name string) (io.Writer, error) {
	if dir := filepath.Dir(name); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return os.Create(name)
}
func (osFiles) Rename(oldname, newname string) error { return os.Rename(oldname, newname) }
func (osFiles) Remove(name string) error             { return os.Remove(name) }

// Dir is the files under root and nothing else: a path that escapes root,
// through .. or a symlink, is refused. A control server hands attached
// programs a Dir. Each operation opens root for its own duration, so a Dir
// holds no handle: it needs no Close, and root stays removable.
func Dir(root string) (FileSystem, error) {
	r, err := os.OpenRoot(root)
	if err != nil {
		return nil, err
	}
	return rootFiles{dir: root}, r.Close()
}

type rootFiles struct{ dir string }

// in runs fn on root, opened for the call: files fn opens stay open after.
func (f rootFiles) in(fn func(*os.Root) error) error {
	r, err := os.OpenRoot(f.dir)
	if err != nil {
		return err
	}
	defer r.Close()
	return fn(r)
}

func (f rootFiles) Open(name string) (io.Reader, error) {
	var file *os.File
	if err := f.in(func(r *os.Root) (err error) { file, err = r.Open(name); return err }); err != nil {
		return nil, err
	}
	return file, nil
}

func (f rootFiles) Create(name string) (io.Writer, error) {
	var file *os.File
	err := f.in(func(r *os.Root) (err error) {
		if dir := filepath.Dir(name); dir != "." {
			if err := r.MkdirAll(dir, 0o755); err != nil {
				return err
			}
		}
		file, err = r.Create(name)
		return err
	})
	if err != nil {
		return nil, err
	}
	return file, nil
}

func (f rootFiles) Rename(oldname, newname string) error {
	return f.in(func(r *os.Root) error { return r.Rename(oldname, newname) })
}

func (f rootFiles) Remove(name string) error {
	return f.in(func(r *os.Root) error { return r.Remove(name) })
}

// fileBytes is a program's file input: the runtime opens it when the run
// starts, so compiling or describing a program opens nothing, and the file
// it opens keeps its own capabilities: a regular file seeks, a pipe does not.
type fileBytes struct {
	fs   FileSystem
	name string
}

func (f fileBytes) Name() string            { return f.name }
func (f fileBytes) Protocol() av.ProtocolID { return av.ProtocolFile }
func (f fileBytes) SourceShape() shape.Spec { return shape.Spec{} }

func (f fileBytes) OpenBytes(ctx context.Context) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reader, err := f.fs.Open(f.name)
	if err != nil {
		return nil, err
	}
	if closer, ok := reader.(io.ReadCloser); ok {
		return closer, nil
	}
	if seeker, ok := reader.(io.ReadSeeker); ok {
		return nopSeekCloser{seeker}, nil
	}
	return io.NopCloser(reader), nil
}

type nopSeekCloser struct{ io.ReadSeeker }

func (nopSeekCloser) Close() error { return nil }

// fileDestination is a program's output file, created when the run opens
// the destination: compiling or describing a program creates nothing, and a
// file the FileSystem refuses fails the build (or the attach) that opens it.
// The file keeps its own capabilities: muxers that backpatch see it seek.
type fileDestination struct {
	fs   FileSystem
	name string
}

func (f fileDestination) Name() string { return f.name }

func (f fileDestination) Contract() provider.Contract {
	return provider.Contract{ByteStream: true, Protocol: av.ProtocolFile}
}

func (f fileDestination) Open(ctx context.Context, _ provider.Info) (provider.Writer, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	w, err := f.fs.Create(f.name)
	if err != nil {
		return nil, err
	}
	switch w := w.(type) {
	case provider.Writer:
		return w, nil
	case io.WriteSeeker:
		return nopWriteSeekCloser{w}, nil
	}
	return nopWriteCloser{w}, nil
}

type nopWriteSeekCloser struct{ io.WriteSeeker }

func (nopWriteSeekCloser) Close() error { return nil }

type nopWriteCloser struct{ io.Writer }

func (nopWriteCloser) Close() error { return nil }

var errNoTask = errors.New("this program is not attached to a running task")

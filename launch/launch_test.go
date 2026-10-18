package launch_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/container/matroska"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/launch"
	"github.com/thesyncim/goav/pipeline"
)

// memFS is an in-memory FileSystem: Open reads inputs, Create records outputs.
type memFS struct {
	inputs  map[string][]byte
	outputs map[string]*bytes.Buffer
}

func newMemFS() *memFS {
	return &memFS{inputs: map[string][]byte{}, outputs: map[string]*bytes.Buffer{}}
}

func (m *memFS) Open(name string) (io.Reader, error) {
	b, ok := m.inputs[name]
	if !ok {
		return nil, os.ErrNotExist
	}
	return bytes.NewReader(b), nil
}

func (m *memFS) Create(name string) (io.Writer, error) {
	b := &bytes.Buffer{}
	m.outputs[name] = b
	return b, nil
}

type specShape struct {
	Nodes []string
	Edges []string
}

func shapeOf(t *testing.T, outs []goav.Output) specShape {
	t.Helper()
	spec, err := bundle.Describe(outs...)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	var s specShape
	for _, n := range spec.Nodes {
		s.Nodes = append(s.Nodes, string(n.Kind)+" "+n.Name+" ["+n.Detail+"]")
	}
	for _, e := range spec.Edges {
		s.Edges = append(s.Edges, string(e.From)+" -> "+string(e.To))
	}
	return s
}

// TestProgramsBuildTheGraphTheirGoBuilds pins the zero-cost promise: a
// program compiles to the same Outputs the recipe written in Go produces, so
// the described graph — every node, detail, and edge — is identical.
func TestProgramsBuildTheGraphTheirGoBuilds(t *testing.T) {
	r := func() io.Reader { return bytes.NewReader(nil) }
	w := func() io.Writer { return new(bytes.Buffer) }
	// Joins over application sources: hand the same values to the program as
	// custom sources and to the Go form directly.
	mic, music := goavtest.Audio(48_000, 1, []int16{1}), goavtest.Audio(48_000, 1, []int16{2})
	cam, screen := goavtest.Video(64, 48, 1), goavtest.Video(16, 16, 1)
	nullSink := goav.Sink(component.SinkFunc("null", func(context.Context, component.Message) error { return nil }))
	source := func(name string, in goav.InputSpec) launch.Element {
		return launch.Source(name, "a test input", func(struct{}) (goav.InputSpec, error) { return in, nil })
	}
	inputs := launch.Use(source("mic", mic), source("music", music), source("cam", cam), source("screen", screen))
	for _, tc := range []struct {
		name    string
		program string
		gofor   func() []goav.Output
	}{
		{
			name:    "transcode",
			program: `file in.webm ! video ! decode ! resize 1280x720 ! encode vp9 bitrate=2M ! file out.webm`,
			gofor: func() []goav.Output {
				v := goav.FileInput("in.webm", r()).Video().Decode().Resize(1280, 720).Encode(codec.VP9(codec.Bitrate(2_000_000)))
				return []goav.Output{v.To(goav.Write("out.webm", w()))}
			},
		},
		{
			name: "fanout with shared audio",
			program: `
				in := file in.webm
				v := in ! video ! decode
				v ! resize 1280x720 ! encode vp9 ! as hd ! file hd.webm
				v ! resize 640x360 ! encode vp9 ! as sd ! file sd.webm
				in ! audio ! decode ! encode opus ! file hd.webm, file sd.webm
			`,
			gofor: func() []goav.Output {
				in := goav.FileInput("in.webm", r())
				v := in.Video().Decode()
				hd, sd := goav.Write("hd.webm", w()), goav.Write("sd.webm", w())
				return []goav.Output{
					v.Resize(1280, 720).Encode(codec.VP9()).As("hd").To(hd),
					v.Resize(640, 360).Encode(codec.VP9()).As("sd").To(sd),
					in.Audio().Decode().Encode(codec.Opus()).To(hd, sd),
				}
			},
		},
		{
			name:    "remux",
			program: `file in.mp4 ! file out.mkv`,
			gofor: func() []goav.Output {
				return []goav.Output{goav.FileInput("in.mp4", r()).To(goav.Write("out.mkv", w()))}
			},
		},
		{
			name: "audio and video into one file",
			program: `
				file in.mp4 ! video ! file out.mkv
				file in.mp4 ! audio ! file out.mkv
			`,
			gofor: func() []goav.Output {
				in := goav.FileInput("in.mp4", r())
				out := goav.Write("out.mkv", w())
				return []goav.Output{in.Video().To(out), in.Audio().To(out)}
			},
		},
		{
			name: "several files",
			program: `
				file a.webm ! video ! decode ! encode vp9 ! file x.webm
				file b.webm ! video ! decode ! encode vp9 ! file y.webm
			`,
			gofor: func() []goav.Output {
				a, b := goav.FileInput("a.webm", r()), goav.FileInput("b.webm", r())
				return []goav.Output{
					a.Video().Decode().Encode(codec.VP9()).To(goav.Write("x.webm", w())),
					b.Video().Decode().Encode(codec.VP9()).To(goav.Write("y.webm", w())),
				}
			},
		},
		{
			name: "two files into one mux",
			program: `
				file a.webm ! video ! file m.webm
				file b.webm ! audio ! file m.webm
			`,
			gofor: func() []goav.Output {
				out := goav.Write("m.webm", w())
				return []goav.Output{goav.FileInput("a.webm", r()).Video().To(out), goav.FileInput("b.webm", r()).Audio().To(out)}
			},
		},
		{
			name: "ladder from one input beside another's audio",
			program: `
				v := cam ! video
				v ! encode vp9 bitrate=400k ! as hd ! file hd.webm
				v ! resize 32x24 ! encode vp9 bitrate=80k ! as sd ! file sd.webm
				mic ! audio ! encode opus ! file hd.webm, file sd.webm
			`,
			gofor: func() []goav.Output {
				v := cam.Video()
				hd, sd := goav.Write("hd.webm", w()), goav.Write("sd.webm", w())
				return []goav.Output{
					v.Encode(codec.VP9(codec.Bitrate(400_000))).As("hd").To(hd),
					v.Resize(32, 24).Encode(codec.VP9(codec.Bitrate(80_000))).As("sd").To(sd),
					mic.Audio().Encode(codec.Opus()).To(hd, sd),
				}
			},
		},
		{
			name: "forks of two files",
			program: `
				a := file a.webm ! video ! decode ! resize 32x24
				a ! encode vp8 ! as a-vp8 ! file a-vp8.webm
				a ! encode vp9 ! as a-vp9 ! file a-vp9.webm
				b := file b.webm ! video ! decode
				b ! encode vp8 ! as b-vp8 ! file b-vp8.webm
				b ! resize 16x12 ! encode vp9 ! as b-small ! file b-small.webm
			`,
			gofor: func() []goav.Output {
				a := goav.FileInput("a.webm", r()).Video().Decode().Resize(32, 24)
				b := goav.FileInput("b.webm", r()).Video().Decode()
				return []goav.Output{
					a.Encode(codec.VP8()).As("a-vp8").To(goav.Write("a-vp8.webm", w())),
					a.Encode(codec.VP9()).As("a-vp9").To(goav.Write("a-vp9.webm", w())),
					b.Encode(codec.VP8()).As("b-vp8").To(goav.Write("b-vp8.webm", w())),
					b.Resize(16, 12).Encode(codec.VP9()).As("b-small").To(goav.Write("b-small.webm", w())),
				}
			},
		},
		{
			name:    "mix",
			program: `mix(mic ! audio, music ! audio) ! encode opus bitrate=96k ! file mix.webm`,
			gofor: func() []goav.Output {
				mixed := goav.Mix(mic.Audio(), music.Audio())
				return []goav.Output{mixed.Encode(codec.Opus(codec.Bitrate(96_000))).To(goav.Write("mix.webm", w()))}
			},
		},
		{
			name:    "composite",
			program: `composite(cam ! video, screen ! video ! at 32 24) ! encode vp8 ! file pip.webm`,
			gofor: func() []goav.Output {
				pip := goav.Composite(cam.Video(), goav.At(screen.Video(), 32, 24))
				return []goav.Output{pip.Encode(codec.VP8()).To(goav.Write("pip.webm", w()))}
			},
		},
		{
			name:    "mix files",
			program: `mix(file a.webm ! audio ! decode, file b.webm ! audio ! decode) ! encode opus ! file m.webm`,
			gofor: func() []goav.Output {
				a, b := goav.FileInput("a.webm", r()), goav.FileInput("b.webm", r())
				return []goav.Output{goav.Mix(a.Audio().Decode(), b.Audio().Decode()).Encode(codec.Opus()).To(goav.Write("m.webm", w()))}
			},
		},
		{
			name:    "composite files",
			program: `composite(file a.webm ! video ! decode, file b.webm ! video ! decode ! at 40 30) ! encode vp8 ! file pip.webm`,
			gofor: func() []goav.Output {
				a, b := goav.FileInput("a.webm", r()), goav.FileInput("b.webm", r())
				pip := goav.Composite(a.Video().Decode(), goav.At(b.Video().Decode(), 40, 30))
				return []goav.Output{pip.Encode(codec.VP8()).To(goav.Write("pip.webm", w()))}
			},
		},
		{
			name:    "select",
			program: `select(mic ! audio, music ! audio) ! null`,
			gofor: func() []goav.Output {
				return []goav.Output{goav.Select(mic.Audio(), music.Audio()).To(nullSink)}
			},
		},
		{
			name:    "taps, buffers, names",
			program: `file in.webm ! video ! decode ! tap live ! buffer drop-oldest 3 ! encode vp8 keyframe_interval=60 ! as preview ! file p.webm`,
			gofor: func() []goav.Output {
				s := goav.FileInput("in.webm", r()).Video().Decode().Tap("live").Buffer(flow.DropOldest(3)).Encode(codec.VP8(codec.KeyframeInterval(60))).As("preview")
				return []goav.Output{s.To(goav.Write("p.webm", w()))}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			outs, err := launch.Compile(tc.program, launch.Files(newMemFS()), inputs)
			if err != nil {
				t.Fatalf("compile: %v", err)
			}
			got, want := shapeOf(t, outs), shapeOf(t, tc.gofor())
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("program graph differs from its Go:\nprogram: %+v\ngo:      %+v", got, want)
			}
		})
	}
}

// TestProgramRunsEndToEnd runs generated audio and video into one WebM: two
// pipelines ending at one path share one mux.
func TestProgramRunsEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fs := newMemFS()
	outs, err := launch.Compile(`
		testsrc video size=64x48 frames=3 ! encode vp8 ! file av.webm
		testsrc audio frames=3 ! encode opus ! file av.webm
	`, launch.Files(fs))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := bundle.Run(ctx, outs...); err != nil {
		t.Fatalf("run: %v", err)
	}
	out, ok := fs.outputs["av.webm"]
	if !ok {
		t.Fatal("av.webm was never created")
	}
	demuxer, err := matroska.NewDemuxer(bytes.NewReader(out.Bytes()), matroska.DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	perTrack := map[uint32]int{}
	packet := matroska.Packet{Data: make([]byte, 0, 1<<16)}
	for demuxer.ReadPacket(&packet) == nil {
		perTrack[packet.TrackID]++
	}
	if len(demuxer.Tracks()) != 2 || len(perTrack) != 2 {
		t.Fatalf("av.webm tracks = %d, packets per track = %v; want video and audio in one file", len(demuxer.Tracks()), perTrack)
	}
}

// TestProgramMixesFileProviderAndSourceInputs runs a file, a generated
// (provider) input, and an application source in one task, all into one WebM.
func TestProgramMixesFileProviderAndSourceInputs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	fs := newMemFS()
	fixture, err := launch.Compile(`testsrc video size=64x48 frames=5 ! encode vp8 ! file a.webm`, launch.Files(fs))
	if err != nil {
		t.Fatal(err)
	}
	if err := bundle.Run(ctx, fixture...); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	fs.inputs["a.webm"] = fs.outputs["a.webm"].Bytes()
	mic := launch.Source("mic", "a test input", func(struct{}) (goav.InputSpec, error) {
		return goavtest.Audio(48_000, 2, make([]int16, 1920), make([]int16, 1920)), nil
	})
	outs, err := launch.Compile(`
		file a.webm ! video ! decode ! encode vp9 ! file o.webm
		testsrc audio frames=5 ! encode opus ! file o.webm
		mic ! audio ! encode opus ! file o.webm
	`, launch.Files(fs), launch.Use(mic))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := bundle.Run(ctx, outs...); err != nil {
		t.Fatalf("run: %v", err)
	}
	out := fs.outputs["o.webm"].Bytes()
	demuxer, err := matroska.NewDemuxer(bytes.NewReader(out), matroska.DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	perTrack := map[uint32]int{}
	packet := matroska.Packet{Data: make([]byte, 0, 1<<16)}
	for demuxer.ReadPacket(&packet) == nil {
		perTrack[packet.TrackID]++
	}
	if len(demuxer.Tracks()) != 3 || len(perTrack) != 3 {
		t.Fatalf("o.webm tracks = %d, packets per track = %v; want the file's video and two audio tracks", len(demuxer.Tracks()), perTrack)
	}
	frames := goavtest.NewCollector()
	if err := bundle.Run(ctx, goav.FileInput("o.webm", bytes.NewReader(out)).Video().Decode().To(frames.Sink())); err != nil {
		t.Fatal(err)
	}
	if got := len(frames.Frames()); got != 5 {
		t.Fatalf("o.webm video decodes to %d frames, want the file's 5", got)
	}
}

// TestCompilingTouchesNoFiles pins that a program opens and creates files only
// when it runs.
func TestCompilingTouchesNoFiles(t *testing.T) {
	fs := newMemFS()
	outs, err := launch.Compile(`file in.webm ! video ! file out.webm`, launch.Files(fs))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bundle.Describe(outs...); err != nil {
		t.Fatal(err)
	}
	if len(fs.outputs) != 0 {
		t.Fatalf("describing created %v", fs.outputs)
	}
}

// TestRefusalsPointAtTheMistake pins the refusals: each names the problem,
// underlines it in the program, and says what to write instead.
func TestRefusalsPointAtTheMistake(t *testing.T) {
	files := launch.Files(newMemFS())
	for _, tc := range []struct {
		program string
		want    []string
	}{
		{"file in.webm ! video ! resise 1280x720 ! file out.webm", []string{`1:24: unknown element "resise"`, "^^^^^^", `did you mean "resize"?`}},
		{"file in.webm ! video ! encode vp9 bitrat=2M ! file out.webm", []string{`encode has no setting "bitrat"`}},
		{"file in.webm ! video ! encode vp9 bitrate=fast ! file out.webm", []string{`"fast" is not a whole number`}},
		{"file in.webm ! decode ! file out.webm", []string{"decode needs a stream", "select one first: ... ! video ! decode"}},
		{"file in.webm ! video ! decode", []string{"this pipeline goes nowhere"}},
		{"v := file in.webm ! video ! file out.webm", []string{"a binding names a stream"}},
		{"file in.webm ! video ! file a.webm ! file b.webm", []string{"file is a destination, which comes last"}},
		{"file in.webm, file b.webm ! file out.webm", []string{"a comma separates destinations"}},
		{"tap live ! encode vp8 ! file p.webm", []string{"a tap starts a pipeline only on a running task"}},
		{"file 'in.webm ! video", []string{"unterminated quoted value"}},
		{"mix(file a.webm ! audio ! decode file b.webm) ! file m.webm", []string{"decode takes no settings", "separate a join's inputs with ,"}},
		{"mix(file a.webm ! audio, file b.webm ! audio ! file m.webm", []string{"mix( is not closed"}},
		{"mix(file a.webm ! audio ! decode) ! file m.webm", []string{"mix needs at least two streams"}},
		{"resize 640x360 ! file out.webm", []string{"resize cannot start a pipeline", "put it after one"}},
		{"file in.webm ! video ! resize ! file out.webm", []string{"resize needs a size"}},
		{"file in.webm ! video ! encode acme ! file out.webm", []string{"which media acme encodes", "media=video"}},
		{"in := file in.webm\nin := file b.webm", []string{"2:1: in is already bound"}},
		{"", []string{"the program routes nothing"}},
	} {
		_, err := launch.Compile(tc.program, files)
		var lerr *launch.Error
		if !errors.As(err, &lerr) {
			t.Fatalf("%q: err = %v, want a *launch.Error", tc.program, err)
		}
		for _, want := range tc.want {
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("%q: refusal lacks %q:\n%v", tc.program, want, err)
			}
		}
	}
	if _, err := launch.Compile(`file in.webm ! file out.webm`); err == nil || !strings.Contains(err.Error(), "launch.Files") {
		t.Fatalf("file access without Files: err = %v, want a refusal naming launch.Files", err)
	}
}

// TestRefusalRendering pins the full shape of a rendered refusal.
func TestRefusalRendering(t *testing.T) {
	_, err := launch.Compile("in := file in.webm\nin ! video ! resise 1280x720 ! file out.webm", launch.Files(newMemFS()))
	want := "launch: 2:14: unknown element \"resise\"\n" +
		"    in ! video ! resise 1280x720 ! file out.webm\n" +
		"                 ^^^^^^\n" +
		"    did you mean \"resize\"?"
	if err == nil || err.Error() != want {
		t.Fatalf("rendered refusal:\n%v\nwant:\n%s", err, want)
	}
}

// TestCustomElements pins the extension constructors: a host's source,
// stage, encoder, and sink are used like the built-ins, with typed settings.
func TestCustomElements(t *testing.T) {
	type gainArgs struct {
		DB float64 `launch:"db,positional,required"`
	}
	var gotDB float64
	gain := launch.Stage("gain", "scale audio", func(a gainArgs) (pipeline.Stage, error) {
		gotDB = a.DB
		return component.FrameFunc("gain", func(_ context.Context, f *av.Frame, emit component.Emit) error { return emit.Frame(f) }), nil
	})
	type acmeArgs struct {
		Bitrate int `launch:"bitrate"`
	}
	acme := launch.Encoder("acme", "the acme codec", func(a acmeArgs) (codec.Spec, error) {
		return codec.Opus(codec.Bitrate(a.Bitrate)), nil
	})
	var delivered int
	counter := launch.Sink("counter", "count messages", func(struct{}) (goav.Destination, error) {
		return goav.Sink(component.SinkFunc("counter", func(context.Context, component.Message) error { delivered++; return nil })), nil
	})
	outs, err := launch.Compile(`testsrc audio frames=2 ! gain -6 ! acme bitrate=64k ! counter`, launch.Use(gain, acme, counter))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if gotDB != -6 {
		t.Fatalf("gain db = %v, want -6", gotDB)
	}
	if err := bundle.Run(context.Background(), outs...); err != nil {
		t.Fatalf("run: %v", err)
	}
	if delivered == 0 {
		t.Fatal("the custom sink received nothing")
	}
	infos := launch.Elements(launch.Use(gain))
	var found bool
	for _, info := range infos {
		if info.Name == "gain" {
			found = info.Kind == "operation" && info.Usage == "gain <db>"
		}
	}
	if !found {
		t.Fatalf("Elements lacks gain as an operation with usage \"gain <db>\": %+v", infos)
	}
}

// TestAttachedProgramsStartAtTaps pins Task: a program compiled against a
// running task starts at its taps and attaches like any Output.
func TestAttachedProgramsStartAtTaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	base, err := launch.Compile(`testsrc video size=32x32 live ! tap live ! null`)
	if err != nil {
		t.Fatal(err)
	}
	task, err := bundle.Build(ctx, base...)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	done := make(chan error, 1)
	go func() { done <- task.Run(ctx) }()
	var frames int
	got := make(chan struct{}, 1)
	monitor := launch.Sink("monitor", "count frames", func(struct{}) (goav.Destination, error) {
		return goav.Sink(component.SinkFunc("monitor", func(_ context.Context, m component.Message) error {
			if m.Frame != nil {
				frames++
				select {
				case got <- struct{}{}:
				default:
				}
			}
			return nil
		})), nil
	})
	attach, err := launch.Compile(`tap live ! as monitor ! monitor`, launch.Task(task), launch.Use(monitor))
	if err != nil {
		t.Fatal(err)
	}
	attachment, err := task.Attach(ctx, attach...)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	select {
	case <-got:
	case <-ctx.Done():
		t.Fatal("the attached program received no frames")
	}
	if err := task.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	cancel()
	<-done
}

func FuzzCompile(f *testing.F) {
	for _, seed := range []string{
		`file in.webm ! video ! decode ! resize 1280x720 ! encode vp9 bitrate=2M ! file out.webm`,
		"in := file a.webm\nin ! audio ! file b.webm, null",
		`mix(testsrc audio, testsrc audio ! at 1 2, x=1) ! encode opus ! null`,
		`'unterminated`, `a := := b`, `(((`, `!!`, `,,`, `x=`, "#c\n;;",
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, program string) {
		_, _ = launch.Compile(program, launch.Files(newMemFS()))
	})
}

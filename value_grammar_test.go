package goav_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/pipeline"
)

func specNodeNames(spec pipeline.Spec) []string {
	names := make([]string, 0, len(spec.Nodes))
	for _, node := range spec.Nodes {
		names = append(names, node.Name)
	}
	return names
}

func countNodesWithPrefix(spec pipeline.Spec, prefix string) int {
	n := 0
	for _, node := range spec.Nodes {
		if strings.HasPrefix(node.Name, prefix) {
			n++
		}
	}
	return n
}

// TestValueGrammarTranscodes pins the plain value recipe: an input's stream,
// operations as values, routed to a container and run by the runtime.
func TestValueGrammarTranscodes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var out bytes.Buffer
	in := goavtest.Video(64, 48, 3)
	if err := offlineRuntime().Run(ctx, in.Video().Resize(32, 24).Encode(codec.VP8()).To(goav.Write("out.webm", &out))); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := matroskaTracks(t, out.Bytes()); got != 1 {
		t.Fatalf("out.webm has %d tracks, want 1", got)
	}
}

// TestValueGrammarReuseFansOut pins fanout by reuse: one decoded Stream used
// by two renditions decodes once and encodes twice, each rendition named by
// As and written to its own file.
func TestValueGrammarReuseFansOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	var hd, sd bytes.Buffer
	in := goav.FileInput("in.mp4", bytes.NewReader(data))
	audio := in.Audio().Decode()
	outs := []goav.Output{
		audio.Resample(48_000, 2).Encode(codec.Opus()).As("hd").To(goav.Write("hd.webm", &hd)),
		audio.Resample(24_000, 1).Encode(codec.Opus()).As("sd").To(goav.Write("sd.webm", &sd)),
	}
	rt := offlineRuntime()
	spec, err := rt.Describe(outs...)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got := countNodesWithPrefix(spec, "decode-"); got != 1 {
		t.Fatalf("decode nodes = %d in %v, want one shared decode", got, specNodeNames(spec))
	}
	for _, name := range []string{"encode-hd", "encode-sd"} {
		if countNodesWithPrefix(spec, name) != 1 {
			t.Fatalf("nodes = %v, want %s: As names the rendition", specNodeNames(spec), name)
		}
	}
	if err := rt.Run(ctx, outs...); errors.Is(err, codec.ErrUnavailable) {
		t.Skip("goaac backend unavailable on this target")
	} else if err != nil {
		t.Fatalf("run: %v", err)
	}
	for name, out := range map[string]*bytes.Buffer{"hd": &hd, "sd": &sd} {
		if got := matroskaTracks(t, out.Bytes()); got != 1 {
			t.Fatalf("%s.webm has %d tracks, want 1", name, got)
		}
	}
}

// TestValueGrammarMuxesStreamsIntoOneFile pins mux grouping: two streams of
// one input routed to one Destination value share its container, copied
// without decoding.
func TestValueGrammarMuxesStreamsIntoOneFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	in := goav.FileInput("in.mp4", bytes.NewReader(data))
	out := goav.Write("out.mkv", &buf)
	if err := offlineRuntime().Run(ctx, in.Video().To(out), in.Audio().To(out)); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := matroskaTracks(t, buf.Bytes()); got != 2 {
		t.Fatalf("out.mkv has %d tracks, want video and audio", got)
	}
}

// TestValueGrammarCopiesEveryStream pins input.To: every stream of the input
// copies into the destination.
func TestValueGrammarCopiesEveryStream(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := offlineRuntime().Run(ctx, goav.FileInput("in.mp4", bytes.NewReader(data)).To(goav.Write("out.mkv", &buf))); err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := matroskaTracks(t, buf.Bytes()); got != 2 {
		t.Fatalf("out.mkv has %d tracks, want both input streams", got)
	}
}

// TestValueGrammarJoinsStreams pins joins as Streams: Mix output feeds a sink
// like any stream, and Composite places a Layer At a position.
func TestValueGrammarJoinsStreams(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	frames := 0
	sink := goav.Sink(component.SinkFunc("mixed", func(_ context.Context, msg component.Message) error {
		if msg.Frame != nil {
			frames++
		}
		return nil
	}))
	mix := goav.Mix(
		goavtest.Audio(48_000, 1, []int16{1, 2}).Audio(),
		goavtest.Audio(48_000, 1, []int16{3, 4}).Audio(),
	)
	if err := offlineRuntime().Run(ctx, mix.To(sink)); err != nil {
		t.Fatalf("run mix: %v", err)
	}
	if frames == 0 {
		t.Fatal("no mixed frames reached the sink")
	}
	pip := goav.Composite(goavtest.Video(64, 48, 2).Video(), goav.At(goavtest.Video(16, 16, 2).Video(), 8, 8))
	spec, err := offlineRuntime().Describe(pip.Encode(codec.VP8()).To(goav.Write("pip.webm", new(bytes.Buffer))))
	if err != nil {
		t.Fatalf("describe composite: %v", err)
	}
	if got := countNodesWithPrefix(spec, "composite"); got != 1 {
		t.Fatalf("composite nodes = %d in %v, want 1", got, specNodeNames(spec))
	}
}

// TestValueGrammarTapsAreListed pins Tap: a named point of a Stream is a task
// outlet.
func TestValueGrammarTapsAreListed(t *testing.T) {
	ctx := context.Background()
	in := goavtest.Video(64, 48, 1)
	task, err := offlineRuntime().Build(ctx, in.Video().Tap("preview").Encode(codec.VP8()).To(goav.Write("out.webm", new(bytes.Buffer))))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer task.Close()
	for _, tap := range task.Taps() {
		if tap.Name == "preview" {
			return
		}
	}
	t.Fatalf("taps = %+v, want preview", task.Taps())
}

// TestValueGrammarRefusals pins the recipe-level refusals.
func TestValueGrammarRefusals(t *testing.T) {
	sink := goav.Sink(component.SinkFunc("s", func(context.Context, component.Message) error { return nil }))
	in := goavtest.Video(64, 48, 1)
	mix := goav.Mix(goavtest.Audio(48_000, 1, []int16{1}).Audio(), goavtest.Audio(48_000, 1, []int16{2}).Audio())
	for name, tc := range map[string]struct {
		outs []goav.Output
		code string
	}{
		"no outputs":             {code: "stream_invalid"},
		"zero stream":            {outs: []goav.Output{goav.Stream{}.To(sink)}, code: "stream_invalid"},
		"no destination":         {outs: []goav.Output{in.Video().To()}, code: "stream_invalid"},
		"sync on a plain stream": {outs: []goav.Output{in.Video().SyncByPTS().To(sink)}, code: "stream_invalid"},
		"join beside a stream":   {outs: []goav.Output{mix.To(sink), in.Video().To(sink)}, code: "stream_beside_join"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := offlineRuntime().Describe(tc.outs...)
			var buildErr *goav.BuildError
			if !errors.As(err, &buildErr) || string(buildErr.Code) != tc.code {
				t.Fatalf("Describe = %v, want %s", err, tc.code)
			}
		})
	}
}

var _ = av.MediaVideo

// TestValueGrammarOneEncodeFeedsTwoFiles pins that routing one encoded Stream
// to two destinations encodes once.
func TestValueGrammarOneEncodeFeedsTwoFiles(t *testing.T) {
	in := goavtest.Video(64, 48, 1)
	encoded := in.Video().Encode(codec.VP8())
	spec, err := offlineRuntime().Describe(
		encoded.To(goav.Write("a.webm", new(bytes.Buffer))),
		encoded.As("ignored").To(goav.Write("b.webm", new(bytes.Buffer))),
	)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if got := countNodesWithPrefix(spec, "encode-"); got != 1 {
		t.Fatalf("encode nodes = %d in %v, want one encode feeding both files", got, specNodeNames(spec))
	}
}

package goav_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/source"
)

// The compiler oracle pins what the recipe compiler produces for a corpus
// that crosses every lowering path (single stream, packet copy, multi-stream,
// branches, joins, taps, gates, stream rules) and the common refusals: the
// Describe graph, the Explain report, and each refusal's code, family,
// operation, and node. A compiler change that alters any of them shows up as
// a golden diff. Regenerate with GOAV_UPDATE_GOLDEN=1 go test -run CompilerOracle.

type oracleError struct {
	Code      string `json:"code"`
	Family    string `json:"family"`
	Operation string `json:"operation"`
	Node      string `json:"node"`
}

type oracleResult struct {
	Describe      *pipeline.Spec `json:"describe,omitempty"`
	DescribeError *oracleError   `json:"describe_error,omitempty"`
	Explain       *plan.Report   `json:"explain,omitempty"`
	ExplainError  *oracleError   `json:"explain_error,omitempty"`
	BuildError    *oracleError   `json:"build_error,omitempty"`
}

func oracleErrorFrom(err error) *oracleError {
	if leak := internalErrorIn(err); leak != "" {
		return &oracleError{Code: "leaked: " + leak}
	}
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) {
		return &oracleError{Code: "untyped: " + err.Error()}
	}
	return &oracleError{Code: string(buildErr.Code), Family: string(buildErr.Family), Operation: buildErr.Operation, Node: buildErr.Node}
}

func oracleSink(name string) goav.Destination {
	return goav.Sink(component.SinkFunc(name, func(context.Context, component.Message) error { return nil }))
}

func oracleMP4(t *testing.T) goav.InputSpec {
	t.Helper()
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	return goav.FileInput("in.mp4", bytes.NewReader(data))
}

// oracleSeq names synthetic sources per case: goavtest numbers them from a
// process-wide counter, which would make the goldens depend on test order.
var oracleSeq int

func oracleSource(kind string, spec func(name string) shape.Spec) goav.InputSpec {
	oracleSeq++
	name := fmt.Sprintf("%s-%d", kind, oracleSeq)
	return goav.Source(name, spec(name), func(context.Context, source.Push) error { return nil })
}

func oracleAudio() goav.InputSpec {
	return oracleSource("audio", func(name string) shape.Spec {
		return shape.Frame(av.MediaAudio, shape.Audio(48_000, 2, av.SampleFormatS16), shape.Stream(av.StreamID(name)))
	})
}

func oracleVideoSized(width, height int) goav.InputSpec {
	return oracleSource("video", func(name string) shape.Spec {
		return shape.Frame(av.MediaVideo, shape.Video(width, height, av.PixelFormatI420), shape.Stream(av.StreamID(name)))
	})
}
func oracleAudio44() goav.InputSpec {
	return goav.Source("mic44", shape.Frame(av.MediaAudio, shape.Audio(44_100, 1, av.SampleFormatS16)), func(context.Context, source.Push) error { return nil })
}
func oracleVideo() goav.InputSpec { return oracleVideoSized(64, 48) }

// oracleVP8 is a VP8 packet source: decoding it needs no platform-specific
// backend, so the goldens hold on every CI target.
func oracleVP8() goav.InputSpec {
	return oracleSource("vp8", func(name string) shape.Spec {
		return shape.Packet(av.MediaVideo, av.CodecVP8, shape.Video(64, 48, ""), shape.Stream(av.StreamID(name)))
	})
}

var oraclePacketStage = component.PacketFunc("bsf", func(_ context.Context, p *av.Packet, emit component.Emit) error {
	return emit.Packet(p)
})

var oracleFrameStage = component.FrameFunc("grade", func(_ context.Context, f *av.Frame, emit component.Emit) error {
	return emit.Frame(f)
})

// oracleCorpus is ordered by lowering path; each entry builds fresh Outputs.
func oracleCorpus(t *testing.T) map[string]func() []goav.Output {
	w := func(name string) goav.Destination { return goav.Write(name, io.Discard) }
	outs := func(outs ...goav.Output) []goav.Output { return outs }
	webmA, webmB := mfWebM(t, 64, 48, 3, 48_000, 2, 3), mfWebM(t, 32, 24, 3, 24_000, 1, 3)
	files := func() (goav.InputSpec, goav.InputSpec) {
		return goav.FileInput("a.webm", bytes.NewReader(webmA)), goav.FileInput("b.webm", bytes.NewReader(webmB))
	}
	twoAudio := oracleTwoAudioWebM(t)
	return map[string]func() []goav.Output{
		// Single stream.
		"stream_video_transcode": func() []goav.Output {
			return outs(oracleVideo().Video().Resize(32, 24).Encode(codec.VP8()).To(w("out.webm")))
		},
		"stream_audio_resample_encode": func() []goav.Output {
			return outs(oracleAudio44().Audio().Resample(48_000, 2).Encode(codec.Opus()).To(oracleSink("packets")))
		},
		"stream_audio_auto_resample": func() []goav.Output {
			return outs(oracleAudio44().Audio().Auto(shape.AllowResample()).Encode(codec.Opus()).To(oracleSink("packets")))
		},
		"stream_decode_to_sink": func() []goav.Output {
			return outs(oracleVP8().Video().Decode().To(oracleSink("frames")))
		},
		"stream_custom_stage_tap": func() []goav.Output {
			return outs(oracleVideo().Video().Do(oracleFrameStage).Tap("graded").Encode(codec.VP8()).To(w("out.webm")))
		},
		"stream_playout": func() []goav.Output {
			return outs(oracleVideo().Video().Playout(flow.Playout("screen")).To(oracleSink("screen")))
		},
		"stream_sync": func() []goav.Output {
			return outs(oracleAudio().Audio().Sync(flow.Sync("lip")).To(oracleSink("speaker")))
		},
		"stream_encode_playout": func() []goav.Output {
			return outs(oracleVideo().Video().Encode(codec.VP8()).Playout(flow.Playout("rtp")).To(oracleSink("rtp")))
		},
		"stream_encode_packet_stage": func() []goav.Output {
			return outs(oracleVideo().Video().Encode(codec.VP8()).Do(oraclePacketStage).To(w("out.webm")))
		},
		// Packet copy.
		"copy_packet_stage": func() []goav.Output {
			return outs(oracleMP4(t).Video().Do(oraclePacketStage).To(w("out.mkv")))
		},
		"copy_remux_all": func() []goav.Output {
			return outs(oracleMP4(t).To(w("out.mkv")))
		},
		"copy_selected_stream": func() []goav.Output {
			return outs(oracleMP4(t).Video().To(w("out.mkv")))
		},
		"branches_copy_fanout": func() []goav.Output {
			return outs(oracleMP4(t).Video().To(w("a.mkv"), w("b.mkv")))
		},
		"stream_copy_playout": func() []goav.Output {
			return outs(oracleMP4(t).Video().Playout(flow.Playout("p")).To(oracleSink("p")))
		},
		// Several streams.
		"multi_av_one_file": func() []goav.Output {
			in, out := oracleMP4(t), w("out.mkv")
			return outs(in.Video().To(out), in.Audio().To(out))
		},
		"multi_two_inputs_one_mux": func() []goav.Output {
			out := w("call.webm")
			return outs(oracleVideo().Video().Encode(codec.VP8()).To(out), oracleAudio().Audio().Encode(codec.Opus()).To(out))
		},
		"branches_plain_beside": func() []goav.Output {
			in, a := oracleMP4(t), w("a.mkv")
			return outs(in.Video().To(a, w("b.mkv")), in.Audio().To(a))
		},
		"multi_files_transcode": func() []goav.Output {
			a, b := files()
			return outs(
				a.Video().Decode().Encode(codec.VP9()).To(w("x.webm")),
				b.Video().Decode().Encode(codec.VP9()).To(w("y.webm")),
			)
		},
		"multi_files_copy_one_mux": func() []goav.Output {
			a, b := files()
			out := w("m.webm")
			return outs(a.Video().To(out), b.Video().To(out), b.Audio().To(out))
		},
		"multi_files_remux": func() []goav.Output {
			a, b := files()
			out := w("r.webm")
			return outs(a.To(out), b.To(out))
		},
		"multi_file_and_source": func() []goav.Output {
			a, _ := files()
			out := w("o.webm")
			return outs(a.Video().To(out), oracleAudio().Audio().Encode(codec.Opus()).To(out))
		},
		// Forks.
		"branches_ladder": func() []goav.Output {
			v := oracleVideo().Video()
			return outs(
				v.Resize(64, 48).Encode(codec.VP8()).As("hd").To(w("hd.webm")),
				v.Resize(32, 24).Encode(codec.VP8()).As("sd").To(w("sd.webm")),
			)
		},
		"branches_buffered": func() []goav.Output {
			v := oracleVideo().Video()
			return outs(
				v.Buffer(flow.DropOldest(4)).Encode(codec.VP8()).As("live").To(w("live.webm")),
				v.Buffer(flow.Blocking(8)).Encode(codec.VP8()).As("archive").To(w("archive.webm")),
			)
		},
		"branches_on_rule": func() []goav.Output {
			in := oracleMP4(t)
			return outs(in.Video().To(w("video.mkv")), in.OnStream(source.MatchMedia(av.MediaAudio)).As("rec").To(w("rec.mkv")))
		},
		// Forks across several inputs: each input's forks share its own select,
		// decode, and steps.
		"forks_two_inputs": func() []goav.Output {
			v := oracleVideo().Video()
			hd, sd := w("hd.webm"), w("sd.webm")
			return outs(
				v.Encode(codec.VP8()).As("hd").To(hd),
				v.Resize(32, 24).Encode(codec.VP8()).As("sd").To(sd),
				oracleAudio().Audio().Encode(codec.Opus()).To(hd, sd),
			)
		},
		"forks_two_files": func() []goav.Output {
			a, b := files()
			av, bv := a.Video().Decode().Resize(32, 24), b.Video().Decode().Resize(32, 24)
			return outs(
				av.Encode(codec.VP8()).As("a-vp8").To(w("a-vp8.webm")),
				av.Encode(codec.VP9()).As("a-vp9").To(w("a-vp9.webm")),
				bv.Encode(codec.VP8()).As("b-vp8").To(w("b-vp8.webm")),
				bv.Encode(codec.VP9()).As("b-vp9").To(w("b-vp9.webm")),
			)
		},
		// Work shared below the first fork runs once.
		"forks_deep_shared": func() []goav.Output {
			v := oracleVideo().Video()
			sd := v.Resize(32, 24)
			return outs(
				v.Encode(codec.VP8()).As("hd").To(w("hd.webm")),
				sd.Encode(codec.VP8()).As("sd-vp8").To(w("sd-vp8.webm")),
				sd.Encode(codec.VP9()).As("sd-vp9").To(w("sd-vp9.webm")),
			)
		},
		"joins_share_decoded_arm": func() []goav.Output {
			d := oracleVP8().Video().Decode()
			return outs(goav.Composite(d, oracleVideo().Video()).To(oracleSink("c1")), goav.Composite(d, oracleVideoSized(64, 48).Video()).To(oracleSink("c2")))
		},
		"fork_tap_after_shared_encode": func() []goav.Output {
			e := oracleAudio().Audio().Encode(codec.Opus())
			return outs(e.As("a").To(oracleSink("a")), e.Tap("enc").As("b").To(oracleSink("b")))
		},
		"named_stream_two_files": func() []goav.Output {
			a, b := files()
			out := w("o.webm")
			return outs(a.Video().Decode().Encode(codec.VP8()).As("cam").To(out), b.Audio().To(out))
		},
		// Joins.
		"join_mix_sink": func() []goav.Output {
			return outs(goav.Mix(oracleAudio().Audio(), oracleAudio44().Audio()).To(oracleSink("mixed")))
		},
		"join_mix_encode": func() []goav.Output {
			return outs(goav.Mix(oracleAudio().Audio(), oracleAudio().Audio()).Encode(codec.Opus()).To(w("mix.webm")))
		},
		"join_composite_regions": func() []goav.Output {
			return outs(goav.Composite(oracleVideo().Video(), goav.At(oracleVideoSized(16, 16).Video(), 8, 8)).Encode(codec.VP8()).To(w("pip.webm")))
		},
		"join_select": func() []goav.Output {
			return outs(goav.Select(oracleAudio().Audio(), oracleAudio().Audio()).To(oracleSink("active")))
		},
		"join_nested_mix": func() []goav.Output {
			inner := goav.Mix(oracleAudio().Audio(), oracleAudio().Audio())
			return outs(goav.Mix(inner, oracleAudio().Audio()).To(oracleSink("mixed")))
		},
		"join_branches": func() []goav.Output {
			m := goav.Mix(oracleAudio().Audio(), oracleAudio().Audio())
			return outs(m.Encode(codec.Opus()).As("room").To(w("room.webm")), m.As("monitor").To(oracleSink("monitor")))
		},
		"join_custom": func() []goav.Output {
			stage := component.FrameFunc("ducker", func(_ context.Context, f *av.Frame, emit component.Emit) error { return emit.Frame(f) })
			return outs(goav.Join("ducker", stage, oracleAudio().Audio(), oracleAudio().Audio()).To(oracleSink("ducked")))
		},
		"join_mix_files": func() []goav.Output {
			a, b := files()
			return outs(goav.Mix(a.Audio().Decode(), b.Audio().Decode()).Encode(codec.Opus()).To(w("mix.webm")))
		},
		"join_composite_files": func() []goav.Output {
			a, b := files()
			return outs(goav.Composite(a.Video().Decode(), goav.At(b.Video().Decode(), 40, 30)).Encode(codec.VP8()).To(w("pip.webm")))
		},
		"join_mix_source_then_file": func() []goav.Output {
			a, _ := files()
			return outs(goav.Mix(oracleAudio44().Audio(), a.Audio().Decode()).To(oracleSink("mixed")))
		},
		// A join is a stream like any other: beside other streams, twice in
		// one run, tapped anywhere, and fed by a stream other work reads too.
		"join_beside_stream": func() []goav.Output {
			return outs(goav.Mix(oracleAudio().Audio(), oracleAudio().Audio()).To(oracleSink("mixed")), oracleVideo().Video().To(oracleSink("video")))
		},
		"joins_two": func() []goav.Output {
			return outs(
				goav.Mix(oracleAudio().Audio(), oracleAudio().Audio()).To(oracleSink("m1")),
				goav.Mix(oracleAudio().Audio(), oracleAudio().Audio()).To(oracleSink("m2")),
			)
		},
		"join_tap_after_encode": func() []goav.Output {
			return outs(goav.Mix(oracleAudio().Audio(), oracleAudio().Audio()).Encode(codec.Opus()).Tap("mixed").To(w("mix.webm")))
		},
		"join_arm_shared_input": func() []goav.Output {
			dry := oracleAudio().Audio()
			return outs(goav.Mix(dry, oracleAudio().Audio()).To(oracleSink("mixed")), dry.To(oracleSink("dry")))
		},
		// Refusals.
		"error_frames_into_mux": func() []goav.Output {
			return outs(oracleVP8().Video().Decode().To(w("out.webm")))
		},
		"error_resize_after_copy": func() []goav.Output {
			return outs(oracleMP4(t).Video().Resize(32, 24).To(w("out.mkv")))
		},
		"error_write_without_format": func() []goav.Output {
			return outs(oracleVideo().Video().Encode(codec.VP8()).To(goav.Write("out", io.Discard)))
		},
		"error_ambiguous_audio": func() []goav.Output {
			return outs(goav.FileInput("two.webm", bytes.NewReader(twoAudio)).Audio().Decode().To(oracleSink("frames")))
		},
		"error_mix_one_arm": func() []goav.Output {
			return outs(goav.Mix(oracleAudio().Audio()).To(oracleSink("mixed")))
		},
		"error_stage_after_encode": func() []goav.Output {
			return outs(oracleVideo().Video().Encode(codec.VP8()).Do(oracleFrameStage).To(oracleSink("s")))
		},
	}
}

// oracleTwoAudioWebM is a WebM carrying two audio tracks, so an unnarrowed
// .Audio() is ambiguous.
func oracleTwoAudioWebM(t *testing.T) []byte {
	t.Helper()
	var out bytes.Buffer
	file := goav.Write("two.webm", &out)
	mfRun(t,
		mfTone(48_000, 2, 3).With(goav.Name("en")).Audio().Encode(codec.Opus()).As("en").To(file),
		mfTone(48_000, 2, 3).With(goav.Name("es")).Audio().Encode(codec.Opus()).As("es").To(file),
	)
	return out.Bytes()
}

// oracleExpectedGraphs are the graphs the shapes the one compiler opens are
// expected to plan, written by hand before it landed: node names in schedule
// order and edges grouped by producer. oraclePending lists the ones the
// compiler still refuses, with the refusal code they carry.
var oracleExpectedGraphs = map[string]struct {
	nodes []string
	edges []string
}{
	"join_beside_stream": {
		nodes: []string{"video-3", "audio-1", "audio-2", "mix", "select-video@video-3", "mixed", "video"},
		edges: []string{"video-3>select-video@video-3", "audio-1>mix", "audio-2>mix", "mix>mixed", "select-video@video-3>video"},
	},
	"joins_two": {
		nodes: []string{"audio-1", "audio-2", "mix", "audio-3", "audio-4", "mix-2", "m1", "m2"},
		edges: []string{"audio-1>mix", "audio-2>mix", "mix>m1", "audio-3>mix-2", "audio-4>mix-2", "mix-2>m2"},
	},
	"join_tap_after_encode": {
		nodes: []string{"audio-1", "audio-2", "mix", "encode-mix", "mix.webm"},
		edges: []string{"audio-1>mix", "audio-2>mix", "mix>encode-mix", "encode-mix>mix.webm"},
	},
	"join_arm_shared_input": {
		nodes: []string{"audio-1", "select-audio@audio-1", "audio-2", "mix", "mixed", "dry"},
		edges: []string{"audio-1>select-audio@audio-1", "select-audio@audio-1>mix", "select-audio@audio-1>dry", "audio-2>mix", "mix>mixed"},
	},
	"forks_deep_shared": {
		nodes: []string{"video-1", "select-video", "encode-hd", "resize-video", "encode-sd-vp8", "encode-sd-vp9", "hd.webm", "sd-vp8.webm", "sd-vp9.webm"},
		edges: []string{"video-1>select-video", "select-video>encode-hd", "select-video>resize-video", "encode-hd>hd.webm", "resize-video>encode-sd-vp8", "resize-video>encode-sd-vp9", "encode-sd-vp8>sd-vp8.webm", "encode-sd-vp9>sd-vp9.webm"},
	},
}

// oraclePending maps each expected shape the compiler does not plan yet to
// the refusal code it raises today; an empty code means it plans, but not
// the expected graph yet.
var oraclePending = map[string]string{}

func oracleGraph(spec pipeline.Spec) ([]string, []string) {
	nodes := make([]string, 0, len(spec.Nodes))
	for _, node := range spec.Nodes {
		nodes = append(nodes, node.Name)
	}
	edges := make([]string, 0, len(spec.Edges))
	for _, edge := range spec.Edges {
		edges = append(edges, edge.From.String()+">"+edge.To.String())
	}
	return nodes, edges
}

func TestCompilerOracle(t *testing.T) {
	ctx := context.Background()
	for name, build := range oracleCorpus(t) {
		t.Run(name, func(t *testing.T) {
			var result oracleResult
			oracleSeq = 0
			if spec, err := offlineRuntime().Describe(build()...); err != nil {
				result.DescribeError = oracleErrorFrom(err)
			} else {
				result.Describe = &spec
			}
			oracleSeq = 0
			if task, err := offlineRuntime().Build(ctx, build()...); err != nil {
				result.BuildError = oracleErrorFrom(err)
			} else {
				// Describe is the graph Build makes: the same nodes and edges.
				if result.Describe != nil {
					if got, want := marshalGolden(t, task.Spec()), marshalGolden(t, *result.Describe); !bytes.Equal(got, want) {
						t.Errorf("built graph differs from Describe:\n--- built ---\n%s--- described ---\n%s", got, want)
					}
				}
				task.Close()
			}
			oracleSeq = 0
			if report, err := offlineRuntime().Explain(ctx, build()...); err != nil {
				result.ExplainError = oracleErrorFrom(err)
			} else {
				result.Explain = &report
			}
			if expected, ok := oracleExpectedGraphs[name]; ok {
				code, pending := oraclePending[name]
				switch {
				case pending && code != "":
					if result.DescribeError == nil || result.DescribeError.Code != code {
						t.Errorf("pending shape %s: Describe = %+v, want refusal %s", name, result.DescribeError, code)
					}
				case result.Describe == nil:
					t.Errorf("shape %s refused: %+v", name, result.DescribeError)
				default:
					nodes, edges := oracleGraph(*result.Describe)
					if !pending && (!slices.Equal(nodes, expected.nodes) || !slices.Equal(edges, expected.edges)) {
						t.Errorf("shape %s graph:\n nodes %q\n edges %q\nwant\n nodes %q\n edges %q", name, nodes, edges, expected.nodes, expected.edges)
					}
				}
			}
			assertGolden(t, "oracle/"+name+".json", marshalGolden(t, result))
		})
	}
}

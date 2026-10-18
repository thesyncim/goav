package goav_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
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

// oracleCorpus is ordered by lowering path; each entry builds a fresh job.
func oracleCorpus(t *testing.T) map[string]func() *goav.Job {
	w := func(name string) goav.Destination { return goav.Write(name, io.Discard) }
	// Value-grammar entries compile through the same front door Run takes.
	outputs := func(outs ...goav.Output) *goav.Job {
		job, err := goav.CompileOutputs(outs)
		if err != nil {
			t.Fatalf("compile outputs: %v", err)
		}
		return job
	}
	webmA, webmB := mfWebM(t, 64, 48, 3, 48_000, 2, 3), mfWebM(t, 32, 24, 3, 24_000, 1, 3)
	files := func() (goav.InputSpec, goav.InputSpec) {
		return goav.FileInput("a.webm", bytes.NewReader(webmA)), goav.FileInput("b.webm", bytes.NewReader(webmB))
	}
	return map[string]func() *goav.Job{
		// Single stream.
		"stream_video_transcode": func() *goav.Job {
			return goav.From(oracleVideo()).Video().Resize(32, 24).Encode(codec.VP8()).To(w("out.webm"))
		},
		"stream_audio_resample_encode": func() *goav.Job {
			return goav.From(oracleAudio44()).Audio().Resample(48_000, 2).Encode(codec.Opus()).To(oracleSink("packets"))
		},
		"stream_audio_auto_resample": func() *goav.Job {
			return goav.From(oracleAudio44()).Audio().Auto(shape.AllowResample()).Encode(codec.Opus()).To(oracleSink("packets"))
		},
		"stream_decode_to_sink": func() *goav.Job {
			return goav.From(oracleVP8()).Video().Decode().To(oracleSink("frames"))
		},
		"stream_custom_stage_tap": func() *goav.Job {
			return goav.From(oracleVideo()).Video().Do(oracleFrameStage).Tap(goav.FrameTap("graded")).Encode(codec.VP8()).To(w("out.webm"))
		},
		"stream_playout": func() *goav.Job {
			return goav.From(oracleVideo()).Video().Playout(flow.Playout("screen")).To(oracleSink("screen"))
		},
		"stream_sync": func() *goav.Job {
			return goav.From(oracleAudio()).Audio().Sync(flow.Sync("lip")).To(oracleSink("speaker"))
		},
		"stream_encode_playout": func() *goav.Job {
			return goav.From(oracleVideo()).Video().Encode(codec.VP8()).Playout(flow.Playout("rtp")).To(oracleSink("rtp"))
		},
		"stream_encode_packet_stage": func() *goav.Job {
			return goav.From(oracleVideo()).Video().Encode(codec.VP8()).Do(oraclePacketStage).To(w("out.webm"))
		},
		// Packet copy.
		"copy_packet_stage": func() *goav.Job {
			return goav.From(oracleMP4(t)).Video().Copy().Do(oraclePacketStage).To(w("out.mkv"))
		},
		"copy_remux_all": func() *goav.Job {
			return goav.From(oracleMP4(t)).Copy().To(w("out.mkv"))
		},
		"copy_selected_stream": func() *goav.Job {
			return goav.From(oracleMP4(t)).Video().Copy().To(w("out.mkv"))
		},
		// Multi-stream jobs.
		"multi_av_one_file": func() *goav.Job {
			out := w("out.mkv")
			return goav.From(oracleMP4(t)).Video().Copy().To(out).Audio().Copy().To(out)
		},
		"multi_two_inputs_one_mux": func() *goav.Job {
			out := w("call.webm")
			return goav.From(oracleVideo(), oracleAudio()).
				Video().Encode(codec.VP8()).To(out).
				Audio().Encode(codec.Opus()).To(out)
		},
		"multi_files_transcode": func() *goav.Job {
			a, b := files()
			return outputs(
				a.Video().Decode().Encode(codec.VP9()).To(w("x.webm")),
				b.Video().Decode().Encode(codec.VP9()).To(w("y.webm")),
			)
		},
		"multi_files_copy_one_mux": func() *goav.Job {
			a, b := files()
			out := w("m.webm")
			return outputs(a.Video().To(out), b.Video().To(out), b.Audio().To(out))
		},
		"multi_files_remux": func() *goav.Job {
			a, b := files()
			out := w("r.webm")
			return outputs(a.To(out), b.To(out))
		},
		"multi_file_and_source": func() *goav.Job {
			a, _ := files()
			out := w("o.webm")
			return outputs(a.Video().To(out), oracleAudio().Audio().Encode(codec.Opus()).To(out))
		},
		// Branches.
		"branches_ladder": func() *goav.Job {
			return goav.From(oracleVideo()).Video().Branches(
				goav.Branch("hd").Resize(64, 48).Encode(codec.VP8()).To(w("hd.webm")),
				goav.Branch("sd").Resize(32, 24).Encode(codec.VP8()).To(w("sd.webm")),
			)
		},
		"branches_copy_fanout": func() *goav.Job {
			return goav.From(oracleMP4(t)).Video().Copy().Branches(
				goav.Branch("a").To(w("a.mkv")),
				goav.Branch("b").To(w("b.mkv")),
			)
		},
		"branches_buffered": func() *goav.Job {
			return goav.From(oracleVideo()).Video().Branches(
				goav.Branch("live").Buffer(flow.DropOldest(4)).Encode(codec.VP8()).To(w("live.webm")),
				goav.Branch("archive").Buffer(flow.Blocking(8)).Encode(codec.VP8()).To(w("archive.webm")),
			)
		},
		"branches_plain_beside": func() *goav.Job {
			a := w("a.mkv")
			return goav.From(oracleMP4(t)).
				Video().Copy().Branches(goav.Branch("a").To(a), goav.Branch("b").To(w("b.mkv"))).
				Audio().Copy().To(a)
		},
		"branches_on_rule": func() *goav.Job {
			return goav.From(oracleMP4(t)).Video().Copy().To(w("video.mkv")).OnStream(source.MatchMedia(av.MediaAudio),
				goav.Branch("rec").Copy().To(w("rec.mkv")))
		},
		// Forks across several inputs: each input's forks share its own select,
		// decode, and steps.
		"forks_two_inputs": func() *goav.Job {
			v := oracleVideo().Video()
			hd, sd := w("hd.webm"), w("sd.webm")
			return outputs(
				v.Encode(codec.VP8()).As("hd").To(hd),
				v.Resize(32, 24).Encode(codec.VP8()).As("sd").To(sd),
				oracleAudio().Audio().Encode(codec.Opus()).To(hd, sd),
			)
		},
		"forks_two_files": func() *goav.Job {
			a, b := files()
			av, bv := a.Video().Decode().Resize(32, 24), b.Video().Decode().Resize(32, 24)
			return outputs(
				av.Encode(codec.VP8()).As("a-vp8").To(w("a-vp8.webm")),
				av.Encode(codec.VP9()).As("a-vp9").To(w("a-vp9.webm")),
				bv.Encode(codec.VP8()).As("b-vp8").To(w("b-vp8.webm")),
				bv.Encode(codec.VP9()).As("b-vp9").To(w("b-vp9.webm")),
			)
		},
		"named_stream_two_files": func() *goav.Job {
			a, b := files()
			out := w("o.webm")
			return outputs(a.Video().Decode().Encode(codec.VP8()).As("cam").To(out), b.Audio().To(out))
		},
		// Joins.
		"join_mix_sink": func() *goav.Job {
			return goav.LegacyMix(goav.From(oracleAudio()).Audio(), goav.From(oracleAudio44()).Audio()).To(oracleSink("mixed"))
		},
		"join_mix_encode": func() *goav.Job {
			return goav.LegacyMix(goav.From(oracleAudio()).Audio(), goav.From(oracleAudio()).Audio()).Encode(codec.Opus()).To(w("mix.webm"))
		},
		"join_composite_regions": func() *goav.Job {
			return goav.LegacyComposite(
				goav.From(oracleVideo()).Video(),
				goav.From(oracleVideoSized(16, 16)).Video().Region(8, 8),
			).Encode(codec.VP8()).To(w("pip.webm"))
		},
		"join_select": func() *goav.Job {
			return goav.LegacySelect(goav.From(oracleAudio()).Audio(), goav.From(oracleAudio()).Audio()).To(oracleSink("active"))
		},
		"join_nested_mix": func() *goav.Job {
			inner := goav.LegacyMix(goav.From(oracleAudio()).Audio(), goav.From(oracleAudio()).Audio())
			return goav.LegacyMix(inner, goav.From(oracleAudio()).Audio()).To(oracleSink("mixed"))
		},
		"join_tap_arm": func() *goav.Job {
			return goav.LegacyMix(
				goav.From(oracleAudio()).Audio().Tap(goav.FrameTap("dry")),
				goav.FrameTap("dry"),
			).To(oracleSink("mixed"))
		},
		"join_branches": func() *goav.Job {
			return goav.LegacyMix(goav.From(oracleAudio()).Audio(), goav.From(oracleAudio()).Audio()).Branches(
				goav.Branch("room").Encode(codec.Opus()).To(w("room.webm")),
				goav.Branch("monitor").To(oracleSink("monitor")),
			)
		},
		"join_custom": func() *goav.Job {
			return goav.LegacyJoin("ducker", component.FrameFunc("ducker", func(_ context.Context, f *av.Frame, emit component.Emit) error { return emit.Frame(f) }), goav.From(oracleAudio()).Audio(), goav.From(oracleAudio()).Audio()).To(oracleSink("ducked"))
		},
		"join_mix_files": func() *goav.Job {
			a, b := files()
			return outputs(goav.Mix(a.Audio().Decode(), b.Audio().Decode()).Encode(codec.Opus()).To(w("mix.webm")))
		},
		"join_composite_files": func() *goav.Job {
			a, b := files()
			return outputs(goav.Composite(a.Video().Decode(), goav.At(b.Video().Decode(), 40, 30)).Encode(codec.VP8()).To(w("pip.webm")))
		},
		"join_mix_source_then_file": func() *goav.Job {
			a, _ := files()
			return outputs(goav.Mix(oracleAudio44().Audio(), a.Audio().Decode()).To(oracleSink("mixed")))
		},
		// Refusals.
		"error_frames_into_mux": func() *goav.Job {
			return goav.From(oracleVP8()).Video().Decode().To(w("out.webm"))
		},
		"error_resize_after_copy": func() *goav.Job {
			return goav.From(oracleMP4(t)).Video().Copy().Resize(32, 24).To(w("out.mkv"))
		},
		"error_write_without_format": func() *goav.Job {
			return goav.From(oracleVideo()).Video().Encode(codec.VP8()).To(goav.Write("out", io.Discard))
		},
		"error_ambiguous_audio": func() *goav.Job {
			return goav.From(oracleAudio(), oracleAudio()).Audio().Decode().To(oracleSink("frames"))
		},
		"error_unknown_tap_arm": func() *goav.Job {
			return goav.LegacyMix(goav.From(oracleAudio()).Audio(), goav.FrameTap("nope")).To(oracleSink("mixed"))
		},
		"error_mix_one_arm": func() *goav.Job {
			return goav.LegacyMix(goav.From(oracleAudio()).Audio()).To(oracleSink("mixed"))
		},
		"stream_copy_playout": func() *goav.Job {
			return goav.From(oracleMP4(t)).Video().Copy().Playout(flow.Playout("p")).To(oracleSink("p"))
		},
		"error_stage_after_encode": func() *goav.Job {
			return goav.From(oracleVideo()).Video().Encode(codec.VP8()).Do(oracleFrameStage).To(oracleSink("s"))
		},
		"error_stream_beside_join": func() *goav.Job {
			job := goav.LegacyMix(goav.From(oracleAudio()).Audio(), goav.From(oracleAudio()).Audio()).To(oracleSink("mixed"))
			return job.And(oracleVideo()).Video().To(oracleSink("video"))
		},
		"error_region_outside_composite": func() *goav.Job {
			return goav.From(oracleVideo()).Video().Region(4, 4).To(oracleSink("s"))
		},
	}
}

func TestCompilerOracle(t *testing.T) {
	ctx := context.Background()
	for name, build := range oracleCorpus(t) {
		t.Run(name, func(t *testing.T) {
			var result oracleResult
			oracleSeq = 0
			if spec, err := build().UseRuntime(offlineRuntime()).Describe(); err != nil {
				result.DescribeError = oracleErrorFrom(err)
			} else {
				result.Describe = &spec
			}
			oracleSeq = 0
			if task, err := build().UseRuntime(offlineRuntime()).BuildLive(ctx); err != nil {
				result.BuildError = oracleErrorFrom(err)
			} else {
				// Describe is the graph Build makes: the same nodes and edges.
				if result.Describe != nil {
					if got, want := marshalGolden(t, task.Describe()), marshalGolden(t, *result.Describe); !bytes.Equal(got, want) {
						t.Errorf("built graph differs from Describe:\n--- built ---\n%s--- described ---\n%s", got, want)
					}
				}
				task.Close()
			}
			oracleSeq = 0
			if report, err := build().UseRuntime(offlineRuntime()).Explain(ctx); err != nil {
				result.ExplainError = oracleErrorFrom(err)
			} else {
				result.Explain = &report
			}
			assertGolden(t, "oracle/"+name+".json", marshalGolden(t, result))
		})
	}
}

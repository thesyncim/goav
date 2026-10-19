package goav_test

import (
	"context"
	"io"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/lifecycle"
	"github.com/thesyncim/goav/pipeline"
)

// TestAttachPatchDescribeEqualsBuild is the attach twin of Describe() ==
// Build(): the graph patch an attach plans before it touches the running
// graph is the patch the graph holds once it is applied, node for node and
// edge for edge, whatever the attached Outputs read and share.
func TestAttachPatchDescribeEqualsBuild(t *testing.T) {
	named := func(name string) goav.Destination {
		return goav.Sink(goav.SinkFunc(name, func(context.Context, goav.Message) error { return nil }))
	}
	packets := func() goav.InputSpec {
		return goavtest.Packets(av.CodecOpus, av.Packet{Payload: av.Buffer{Bytes: []byte{1, 2}}, Keyframe: true})
	}
	type attachCase struct {
		name   string
		input  func() goav.InputSpec
		base   func(goav.InputSpec) []goav.Output
		attach func(goav.LiveTask, goav.InputSpec) []goav.Output
		// rebranch attaches the Outputs as a gated Rebranch's replacements.
		rebranch bool
		want     []string
	}
	frames := func() goav.InputSpec { return goavtest.Audio(48_000, 1, make([]int16, 960)) }
	frameTap := func(in goav.InputSpec) []goav.Output {
		return []goav.Output{in.Audio().Tap("audio.frames").To(named("main"))}
	}
	packetTap := func(in goav.InputSpec) []goav.Output {
		return []goav.Output{in.Audio().Tap("audio.packets").To(named("main"))}
	}
	for _, tc := range []attachCase{
		{
			name:  "decode from a packet tap",
			input: packets, base: packetTap,
			attach: func(task goav.LiveTask, _ goav.InputSpec) []goav.Output {
				return []goav.Output{task.Tap("audio.packets").Decode().As("pcm").To(named("pcm"))}
			},
			want: []string{"pcm/decode-pcm", "pcm/pcm"},
		},
		{
			name:  "resample and encode into a mux",
			input: frames, base: frameTap,
			attach: func(task goav.LiveTask, _ goav.InputSpec) []goav.Output {
				return []goav.Output{task.Tap("audio.frames").Resample(16_000, codec.Mono).Encode(codec.Opus()).As("voice").To(goav.Write("voice.ogg", io.Discard, goav.Format(av.FormatOgg)))}
			},
			want: []string{"voice/resample-voice", "voice/encode-voice", "voice/voice.ogg"},
		},
		{
			name:  "a sink two branches share",
			input: frames, base: frameTap,
			attach: func(task goav.LiveTask, _ goav.InputSpec) []goav.Output {
				shared := named("monitor")
				return []goav.Output{
					task.Tap("audio.frames").As("a").To(shared),
					task.Tap("audio.frames").Resample(16_000, codec.Mono).As("b").To(shared),
				}
			},
			want: []string{"b/resample-b", "monitor"},
		},
		{
			name:  "work two branches share",
			input: frames, base: frameTap,
			attach: func(task goav.LiveTask, _ goav.InputSpec) []goav.Output {
				low := task.Tap("audio.frames").Resample(16_000, codec.Mono)
				return []goav.Output{low.As("a").To(named("a")), low.Encode(codec.Opus()).As("b").To(named("b"))}
			},
			want: []string{"a+b/resample-a", "b/encode-b", "a/a", "b/b"},
		},
		{
			name:  "a tap an earlier branch declares",
			input: packets, base: packetTap,
			attach: func(task goav.LiveTask, _ goav.InputSpec) []goav.Output {
				return []goav.Output{
					task.Tap("audio.packets").Decode().Tap("late.frames").As("pcm").To(named("pcm")),
					task.Tap("late.frames").Encode(codec.Opus()).As("again").To(named("again")),
				}
			},
			want: []string{"pcm/decode-pcm", "again/encode-again", "pcm/pcm", "again/again"},
		},
		{
			name:  "an input's track",
			input: packets, base: packetTap,
			attach: func(task goav.LiveTask, in goav.InputSpec) []goav.Output {
				tap := task.Taps()[0]
				stream := av.Stream{ID: tap.Shape.StreamID, Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}}
				return []goav.Output{task.Track(in, stream).As("track").To(named("track"))}
			},
		},
		{
			name:  "a gated rebranch",
			input: frames, base: frameTap, rebranch: true,
			attach: func(task goav.LiveTask, _ goav.InputSpec) []goav.Output {
				return []goav.Output{task.Tap("audio.frames").As("next").To(named("next"))}
			},
			want: []string{"next/switch-gate", "next/next"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			in := tc.input()
			task, err := goavtest.Runtime().Build(ctx, tc.base(in)...)
			if err != nil {
				t.Fatal(err)
			}
			defer task.Close()
			var planned pipeline.Spec
			var attachment goav.Attachment
			if tc.rebranch {
				args := []lifecycle.RebranchArg{lifecycle.SwitchAt(lifecycle.NextKeyframe())}
				for _, out := range tc.attach(task, in) {
					args = append(args, out)
				}
				planned, attachment, err = goav.RebranchPlannedForTest(ctx, task, args...)
			} else {
				planned, attachment, err = goav.AttachPlannedForTest(ctx, task, tc.attach(task, in)...)
			}
			if err != nil {
				t.Fatal(err)
			}
			built := attachment.Spec()
			if !reflect.DeepEqual(planned.Nodes, built.Nodes) || !reflect.DeepEqual(sortedEdges(planned.Edges), sortedEdges(built.Edges)) {
				t.Fatalf("planned attach patch != applied patch\nplanned:\n%s\napplied:\n%s", specText(planned), specText(built))
			}
			if tc.want != nil {
				var names []string
				for _, node := range built.Nodes {
					names = append(names, node.Name)
				}
				if !slices.Equal(names, tc.want) {
					t.Fatalf("attached nodes = %v, want %v", names, tc.want)
				}
			}
		})
	}
}

func sortedEdges(edges []pipeline.EdgeSpec) []pipeline.EdgeSpec {
	out := slices.Clone(edges)
	slices.SortFunc(out, func(a, b pipeline.EdgeSpec) int {
		return strings.Compare(string(a.From)+"\x00"+string(a.To)+"\x00"+a.Label, string(b.From)+"\x00"+string(b.To)+"\x00"+b.Label)
	})
	return out
}

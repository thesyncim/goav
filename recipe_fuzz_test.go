package goav_test

import (
	"context"
	"io"
	"reflect"
	"strconv"
	"testing"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/shape"
)

// FuzzRecipe builds random value-grammar recipes from the fuzz bytes —
// inputs, operations, forks (a Stream reused), joins, taps, names, queues,
// and destinations, sinks and muxes, some shared — and holds the compiler to
// its laws: Describe never panics, and whenever the recipe builds, Describe
// succeeds and equals the built graph node for node.
func FuzzRecipe(f *testing.F) {
	for _, seed := range [][]byte{
		{0, 0, 2, 0, 20, 0},
		{0, 1, 1, 0, 2, 0, 20, 1},
		{0, 0, 0, 1, 8, 0, 1, 20, 2},
		{0, 2, 4, 0, 5, 0, 20, 0, 20, 0},
		{0, 0, 3, 0, 2, 0, 20, 3, 20, 3},
		{0, 2, 0, 2, 9, 0, 1, 20, 0},
		{0, 0, 6, 0, 2, 0, 20, 4, 6, 0, 20, 5},
		{0, 3, 1, 0, 2, 0, 21, 0},
		{0, 0, 0, 0, 10, 0, 1, 20, 1, 7, 0, 20, 0},
		{0, 1, 5, 0, 1, 0, 3, 0, 20, 2, 20, 0},
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		outs := newRecipeGen(data).recipe()
		if len(outs) == 0 {
			return
		}
		rt := goavtest.Runtime()
		planned, describeErr := rt.Describe(outs...)
		_, _ = rt.Explain(context.Background(), outs...)
		task, err := rt.Build(context.Background(), outs...)
		if err != nil {
			return
		}
		defer task.Close()
		if describeErr != nil {
			t.Fatalf("Build succeeds but Describe fails: %v", describeErr)
		}
		if built := task.Describe(); !reflect.DeepEqual(planned, built) {
			t.Fatalf("Describe() != Build()\nplanned:\n%s\nbuilt:\n%s", specText(planned), specText(built))
		}
		// The attach twin: an Output attached at one of the task's taps plans
		// the patch it applies.
		taps := task.Taps()
		if len(taps) == 0 {
			return
		}
		s := task.Tap(taps[int(len(data))%len(taps)].Name)
		switch len(data) % 5 {
		case 1:
			s = s.Decode()
		case 2:
			s = s.Encode(codec.Opus())
		case 3:
			s = s.Resample(16_000, codec.Mono)
		case 4:
			s = s.Decode().Encode(codec.Opus())
		}
		patch, attachment, err := goav.AttachPlannedForTest(context.Background(), task, s.As("late").To(goavtest.NewCollector().Sink()))
		if err != nil {
			return
		}
		applied := attachment.Spec()
		if !reflect.DeepEqual(patch.Nodes, applied.Nodes) || !reflect.DeepEqual(sortedEdges(patch.Edges), sortedEdges(applied.Edges)) {
			t.Fatalf("planned attach patch != applied patch\nplanned:\n%s\napplied:\n%s", specText(patch), specText(applied))
		}
	})
}

// recipeGen reads a recipe from fuzz bytes: each step is one opcode byte
// and one operand byte, and the recipe ends when the bytes do.
type recipeGen struct {
	data    []byte
	streams []goav.Stream
	inputs  []goav.InputSpec
	dests   []goav.Destination
	outs    []goav.Output
	names   int
}

func newRecipeGen(data []byte) *recipeGen { return &recipeGen{data: data} }

const (
	recipeGenMaxSteps   = 16
	recipeGenMaxStreams = 8
)

func (g *recipeGen) byte() byte {
	if len(g.data) == 0 {
		return 0
	}
	b := g.data[0]
	g.data = g.data[1:]
	return b
}

// stream picks a Stream already built: picking one twice forks it.
func (g *recipeGen) stream(b byte) (goav.Stream, bool) {
	if len(g.streams) == 0 {
		return goav.Stream{}, false
	}
	return g.streams[int(b)%len(g.streams)], true
}

func (g *recipeGen) push(s goav.Stream) {
	if len(g.streams) < recipeGenMaxStreams {
		g.streams = append(g.streams, s)
		return
	}
	g.streams[len(g.streams)-1] = s
}

func (g *recipeGen) name(prefix string) string {
	g.names++
	return prefix + strconv.Itoa(g.names)
}

func (g *recipeGen) input(b byte) goav.InputSpec {
	var in goav.InputSpec
	switch b % 4 {
	case 0:
		in = goavtest.Audio(48_000, 1, make([]int16, 960))
	case 1:
		in = goavtest.Video(16, 16, 1)
	case 2:
		in = goavtest.Packets(av.CodecOpus, av.Packet{Payload: av.Buffer{Bytes: []byte{1, 2, 3}}, Keyframe: true})
	default:
		in = goavtest.Packets(av.CodecVP8, av.Packet{Payload: av.Buffer{Bytes: []byte{1, 2, 3}}, Keyframe: true})
	}
	g.inputs = append(g.inputs, in)
	return in
}

// destination is a sink or a mux, or one used already: reusing a value
// shares it.
func (g *recipeGen) destination(b byte) goav.Destination {
	if b%5 == 4 && len(g.dests) != 0 {
		return g.dests[int(b/5)%len(g.dests)]
	}
	var d goav.Destination
	switch b % 4 {
	case 0, 1:
		d = goavtest.NewCollector().Sink()
	case 2:
		d = goav.Write(g.name("out")+".ogg", io.Discard, goav.Format(av.FormatOgg))
	default:
		d = goav.Write(g.name("out")+".webm", io.Discard, goav.Format(av.FormatWebM))
	}
	g.dests = append(g.dests, d)
	return d
}

func (g *recipeGen) recipe() []goav.Output {
	for step := 0; step < recipeGenMaxSteps && len(g.data) != 0; step++ {
		op, arg := g.byte(), g.byte()
		if op%22 == 0 {
			in := g.input(arg)
			if arg/4%2 == 0 {
				g.push(in.Audio())
			} else {
				g.push(in.Video())
			}
			continue
		}
		s, ok := g.stream(arg)
		if !ok {
			continue
		}
		switch op % 22 {
		case 1:
			g.push(s.Decode())
		case 2:
			g.push(s.Encode(codec.Opus()))
		case 3:
			g.push(s.Encode(codec.VP8()))
		case 4:
			g.push(s.Resample(16_000, codec.Mono))
		case 5:
			g.push(s.Resize(8, 8))
		case 6:
			g.push(s.Tap(g.name("tap")))
		case 7:
			g.push(s.As(g.name("fork")))
		case 8:
			other, _ := g.stream(arg / 3)
			g.push(goav.Mix(s, other))
		case 9:
			other, _ := g.stream(arg / 3)
			g.push(goav.Composite(s, goav.At(other, 16, 0)))
		case 10:
			other, _ := g.stream(arg / 3)
			g.push(goav.Select(s, other))
		case 11:
			g.push(s.Buffer(flow.Blocking(4)))
		case 12:
			g.push(s.Auto(shape.AllowResample(), shape.AllowResize()))
		case 13:
			g.push(s.Require(shape.Frame(av.MediaAudio, shape.Audio(48_000, 1, ""))))
		case 14:
			g.push(goav.Mix(s, s))
		case 20:
			g.outs = append(g.outs, s.To(g.destination(g.byte())))
		case 21:
			if len(g.inputs) != 0 {
				in := g.inputs[int(arg)%len(g.inputs)]
				g.outs = append(g.outs, in.To(g.destination(g.byte())))
			}
		default:
			g.outs = append(g.outs, s.To(g.destination(arg)))
		}
	}
	return g.outs
}

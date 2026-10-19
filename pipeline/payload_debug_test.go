//go:build goavdebug

package pipeline

import (
	"context"
	"testing"
)

// rawKeepingSink keeps the bytes it is lent without taking a reference: the
// use after release the goavdebug build exists to expose.
type rawKeepingSink struct {
	name string
	kept [][]byte
}

func (s *rawKeepingSink) Name() string { return s.name }
func (s *rawKeepingSink) Close() error { return nil }
func (s *rawKeepingSink) Handle(_ context.Context, msg *Message) error {
	if msg.Frame != nil {
		s.kept = append(s.kept, msg.Frame.Planes[0].Buffer.Bytes)
	}
	return nil
}

// TestUnretainedBytesReadPoison pins what the goavdebug build catches: in
// either runner, a sink that keeps lent bytes without Ref reads poison once the
// producer and the queues have released them, while a sibling that took its
// references (Ref) still reads every frame as delivered.
func TestUnretainedBytesReadPoison(t *testing.T) {
	for _, mode := range payloadModes {
		t.Run(mode.name, func(t *testing.T) {
			graph, err := NewGraph(GraphConfig{Name: "poison", Buffer: mode.buffer})
			if err != nil {
				t.Fatal(err)
			}
			raw := &rawKeepingSink{name: "raw"}
			keeper := &keepingSink{name: "keeper"}
			if _, err := graph.AddSource(&pooledFrameSource{name: "source", n: 4, size: 64}, BufferPolicy{}); err != nil {
				t.Fatal(err)
			}
			for _, sink := range []Sink{raw, keeper} {
				if _, err := graph.AddSink(sink, BufferPolicy{}); err != nil {
					t.Fatal(err)
				}
			}
			if err := graph.Connect(route("source", "raw", "keeper")); err != nil {
				t.Fatal(err)
			}
			if err := graph.Run(context.Background()); err != nil {
				t.Fatal(err)
			}
			if i, bad := keeper.corrupt(); bad {
				t.Fatalf("referenced frame %d changed", i)
			}
			// The keeper's references keep every frame alive: release them,
			// and the bytes the raw sink kept are poisoned.
			for _, frame := range keeper.kept {
				frame.Release()
			}
			for i, bytes := range raw.kept {
				if bytes[0] != 0xDB {
					t.Fatalf("unreferenced frame %d reads %#x after release, want poison 0xdb", i, bytes[0])
				}
			}
			if err := graph.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

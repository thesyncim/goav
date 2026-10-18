package goav_test

import (
	"bytes"
	"context"
	"errors"
	"os"
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
)

// TestPlainChainBesideBranchesIsBuilt pins that a recipe keeps every stream it
// routes: a plain stream routed before or after a forked one is built, sharing
// the mux of any handle a fork also writes. Both orders used to drop the plain
// stream silently: Describe and Run succeeded with no node for it.
func TestPlainChainBesideBranchesIsBuilt(t *testing.T) {
	for name, build := range map[string]func(in goav.InputSpec, a, b goav.Destination) []goav.Output{
		"plain after forks": func(in goav.InputSpec, a, b goav.Destination) []goav.Output {
			v := in.Video()
			return []goav.Output{v.As("a").To(a), v.As("b").To(b), in.Audio().To(a)}
		},
		"plain before forks": func(in goav.InputSpec, a, b goav.Destination) []goav.Output {
			v := in.Video()
			return []goav.Output{in.Audio().To(a), v.As("a").To(a), v.As("b").To(b)}
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			data, err := os.ReadFile(mp4Fixture)
			if err != nil {
				t.Fatal(err)
			}
			var a, b bytes.Buffer
			outs := build(goav.FileInput("in.mp4", bytes.NewReader(data)), goav.Write("a.mkv", &a), goav.Write("b.mkv", &b))
			if err := offlineRuntime().Run(ctx, outs...); err != nil {
				t.Fatalf("run: %v", err)
			}
			if got := matroskaTracks(t, a.Bytes()); got != 2 {
				t.Fatalf("a.mkv has %d tracks, want video and audio", got)
			}
			if got := matroskaTracks(t, b.Bytes()); got != 1 {
				t.Fatalf("b.mkv has %d tracks, want video only", got)
			}
		})
	}
}

func matroskaTracks(t *testing.T, data []byte) int {
	t.Helper()
	demuxer, err := matroska.NewDemuxer(bytes.NewReader(data), matroska.DemuxerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return len(demuxer.Tracks())
}

// TestPlainChainBesideJoinIsRefused pins that a stream beside a Mix fails the
// build instead of vanishing: the join lowering builds only the joined
// stream, so Describe and Run used to succeed without it.
func TestPlainChainBesideJoinIsRefused(t *testing.T) {
	sink := func(name string) goav.Destination {
		return goav.Sink(component.SinkFunc(name, func(context.Context, component.Message) error { return nil }))
	}
	mixed := goav.Mix(
		goavtest.Audio(48000, 1, []int16{1, 2}).Audio(),
		goavtest.Audio(48000, 1, []int16{3, 4}).Audio(),
	).To(sink("mixed"))
	_, err := bundle.Default().Describe(mixed, goavtest.Video(64, 64, 3).Video().To(sink("video")))
	var buildErr *goav.BuildError
	if !errors.As(err, &buildErr) || string(buildErr.Code) != "stream_beside_join" {
		t.Fatalf("Describe = %v, want a stream_beside_join refusal", err)
	}
}

// TestCopiedPacketsPassAPlayoutGate pins that a single copied stream can be
// paced: .Copy().Playout(...) lowers as one branch with the gate on the
// selected packets. No lowerer claimed a lone copy chain with steps, so it was
// refused although the same chain lowered fine beside a second stream.
func TestCopiedPacketsPassAPlayoutGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	data, err := os.ReadFile(mp4Fixture)
	if err != nil {
		t.Fatal(err)
	}
	packets := 0
	sink := goav.Sink(component.SinkFunc("screen", func(_ context.Context, msg component.Message) error {
		if msg.Packet != nil {
			packets++
		}
		return nil
	}))
	if err := offlineRuntime().Run(ctx, goav.FileInput("in.mp4", bytes.NewReader(data)).
		Video().
		Playout(flow.Playout("screen")).
		To(sink)); err != nil {
		t.Fatalf("run: %v", err)
	}
	if packets == 0 {
		t.Fatal("no copied packets passed the playout gate")
	}
}

// TestPacketStageRunsAfterEncode pins bitstream-filter placement: a packet
// stage declared after .Encode(...) sees the encoded packets, in order, under
// the identity of the stream it encodes, and the muxer receives what it
// emits. .Do after .Encode was refused, and a stage before it ran on frames.
func TestPacketStageRunsAfterEncode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var filtered int
	var ids []av.StreamID
	bsf := component.PacketFunc("bsf", func(_ context.Context, p *av.Packet, emit component.Emit) error {
		filtered++
		if len(ids) == 0 {
			ids = append(ids, p.StreamID)
		}
		return emit.Packet(p)
	})
	var out bytes.Buffer
	if err := offlineRuntime().Run(ctx, goavtest.Video(64, 48, 3).
		Video().
		Encode(codec.VP8()).
		Do(bsf).
		To(goav.Write("out.webm", &out))); err != nil {
		t.Fatalf("run: %v", err)
	}
	if filtered != 3 {
		t.Fatalf("packet stage saw %d packets, want the 3 encoded frames", filtered)
	}
	// The chain lowers as one branch named "video"; the encoded stream keeps
	// the source stream's id instead of taking the branch name.
	if ids[0] == "video" {
		t.Fatalf("packet stage saw stream %q, want the source stream id kept", ids[0])
	}
	if got := matroskaTracks(t, out.Bytes()); got != 1 {
		t.Fatalf("out.webm has %d tracks, want the filtered video", got)
	}
}

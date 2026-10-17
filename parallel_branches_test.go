package goav_test

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/component"
	"github.com/thesyncim/goav/goavtest"
	"github.com/thesyncim/goav/runconfig"
)

// rendezvousEncoders counts encoders inside EncodeInto at once. Each call
// waits briefly for a second encoder to join it, so the peak shows whether
// two branches ever encoded concurrently.
type rendezvousEncoders struct {
	inside atomic.Int64
	peak   atomic.Int64
}

func (r *rendezvousEncoders) NewEncoder(context.Context, codec.EncodeConfig) (codec.Encoder, error) {
	return &rendezvousEncoder{shared: r}, nil
}

type rendezvousEncoder struct{ shared *rendezvousEncoders }

func (e *rendezvousEncoder) Descriptor() codec.Descriptor {
	return codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}
}
func (e *rendezvousEncoder) Open(context.Context, codec.EncodeConfig) error { return nil }
func (e *rendezvousEncoder) EncodeInto(_ context.Context, frame *av.Frame, out *codec.EncodeResult) error {
	n := e.shared.inside.Add(1)
	defer e.shared.inside.Add(-1)
	for deadline := time.Now().Add(50 * time.Millisecond); n < 2 && time.Now().Before(deadline); n = e.shared.inside.Load() {
		time.Sleep(time.Millisecond)
	}
	if peak := e.shared.peak.Load(); n > peak {
		e.shared.peak.CompareAndSwap(peak, n)
	}
	index := len(out.Packets)
	out.Packets = out.Packets[:index+1]
	packet := &out.Packets[index]
	packet.Reset()
	packet.StreamID, packet.Type, packet.PTS, packet.DTS, packet.Duration = frame.StreamID, av.MediaVideo, frame.PTS, frame.PTS, frame.Duration
	packet.Keyframe = true
	packet.Payload.Bytes = append(packet.Payload.Bytes[:0], 1)
	return nil
}
func (e *rendezvousEncoder) FlushInto(context.Context, *codec.EncodeResult) error { return nil }
func (e *rendezvousEncoder) HandleEvent(context.Context, *av.Event) error         { return nil }
func (e *rendezvousEncoder) Close() error                                         { return nil }

// TestEncodeLadderRunsRungsConcurrently pins the default: a fanout whose
// branches encode into goav-owned outputs runs its rungs on their own
// workers, while a fanout that hands frames to caller code (a Sink) keeps
// them serial, because concurrency would then be visible to the caller, and
// so does a shared output, whose packet interleave must stay deterministic.
func TestEncodeLadderRunsRungsConcurrently(t *testing.T) {
	for _, tt := range []struct {
		name     string
		output   func(string) goav.Destination
		wantPeak int64
	}{
		{"muxed outputs encode in parallel", func(name string) goav.Destination { return goav.Write(name+".webm", io.Discard) }, 2},
		{"a caller sink keeps rungs serial", func(name string) goav.Destination {
			if name == "low" {
				return goav.Sink(component.SinkFunc("low", func(context.Context, component.Message) error { return nil }))
			}
			return goav.Write(name+".webm", io.Discard)
		}, 1},
		{"a shared output keeps rungs serial", func() func(string) goav.Destination {
			shared := goav.Write("both.webm", io.Discard)
			return func(string) goav.Destination { return shared }
		}(), 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			encoders := &rendezvousEncoders{}
			rt := goavtest.Runtime(runconfig.WithEncoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, encoders))
			err := goav.From(goavtest.Video(64, 48, 4)).Video().Branches(
				goav.Branch("high").Encode(codec.VP8()).To(tt.output("high")),
				goav.Branch("low").Resize(32, 24).Encode(codec.VP8()).To(tt.output("low")),
			).UseRuntime(rt).Run(context.Background())
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got := encoders.peak.Load(); got != tt.wantPeak {
				t.Fatalf("peak concurrent encoders = %d, want %d", got, tt.wantPeak)
			}
		})
	}
}

// TestParallelLadderMatchesSerialRungs pins that running rungs concurrently
// is invisible in the output: each rung of a real decode-resize-encode ladder
// carries exactly the encoded frames of the same rung built as its own
// single-chain job, which runs serially. (Container metadata differs between
// the two shapes, so the frames are compared, not the files.)
func TestParallelLadderMatchesSerialRungs(t *testing.T) {
	input := encodeTestWebM(t, 30)
	type rung struct {
		name          string
		width, height int
	}
	rungs := []rung{{"full", 0, 0}, {"half", 32, 24}, {"quarter", 16, 12}}
	ladder := make([]bytes.Buffer, len(rungs))
	branches := make([]goav.BranchSpec, len(rungs))
	for i, r := range rungs {
		branch := goav.Branch(r.name)
		if r.width != 0 {
			branch = branch.Resize(r.width, r.height)
		}
		branches[i] = branch.Encode(codec.VP8(codec.Bitrate(200_000))).To(goav.Write(r.name+".webm", &ladder[i]))
	}
	if err := bundle.Run(context.Background(), goav.From(goav.FileInput("in.webm", bytes.NewReader(input))).Video().Decode().Branches(branches...)); err != nil {
		t.Fatalf("ladder: %v", err)
	}
	for i, r := range rungs {
		var serial bytes.Buffer
		chain := goav.From(goav.FileInput("in.webm", bytes.NewReader(input))).Video().Decode()
		if r.width != 0 {
			chain = chain.Resize(r.width, r.height)
		}
		if err := bundle.Run(context.Background(), chain.Encode(codec.VP8(codec.Bitrate(200_000))).To(goav.Write(r.name+".webm", &serial))); err != nil {
			t.Fatalf("rung %s alone: %v", r.name, err)
		}
		got, want := encodedFrames(t, ladder[i].Bytes()), encodedFrames(t, serial.Bytes())
		if len(got) != 30 || len(got) != len(want) {
			t.Fatalf("rung %s: ladder carries %d frames, alone %d, want 30 each", r.name, len(got), len(want))
		}
		for j := range want {
			if !bytes.Equal(got[j], want[j]) {
				t.Fatalf("rung %s frame %d: ladder bytes differ from the rung built alone", r.name, j)
			}
		}
	}
}

// encodedFrames demuxes a WebM file and returns its video packet payloads.
func encodedFrames(t *testing.T, webm []byte) [][]byte {
	t.Helper()
	out := goavtest.NewCollector()
	if err := bundle.Run(context.Background(), goav.From(goav.FileInput("out.webm", bytes.NewReader(webm))).Video().Copy().To(out.Sink())); err != nil {
		t.Fatalf("demux: %v", err)
	}
	frames := make([][]byte, 0, len(out.Packets()))
	for _, packet := range out.Packets() {
		frames = append(frames, packet.Payload.Bytes)
	}
	return frames
}

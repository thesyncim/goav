package goav

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/shape"
)

// PTS-aligned joins use millisecond stamps with explicit 20ms frame durations,
// so the tolerance (half a frame: 10ms) and the step math stay readable.

func joinSyncMS(value int64) av.Timestamp {
	return av.Timestamp{Value: value, Base: av.TimeBase{Num: 1, Den: 1000}}
}

func mixSyncTestFrame(id av.StreamID, ptsMS int64, samples ...int16) *av.Frame {
	frame := mixTestS16Frame(id, samples...)
	frame.PTS = joinSyncMS(ptsMS)
	frame.Duration = av.Duration{Value: 20, Base: av.TimeBase{Num: 1, Den: 1000}}
	return frame
}

func frameMsg(frame *av.Frame) *pipeline.Message {
	return &pipeline.Message{Kind: pipeline.MessageFrame, Frame: frame}
}

func eventMsg(eventType av.EventType, id av.StreamID) *pipeline.Message {
	return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: eventType, StreamID: id}}
}

// TestAudioMixSyncByPTSAlignsOffsetArms: arm b starts one frame later than arm
// a. Under SyncByPTS the leading region plays a alone (b's silence is the
// summing identity), then the PTS-matched pairs sum — never a[i]+b[i+1] as
// arrival pairing would produce.
func TestAudioMixSyncByPTSAlignsOffsetArms(t *testing.T) {
	mix := newAudioMixStage("mix", []av.StreamID{"a", "b"}, "mix", joinSyncPTS)
	emit := &mixTestEmitter{}
	ctx := context.Background()

	for _, frame := range []*av.Frame{
		mixSyncTestFrame("a", 0, 10, 10),
		mixSyncTestFrame("b", 20, 1, 1), // b's head is one step ahead: a@0 plays solo
		mixSyncTestFrame("a", 20, 20, 20),
		mixSyncTestFrame("a", 40, 30, 30),
		mixSyncTestFrame("b", 40, 2, 2),
	} {
		if err := mix.Handle(ctx, frameMsg(frame), emit); err != nil {
			t.Fatal(err)
		}
	}

	want := [][]int16{{10, 10}, {21, 21}, {32, 32}}
	if len(emit.frames) != len(want) {
		t.Fatalf("frames=%d, want %d", len(emit.frames), len(want))
	}
	for i := range want {
		if got := mixTestReadS16(emit.frames[i]); !reflect.DeepEqual(got, want[i]) {
			t.Fatalf("step %d mixed=%v, want %v", i, got, want[i])
		}
	}
	for i, wantPTS := range []int64{0, 20, 40} {
		if emit.frames[i].PTS != joinSyncMS(wantPTS) {
			t.Fatalf("step %d pts=%+v, want %dms (the step's timing reference)", i, emit.frames[i].PTS, wantPTS)
		}
	}
}

// TestAudioMixSyncByPTSDropsStaleFrames: a head entirely behind the
// already-mixed timeline (duplicate/backwards PTS without a discontinuity) is
// dropped to catch up, never replayed.
func TestAudioMixSyncByPTSDropsStaleFrames(t *testing.T) {
	mix := newAudioMixStage("mix", []av.StreamID{"a", "b"}, "mix", joinSyncPTS)
	emit := &mixTestEmitter{}
	ctx := context.Background()

	for _, frame := range []*av.Frame{
		mixSyncTestFrame("a", 0, 10, 10),
		mixSyncTestFrame("b", 0, 1, 1),
		mixSyncTestFrame("a", 0, 99, 99), // stale duplicate: behind the emitted step
		mixSyncTestFrame("a", 20, 20, 20),
		mixSyncTestFrame("b", 20, 2, 2),
	} {
		if err := mix.Handle(ctx, frameMsg(frame), emit); err != nil {
			t.Fatal(err)
		}
	}

	want := [][]int16{{11, 11}, {22, 22}}
	if len(emit.frames) != len(want) {
		t.Fatalf("frames=%d, want %d (stale head dropped, not mixed)", len(emit.frames), len(want))
	}
	for i := range want {
		if got := mixTestReadS16(emit.frames[i]); !reflect.DeepEqual(got, want[i]) {
			t.Fatalf("step %d mixed=%v, want %v", i, got, want[i])
		}
	}
	if got := mix.DroppedMessages(); got != 1 {
		t.Fatalf("dropped=%d, want 1", got)
	}
}

// TestAudioMixSyncByPTSFlushesArmOnDiscontinuity: a discontinuity on one arm
// (Seek/Segment repositioned it) flushes that arm's pending queue, re-syncs at
// the new position, and forwards one discontinuity on the joined stream.
func TestAudioMixSyncByPTSFlushesArmOnDiscontinuity(t *testing.T) {
	mix := newAudioMixStage("mix", []av.StreamID{"a", "b"}, "mix", joinSyncPTS)
	emit := &mixTestEmitter{}
	ctx := context.Background()

	if err := mix.Handle(ctx, frameMsg(mixSyncTestFrame("a", 0, 99, 99)), emit); err != nil {
		t.Fatal(err)
	}
	if err := mix.Handle(ctx, eventMsg(av.EventDiscontinuity, "a"), emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.events) != 1 || emit.events[0].Type != av.EventDiscontinuity || emit.events[0].StreamID != "mix" {
		t.Fatalf("events=%+v, want one discontinuity re-stamped to the join output", emit.events)
	}
	if got := mix.DroppedMessages(); got != 1 {
		t.Fatalf("dropped=%d, want 1 (the stale pre-seek frame flushed)", got)
	}

	if err := mix.Handle(ctx, frameMsg(mixSyncTestFrame("a", 100, 10, 10)), emit); err != nil {
		t.Fatal(err)
	}
	if err := mix.Handle(ctx, frameMsg(mixSyncTestFrame("b", 100, 1, 1)), emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.frames) != 1 {
		t.Fatalf("frames=%d, want 1 (stale pre-seek frame must not mix)", len(emit.frames))
	}
	if got := mixTestReadS16(emit.frames[0]); !reflect.DeepEqual(got, []int16{11, 11}) {
		t.Fatalf("mixed=%v, want [11 11] (post-seek pair)", got)
	}
	if emit.frames[0].PTS != joinSyncMS(100) {
		t.Fatalf("pts=%+v, want 100ms (re-synced at the new position)", emit.frames[0].PTS)
	}
}

// TestAudioMixArrivalIgnoresDiscontinuity pins the arrival contract: arrival
// joins pair by order only, so a discontinuity neither flushes nor forwards.
func TestAudioMixArrivalIgnoresDiscontinuity(t *testing.T) {
	mix := newAudioMixStage("mix", []av.StreamID{"a", "b"}, "mix", joinSyncArrival)
	emit := &mixTestEmitter{}
	ctx := context.Background()

	_ = mix.Handle(ctx, frameMsg(mixTestS16Frame("a", 99, 99)), emit)
	if err := mix.Handle(ctx, eventMsg(av.EventDiscontinuity, "a"), emit); err != nil {
		t.Fatal(err)
	}
	_ = mix.Handle(ctx, frameMsg(mixTestS16Frame("b", 1, 1)), emit)
	if len(emit.events) != 0 {
		t.Fatalf("events=%+v, want none (arrival joins ignore discontinuities)", emit.events)
	}
	if len(emit.frames) != 1 || !reflect.DeepEqual(mixTestReadS16(emit.frames[0]), []int16{100, 100}) {
		t.Fatalf("frames=%+v, want the buffered frame still mixed", emit.frames)
	}
}

// TestAudioMixEndedArmStopsGating: when one arm ends early the remaining arms
// keep mixing without it; the joined stream ends only when every arm ends.
func TestAudioMixEndedArmStopsGating(t *testing.T) {
	mix := newAudioMixStage("mix", []av.StreamID{"a", "b"}, "mix", joinSyncArrival)
	emit := &mixTestEmitter{}
	ctx := context.Background()

	_ = mix.Handle(ctx, frameMsg(mixTestS16Frame("a", 10, 10)), emit)
	_ = mix.Handle(ctx, frameMsg(mixTestS16Frame("b", 1, 1)), emit)
	if err := mix.Handle(ctx, eventMsg(av.EventEndOfStream, "a"), emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.events) != 0 {
		t.Fatal("joined EOS before all arms ended")
	}
	// Arm a ended: b continues solo instead of stalling the join.
	if err := mix.Handle(ctx, frameMsg(mixTestS16Frame("b", 2, 2)), emit); err != nil {
		t.Fatal(err)
	}
	if err := mix.Handle(ctx, eventMsg(av.EventEndOfStream, "b"), emit); err != nil {
		t.Fatal(err)
	}

	want := [][]int16{{11, 11}, {2, 2}}
	if len(emit.frames) != len(want) {
		t.Fatalf("frames=%d, want %d (b keeps mixing after a ends)", len(emit.frames), len(want))
	}
	for i := range want {
		if got := mixTestReadS16(emit.frames[i]); !reflect.DeepEqual(got, want[i]) {
			t.Fatalf("step %d mixed=%v, want %v", i, got, want[i])
		}
	}
	if len(emit.events) != 1 || emit.events[0].Type != av.EventEndOfStream || emit.events[0].StreamID != "mix" {
		t.Fatalf("events=%+v, want one mix EOS at the end", emit.events)
	}
}

func compositeSyncTestFrame(id av.StreamID, ptsMS int64, w, h int, y, u, v byte) *av.Frame {
	frame := compositeTestI420Frame(id, w, h, y, u, v)
	frame.PTS = joinSyncMS(ptsMS)
	frame.Duration = av.Duration{Value: 20, Base: av.TimeBase{Num: 1, Den: 1000}}
	return frame
}

// TestVideoCompositeSyncByPTSPaintsPresentArms: a gap step paints only the
// arms whose head matches the step time; the canvas keeps covering the absent
// arm's last-known extent so the geometry stays stable.
func TestVideoCompositeSyncByPTSPaintsPresentArms(t *testing.T) {
	stage := newVideoCompositeStage("composite", []av.StreamID{"a", "b"}, "out",
		[]compositeLayout{{X: 0, Y: 0}, {X: 4, Y: 0}}, joinSyncPTS)
	emit := &mixTestEmitter{}
	ctx := context.Background()

	if err := stage.Handle(ctx, frameMsg(compositeSyncTestFrame("a", 0, 4, 4, 100, 10, 20)), emit); err != nil {
		t.Fatal(err)
	}
	if err := stage.Handle(ctx, frameMsg(compositeSyncTestFrame("b", 20, 4, 4, 200, 30, 40)), emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.frames) != 1 {
		t.Fatalf("frames=%d, want 1 (a@0 steps solo; b@20 is a gap arm)", len(emit.frames))
	}
	gap := emit.frames[0]
	if gap.Video.Width != 8 || gap.Video.Height != 4 {
		t.Fatalf("gap-step canvas=%dx%d, want 8x4 (stable geometry over b's extent)", gap.Video.Width, gap.Video.Height)
	}
	if gap.PTS != joinSyncMS(0) {
		t.Fatalf("gap-step pts=%+v, want 0ms", gap.PTS)
	}
	if got := compositeTestYAt(gap, 0, 0); got != 100 {
		t.Fatalf("Y at (0,0)=%d, want 100 (arm a painted)", got)
	}
	if got := compositeTestYAt(gap, 5, 1); got != 0 {
		t.Fatalf("Y at (5,1)=%d, want 0 (absent arm b not painted)", got)
	}

	if err := stage.Handle(ctx, frameMsg(compositeSyncTestFrame("a", 20, 4, 4, 100, 10, 20)), emit); err != nil {
		t.Fatal(err)
	}
	if len(emit.frames) != 2 {
		t.Fatalf("frames=%d, want 2 (a@20 + b@20 PTS-matched)", len(emit.frames))
	}
	pair := emit.frames[1]
	if pair.PTS != joinSyncMS(20) {
		t.Fatalf("paired-step pts=%+v, want 20ms", pair.PTS)
	}
	if got := compositeTestYAt(pair, 5, 1); got != 200 {
		t.Fatalf("Y at (5,1)=%d, want 200 (arm b painted on the matched step)", got)
	}
}

func mixSyncTestSource(id av.StreamID, ptsMS []int64, frames [][]int16) InputSpec {
	return Source(string(id),
		shape.Frame(av.MediaAudio, shape.Audio(48000, codec.Mono, av.SampleFormatS16), shape.Stream(id)),
		func(_ context.Context, push SourcePush) error {
			for i := range frames {
				if _, err := push.Frame(mixSyncTestFrame(id, ptsMS[i], frames[i]...)); err != nil {
					return err
				}
			}
			return push.EOS()
		})
}

// TestMixSyncByPTSEndToEnd runs two offset sources through the full
// Mix(...).SyncByPTS().To(sink) recipe. Whatever the arrival interleaving, the
// PTS alignment makes the output deterministic: a leads solo, the matched pair
// sums, and b finishes solo after a ends.
func TestMixSyncByPTSEndToEnd(t *testing.T) {
	ctx := context.Background()
	var got [][]int16
	var gotPTS []int64
	sink := Sink(SinkFunc("out", func(_ context.Context, m Message) error {
		if m.Kind == pipeline.MessageFrame && m.Frame != nil && m.Frame.Audio != nil {
			got = append(got, mixTestReadS16(m.Frame))
			gotPTS = append(gotPTS, m.Frame.PTS.Value)
		}
		return nil
	}))

	task, err := Mix(
		From(mixSyncTestSource("a", []int64{0, 20}, [][]int16{{10, 10}, {20, 20}})).Audio(),
		From(mixSyncTestSource("b", []int64{20, 40}, [][]int16{{1, 1}, {2, 2}})).Audio(),
	).SyncByPTS().To(sink).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}

	want := [][]int16{{10, 10}, {21, 21}, {2, 2}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("mixed=%v, want %v", got, want)
	}
	if wantPTS := []int64{0, 20, 40}; !reflect.DeepEqual(gotPTS, wantPTS) {
		t.Fatalf("pts=%v, want %v", gotPTS, wantPTS)
	}
}

// TestMixSyncByPTSDropsVisibleInStats pins the drop-visibility surface: a
// stale duplicate frame the PTS-sync join sheds to stay aligned shows up on
// the join node's counters — task.Stats().Nodes["mix"].Dropped under the
// "sync" reason — and through Snapshot(). No silent loss.
func TestMixSyncByPTSDropsVisibleInStats(t *testing.T) {
	ctx := context.Background()
	task, err := Mix(
		// Arm a repeats PTS 0: the duplicate lands behind the emitted timeline
		// and is dropped to catch up, whatever the arrival interleaving.
		From(mixSyncTestSource("a", []int64{0, 0, 20}, [][]int16{{10, 10}, {99, 99}, {20, 20}})).Audio(),
		From(mixSyncTestSource("b", []int64{0, 20}, [][]int16{{1, 1}, {2, 2}})).Audio(),
	).SyncByPTS().To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil }))).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}

	stats := task.Stats()
	node, ok := stats.Nodes["mix"]
	if !ok {
		t.Fatalf("stats.Nodes = %+v, want a mix entry", stats.Nodes)
	}
	if node.Dropped != 1 || node.DropReasons[pipeline.DropSync] != 1 {
		t.Fatalf("mix dropped=%d reasons=%+v, want 1 under sync", node.Dropped, node.DropReasons)
	}
	if stats.Dropped < 1 || stats.DropReasons[pipeline.DropSync] != 1 {
		t.Fatalf("graph dropped=%d reasons=%+v, want the join drop folded in", stats.Dropped, stats.DropReasons)
	}
	if snap := task.Snapshot(); snap.Stats.Nodes["mix"].Dropped != 1 {
		t.Fatalf("snapshot mix dropped=%d, want 1", snap.Stats.Nodes["mix"].Dropped)
	}
}

// TestJoinDescribeEqualsBuildMixSyncByPTS: the sync mode is part of the plan —
// the planned join node carries the sync=pts detail, the built stage reports
// the same through DescribeNode, and Describe() ≡ Build() stays exact.
func TestJoinDescribeEqualsBuildMixSyncByPTS(t *testing.T) {
	job := Mix(
		From(mixSyncTestSource("a", []int64{0}, [][]int16{{1, 1}})).Audio(),
		From(mixSyncTestSource("b", []int64{0}, [][]int16{{2, 2}})).Audio(),
	).SyncByPTS().To(Sink(SinkFunc("out", func(context.Context, Message) error { return nil })))

	planned := joinPlanGuard(t, job)
	if text := specText(planned); !strings.Contains(text, "sync=pts") {
		t.Fatalf("planned spec missing the sync=pts join detail:\n%s", text)
	}
}

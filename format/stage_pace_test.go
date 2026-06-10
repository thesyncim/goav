package format

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

// fakePaceClock is the deterministic av.Clock the pacing tests run on: Sleep
// records each requested wait and advances Now by it, so a paced pump runs
// instantly and tests assert the exact sleep sequence the pacer asked for —
// no wall-clock anywhere.
type fakePaceClock struct {
	now    time.Duration
	sleeps []time.Duration
}

func (c *fakePaceClock) Now() time.Duration { return c.now }

func (c *fakePaceClock) Sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.sleeps = append(c.sleeps, d)
	if d > 0 {
		c.now += d
	}
	return nil
}

func rateMessage(rate float64) *pipeline.Message {
	return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type:     av.EventRate,
		Metadata: av.RateMetadata(rate),
	}}
}

// pacedTimeline builds a realtime DemuxSource over a seekable fake demuxer
// whose packets sit at the given media times, paced on the fake clock.
func pacedTimeline(t *testing.T, clock *fakePaceClock, times ...time.Duration) pipeline.ControllableSource {
	t.Helper()
	packets := make([]timelinePacket, len(times))
	for i, at := range times {
		packets[i] = timelinePacket{stream: "video", ptsNS: int64(at), keyframe: true}
	}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: &seekableFakeDemuxer{
			streams: []av.Stream{{ID: "video", Type: av.MediaVideo}},
			packets: packets,
		},
		Result:   ReadResult{Packet: &packet},
		Realtime: true,
		Clock:    clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	return source.(pipeline.ControllableSource)
}

func assertSleeps(t *testing.T, got []time.Duration, want []time.Duration) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("sleeps = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("sleeps = %v, want %v", got, want)
		}
	}
}

func TestDemuxSourceRealtimePacesPacketCadence(t *testing.T) {
	clock := &fakePaceClock{}
	source := pacedTimeline(t, clock,
		0, 20*time.Millisecond, 40*time.Millisecond, 60*time.Millisecond)

	emitter := &seekLogEmitter{}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	// The first packet anchors (no sleep); every following packet waits its
	// media-time delta. Nothing else — stream events and EOS — paces.
	assertSleeps(t, clock.sleeps, []time.Duration{
		20 * time.Millisecond, 20 * time.Millisecond, 20 * time.Millisecond,
	})
	assertSeekLog(t, emitter.log, []int64{
		0, int64(20 * time.Millisecond), int64(40 * time.Millisecond), int64(60 * time.Millisecond), -2,
	})
}

func TestDemuxSourceOfflinePumpRunsUnpaced(t *testing.T) {
	// The same timeline without Realtime never consults a clock: offline tasks
	// (a transcode job) pump as fast as the sink drains.
	demuxer := &seekableFakeDemuxer{
		streams: []av.Stream{{ID: "video", Type: av.MediaVideo}},
		packets: []timelinePacket{
			{stream: "video", ptsNS: 0, keyframe: true},
			{stream: "video", ptsNS: int64(20 * time.Millisecond), keyframe: true},
		},
	}
	clock := &fakePaceClock{}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: demuxer,
		Result:  ReadResult{Packet: &packet},
		Clock:   clock, // ignored without Realtime
	})
	if err != nil {
		t.Fatal(err)
	}
	emitter := &seekLogEmitter{}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	if len(clock.sleeps) != 0 {
		t.Fatalf("offline pump slept %v, want no pacing", clock.sleeps)
	}

	// And rate control stays honestly rejected: there is no pacing to scale.
	controllable := source.(pipeline.ControllableSource)
	err = controllable.Control(context.Background(), rateMessage(2))
	if !errors.Is(err, ErrRateUnsupported) {
		t.Fatalf("offline rate err = %v, want ErrRateUnsupported", err)
	}
	if !strings.Contains(err.Error(), "unpaced") {
		t.Fatalf("offline rate err = %v, want it to say offline pumps run unpaced", err)
	}
}

func TestDemuxSourceRateControlScalesPacing(t *testing.T) {
	clock := &fakePaceClock{}
	source := pacedTimeline(t, clock,
		0, 20*time.Millisecond, 40*time.Millisecond, 60*time.Millisecond)

	// Double speed after the first packet: the loop observes the new rate at
	// the next packet and re-anchors there (no sleep), then paces 20ms of
	// media per 10ms of clock.
	emitter := &seekLogEmitter{}
	emitter.onPacket = func(ptsNS int64) {
		if ptsNS == 0 {
			if err := source.Control(context.Background(), rateMessage(2)); err != nil {
				t.Errorf("rate control: %v", err)
			}
		}
	}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	assertSleeps(t, clock.sleeps, []time.Duration{10 * time.Millisecond, 10 * time.Millisecond})
	// A pure pacing change never repositions: full timeline, no discontinuity.
	assertSeekLog(t, emitter.log, []int64{
		0, int64(20 * time.Millisecond), int64(40 * time.Millisecond), int64(60 * time.Millisecond), -2,
	})
}

func TestDemuxSourceRateBeforeStartPacesFromFirstPacket(t *testing.T) {
	clock := &fakePaceClock{}
	source := pacedTimeline(t, clock, 0, 20*time.Millisecond, 40*time.Millisecond)

	// A rate recorded before Start (the direct runner delivers pre-Run
	// controls) paces the whole playback at half speed: 20ms of media per
	// 40ms of clock.
	if err := source.Control(context.Background(), rateMessage(0.5)); err != nil {
		t.Fatal(err)
	}
	emitter := &seekLogEmitter{}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	assertSleeps(t, clock.sleeps, []time.Duration{40 * time.Millisecond, 40 * time.Millisecond})
}

func TestDemuxSourceSeekReanchorsPacingComposedWithRate(t *testing.T) {
	clock := &fakePaceClock{}
	source := pacedTimeline(t, clock,
		0, 20*time.Millisecond, 40*time.Millisecond, 60*time.Millisecond,
		80*time.Millisecond, 100*time.Millisecond)

	// Rate and Seek compose: at 2x, packets pace 10ms apart; the seek resets
	// the anchor, so the first packet at the new position emits immediately
	// (no giant sleep, no burst) and playback resumes paced at 2x.
	if err := source.Control(context.Background(), rateMessage(2)); err != nil {
		t.Fatal(err)
	}
	emitter := &seekLogEmitter{}
	emitter.onPacket = func(ptsNS int64) {
		if ptsNS == int64(20*time.Millisecond) {
			if err := source.Control(context.Background(), seekMessage(80*time.Millisecond)); err != nil {
				t.Errorf("mid-pump seek: %v", err)
			}
		}
	}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	assertSleeps(t, clock.sleeps, []time.Duration{10 * time.Millisecond, 10 * time.Millisecond})
	assertSeekLog(t, emitter.log, []int64{
		0, int64(20 * time.Millisecond), -1, int64(80 * time.Millisecond), int64(100 * time.Millisecond), -2,
	})
}

func TestDemuxSourceSegmentReanchorsPacingAndEndsNaturally(t *testing.T) {
	clock := &fakePaceClock{}
	source := pacedTimeline(t, clock,
		0, 20*time.Millisecond, 40*time.Millisecond, 60*time.Millisecond, 80*time.Millisecond)

	// A segment behaves like a seek to start plus a natural end: pacing
	// re-anchors at the window's first packet and the crossing packet EOSes
	// without sleeping for media that never emits.
	if err := source.Control(context.Background(), segmentMessage(40*time.Millisecond, 80*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	emitter := &seekLogEmitter{}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	assertSleeps(t, clock.sleeps, []time.Duration{20 * time.Millisecond})
	assertSeekLog(t, emitter.log, []int64{
		-1, int64(40 * time.Millisecond), int64(60 * time.Millisecond), -2,
	})
}

func TestDemuxSourcePacingClampsPathologicalGaps(t *testing.T) {
	clock := &fakePaceClock{}
	source := pacedTimeline(t, clock,
		0,
		5*time.Second,         // forward gap beyond the cap
		5020*time.Millisecond, // normal cadence resumes after re-anchor
		10*time.Millisecond,   // unflagged backward jump beyond the cap
		30*time.Millisecond,   // normal cadence resumes after re-anchor
	)

	emitter := &seekLogEmitter{}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	// The 5s gap sleeps exactly the cap and re-anchors (a sparse timeline must
	// not stall delivery for the whole gap); the far-past packet emits
	// immediately and re-anchors (no unpaced burst afterwards).
	assertSleeps(t, clock.sleeps, []time.Duration{
		maxPacingSleep, 20 * time.Millisecond, 20 * time.Millisecond,
	})
	assertSeekLog(t, emitter.log, []int64{
		0, int64(5 * time.Second), int64(5020 * time.Millisecond),
		int64(10 * time.Millisecond), int64(30 * time.Millisecond), -2,
	})
}

// discTimelineDemuxer emits timestamped packets and demuxer-reported
// discontinuity events (a timeline break the container itself flagged).
type discTimelineDemuxer struct {
	packets []timelinePacket
	discAt  int // emit av.EventDiscontinuity with the packet at this index
	index   int
}

func (d *discTimelineDemuxer) Format() av.FormatID                            { return av.FormatMatroska }
func (d *discTimelineDemuxer) Open(context.Context, Input, OpenOptions) error { return nil }
func (d *discTimelineDemuxer) Streams() []av.Stream {
	return []av.Stream{{ID: "video", Type: av.MediaVideo}}
}
func (d *discTimelineDemuxer) Close() error { return nil }

func (d *discTimelineDemuxer) ReadInto(_ context.Context, out *ReadResult) error {
	if d.index >= len(d.packets) {
		return io.EOF
	}
	entry := &d.packets[d.index]
	if d.index == d.discAt {
		if err := out.AddEvent(av.Event{Type: av.EventDiscontinuity, StreamID: "video"}); err != nil {
			return err
		}
	}
	d.index++
	out.Packet.StreamID = entry.stream
	out.Packet.PTS = av.Timestamp{Value: entry.ptsNS, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}}
	out.PacketReady = true
	return nil
}

func TestDemuxSourceReanchorsOnDemuxerDiscontinuity(t *testing.T) {
	clock := &fakePaceClock{}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: &discTimelineDemuxer{
			packets: []timelinePacket{
				{stream: "video", ptsNS: 0},
				{stream: "video", ptsNS: int64(20 * time.Millisecond)},
				// The demuxer flags the break itself; the jump must neither
				// burst (backward) nor stall — pacing re-anchors here.
				{stream: "video", ptsNS: int64(10 * time.Second)},
				{stream: "video", ptsNS: int64(10020 * time.Millisecond)},
			},
			discAt: 2,
		},
		Result:   ReadResult{Packet: &packet, Events: make([]av.Event, 0, 1)},
		Realtime: true,
		Clock:    clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	emitter := &seekLogEmitter{}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	assertSleeps(t, clock.sleeps, []time.Duration{20 * time.Millisecond, 20 * time.Millisecond})
	assertSeekLog(t, emitter.log, []int64{
		0, int64(20 * time.Millisecond), -1, int64(10 * time.Second), int64(10020 * time.Millisecond), -2,
	})
}

func TestDemuxSourceRealtimeRateControlValidation(t *testing.T) {
	clock := &fakePaceClock{}
	source := pacedTimeline(t, clock, 0)
	ctx := context.Background()

	// A realtime pump accepts a well-formed rate...
	if err := source.Control(ctx, rateMessage(2)); err != nil {
		t.Fatalf("realtime rate err = %v, want acceptance", err)
	}
	// ...and rejects a malformed one before anything changes pace.
	bad := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type:     av.EventRate,
		Metadata: av.Metadata{av.MetadataRate: "-1"},
	}}
	if err := source.Control(ctx, bad); err == nil || !strings.Contains(err.Error(), "positive, finite") {
		t.Fatalf("malformed rate err = %v, want positive-finite rejection", err)
	}
}

// cancellingClock simulates a task shutdown landing mid-sleep: the Sleep
// contract returns ctx.Err(), and the pump must stop with it.
type cancellingClock struct {
	cancel context.CancelFunc
}

func (c *cancellingClock) Now() time.Duration { return 0 }

func (c *cancellingClock) Sleep(ctx context.Context, _ time.Duration) error {
	c.cancel()
	return ctx.Err()
}

func TestDemuxSourcePacingSleepInterruptedByCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clock := &cancellingClock{cancel: cancel}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: &seekableFakeDemuxer{
			streams: []av.Stream{{ID: "video", Type: av.MediaVideo}},
			packets: []timelinePacket{
				{stream: "video", ptsNS: 0, keyframe: true},
				{stream: "video", ptsNS: int64(20 * time.Millisecond), keyframe: true},
			},
		},
		Result:   ReadResult{Packet: &packet},
		Realtime: true,
		Clock:    clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	emitter := &seekLogEmitter{}
	err = source.Start(ctx, emitter)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Start err = %v, want context.Canceled from the interrupted sleep", err)
	}
	// Only the anchor packet flowed; the packet whose sleep was interrupted
	// never emitted.
	assertSeekLog(t, emitter.log, []int64{0})
}

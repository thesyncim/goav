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

// timelinePacket is one packet of a seekable fake's media timeline.
type timelinePacket struct {
	stream   av.StreamID
	ptsNS    int64
	keyframe bool
}

// seekableFakeDemuxer is a Demuxer + Seeker over a fixed timeline: ReadInto
// returns one packet per entry; Seek repositions to the latest keyframe at or
// before the target (the format.Seeker at-or-before contract).
type seekableFakeDemuxer struct {
	streams []av.Stream
	packets []timelinePacket
	index   int
	seekErr error
	seeks   []time.Duration
}

func (d *seekableFakeDemuxer) Format() av.FormatID { return av.FormatMatroska }

func (d *seekableFakeDemuxer) Open(context.Context, Input, OpenOptions) error { return nil }

func (d *seekableFakeDemuxer) Streams() []av.Stream { return d.streams }

func (d *seekableFakeDemuxer) ReadInto(_ context.Context, out *ReadResult) error {
	if d.index >= len(d.packets) {
		return io.EOF
	}
	entry := &d.packets[d.index]
	d.index++
	out.Packet.StreamID = entry.stream
	out.Packet.PTS = av.Timestamp{Value: entry.ptsNS, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}}
	out.Packet.Keyframe = entry.keyframe
	out.PacketReady = true
	return nil
}

func (d *seekableFakeDemuxer) Close() error { return nil }

func (d *seekableFakeDemuxer) Seek(_ context.Context, position time.Duration) error {
	d.seeks = append(d.seeks, position)
	if d.seekErr != nil {
		return d.seekErr
	}
	target := 0
	for i := range d.packets {
		if d.packets[i].keyframe && d.packets[i].ptsNS <= int64(position) {
			target = i
		}
	}
	d.index = target
	return nil
}

// seekLogEmitter records frame-of-reference entries in arrival order: packet
// PTS ticks, discontinuities (-1), end-of-stream (-2). Other events are
// ignored. onPacket, when set, runs after each recorded packet — it is how a
// test injects a mid-pump Control deterministically.
type seekLogEmitter struct {
	log      []int64
	onPacket func(ptsNS int64)
}

func (e *seekLogEmitter) Emit(_ context.Context, msg *pipeline.Message) error {
	switch {
	case msg.Kind == pipeline.MessagePacket && msg.Packet != nil:
		e.log = append(e.log, msg.Packet.PTS.Value)
		if e.onPacket != nil {
			e.onPacket(msg.Packet.PTS.Value)
		}
	case msg.Kind == pipeline.MessageEvent && msg.Event != nil && msg.Event.Type == av.EventDiscontinuity:
		e.log = append(e.log, -1)
	case msg.Kind == pipeline.MessageEvent && msg.Event != nil && msg.Event.Type == av.EventEndOfStream:
		e.log = append(e.log, -2)
	}
	return nil
}

func assertSeekLog(t *testing.T, got []int64, want []int64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("log = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("log = %v, want %v", got, want)
		}
	}
}

func seekMessage(position time.Duration) *pipeline.Message {
	return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type:      av.EventSeek,
		Timestamp: av.Timestamp{Value: int64(position), Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
	}}
}

func segmentMessage(start time.Duration, end time.Duration) *pipeline.Message {
	return &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type:      av.EventSegment,
		Timestamp: av.Timestamp{Value: int64(start), Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
		Metadata:  av.SegmentEndMetadata(end),
	}}
}

func TestNewDemuxSourceControllableOnlyWithSeeker(t *testing.T) {
	packet := av.Packet{}
	plain, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: &fakeDemuxer{},
		Result:  ReadResult{Packet: &packet},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := plain.(pipeline.ControllableSource); ok {
		t.Fatal("source over a non-seeking demuxer must NOT implement pipeline.ControllableSource")
	}

	seekPacket := av.Packet{}
	seekable, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: &seekableFakeDemuxer{},
		Result:  ReadResult{Packet: &seekPacket},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := seekable.(pipeline.ControllableSource); !ok {
		t.Fatal("source over a Seeker demuxer must implement pipeline.ControllableSource")
	}
}

func TestDemuxSourceSeekControlRepositionsPump(t *testing.T) {
	demuxer := &seekableFakeDemuxer{
		streams: []av.Stream{{ID: "video", Type: av.MediaVideo, Epoch: 2}},
		packets: []timelinePacket{
			{stream: "video", ptsNS: 0, keyframe: true},
			{stream: "video", ptsNS: 1000, keyframe: true},
			{stream: "video", ptsNS: 2000, keyframe: true},
			{stream: "video", ptsNS: 3000, keyframe: true},
		},
	}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: demuxer,
		Result:  ReadResult{Packet: &packet},
	})
	if err != nil {
		t.Fatal(err)
	}
	controllable := source.(pipeline.ControllableSource)

	// A control recorded before Start pre-positions the pump: discontinuity
	// first, then packets from the at-or-before keyframe.
	if err := controllable.Control(context.Background(), seekMessage(2500*time.Nanosecond)); err != nil {
		t.Fatal(err)
	}
	emitter := &seekLogEmitter{}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	assertSeekLog(t, emitter.log, []int64{-1, 2000, 3000, -2})
	if len(demuxer.seeks) != 1 || demuxer.seeks[0] != 2500*time.Nanosecond {
		t.Fatalf("seeks = %v, want one seek to 2500ns", demuxer.seeks)
	}
}

func TestDemuxSourceSeekControlMidPump(t *testing.T) {
	demuxer := &seekableFakeDemuxer{
		streams: []av.Stream{{ID: "video", Type: av.MediaVideo}},
		packets: []timelinePacket{
			{stream: "video", ptsNS: 0, keyframe: true},
			{stream: "video", ptsNS: 1000, keyframe: true},
			{stream: "video", ptsNS: 2000, keyframe: true},
			{stream: "video", ptsNS: 3000, keyframe: true},
		},
	}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: demuxer,
		Result:  ReadResult{Packet: &packet},
	})
	if err != nil {
		t.Fatal(err)
	}
	controllable := source.(pipeline.ControllableSource)

	// Inject the seek after the first packet is emitted; the loop applies it
	// before the next read, so exactly one pre-seek packet flows.
	emitter := &seekLogEmitter{}
	emitter.onPacket = func(ptsNS int64) {
		if ptsNS == 0 {
			if err := controllable.Control(context.Background(), seekMessage(3000*time.Nanosecond)); err != nil {
				t.Errorf("mid-pump seek: %v", err)
			}
		}
	}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	assertSeekLog(t, emitter.log, []int64{0, -1, 3000, -2})
}

func TestDemuxSourceSegmentControlPlaysWindowThenEnds(t *testing.T) {
	demuxer := &seekableFakeDemuxer{
		streams: []av.Stream{
			{ID: "a", Type: av.MediaAudio},
			{ID: "b", Type: av.MediaVideo},
		},
		packets: []timelinePacket{
			{stream: "a", ptsNS: 0, keyframe: true},
			{stream: "b", ptsNS: 500, keyframe: true},
			{stream: "a", ptsNS: 1000, keyframe: true},
			{stream: "b", ptsNS: 1500, keyframe: true},
			{stream: "a", ptsNS: 2000, keyframe: true},
			{stream: "b", ptsNS: 2500, keyframe: true},
			{stream: "a", ptsNS: 3000, keyframe: true},
			{stream: "b", ptsNS: 3500, keyframe: true},
		},
	}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: demuxer,
		Result:  ReadResult{Packet: &packet},
	})
	if err != nil {
		t.Fatal(err)
	}
	controllable := source.(pipeline.ControllableSource)

	if err := controllable.Control(context.Background(), segmentMessage(1000*time.Nanosecond, 3000*time.Nanosecond)); err != nil {
		t.Fatal(err)
	}
	emitter := &seekLogEmitter{}
	// The window plays [1000, 3000): the crossing packets (a:3000, b:3500) are
	// dropped, EOS fires only after EVERY stream passed the bound, and Start
	// returns nil exactly like the end of the media.
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	assertSeekLog(t, emitter.log, []int64{-1, 1000, 1500, 2000, 2500, -2})
}

func TestDemuxSourceSegmentEndsNaturallyAtEOF(t *testing.T) {
	demuxer := &seekableFakeDemuxer{
		streams: []av.Stream{{ID: "a", Type: av.MediaAudio}},
		packets: []timelinePacket{
			{stream: "a", ptsNS: 0, keyframe: true},
			{stream: "a", ptsNS: 1000, keyframe: true},
		},
	}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: demuxer,
		Result:  ReadResult{Packet: &packet},
	})
	if err != nil {
		t.Fatal(err)
	}
	controllable := source.(pipeline.ControllableSource)

	// A window ending past the media simply runs into EOF: same natural end.
	if err := controllable.Control(context.Background(), segmentMessage(1000*time.Nanosecond, time.Hour)); err != nil {
		t.Fatal(err)
	}
	emitter := &seekLogEmitter{}
	if err := source.Start(context.Background(), emitter); err != nil {
		t.Fatal(err)
	}
	assertSeekLog(t, emitter.log, []int64{-1, 1000, -2})
}

func TestDemuxSourceControlRejections(t *testing.T) {
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: &seekableFakeDemuxer{},
		Result:  ReadResult{Packet: &packet},
	})
	if err != nil {
		t.Fatal(err)
	}
	controllable := source.(pipeline.ControllableSource)
	ctx := context.Background()

	// Rate is not a source control: it scales the task-wide timeline, so the
	// pump refuses it like any other unsupported control.
	rate := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type:     av.EventRate,
		Metadata: av.RateMetadata(2),
	}}
	if err := controllable.Control(ctx, rate); err == nil || !strings.Contains(err.Error(), "unsupported source control") {
		t.Fatalf("rate err = %v, want unsupported-control rejection", err)
	}

	// Malformed payloads are rejected at Control, before anything repositions.
	if err := controllable.Control(ctx, nil); err == nil {
		t.Fatal("nil message must be rejected")
	}
	noPosition := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventSeek}}
	if err := controllable.Control(ctx, noPosition); err == nil || !strings.Contains(err.Error(), "non-negative position") {
		t.Fatalf("seek without position err = %v, want position rejection", err)
	}
	noEnd := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{
		Type:      av.EventSegment,
		Timestamp: av.Timestamp{Value: 0, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}},
	}}
	if err := controllable.Control(ctx, noEnd); err == nil || !strings.Contains(err.Error(), "exclusive end") {
		t.Fatalf("segment without end err = %v, want end rejection", err)
	}
	unknown := &pipeline.Message{Kind: pipeline.MessageEvent, Event: &av.Event{Type: av.EventStats}}
	if err := controllable.Control(ctx, unknown); err == nil || !strings.Contains(err.Error(), "unsupported source control") {
		t.Fatalf("unknown control err = %v, want unsupported rejection", err)
	}
}

func TestDemuxSourceSeekFailureStopsPump(t *testing.T) {
	demuxer := &seekableFakeDemuxer{
		streams: []av.Stream{{ID: "a", Type: av.MediaAudio}},
		packets: []timelinePacket{{stream: "a", ptsNS: 0, keyframe: true}},
		seekErr: ErrNotSeekable,
	}
	packet := av.Packet{}
	source, err := NewDemuxSource(DemuxSourceConfig{
		Demuxer: demuxer,
		Result:  ReadResult{Packet: &packet},
	})
	if err != nil {
		t.Fatal(err)
	}
	controllable := source.(pipeline.ControllableSource)

	if err := controllable.Control(context.Background(), seekMessage(time.Second)); err != nil {
		t.Fatal(err)
	}
	emitter := &seekLogEmitter{}
	err = source.Start(context.Background(), emitter)
	if !errors.Is(err, ErrNotSeekable) {
		t.Fatalf("Start err = %v, want it to wrap ErrNotSeekable", err)
	}
	if len(emitter.log) != 0 {
		t.Fatalf("log = %v, want no media after a failed seek", emitter.log)
	}
}

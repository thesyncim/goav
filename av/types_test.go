package av

import (
	"testing"
	"time"
)

func TestPacketResetReusesPayloadStorage(t *testing.T) {
	payload := make([]byte, 128)
	packet := Packet{
		StreamID:   "audio",
		Payload:    Buffer{Bytes: payload},
		Keyframe:   true,
		LossBefore: true,
		Metadata:   Metadata{"k": "v"},
	}

	packet.Reset()

	if len(packet.Payload.Bytes) != 0 {
		t.Fatalf("payload len = %d, want 0", len(packet.Payload.Bytes))
	}
	if cap(packet.Payload.Bytes) != cap(payload) {
		t.Fatalf("payload cap = %d, want %d", cap(packet.Payload.Bytes), cap(payload))
	}
	if packet.StreamID != "" || packet.Keyframe || packet.LossBefore || packet.Metadata != nil {
		t.Fatalf("packet not fully reset: %+v", packet)
	}
}

func TestCoreResetAllocs(t *testing.T) {
	payload := make([]byte, 128)
	packet := Packet{Payload: Buffer{Bytes: payload}}

	planes := make([]Plane, 2)
	planes[0].Buffer.Bytes = make([]byte, 64)
	planes[1].Buffer.Bytes = make([]byte, 32)
	frame := Frame{Planes: planes}

	event := Event{Type: EventPacketLoss, StreamID: "video", Metadata: Metadata{"a": "b"}}

	if allocs := testing.AllocsPerRun(1000, func() {
		packet.Reset()
		packet.Payload.Bytes = payload

		frame.Reset()
		frame.Planes = planes

		event.Reset()
		event.Type = EventPacketLoss
	}); allocs != 0 {
		t.Fatalf("reset allocs = %v, want 0", allocs)
	}
}

func TestRTPTimebaseHelpers(t *testing.T) {
	ts := RTPToTimestamp(960, 48000)
	if ts.Value != 960 || ts.Base.Num != 1 || ts.Base.Den != 48000 {
		t.Fatalf("timestamp = %+v", ts)
	}

	duration := SamplesDuration(960, 48000)
	if duration.Value != 960 || duration.Base.Num != 1 || duration.Base.Den != 48000 {
		t.Fatalf("duration = %+v", duration)
	}
}

func TestTimeBaseRescaleHelpers(t *testing.T) {
	audio := RTPTimeBase(48000)
	video := RTPTimeBase(90000)

	ts, ok := Timestamp{Value: 48000, Base: audio}.Rescale(video)
	if !ok || ts.Value != 90000 || ts.Base != video {
		t.Fatalf("timestamp = %+v ok=%v", ts, ok)
	}

	duration, ok := (Duration{Value: 3000, Base: video}).Rescale(audio)
	if !ok || duration.Value != 1600 || duration.Base != audio {
		t.Fatalf("duration = %+v ok=%v", duration, ok)
	}

	negative, ok := RescaleValue(-3, TimeBase{Num: 1, Den: 2}, TimeBase{Num: 1, Den: 1})
	if !ok || negative != -1 {
		t.Fatalf("negative rescale = %d ok=%v", negative, ok)
	}

	// Identity timebase fast path.
	same, ok := RescaleValue(123456789, video, video)
	if !ok || same != 123456789 {
		t.Fatalf("same timebase rescale = %d ok=%v", same, ok)
	}

	// Nearest rounding (halves away from zero).
	cases := []struct {
		val  int64
		from TimeBase
		to   TimeBase
		want int64
	}{
		{3, TimeBase{Num: 1, Den: 2}, TimeBase{Num: 1, Den: 1}, 2},   // 1.5 -> 2
		{-3, TimeBase{Num: 1, Den: 2}, TimeBase{Num: 1, Den: 1}, -2}, // -1.5 -> -2
		{5, TimeBase{Num: 1, Den: 3}, TimeBase{Num: 1, Den: 1}, 2},   // 1.666 -> 2
		{4, TimeBase{Num: 1, Den: 3}, TimeBase{Num: 1, Den: 1}, 1},   // 1.333 -> 1
		{-4, TimeBase{Num: 1, Den: 3}, TimeBase{Num: 1, Den: 1}, -1}, // -1.333 -> -1
		{3000, video, TimeBase{Num: 1, Den: 1000}, 33},               // 33.333ms -> 33ms
		{6000, video, TimeBase{Num: 1, Den: 1000}, 67},               // 66.666ms -> 67ms
	}
	for _, tc := range cases {
		got, ok := RescaleValueNearest(tc.val, tc.from, tc.to)
		if !ok || got != tc.want {
			t.Fatalf("RescaleValueNearest(%d, %v, %v) = %d, %v; want %d, true", tc.val, tc.from, tc.to, got, ok, tc.want)
		}
	}
	tsNearest, ok := Timestamp{Value: 6000, Base: video}.RescaleNearest(TimeBase{Num: 1, Den: 1000})
	if !ok || tsNearest.Value != 67 {
		t.Fatalf("Timestamp.RescaleNearest = %+v ok=%v, want 67", tsNearest, ok)
	}
	durNearest, ok := Duration{Value: 6000, Base: video}.RescaleNearest(TimeBase{Num: 1, Den: 1000})
	if !ok || durNearest.Value != 67 {
		t.Fatalf("Duration.RescaleNearest = %+v ok=%v, want 67", durNearest, ok)
	}
}

func TestTimestampDeltaAndDurationCompare(t *testing.T) {
	audio := RTPTimeBase(48000)
	video := RTPTimeBase(90000)

	delta, ok := (Timestamp{Value: 90000, Base: video}).Sub(Timestamp{Value: 48000, Base: audio})
	if !ok || delta.Value != 0 || delta.Base != video {
		t.Fatalf("delta = %+v ok=%v", delta, ok)
	}

	backward, ok := (Timestamp{Value: 40000, Base: audio}).Sub(Timestamp{Value: 90000, Base: video})
	if !ok || backward.Value != -8000 || backward.Base != audio {
		t.Fatalf("backward = %+v ok=%v", backward, ok)
	}

	cmp, ok := (Duration{Value: 960, Base: audio}).Compare(Duration{Value: 1800, Base: video})
	if !ok || cmp != 0 {
		t.Fatalf("compare equal = %d ok=%v", cmp, ok)
	}

	cmp, ok = (Duration{Value: 961, Base: audio}).Compare(Duration{Value: 1800, Base: video})
	if !ok || cmp <= 0 {
		t.Fatalf("compare greater = %d ok=%v", cmp, ok)
	}
}

func TestTimeBaseStdDurationHelpers(t *testing.T) {
	video := RTPTimeBase(90000)

	std, ok := (Duration{Value: 3000, Base: video}).ToDuration()
	if !ok || std != 33333333*time.Nanosecond {
		t.Fatalf("std duration = %s ok=%v", std, ok)
	}

	ts, ok := TimestampFromStdDuration(20*time.Millisecond, RTPTimeBase(48000))
	if !ok || ts.Value != 960 || ts.Base != RTPTimeBase(48000) {
		t.Fatalf("timestamp = %+v ok=%v", ts, ok)
	}

	duration, ok := DurationFromStdDuration(20*time.Millisecond, RTPTimeBase(48000))
	if !ok || duration.Value != 960 || duration.Base != RTPTimeBase(48000) {
		t.Fatalf("duration = %+v ok=%v", duration, ok)
	}
}

func TestTimeBaseHelpersRejectInvalidOrOverflow(t *testing.T) {
	if RTPTimeBase(0).Valid() {
		t.Fatal("zero RTP timebase should be invalid")
	}
	if _, ok := RescaleValue(1, TimeBase{}, RTPTimeBase(90000)); ok {
		t.Fatal("invalid source timebase accepted")
	}
	if _, ok := RescaleValue(1, RTPTimeBase(90000), TimeBase{}); ok {
		t.Fatal("invalid target timebase accepted")
	}
	if _, ok := RescaleValue(maxInt64, TimeBase{Num: maxInt64, Den: 1}, TimeBase{Num: 1, Den: 1}); ok {
		t.Fatal("overflow accepted")
	}
}

func TestTimeBaseHelpersAllocs(t *testing.T) {
	audio := RTPTimeBase(48000)
	video := RTPTimeBase(90000)
	ts := Timestamp{Value: 48000, Base: audio}
	duration := Duration{Value: 3000, Base: video}

	if allocs := testing.AllocsPerRun(1000, func() {
		_, _ = ts.Rescale(video)
		_, _ = ts.Sub(Timestamp{Value: 90000, Base: video})
		_, _ = duration.Rescale(audio)
		_, _ = duration.Compare(Duration{Value: 1600, Base: audio})
		_, _ = duration.ToDuration()
		_, _ = TimestampFromStdDuration(20*time.Millisecond, audio)
		_, _ = DurationFromStdDuration(20*time.Millisecond, audio)
	}); allocs != 0 {
		t.Fatalf("timebase helper allocs = %v, want 0", allocs)
	}
}

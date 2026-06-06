package av

import (
	"testing"
	"time"
)

func TestPacketResetReusesPayloadStorage(t *testing.T) {
	payload := make([]byte, 128)
	packet := Packet{
		StreamID:   "audio",
		Payload:    Buffer{Bytes: payload, Ownership: BufferOwned},
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
	packet := Packet{Payload: Buffer{Bytes: payload, Ownership: BufferOwned}}

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
		_, _ = duration.Rescale(audio)
		_, _ = duration.ToDuration()
		_, _ = TimestampFromStdDuration(20*time.Millisecond, audio)
		_, _ = DurationFromStdDuration(20*time.Millisecond, audio)
	}); allocs != 0 {
		t.Fatalf("timebase helper allocs = %v, want 0", allocs)
	}
}

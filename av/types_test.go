package av

import "testing"

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

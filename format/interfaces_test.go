package format

import (
	"errors"
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestReadResultReset(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	result := ReadResult{
		Packet:      &packet,
		PacketReady: true,
		Events:      []av.Event{{Type: av.EventStats}},
	}

	result.Reset()

	if result.Packet == nil {
		t.Fatal("packet pointer was cleared")
	}
	if result.PacketReady {
		t.Fatal("packet ready was not reset")
	}
	if packet.StreamID != "" {
		t.Fatalf("packet stream = %s, want empty", packet.StreamID)
	}
	if len(result.Events) != 0 {
		t.Fatalf("events len = %d, want 0", len(result.Events))
	}
}

func TestReadResultAddEventCapacity(t *testing.T) {
	result := ReadResult{Events: make([]av.Event, 0, 1)}
	if err := result.AddEvent(av.Event{Type: av.EventStats}); err != nil {
		t.Fatal(err)
	}
	if err := result.AddEvent(av.Event{Type: av.EventStats}); !errors.Is(err, ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
}

func TestWriteResultAddEventCapacity(t *testing.T) {
	result := WriteResult{Events: make([]av.Event, 0, 1)}
	if err := result.AddEvent(av.Event{Type: av.EventStats}); err != nil {
		t.Fatal(err)
	}
	if err := result.AddEvent(av.Event{Type: av.EventStats}); !errors.Is(err, ErrResultFull) {
		t.Fatalf("err = %v, want ErrResultFull", err)
	}
}

func TestFormatResultResetAllocs(t *testing.T) {
	packet := av.Packet{StreamID: "audio"}
	read := ReadResult{
		Packet:      &packet,
		PacketReady: true,
		Events:      make([]av.Event, 1),
	}
	write := WriteResult{Events: make([]av.Event, 1)}

	if allocs := testing.AllocsPerRun(1000, func() {
		read.Reset()
		read.PacketReady = true
		read.Events = read.Events[:1]
		write.Reset()
		write.Events = write.Events[:1]
	}); allocs != 0 {
		t.Fatalf("format result reset allocs = %v, want 0", allocs)
	}
}

package pipeline

import (
	"testing"

	"github.com/thesyncim/goav/av"
)

func TestDropControllerBackpressureWhenFull(t *testing.T) {
	controller := newDropController(BufferPolicy{Capacity: 1})
	msg := Message{Kind: MessagePacket, Packet: &av.Packet{}}

	if action := controller.decide(1, &msg); action != bufferBackpressure {
		t.Fatalf("action = %v, want backpressure", action)
	}
}

func TestDropControllerOldestWhenFull(t *testing.T) {
	controller := newDropController(BufferPolicy{Capacity: 1, Drop: DropOldest})
	msg := Message{Kind: MessagePacket, Packet: &av.Packet{}}

	if action := controller.decide(1, &msg); action != bufferDropOldest {
		t.Fatalf("action = %v, want drop oldest", action)
	}
}

func TestDropControllerNewestWhenFull(t *testing.T) {
	controller := newDropController(BufferPolicy{Capacity: 1, Drop: DropNewest})
	msg := Message{Kind: MessagePacket, Packet: &av.Packet{}}

	if action := controller.decide(1, &msg); action != bufferDropIncoming {
		t.Fatalf("action = %v, want drop incoming", action)
	}
}

func TestDropControllerUntilSync(t *testing.T) {
	controller := newDropController(BufferPolicy{Capacity: 1, Drop: DropUntilSync})
	inter := Message{Kind: MessagePacket, Packet: &av.Packet{}}
	key := Message{Kind: MessagePacket, Packet: &av.Packet{Keyframe: true}}

	if action := controller.decide(1, &inter); action != bufferDropIncoming {
		t.Fatalf("first action = %v, want drop incoming", action)
	}
	if !controller.droppingUntilSync {
		t.Fatal("controller should wait for sync")
	}
	if action := controller.decide(0, &inter); action != bufferDropIncoming {
		t.Fatalf("second action = %v, want drop incoming while waiting for sync", action)
	}
	if action := controller.decide(0, &key); action != bufferAdmit {
		t.Fatalf("sync action = %v, want admit", action)
	}
	if controller.droppingUntilSync {
		t.Fatal("controller should leave wait-for-sync state")
	}
}

func TestDropControllerUntilSyncDropsOldestForSyncWhenFull(t *testing.T) {
	controller := newDropController(BufferPolicy{Capacity: 1, Drop: DropUntilSync})
	key := Message{Kind: MessagePacket, Packet: &av.Packet{Keyframe: true}}

	if action := controller.decide(1, &key); action != bufferDropOldest {
		t.Fatalf("action = %v, want drop oldest for sync packet", action)
	}
}

func TestDropControllerAudioPacketIsSyncWithMetadataHint(t *testing.T) {
	controller := newDropController(BufferPolicy{Capacity: 1, Drop: DropUntilSync})
	audio := Message{
		Kind: MessagePacket,
		Packet: &av.Packet{
			Metadata: av.Metadata{av.MetadataMediaType: string(av.MediaAudio)},
		},
	}

	if action := controller.decide(1, &audio); action != bufferDropOldest {
		t.Fatalf("action = %v, want drop oldest for audio packet", action)
	}
}

func TestDropControllerNonKeyVideo(t *testing.T) {
	controller := newDropController(BufferPolicy{Capacity: 1, Drop: DropNonKeyVideo})
	video := Message{
		Kind: MessagePacket,
		Packet: &av.Packet{
			Metadata: av.Metadata{av.MetadataMediaType: string(av.MediaVideo)},
		},
	}
	key := Message{
		Kind: MessagePacket,
		Packet: &av.Packet{
			Keyframe: true,
			Metadata: av.Metadata{
				av.MetadataMediaType: string(av.MediaVideo),
			},
		},
	}

	if action := controller.decide(1, &video); action != bufferDropIncoming {
		t.Fatalf("non-key action = %v, want drop incoming", action)
	}
	if action := controller.decide(1, &key); action != bufferBackpressure {
		t.Fatalf("key action = %v, want backpressure", action)
	}
}

func TestDropControllerSyncEvents(t *testing.T) {
	controller := newDropController(BufferPolicy{Capacity: 1, Drop: DropUntilSync})
	event := av.Event{Type: av.EventCodecChanged}
	msg := Message{Kind: MessageEvent, Event: &event}

	if action := controller.decide(1, &msg); action != bufferDropOldest {
		t.Fatalf("action = %v, want drop oldest for sync event", action)
	}
}

func TestDropControllerDecideAllocs(t *testing.T) {
	controller := newDropController(BufferPolicy{Capacity: 1, Drop: DropUntilSync})
	msg := Message{Kind: MessagePacket, Packet: &av.Packet{}}

	if allocs := testing.AllocsPerRun(1000, func() {
		controller.droppingUntilSync = false
		_ = controller.decide(1, &msg)
	}); allocs != 0 {
		t.Fatalf("drop decision allocs = %v, want 0", allocs)
	}
}

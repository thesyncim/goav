package av

import (
	"testing"
)

func TestNewBufferHoldsOneReference(t *testing.T) {
	b := NewBuffer(100)
	if len(b.Bytes) != 100 || cap(b.Bytes) != 100 {
		t.Fatalf("len=%d cap=%d, want 100/100: holders must not append into the block's spare room", len(b.Bytes), cap(b.Bytes))
	}
	if b.Owner == nil || b.Owner.Refs() != 1 {
		t.Fatalf("owner = %v, want one reference", b.Owner)
	}
	b.Retain()
	if refs := b.Owner.Refs(); refs != 2 {
		t.Fatalf("refs after Retain = %d, want 2", refs)
	}
	kept := b
	b.Release()
	if b.Bytes != nil || b.Owner != nil {
		t.Fatalf("Release left %+v, want a cleared buffer", b)
	}
	if refs := kept.Owner.Refs(); refs != 1 {
		t.Fatalf("refs after one Release = %d, want 1", refs)
	}
	kept.Release()
	if empty := NewBuffer(0); empty.Owner != nil || empty.Bytes != nil {
		t.Fatalf("NewBuffer(0) = %+v, want the zero Buffer", empty)
	}
}

func TestOrdinaryBuffersNeedNoReferences(t *testing.T) {
	b := Buffer{Bytes: []byte{1, 2}}
	b.Retain() // no-op
	kept := b
	b.Release()
	if b.Bytes != nil {
		t.Fatal("Release did not clear the buffer")
	}
	if kept.Bytes[0] != 1 {
		t.Fatal("releasing ordinary memory touched its bytes")
	}
}

func TestMakeWritableCopiesOnlyWhenShared(t *testing.T) {
	unique := NewBuffer(4)
	copy(unique.Bytes, []byte{1, 2, 3, 4})
	before := &unique.Bytes[0]
	unique.MakeWritable()
	if &unique.Bytes[0] != before {
		t.Fatal("MakeWritable copied the only reference")
	}

	shared := NewBuffer(4)
	copy(shared.Bytes, []byte{5, 6, 7, 8})
	other := shared
	other.Retain()
	shared.MakeWritable()
	if &shared.Bytes[0] == &other.Bytes[0] {
		t.Fatal("MakeWritable kept bytes another reference holds")
	}
	if string(shared.Bytes) != string(other.Bytes) {
		t.Fatalf("copy = %v, want %v", shared.Bytes, other.Bytes)
	}
	if refs := other.Owner.Refs(); refs != 1 {
		t.Fatalf("old block refs = %d, want 1 after MakeWritable released its reference", refs)
	}
	shared.Bytes[0] = 0xEE
	if other.Bytes[0] != 5 {
		t.Fatal("write through the writable copy reached the other reference")
	}

	ordinary := Buffer{Bytes: []byte{9}}
	original := ordinary
	ordinary.MakeWritable()
	if &ordinary.Bytes[0] == &original.Bytes[0] || ordinary.Owner == nil {
		t.Fatal("MakeWritable must copy ordinary memory: its producer may share it")
	}
	for _, b := range []*Buffer{&unique, &shared, &other, &ordinary} {
		b.Release()
	}
}

func TestReleasedBufferMisuseIsLoud(t *testing.T) {
	for name, misuse := range map[string]func(stale Buffer){
		"retain":  func(stale Buffer) { stale.Retain() },
		"release": func(stale Buffer) { (&stale).Release() },
	} {
		b := NewBuffer(8)
		stale := b
		b.Release()
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("%s after the last release did not panic", name)
				}
			}()
			misuse(stale)
		}()
	}
}

func TestPooledBuffersRecycleWithoutAllocating(t *testing.T) {
	warm := NewBuffer(3 << 20)
	warm.Release() // warm the class
	if allocs := testing.AllocsPerRun(100, func() {
		b := NewBuffer(3 << 20)
		b.Bytes[0] = 1
		b.Release()
	}); allocs != 0 {
		t.Fatalf("NewBuffer+Release allocs = %v, want 0 once the class is warm", allocs)
	}
}

func TestFrameRefKeepsItsOwnHeaderAndReferences(t *testing.T) {
	video := &VideoFrame{Width: 2, Height: 2, PixelFormat: PixelFormatI420}
	luma := NewBuffer(4)
	frame := &Frame{
		StreamID: "v",
		Video:    video,
		Planes:   []Plane{{Buffer: luma, Stride: 2}},
		Metadata: Metadata{"k": "v"},
	}
	ref := frame.Ref()
	video.Width = 9
	frame.Planes[0].Stride = 99
	frame.Metadata["k"] = "changed"
	if ref.Video == video || ref.Video.Width != 2 || ref.Planes[0].Stride != 2 || ref.Metadata["k"] != "v" {
		t.Fatalf("ref = %+v, want its own header copies", ref)
	}
	if &ref.Planes[0].Buffer.Bytes[0] != &luma.Bytes[0] {
		t.Fatal("Ref copied the plane bytes; it must share them")
	}
	if refs := luma.Owner.Refs(); refs != 2 {
		t.Fatalf("refs = %d, want the producer's and the ref's", refs)
	}
	frame.Release()
	if refs := luma.Owner.Refs(); refs != 1 {
		t.Fatalf("refs = %d after the producer released, want the ref's 1", refs)
	}
	ref.Release()
}

func TestPacketRefKeepsItsOwnReference(t *testing.T) {
	payload := NewBuffer(2)
	packet := &Packet{StreamID: "a", Payload: payload, Metadata: Metadata{"k": "v"}}
	ref := packet.Ref()
	packet.Metadata["k"] = "changed"
	if ref.Metadata["k"] != "v" || &ref.Payload.Bytes[0] != &payload.Bytes[0] {
		t.Fatalf("ref = %+v, want a copied header over shared bytes", ref)
	}
	packet.Release()
	if refs := ref.Payload.Owner.Refs(); refs != 1 {
		t.Fatalf("refs = %d, want the ref's 1", refs)
	}
	ref.Release()
}

func TestBufferResetDropsPooledBytesKeepsScratch(t *testing.T) {
	scratch := Buffer{Bytes: make([]byte, 4, 16)}
	scratch.Reset()
	if len(scratch.Bytes) != 0 || cap(scratch.Bytes) != 16 {
		t.Fatalf("scratch after Reset = len %d cap %d, want its capacity kept", len(scratch.Bytes), cap(scratch.Bytes))
	}
	pooled := NewBuffer(4)
	kept := pooled
	pooled.Reset()
	if pooled.Bytes != nil || pooled.Owner != nil {
		t.Fatal("Reset kept pooled bytes the pool may hand to someone else")
	}
	if refs := kept.Owner.Refs(); refs != 1 {
		t.Fatalf("Reset changed the reference count to %d", refs)
	}
	kept.Release()
}

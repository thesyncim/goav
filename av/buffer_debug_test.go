//go:build goavdebug

package av

import "testing"

// TestReleasedBytesArePoisoned pins the goavdebug build: the last Release
// fills the bytes with poison and keeps the block out of reuse, so a holder
// that kept them without Retain reads 0xDB, and a late Retain panics.
func TestReleasedBytesArePoisoned(t *testing.T) {
	b := NewBuffer(64)
	for i := range b.Bytes {
		b.Bytes[i] = 1
	}
	view, stale := b.Bytes, b
	b.Release()
	for i, c := range view {
		if c != poisonByte {
			t.Fatalf("byte %d = %#x after release, want poison %#x", i, c, poisonByte)
		}
	}
	defer func() {
		if recover() == nil {
			t.Fatal("Retain of a released buffer did not panic")
		}
	}()
	stale.Retain()
}

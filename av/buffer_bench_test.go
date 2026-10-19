package av

import "testing"

// BenchmarkNewBufferRelease is the pool round trip a producer pays per
// published payload: take a block, then give its one reference back.
func BenchmarkNewBufferRelease(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		buf := NewBuffer(1920)
		buf.Release()
	}
}

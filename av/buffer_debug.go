//go:build goavdebug

package av

import (
	"math"
	"sync/atomic"
)

// The goavdebug build makes use after release loud. A released block is
// filled with poisonByte (0xDB) and held in a quarantine before it may be reused,
// so a holder that kept bytes without Retain reads poison instead of the
// next payload, and its reference count sits far below zero, so a late
// Retain or Release panics instead of corrupting a recycled block.

// poisonByte fills released pooled bytes.
const poisonByte = 0xDB

const (
	releasedRefs   = math.MinInt32 / 2
	quarantineSize = 16
)

// quarantine is a lock-free ring: each release takes the next slot by an atomic
// add and swaps its block in, so the debug build keeps the data plane's
// no-lock guarantee.
var quarantine struct {
	blocks [quarantineSize]atomic.Pointer[block]
	next   atomic.Uint64
}

func recycle(b *block) {
	for i := range b.data {
		b.data[i] = poisonByte
	}
	b.refs.Store(releasedRefs)
	evicted := quarantine.blocks[(quarantine.next.Add(1)-1)%quarantineSize].Swap(b)
	if evicted != nil {
		evicted.refs.Store(0)
		putBlock(evicted)
	}
}

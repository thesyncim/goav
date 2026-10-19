//go:build goavdebug

package av

import (
	"math"
	"sync"
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

var quarantine struct {
	mu     sync.Mutex
	blocks [quarantineSize]*block
	next   int
}

func recycle(b *block) {
	for i := range b.data {
		b.data[i] = poisonByte
	}
	b.refs.Store(releasedRefs)
	quarantine.mu.Lock()
	evicted := quarantine.blocks[quarantine.next]
	quarantine.blocks[quarantine.next] = b
	quarantine.next = (quarantine.next + 1) % quarantineSize
	quarantine.mu.Unlock()
	if evicted != nil {
		evicted.refs.Store(0)
		putBlock(evicted)
	}
}

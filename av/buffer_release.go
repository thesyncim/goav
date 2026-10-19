//go:build !goavdebug

package av

// recycle returns a block whose last reference is gone to the pool.
func recycle(b *block) { putBlock(b) }

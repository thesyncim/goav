package kit

import "sync"

// Handles maps Go values to integer handles a C library can carry as an
// opaque refcon and hand back in callbacks, so no Go pointer ever crosses
// into C. purego callbacks are a fixed process-wide budget that is never
// freed: create one per C signature and route each call through a handle.
// Methods are safe for concurrent use, including from C threads.
type Handles[T any] struct {
	mu     sync.Mutex
	next   uintptr
	values map[uintptr]T
}

// New stores v and returns its handle, never zero.
func (h *Handles[T]) New(v T) uintptr {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.values == nil {
		h.values = make(map[uintptr]T)
	}
	h.next++
	h.values[h.next] = v
	return h.next
}

// Get returns the value behind handle, if it is still stored.
func (h *Handles[T]) Get(handle uintptr) (T, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	v, ok := h.values[handle]
	return v, ok
}

// Delete forgets handle; a callback that still carries it finds nothing.
func (h *Handles[T]) Delete(handle uintptr) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.values, handle)
}

// Len reports how many handles are stored: zero once every session closed.
func (h *Handles[T]) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.values)
}

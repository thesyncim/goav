package goav

import (
	"sync"
	"sync/atomic"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/inspect"
)

// defaultWatchCapacity mirrors the pipeline's default event channel capacity so
// an unconfigured task gives every watcher the same buffer depth as Events().
const defaultWatchCapacity = 16

// Watch returns an independent, filtered subscription to the task's event
// stream. See Observable.Watch for the delivery, overflow, and closure contract.
func (t *task) Watch(filters ...inspect.EventFilter) <-chan av.Event {
	return t.watch.subscribe(t.graph.Events(), t.watchCapacity(), filters)
}

// watchCapacity sizes one watcher buffer, normalized exactly like the
// pipeline's event channel: the runtime's configured capacity, or 16.
func (t *task) watchCapacity() int {
	if t.runtime != nil && t.runtime.eventCapacity >= 1 {
		return t.runtime.eventCapacity
	}
	return defaultWatchCapacity
}

// eventWatch fans the task's underlying event stream out to per-watcher
// channels. Events are the cold control plane, so a mutex is fine here: one
// distributor goroutine, started on the first Watch call, moves events from
// the graph channel to every matching watcher with a non-blocking send. A
// watcher whose buffer is full sheds the event for itself only — the
// distributor, the other watchers, and the data plane never block on it.
type eventWatch struct {
	mu       sync.Mutex
	started  bool
	done     bool
	watchers []*eventWatcher

	// dropped counts events shed across all watchers because a watcher buffer
	// was full at delivery time.
	dropped atomic.Uint64
}

type eventWatcher struct {
	filters []inspect.EventFilter
	events  chan av.Event
}

func (w *eventWatch) subscribe(source <-chan av.Event, capacity int, filters []inspect.EventFilter) <-chan av.Event {
	if capacity < 1 {
		capacity = defaultWatchCapacity
	}
	watcher := &eventWatcher{filters: filters, events: make(chan av.Event, capacity)}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done || source == nil {
		// The underlying stream already ended (or the graph has none): hand
		// back a closed channel so the caller observes closure immediately
		// instead of holding a watcher that can never fire.
		close(watcher.events)
		return watcher.events
	}
	w.watchers = append(w.watchers, watcher)
	if !w.started {
		w.started = true
		go w.distribute(source)
	}
	return watcher.events
}

// publish delivers one task-originated event (e.g. a stream-rule attach
// failure) to every matching watcher, with the same non-blocking shed
// contract as graph events. It never reaches the graph's event channel — the
// fan-out is the task's own event surface.
func (w *eventWatch) publish(event av.Event) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.done {
		return
	}
	for _, watcher := range w.watchers {
		if !watcher.matches(event) {
			continue
		}
		select {
		case watcher.events <- event:
		default:
			w.dropped.Add(1)
		}
	}
}

// distribute is the single fan-out goroutine. It exits when the underlying
// event stream closes — the graph closes it when the task closes — and then
// closes every watcher channel, so watchers observe task shutdown and no
// goroutine outlives the subscription set.
func (w *eventWatch) distribute(source <-chan av.Event) {
	for event := range source {
		w.mu.Lock()
		for _, watcher := range w.watchers {
			if !watcher.matches(event) {
				continue
			}
			select {
			case watcher.events <- event:
			default:
				// Slow watcher: shed the event for this watcher only.
				w.dropped.Add(1)
			}
		}
		w.mu.Unlock()
	}
	w.mu.Lock()
	w.done = true
	watchers := w.watchers
	w.watchers = nil
	w.mu.Unlock()
	for _, watcher := range watchers {
		close(watcher.events)
	}
}

func (w *eventWatcher) matches(event av.Event) bool {
	for _, filter := range w.filters {
		if filter != nil && !filter(event) {
			return false
		}
	}
	return true
}

// Package inspect defines opt-in observation helpers for running tasks.
package inspect

import "github.com/thesyncim/goav/av"

// EventFilter reports whether a watcher wants an event. Filters passed to
// Observable.Watch AND together: an event is delivered when every filter
// matches. A watch with zero filters receives every event. Any predicate works;
// WatchTypes and WatchStream cover the common cases.
type EventFilter func(av.Event) bool

// Subscription is a disposable event subscription returned by a watchable task.
// Events is closed when the task ends or Close unsubscribes the watcher. Close
// is idempotent.
type Subscription interface {
	Events() <-chan av.Event
	Close() error
}

// WatchTypes matches events whose Type is one of types.
func WatchTypes(types ...av.EventType) EventFilter {
	set := make(map[av.EventType]struct{}, len(types))
	for i := range types {
		set[types[i]] = struct{}{}
	}
	return func(event av.Event) bool {
		_, ok := set[event.Type]
		return ok
	}
}

// WatchStream matches events published for the given stream.
func WatchStream(id av.StreamID) EventFilter {
	return func(event av.Event) bool {
		return event.StreamID == id
	}
}

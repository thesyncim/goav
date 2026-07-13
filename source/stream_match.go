package source

import (
	"strconv"
	"time"

	"github.com/thesyncim/goav/av"
)

// StreamMatch selects which discovered streams a dynamic-stream rule reacts
// to. Build one with MatchMedia, MatchCodec, MatchStreamID, MatchStream,
// MatchFirst, MatchAfter, or MatchWithin; the zero value matches nothing and
// is rejected by goav.Job.OnStream.
type StreamMatch struct {
	media  av.MediaType
	codec  av.CodecID
	id     av.StreamID
	fn     func(av.Stream) bool
	first  int
	after  time.Duration
	within time.Duration
	desc   string
}

// StreamMatcher is the runtime evaluator for a StreamMatch. It keeps state for
// conditions such as First without storing mutable counters in the public
// StreamMatch value, so the same match config can be reused across jobs.
type StreamMatcher struct {
	match   StreamMatch
	started time.Time
	seen    int
}

// MatchMedia matches discovered streams of the given media kind.
func MatchMedia(media av.MediaType) StreamMatch {
	return StreamMatch{media: media, desc: "media=" + string(media)}
}

// MatchCodec matches discovered streams carrying the given codec.
func MatchCodec(id av.CodecID) StreamMatch {
	return StreamMatch{codec: id, desc: "codec=" + string(id)}
}

// MatchStreamID matches the discovered stream with exactly this id.
func MatchStreamID(id av.StreamID) StreamMatch {
	return StreamMatch{id: id, desc: "stream=" + string(id)}
}

// MatchStream matches discovered streams with a custom predicate.
func MatchStream(fn func(av.Stream) bool) StreamMatch {
	return StreamMatch{fn: fn, desc: "custom"}
}

// MatchFirst matches the first n discovered streams. Combine it with identity
// predicates by calling First on another matcher, for example
// MatchMedia(av.MediaAudio).First(1).
func MatchFirst(n int) StreamMatch {
	return StreamMatch{}.First(n)
}

// MatchAfter matches streams announced at least d after the matcher starts.
// Combine it with identity predicates by calling After on another matcher.
func MatchAfter(d time.Duration) StreamMatch {
	return StreamMatch{}.After(d)
}

// MatchWithin matches streams announced no later than d after the matcher
// starts. Combine it with identity predicates by calling Within on another
// matcher.
func MatchWithin(d time.Duration) StreamMatch {
	return StreamMatch{}.Within(d)
}

// First limits a matcher to the first n streams that satisfy its stateless
// predicates. Non-positive n leaves the matcher unchanged.
func (m StreamMatch) First(n int) StreamMatch {
	if n <= 0 {
		return m
	}
	m.first = n
	m.desc = appendStreamMatchDescription(m.desc, "first="+strconv.Itoa(n))
	return m
}

// After limits a matcher to streams announced at least d after the matcher
// starts. Non-positive d leaves the matcher unchanged.
func (m StreamMatch) After(d time.Duration) StreamMatch {
	if d <= 0 {
		return m
	}
	m.after = d
	m.desc = appendStreamMatchDescription(m.desc, "after="+d.String())
	return m
}

// Within limits a matcher to streams announced no later than d after the
// matcher starts. Non-positive d leaves the matcher unchanged.
func (m StreamMatch) Within(d time.Duration) StreamMatch {
	if d <= 0 {
		return m
	}
	m.within = d
	m.desc = appendStreamMatchDescription(m.desc, "within="+d.String())
	return m
}

// Empty reports whether the matcher has no predicate.
func (m StreamMatch) Empty() bool {
	return m.media == "" && m.codec == "" && m.id == "" && m.fn == nil &&
		m.first <= 0 && m.after <= 0 && m.within <= 0
}

// Matches reports whether stream satisfies the stateless predicates. Stateful
// conditions such as First are applied by StreamMatcher.
func (m StreamMatch) Matches(stream av.Stream) bool {
	if m.Empty() {
		return false
	}
	if m.media != "" && stream.Type != m.media && stream.Codec.Type != m.media {
		return false
	}
	if m.codec != "" && stream.Codec.ID != m.codec {
		return false
	}
	if m.id != "" && stream.ID != m.id {
		return false
	}
	if m.fn != nil && !m.fn(stream) {
		return false
	}
	return true
}

// Matcher returns a runtime evaluator for this matcher config.
func (m StreamMatch) Matcher() StreamMatcher {
	return m.MatcherAt(time.Now())
}

// MatcherAt returns a runtime evaluator whose stateful time conditions are
// measured from start.
func (m StreamMatch) MatcherAt(start time.Time) StreamMatcher {
	return StreamMatcher{match: m, started: start}
}

// Matches reports whether stream satisfies the matcher config and consumes one
// stateful match budget when it does.
func (m *StreamMatcher) Matches(stream av.Stream) bool {
	return m.MatchesAt(stream, time.Now())
}

// MatchesAt is Matches evaluated at a known announcement time. A zero time uses
// the current clock.
func (m *StreamMatcher) MatchesAt(stream av.Stream, at time.Time) bool {
	if m == nil || !m.match.Matches(stream) {
		return false
	}
	if at.IsZero() {
		at = time.Now()
	}
	if m.started.IsZero() {
		m.started = at
	}
	elapsed := at.Sub(m.started)
	if elapsed < 0 {
		return false
	}
	if m.match.after > 0 && elapsed < m.match.after {
		return false
	}
	if m.match.within > 0 && elapsed > m.match.within {
		return false
	}
	if m.match.first > 0 {
		if m.seen >= m.match.first {
			return false
		}
		m.seen++
	}
	return true
}

// Description returns a compact human-readable summary of the matcher.
func (m StreamMatch) Description() string {
	if m.desc != "" {
		return m.desc
	}
	return "none"
}

func appendStreamMatchDescription(current string, next string) string {
	if current == "" {
		return next
	}
	return current + ", " + next
}

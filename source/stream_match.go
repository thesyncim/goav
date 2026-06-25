package source

import "github.com/thesyncim/goav/av"

// StreamMatch selects which discovered streams a dynamic-stream rule reacts
// to. Build one with MatchMedia, MatchCodec, MatchStreamID, or MatchStream;
// the zero value matches nothing and is rejected by goav.Job.OnStream.
type StreamMatch struct {
	media av.MediaType
	codec av.CodecID
	id    av.StreamID
	fn    func(av.Stream) bool
	desc  string
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

// Empty reports whether the matcher has no predicate.
func (m StreamMatch) Empty() bool {
	return m.media == "" && m.codec == "" && m.id == "" && m.fn == nil
}

// Matches reports whether stream satisfies every configured predicate.
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

// Description returns a compact human-readable summary of the matcher.
func (m StreamMatch) Description() string {
	if m.desc != "" {
		return m.desc
	}
	return "none"
}

// Package shape holds the media-format descriptor vocabulary used across goav to
// describe the shape of a stream at a point in a pipeline: its domain, media
// kind, codec, geometry, and audio layout. It keeps the goav root grammar clean
// while letting callers reach for descriptors as shape.Spec, shape.Set, and the
// shape.Frame/Packet/Event constructors.
package shape

import (
	"fmt"
	"strings"

	"github.com/thesyncim/goav/av"
)

// MediaDomain names which representation a Spec describes: packets, frames, or
// out-of-band events.
type MediaDomain string

// The media domains: encoded packets, decoded frames, or event-only flow.
const (
	DomainPacket MediaDomain = "packet"
	DomainFrame  MediaDomain = "frame"
	DomainEvent  MediaDomain = "event"
)

// Spec is the core media-format descriptor: the shape of a stream at a point in
// a pipeline. A zero Spec matches anything; populated fields constrain the match.
type Spec struct {
	Domain       MediaDomain
	MediaKind    av.MediaType
	StreamID     av.StreamID
	Codec        av.CodecID
	Format       av.FormatID
	Width        int
	Height       int
	PixelFormat  string
	SampleRate   int
	Channels     int
	SampleFormat string
	Realtime     bool
}

// Set is a collection of acceptable Specs. An empty Set accepts any Spec.
type Set []Spec

// Contract describes a stage that maps an input Spec to a Set of output Specs and
// advertises the Set of inputs it accepts.
type Contract interface {
	InputShapes() Set
	OutputShapes(Spec) Set
}

// Option mutates a Spec under construction.
type Option func(*Spec)

// New builds a Spec from options.
func New(options ...Option) Spec {
	var spec Spec
	for i := range options {
		if options[i] != nil {
			options[i](&spec)
		}
	}
	return spec
}

// Packet builds a packet-domain Spec for the given media and codec.
func Packet(media av.MediaType, codec av.CodecID, options ...Option) Spec {
	options = append([]Option{Domain(DomainPacket), Media(media), Codec(codec)}, options...)
	return New(options...)
}

// Frame builds a frame-domain Spec for the given media.
func Frame(media av.MediaType, options ...Option) Spec {
	options = append([]Option{Domain(DomainFrame), Media(media)}, options...)
	return New(options...)
}

// Event builds an event-domain Spec.
func Event(options ...Option) Spec {
	options = append([]Option{Domain(DomainEvent)}, options...)
	return New(options...)
}

// Domain pins which representation the Spec describes: packets, frames, or
// events. The Frame/Packet/Event constructors set it for you.
func Domain(domain MediaDomain) Option {
	return func(spec *Spec) {
		if spec != nil {
			spec.Domain = domain
		}
	}
}

// Media pins the media kind (audio, video); unset matches any kind.
func Media(media av.MediaType) Option {
	return func(spec *Spec) {
		if spec != nil {
			spec.MediaKind = media
		}
	}
}

// Stream pins the Spec to one stream id; unset matches any stream.
func Stream(stream av.StreamID) Option {
	return func(spec *Spec) {
		if spec != nil {
			spec.StreamID = stream
		}
	}
}

// Codec pins the codec id (av.CodecOpus, ...); unset matches any codec.
func Codec(codec av.CodecID) Option {
	return func(spec *Spec) {
		if spec != nil {
			spec.Codec = codec
		}
	}
}

// Format pins the container or transport framing format (av.FormatRTP, ...);
// unset matches any format.
func Format(format av.FormatID) Option {
	return func(spec *Spec) {
		if spec != nil {
			spec.Format = format
		}
	}
}

// Video states the video layout facts: geometry and pixel format. Zero or
// empty fields stay unconstrained — Video(0, 0, "i420") pins only the pixel
// format, which is how a Prefer steers an otherwise open solver choice.
func Video(width int, height int, pixelFormat string) Option {
	return func(spec *Spec) {
		if spec == nil {
			return
		}
		spec.MediaKind = av.MediaVideo
		spec.Width = width
		spec.Height = height
		spec.PixelFormat = pixelFormat
	}
}

// Audio states the audio layout facts: sample rate, channel count, and
// sample format. Zero or empty fields stay unconstrained — Audio(0, 0,
// "f32") pins only the sample format, which is how a Prefer steers an
// otherwise open solver choice.
func Audio(sampleRate int, channels int, sampleFormat string) Option {
	return func(spec *Spec) {
		if spec == nil {
			return
		}
		spec.MediaKind = av.MediaAudio
		spec.SampleRate = sampleRate
		spec.Channels = channels
		spec.SampleFormat = sampleFormat
	}
}

// Realtime marks the Spec as a realtime stream.
func Realtime(realtime bool) Option {
	return func(spec *Spec) {
		if spec != nil {
			spec.Realtime = realtime
		}
	}
}

// Clone returns a copy of the Set.
func (s Set) Clone() Set {
	if len(s) == 0 {
		return nil
	}
	out := make(Set, len(s))
	copy(out, s)
	return out
}

// Accepts reports whether the actual Spec satisfies any member of the Set. An
// empty Set accepts anything.
func (s Set) Accepts(actual Spec) bool {
	if len(s) == 0 {
		return true
	}
	for i := range s {
		if actual.CompatibleWith(s[i]) {
			return true
		}
	}
	return false
}

// CompatibleWith reports whether the receiver satisfies the non-empty fields of
// expected.
func (s Spec) CompatibleWith(expected Spec) bool {
	if expected.Domain != "" && s.Domain != expected.Domain {
		return false
	}
	if expected.MediaKind != "" && s.MediaKind != expected.MediaKind {
		return false
	}
	if expected.StreamID != "" && s.StreamID != expected.StreamID {
		return false
	}
	if expected.Codec != "" && s.Codec != expected.Codec {
		return false
	}
	if expected.Format != "" && s.Format != expected.Format {
		return false
	}
	if expected.Width != 0 && s.Width != expected.Width {
		return false
	}
	if expected.Height != 0 && s.Height != expected.Height {
		return false
	}
	if expected.PixelFormat != "" && s.PixelFormat != expected.PixelFormat {
		return false
	}
	if expected.SampleRate != 0 && s.SampleRate != expected.SampleRate {
		return false
	}
	if expected.Channels != 0 && s.Channels != expected.Channels {
		return false
	}
	if expected.SampleFormat != "" && s.SampleFormat != expected.SampleFormat {
		return false
	}
	if expected.Realtime && !s.Realtime {
		return false
	}
	return true
}

// String renders the Spec as a compact key=value description, or "any" when the
// Spec is empty.
func (s Spec) String() string {
	parts := make([]string, 0, 8)
	if s.Domain != "" {
		parts = append(parts, "domain="+string(s.Domain))
	}
	if s.MediaKind != "" {
		parts = append(parts, "media="+string(s.MediaKind))
	}
	if s.StreamID != "" {
		parts = append(parts, "stream="+string(s.StreamID))
	}
	if s.Codec != "" {
		parts = append(parts, "codec="+string(s.Codec))
	}
	if s.Format != "" {
		parts = append(parts, "format="+string(s.Format))
	}
	if s.Width != 0 || s.Height != 0 {
		parts = append(parts, fmt.Sprintf("size=%dx%d", s.Width, s.Height))
	}
	if s.PixelFormat != "" {
		parts = append(parts, "pixel_format="+s.PixelFormat)
	}
	if s.SampleRate != 0 {
		parts = append(parts, fmt.Sprintf("sample_rate=%d", s.SampleRate))
	}
	if s.Channels != 0 {
		parts = append(parts, fmt.Sprintf("channels=%d", s.Channels))
	}
	if s.SampleFormat != "" {
		parts = append(parts, "sample_format="+s.SampleFormat)
	}
	if s.Realtime {
		parts = append(parts, "realtime=true")
	}
	if len(parts) == 0 {
		return "any"
	}
	return strings.Join(parts, " ")
}

// FromStream derives a Spec from a demuxed stream in the given domain.
func FromStream(stream av.Stream, domain MediaDomain) Spec {
	spec := Spec{Domain: domain}
	if stream.Type != "" {
		spec.MediaKind = stream.Type
	}
	if stream.ID != "" {
		spec.StreamID = stream.ID
	}
	return Merge(spec, FromCodecParameters(stream.Codec))
}

// FromCodecParameters derives a Spec from decoded codec parameters.
func FromCodecParameters(parameters av.CodecParameters) Spec {
	return Spec{
		MediaKind:    parameters.Type,
		Codec:        parameters.ID,
		Width:        parameters.Width,
		Height:       parameters.Height,
		PixelFormat:  parameters.PixelFormat,
		SampleRate:   parameters.SampleRate,
		Channels:     parameters.Channels,
		SampleFormat: parameters.SampleFormat,
	}
}

// Merge overlays the non-empty fields of next onto base and returns the result.
func Merge(base Spec, next Spec) Spec {
	if next.Domain != "" {
		base.Domain = next.Domain
	}
	if next.MediaKind != "" {
		base.MediaKind = next.MediaKind
	}
	if next.StreamID != "" {
		base.StreamID = next.StreamID
	}
	if next.Codec != "" {
		base.Codec = next.Codec
	}
	if next.Format != "" {
		base.Format = next.Format
	}
	if next.Width != 0 {
		base.Width = next.Width
	}
	if next.Height != 0 {
		base.Height = next.Height
	}
	if next.PixelFormat != "" {
		base.PixelFormat = next.PixelFormat
	}
	if next.SampleRate != 0 {
		base.SampleRate = next.SampleRate
	}
	if next.Channels != 0 {
		base.Channels = next.Channels
	}
	if next.SampleFormat != "" {
		base.SampleFormat = next.SampleFormat
	}
	base.Realtime = base.Realtime || next.Realtime
	return base
}

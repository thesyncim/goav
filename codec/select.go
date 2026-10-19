package codec

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
)

// Need is what one request asks of the backend that serves a codec. Zero
// fields ask nothing.
type Need struct {
	ID   av.CodecID
	Mode Mode
	// Backend names the backend that must serve the request.
	Backend      string
	Media        av.MediaType
	Profile      string
	PixelFormat  string
	SampleFormat string
	SampleRate   int
	BitDepth     int
	Width        int
	Height       int
	// Realtime ranks backends that declare low-latency support first among
	// otherwise equal candidates.
	Realtime bool
}

// Choice is the backend Select picked for a Need, why it was picked, and the
// other backends that would serve the need too, in preference order.
type Choice struct {
	Descriptor Descriptor
	// Decoder is set for ModeDecode and Encoder for ModeEncode.
	Decoder      DecoderFactory
	Encoder      EncoderFactory
	Reason       string
	Alternatives []Descriptor
}

// SelectError is a Need no registered backend serves. It names every
// backend that was considered and why each cannot serve, and, when the need
// names a backend, the ones that could serve it instead.
type SelectError struct {
	Need Need
	// Rejected lists the backends registered for the codec and mode that
	// cannot serve the need, each with its reason.
	Rejected []Rejection
	// Satisfying lists the backends that would serve the need when the one it
	// names is missing or cannot.
	Satisfying []Descriptor
	// Err is ErrNotFound (no backend, or none by the requested name),
	// ErrUnavailable (only descriptors registered), or ErrUnsupportedFormat
	// (no backend fits the need).
	Err error
}

// Rejection is one backend that cannot serve a Need, and why.
type Rejection struct {
	Descriptor Descriptor
	Reason     string
}

func (e *SelectError) Error() string {
	subject := string(e.Need.ID) + " " + modeNoun(e.Need.Mode)
	var b strings.Builder
	b.WriteString("codec: ")
	switch {
	case e.Need.Backend != "" && errors.Is(e.Err, ErrNotFound):
		b.WriteString("no " + subject + " backend named " + strconv.Quote(e.Need.Backend))
	case e.Need.Backend != "":
		b.WriteString(subject + " backend " + e.Need.Backend + " cannot serve the request")
	case errors.Is(e.Err, ErrUnavailable):
		b.WriteString(subject + " is registered without an implementation")
	case errors.Is(e.Err, ErrNotFound):
		b.WriteString("no " + subject + " is registered")
	default:
		b.WriteString("no " + subject + " backend serves the request")
	}
	for i, rejection := range e.Rejected {
		if i == 0 {
			b.WriteString(": ")
		} else {
			b.WriteString("; ")
		}
		b.WriteString(rejection.Descriptor.Backend.Name + ": " + rejection.Reason)
	}
	if len(e.Satisfying) != 0 {
		b.WriteString("; backends that can: " + BackendList(e.Satisfying))
	}
	return b.String()
}

func (e *SelectError) Unwrap() error { return e.Err }

// BackendList names descriptors' backends for display, each with its kind:
// "videotoolbox (hardware), openh264 (native)".
func BackendList(descriptors []Descriptor) string {
	names := make([]string, 0, len(descriptors))
	for i := range descriptors {
		names = append(names, descriptors[i].Backend.Label())
	}
	return strings.Join(names, ", ")
}

// Label is the backend's display name: its name and, when declared, its kind
// in parentheses, as Describe and Explain show it.
func (b BackendInfo) Label() string {
	name := b.Name
	if name == "" {
		name = "unnamed"
	}
	if b.Kind == "" {
		return name
	}
	return name + " (" + string(b.Kind) + ")"
}

func modeNoun(mode Mode) string {
	switch mode {
	case ModeDecode:
		return "decoder"
	case ModeEncode:
		return "encoder"
	}
	return string(mode)
}

// Select picks the backend that serves need: the one need names, or else
// the most preferred backend whose declared capabilities fit (see Prefer).
// It never substitutes another backend for a named one, and reads only
// declarations: nothing is loaded or opened.
func (r *SimpleRegistry) Select(need Need) (Choice, error) {
	candidates := r.candidates(need)
	if len(candidates) == 0 {
		failure := &SelectError{Need: need, Err: ErrNotFound}
		if r.hasDescriptor(need.ID, need.Mode) {
			failure.Err = ErrUnavailable
			for _, desc := range r.descriptorsFor(need.ID, need.Mode) {
				failure.Rejected = append(failure.Rejected, Rejection{Descriptor: desc, Reason: "registered without an implementation"})
			}
		}
		return Choice{}, failure
	}
	var fitting []registered
	var rejected []Rejection
	for _, candidate := range candidates {
		if err := candidate.desc.Check(need); err != nil {
			rejected = append(rejected, Rejection{Descriptor: cloneDescriptor(candidate.desc), Reason: err.Error()})
			continue
		}
		fitting = append(fitting, candidate)
	}
	if need.Backend != "" {
		index := slices.IndexFunc(candidates, func(candidate registered) bool {
			return strings.EqualFold(candidate.desc.Backend.Name, need.Backend)
		})
		others := slices.DeleteFunc(slices.Clone(fitting), func(candidate registered) bool {
			return strings.EqualFold(candidate.desc.Backend.Name, need.Backend)
		})
		if index < 0 {
			return Choice{}, &SelectError{Need: need, Satisfying: descriptorsOf(others), Err: ErrNotFound}
		}
		if err := candidates[index].desc.Check(need); err != nil {
			return Choice{}, &SelectError{
				Need:       need,
				Rejected:   []Rejection{{Descriptor: cloneDescriptor(candidates[index].desc), Reason: err.Error()}},
				Satisfying: descriptorsOf(others),
				Err:        ErrUnsupportedFormat,
			}
		}
		return choiceOf(candidates[index], "backend "+candidates[index].desc.Backend.Name+" requested", others), nil
	}
	if len(fitting) == 0 {
		return Choice{}, &SelectError{Need: need, Rejected: rejected, Err: ErrUnsupportedFormat}
	}
	reason := r.preferenceReason(fitting[0])
	if len(candidates) == 1 {
		reason = "the only " + string(need.ID) + " " + modeNoun(need.Mode) + " registered"
	}
	return choiceOf(fitting[0], reason, fitting[1:]), nil
}

func choiceOf(chosen registered, reason string, others []registered) Choice {
	return Choice{
		Descriptor:   cloneDescriptor(chosen.desc),
		Decoder:      chosen.decoder,
		Encoder:      chosen.encoder,
		Reason:       reason,
		Alternatives: descriptorsOf(others),
	}
}

func descriptorsOf(backends []registered) []Descriptor {
	if len(backends) == 0 {
		return nil
	}
	out := make([]Descriptor, len(backends))
	for i := range backends {
		out[i] = cloneDescriptor(backends[i].desc)
	}
	return out
}

// candidates are the backends serving need's codec and mode, most preferred
// first.
func (r *SimpleRegistry) candidates(need Need) []registered {
	var out []registered
	for _, backend := range r.backends {
		if backend.mode == need.Mode && backend.desc.ID == need.ID {
			out = append(out, backend)
		}
	}
	slices.SortStableFunc(out, func(a, b registered) int {
		if d := r.preferenceRank(a.desc.Backend) - r.preferenceRank(b.desc.Backend); d != 0 {
			return d
		}
		if d := kindRank(a.desc.Backend.Kind) - kindRank(b.desc.Backend.Kind); d != 0 {
			return d
		}
		if need.Realtime && a.desc.Realtime != b.desc.Realtime {
			if a.desc.Realtime {
				return -1
			}
			return 1
		}
		return b.sequence - a.sequence
	})
	return out
}

// preferenceRank is the position of the first Prefer entry naming the
// backend or its kind, or past the end when none does.
func (r *SimpleRegistry) preferenceRank(backend BackendInfo) int {
	for i, entry := range r.preference {
		if strings.EqualFold(entry, backend.Name) || (backend.Kind != "" && strings.EqualFold(entry, string(backend.Kind))) {
			return i
		}
	}
	return len(r.preference)
}

func kindRank(kind BackendKind) int {
	switch kind {
	case "":
		return 0
	case KindHardware:
		return 1
	case KindNative:
		return 2
	case KindGo:
		return 4
	}
	return 3
}

func (r *SimpleRegistry) preferenceReason(chosen registered) string {
	if r.preferenceRank(chosen.desc.Backend) < len(r.preference) {
		return "runtime preference " + strings.Join(r.preference, " > ")
	}
	return "default preference: hardware > native > go"
}

func (r *SimpleRegistry) descriptorsFor(id av.CodecID, mode Mode) []Descriptor {
	var out []Descriptor
	for i := range r.descriptors {
		if r.descriptors[i].ID == id && r.descriptors[i].Supports(mode) {
			out = append(out, cloneDescriptor(r.descriptors[i]))
		}
	}
	return out
}

// Check reports why the descriptor's declared capabilities cannot serve
// need, or nil when they can. It reads capabilities only: not the backend
// name, the mode, or whether a factory is registered.
func (d Descriptor) Check(need Need) error {
	capabilities := d.Capabilities
	switch {
	case need.Media != "" && d.Type != "" && need.Media != d.Type:
		return fmt.Errorf("codes %s, not %s", d.Type, need.Media)
	case need.PixelFormat != "" && len(capabilities.PixelFormats) != 0 && !slices.Contains(capabilities.PixelFormats, need.PixelFormat):
		return fmt.Errorf("pixel format %s not supported (%s)", need.PixelFormat, strings.Join(capabilities.PixelFormats, ", "))
	case need.SampleFormat != "" && len(capabilities.SampleFormats) != 0 && !slices.Contains(capabilities.SampleFormats, need.SampleFormat):
		return fmt.Errorf("sample format %s not supported (%s)", need.SampleFormat, strings.Join(capabilities.SampleFormats, ", "))
	case need.SampleRate > 0 && len(capabilities.SampleRates) != 0 && !slices.Contains(capabilities.SampleRates, need.SampleRate):
		return fmt.Errorf("sample rate %d not supported (%s)", need.SampleRate, joinInts(capabilities.SampleRates))
	case need.Profile != "" && len(d.Profiles) != 0 && !slices.ContainsFunc(d.Profiles, func(profile string) bool { return strings.EqualFold(profile, need.Profile) }):
		return fmt.Errorf("profile %s not supported (%s)", need.Profile, strings.Join(d.Profiles, ", "))
	case need.BitDepth > 0 && !slices.Contains(bitDepths(capabilities), need.BitDepth):
		return fmt.Errorf("bit depth %d not supported (%s)", need.BitDepth, joinInts(bitDepths(capabilities)))
	case capabilities.MaxWidth > 0 && need.Width > capabilities.MaxWidth, capabilities.MaxHeight > 0 && need.Height > capabilities.MaxHeight:
		return fmt.Errorf("%dx%d exceeds the %dx%d maximum", need.Width, need.Height, capabilities.MaxWidth, capabilities.MaxHeight)
	}
	return nil
}

func bitDepths(capabilities Capabilities) []int {
	if len(capabilities.BitDepths) == 0 {
		return []int{8}
	}
	return capabilities.BitDepths
}

func joinInts(values []int) string {
	parts := make([]string, len(values))
	for i, value := range values {
		parts[i] = strconv.Itoa(value)
	}
	return strings.Join(parts, ", ")
}

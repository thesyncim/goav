package codec

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/internal/spelling"
)

// ErrInvalidSetting reports a codec setting a backend does not take, or a
// value outside what it accepts.
var ErrInvalidSetting = errors.New("codec: invalid setting")

// Knob declares one setting a backend takes: its name, type, range or
// choices, default, and a line of help. A backend that declares knobs has its
// settings checked against them when a recipe is planned and when it opens,
// misspellings get a suggestion, and Explain lists them.
//
// Knobs named for the common settings receive them: bitrate
// (codec.Bitrate), gop (codec.KeyframeInterval), profile, level, and
// realtime (the low-latency mode of realtime work, unless set). rc and
// threads are the common names for rate control and thread count.
//
// Settings reach a backend as codec.Setting(name, value); a name prefixed
// with a backend and a dot (openh264.rc) applies only when that backend
// serves. Anything the knobs leave out passes raw, as one
// codec.Setting("<backend>-params", "key=value:key=value"), checked against
// the backend's RawKnobs when it declares them.
type Knob struct {
	Name    string
	Type    KnobType
	Min     float64
	Max     float64
	Values  []string
	Default string
	Help    string
	// Live marks a knob the backend applies while running: a bitrate change
	// reaches it without a restart.
	Live bool
}

// KnobType is the type of a Knob's value.
type KnobType string

// The knob types. An int accepts SI suffixes (2M, 800k); a bool accepts
// true/false, yes/no, on/off, and 1/0; an enum one of Values, ignoring case.
const (
	KnobBool   KnobType = "bool"
	KnobInt    KnobType = "int"
	KnobFloat  KnobType = "float"
	KnobEnum   KnobType = "enum"
	KnobString KnobType = "string"
)

// The common knob names the typed settings map onto.
const (
	knobBitrate  = "bitrate"
	knobGOP      = "gop"
	knobProfile  = "profile"
	knobLevel    = "level"
	knobRealtime = "realtime"
)

// String is the knob's one-line summary, as Explain lists it:
// "rc=cbr|vbr|cqp (default vbr): rate control".
func (k Knob) String() string {
	var b strings.Builder
	b.WriteString(k.Name + "=")
	switch {
	case k.Type == KnobEnum:
		b.WriteString(strings.Join(k.Values, "|"))
	case (k.Type == KnobInt || k.Type == KnobFloat) && (k.Min != 0 || k.Max != 0):
		b.WriteString(formatKnobNumber(k.Min) + ".." + formatKnobNumber(k.Max))
	default:
		b.WriteString(string(k.Type))
	}
	if k.Default != "" {
		b.WriteString(" (default " + k.Default + ")")
	}
	if k.Live {
		b.WriteString(" live")
	}
	if k.Help != "" {
		b.WriteString(": " + k.Help)
	}
	return b.String()
}

func formatKnobNumber(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}

// Parse checks value against the knob and returns it in canonical form: an
// int as decimal digits, a bool as true or false, an enum as declared.
func (k Knob) Parse(value string) (string, error) {
	invalid := func(want string) error {
		return fmt.Errorf("%w: %s=%s: want %s", ErrInvalidSetting, k.Name, value, want)
	}
	switch k.Type {
	case KnobBool:
		v, ok := spelling.Bool(value)
		if !ok {
			return "", invalid("true or false")
		}
		return strconv.FormatBool(v), nil
	case KnobInt:
		v, ok := spelling.Quantity(value)
		if !ok {
			return "", invalid("a whole number")
		}
		if err := k.checkRange(v, value); err != nil {
			return "", err
		}
		return strconv.FormatInt(int64(v), 10), nil
	case KnobFloat:
		v, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
			return "", invalid("a number")
		}
		if err := k.checkRange(v, value); err != nil {
			return "", err
		}
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	case KnobEnum:
		i := slices.IndexFunc(k.Values, func(choice string) bool { return strings.EqualFold(choice, value) })
		if i < 0 {
			return "", invalid("one of " + strings.Join(k.Values, ", "))
		}
		return k.Values[i], nil
	}
	return value, nil
}

func (k Knob) checkRange(v float64, value string) error {
	if (k.Min != 0 || k.Max != 0) && (v < k.Min || v > k.Max) {
		return fmt.Errorf("%w: %s=%s is outside %s..%s", ErrInvalidSetting, k.Name, value, formatKnobNumber(k.Min), formatKnobNumber(k.Max))
	}
	return nil
}

// KnobValues are a backend's settings resolved against its knobs: each
// knob's value (set, defaulted, or empty) and the raw parameters.
type KnobValues struct {
	values map[string]string
	set    map[string]bool
	raw    [][2]string
}

// String returns the knob's value, or "" when it is neither set nor
// defaulted.
func (v KnobValues) String(name string) string { return v.values[name] }

// IsSet reports whether the request set the knob (not just its default).
func (v KnobValues) IsSet(name string) bool { return v.set[name] }

// Int returns an int or bool knob's value, 0 when empty.
func (v KnobValues) Int(name string) int {
	n, _ := strconv.Atoi(v.values[name])
	return n
}

// Float returns a float knob's value, 0 when empty.
func (v KnobValues) Float(name string) float64 {
	f, _ := strconv.ParseFloat(v.values[name], 64)
	return f
}

// Bool returns a bool knob's value, false when empty.
func (v KnobValues) Bool(name string) bool { return v.values[name] == "true" }

// Raw returns the raw parameters in the order given, each a key and value.
func (v KnobValues) Raw() [][2]string { return slices.Clone(v.raw) }

// With returns a copy of v with knob name set to an already-parsed value, as
// a live control changes it.
func (v KnobValues) With(name, value string) KnobValues {
	next := KnobValues{values: map[string]string{}, set: map[string]bool{}, raw: v.raw}
	for k, val := range v.values {
		next.values[k] = val
	}
	for k, s := range v.set {
		next.set[k] = s
	}
	next.values[name], next.set[name] = value, true
	return next
}

// Knob returns the descriptor's knob called name.
func (d Descriptor) Knob(name string) (Knob, bool) {
	i := slices.IndexFunc(d.Knobs, func(k Knob) bool { return strings.EqualFold(k.Name, name) })
	if i < 0 {
		return Knob{}, false
	}
	return d.Knobs[i], true
}

// ResolveKnobs checks s against the descriptor's knobs and returns their
// values: the typed common settings first, then each codec.Setting, then the
// backend's raw parameters; lowLatency defaults the realtime knob. Settings
// scoped to another backend are skipped. A descriptor that declares no knobs
// accepts its settings unchecked, as the values of their names.
func (d Descriptor) ResolveKnobs(s CodecSettings, lowLatency bool) (KnobValues, error) {
	values := KnobValues{values: map[string]string{}, set: map[string]bool{}}
	backend := strings.ToLower(d.Backend.Name)
	if d.Knobs == nil {
		for key, value := range s.Custom {
			values.values[key], values.set[key] = value, true
		}
		return values, nil
	}
	for _, knob := range d.Knobs {
		if knob.Default != "" {
			values.values[knob.Name] = knob.Default
		}
	}
	set := func(name, value string) error {
		knob, ok := d.Knob(name)
		if !ok {
			return d.unknownKnob(name, value)
		}
		parsed, err := knob.Parse(value)
		if err != nil {
			return d.knobError(err)
		}
		values.values[knob.Name], values.set[knob.Name] = parsed, true
		return nil
	}
	typed := []struct {
		name  string
		value string
	}{
		{knobBitrate, positiveText(s.Bitrate)},
		{knobGOP, positiveText(s.KeyframeInterval)},
		{knobProfile, s.Profile},
		{knobLevel, s.Level},
	}
	for _, t := range typed {
		if t.value == "" {
			continue
		}
		if err := set(t.name, t.value); err != nil {
			return KnobValues{}, err
		}
	}
	keys := make([]string, 0, len(s.Custom))
	for key := range s.Custom {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		value := s.Custom[key]
		lower := strings.ToLower(key)
		switch {
		case strings.HasSuffix(lower, "-params"):
			if strings.TrimSuffix(lower, "-params") != backend {
				continue
			}
			raw, err := d.parseRaw(value)
			if err != nil {
				return KnobValues{}, err
			}
			values.raw = append(values.raw, raw...)
			continue
		case strings.Contains(key, "."):
			scope, name, _ := strings.Cut(key, ".")
			if !strings.EqualFold(scope, backend) {
				continue
			}
			key = name
		}
		if err := set(key, value); err != nil {
			return KnobValues{}, err
		}
	}
	if _, ok := d.Knob(knobRealtime); ok && !values.set[knobRealtime] {
		values.values[knobRealtime] = strconv.FormatBool(lowLatency)
	}
	return values, nil
}

func positiveText(n int) string {
	if n <= 0 {
		return ""
	}
	return strconv.Itoa(n)
}

// parseRaw splits key=value:key=value and checks each key against RawKnobs
// when the backend declares them.
func (d Descriptor) parseRaw(params string) ([][2]string, error) {
	var out [][2]string
	for _, pair := range strings.FieldsFunc(params, func(r rune) bool { return r == ':' }) {
		key, value, ok := strings.Cut(pair, "=")
		if !ok || key == "" {
			return nil, d.knobError(fmt.Errorf("%w: raw parameter %q is not key=value", ErrInvalidSetting, pair))
		}
		if d.RawKnobs != nil {
			i := slices.IndexFunc(d.RawKnobs, func(k Knob) bool { return strings.EqualFold(k.Name, key) })
			if i < 0 {
				names := make([]string, len(d.RawKnobs))
				for j := range d.RawKnobs {
					names[j] = d.RawKnobs[j].Name
				}
				return nil, d.knobError(withSuggestion(fmt.Errorf("%w: %s has no raw parameter %q", ErrInvalidSetting, d.Backend.Name, key), key, names))
			}
			parsed, err := d.RawKnobs[i].Parse(value)
			if err != nil {
				return nil, d.knobError(err)
			}
			key, value = d.RawKnobs[i].Name, parsed
		}
		out = append(out, [2]string{key, value})
	}
	return out, nil
}

func (d Descriptor) unknownKnob(name, value string) error {
	names := make([]string, len(d.Knobs))
	for i := range d.Knobs {
		names[i] = d.Knobs[i].Name
	}
	err := fmt.Errorf("%w: %s takes no setting %q (settings: %s)", ErrInvalidSetting, d.Backend.Name, name, strings.Join(names, ", "))
	if value != "" && len(name) > 0 {
		err = withSuggestion(err, name, names)
	}
	return d.knobError(err)
}

func (d Descriptor) knobError(err error) error {
	return fmt.Errorf("%s %s: %w", string(d.ID), d.Backend.Name, err)
}

func withSuggestion(err error, name string, names []string) error {
	if near := spelling.Nearest(name, names); near != "" {
		return fmt.Errorf("%w; did you mean %s?", err, near)
	}
	return err
}

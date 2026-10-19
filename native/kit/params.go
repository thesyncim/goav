package kit

import (
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/codec"
)

// Params exposes the fields of a C parameter struct T as raw parameters, by
// the C names their c struct tags give, so a backend reaches every knob its
// library has without mapping them by hand. Scalar fields (bools, integers,
// floats) are parameters; a struct field tagged c:",inline" adds its own
// fields, and an array of structs tagged c:",first" adds the fields of its
// first element. Knobs is the raw schema for Codec.RawKnobs, typed and
// ranged by each field's Go type; Apply writes validated values.
type Params[T any] struct {
	fields map[string]param
	knobs  []codec.Knob
}

type param struct {
	path []int // field indexes; -1 steps into element 0 of an array
	kind reflect.Kind
}

// ParamsOf describes T's tagged fields.
func ParamsOf[T any]() *Params[T] {
	p := &Params[T]{fields: map[string]param{}}
	p.collect(reflect.TypeFor[T](), nil)
	for name, field := range p.fields {
		p.knobs = append(p.knobs, knobFor(name, field.kind))
	}
	slices.SortFunc(p.knobs, func(a, b codec.Knob) int { return strings.Compare(a.Name, b.Name) })
	return p
}

func (p *Params[T]) collect(t reflect.Type, prefix []int) {
	for i := range t.NumField() {
		f := t.Field(i)
		name, option, _ := strings.Cut(f.Tag.Get("c"), ",")
		path := append(slices.Clone(prefix), i)
		switch {
		case option == "inline" && f.Type.Kind() == reflect.Struct:
			p.collect(f.Type, path)
		case option == "first" && f.Type.Kind() == reflect.Array && f.Type.Elem().Kind() == reflect.Struct:
			p.collect(f.Type.Elem(), append(path, -1))
		case name != "" && scalar(f.Type.Kind()):
			p.fields[name] = param{path: path, kind: f.Type.Kind()}
		}
	}
}

func scalar(kind reflect.Kind) bool {
	switch kind {
	case reflect.Bool, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
		return true
	}
	return false
}

func knobFor(name string, kind reflect.Kind) codec.Knob {
	knob := codec.Knob{Name: name, Type: codec.KnobInt}
	switch kind {
	case reflect.Bool:
		knob.Type = codec.KnobBool
	case reflect.Float32, reflect.Float64:
		knob.Type = codec.KnobFloat
	case reflect.Int8:
		knob.Min, knob.Max = math.MinInt8, math.MaxInt8
	case reflect.Int16:
		knob.Min, knob.Max = math.MinInt16, math.MaxInt16
	case reflect.Int32:
		knob.Min, knob.Max = math.MinInt32, math.MaxInt32
	case reflect.Uint8:
		knob.Max = math.MaxUint8
	case reflect.Uint16:
		knob.Max = math.MaxUint16
	case reflect.Uint32:
		knob.Max = math.MaxUint32
	}
	return knob
}

// Knobs is the raw schema: one knob per parameter, by C name.
func (p *Params[T]) Knobs() []codec.Knob { return slices.Clone(p.knobs) }

// Apply writes raw parameters (codec.KnobValues.Raw, already checked against
// Knobs) into dst.
func (p *Params[T]) Apply(dst *T, raw [][2]string) error {
	for _, kv := range raw {
		field, ok := p.fields[kv[0]]
		if !ok {
			return fmt.Errorf("%w: no raw parameter %s", codec.ErrInvalidSetting, kv[0])
		}
		v := reflect.ValueOf(dst).Elem()
		for _, i := range field.path {
			if i < 0 {
				v = v.Index(0)
			} else {
				v = v.Field(i)
			}
		}
		if err := setScalar(v, kv[1]); err != nil {
			return fmt.Errorf("%w: raw parameter %s=%s: %v", codec.ErrInvalidSetting, kv[0], kv[1], err)
		}
	}
	return nil
}

func setScalar(v reflect.Value, value string) error {
	switch v.Kind() {
	case reflect.Bool:
		v.SetBool(value == "true" || value == "1")
	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(value, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetFloat(f)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(value, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetUint(n)
	default:
		n, err := strconv.ParseInt(value, 10, v.Type().Bits())
		if err != nil {
			return err
		}
		v.SetInt(n)
	}
	return nil
}

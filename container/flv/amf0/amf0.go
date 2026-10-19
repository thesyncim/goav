// Package amf0 reads and writes AMF0 (Action Message Format 0), the
// encoding of FLV script data (onMetaData) and of RTMP commands (connect,
// publish, onStatus). Values are float64 (number), bool, string, Object
// (an object or ECMA array), []any (a strict array), and nil (null or
// undefined); ECMAArray writes an Object as an ECMA array.
package amf0

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// ErrInvalid reports bytes that are not AMF0, or a value AMF0 cannot hold.
var ErrInvalid = errors.New("amf0: invalid data")

// Markers.
const (
	markerNumber      = 0x00
	markerBoolean     = 0x01
	markerString      = 0x02
	markerObject      = 0x03
	markerNull        = 0x05
	markerUndefined   = 0x06
	markerReference   = 0x07
	markerECMAArray   = 0x08
	markerObjectEnd   = 0x09
	markerStrictArray = 0x0a
	markerDate        = 0x0b
	markerLongString  = 0x0c
	markerXMLDocument = 0x0f
	markerTypedObject = 0x10
)

// maxDepth bounds how deeply objects and arrays nest; onMetaData nests two
// levels (a track map of track objects), commands one or two.
const maxDepth = 16

// Property is one name-value pair of an Object.
type Property struct {
	Name  string
	Value any
}

// Object is an AMF0 object's properties, in order.
type Object []Property

// ECMAArray is an Object written as an ECMA array, as onMetaData is.
type ECMAArray Object

// Get returns the value named name.
func (o Object) Get(name string) (any, bool) {
	for _, p := range o {
		if p.Name == name {
			return p.Value, true
		}
	}
	return nil, false
}

// Number returns the finite number named name.
func (o Object) Number(name string) (float64, bool) {
	v, _ := o.Get(name)
	n, ok := v.(float64)
	return n, ok && !math.IsNaN(n) && !math.IsInf(n, 0)
}

// String returns the string named name.
func (o Object) String(name string) (string, bool) {
	v, _ := o.Get(name)
	s, ok := v.(string)
	return s, ok
}

// Object returns the object named name.
func (o Object) Object(name string) (Object, bool) {
	v, _ := o.Get(name)
	obj, ok := v.(Object)
	return obj, ok
}

// Decode reads every value in data.
func Decode(data []byte) ([]any, error) {
	d := decoder{b: data}
	var values []any
	for len(d.b) > 0 && d.err == nil {
		values = append(values, d.value(0))
	}
	if d.err != nil {
		return values, d.err
	}
	return values, nil
}

type decoder struct {
	b   []byte
	err error
}

func (d *decoder) take(n int) []byte {
	if d.err != nil || n < 0 || n > len(d.b) {
		d.err = ErrInvalid
		return nil
	}
	out := d.b[:n]
	d.b = d.b[n:]
	return out
}

func (d *decoder) u16() int {
	if b := d.take(2); b != nil {
		return int(binary.BigEndian.Uint16(b))
	}
	return 0
}

func (d *decoder) u32() int {
	if b := d.take(4); b != nil {
		return int(binary.BigEndian.Uint32(b))
	}
	return 0
}

func (d *decoder) value(depth int) any {
	if depth > maxDepth {
		d.err = ErrInvalid
	}
	marker := d.take(1)
	if marker == nil {
		return nil
	}
	switch marker[0] {
	case markerNumber:
		if b := d.take(8); b != nil {
			return math.Float64frombits(binary.BigEndian.Uint64(b))
		}
	case markerBoolean:
		if b := d.take(1); b != nil {
			return b[0] != 0
		}
	case markerString:
		return string(d.take(d.u16()))
	case markerLongString, markerXMLDocument:
		return string(d.take(d.u32()))
	case markerObject:
		return d.properties(depth)
	case markerTypedObject:
		d.take(d.u16()) // class name
		return d.properties(depth)
	case markerECMAArray:
		d.take(4) // a count hint; the end marker ends the array
		return d.properties(depth)
	case markerStrictArray:
		count := d.u32()
		if count > len(d.b) { // every value takes at least a byte
			d.err = ErrInvalid
			return nil
		}
		values := make([]any, 0, count)
		for range count {
			values = append(values, d.value(depth+1))
		}
		return values
	case markerDate:
		d.take(10) // milliseconds and a time zone
	case markerReference:
		d.take(2)
	case markerNull, markerUndefined:
	default:
		d.err = ErrInvalid
	}
	return nil
}

// properties reads name-value pairs up to the object end marker.
func (d *decoder) properties(depth int) Object {
	obj := Object{}
	for d.err == nil {
		name := string(d.take(d.u16()))
		if name == "" && len(d.b) > 0 && d.b[0] == markerObjectEnd {
			d.take(1)
			return obj
		}
		obj = append(obj, Property{Name: name, Value: d.value(depth + 1)})
	}
	return obj
}

// Append appends the encoding of each value to dst. Integers write as
// numbers.
func Append(dst []byte, values ...any) ([]byte, error) {
	for _, v := range values {
		var err error
		if dst, err = appendValue(dst, v, 0); err != nil {
			return dst, err
		}
	}
	return dst, nil
}

func appendValue(dst []byte, v any, depth int) ([]byte, error) {
	if depth > maxDepth {
		return dst, fmt.Errorf("%w: nested past %d levels", ErrInvalid, maxDepth)
	}
	switch v := v.(type) {
	case nil:
		return append(dst, markerNull), nil
	case float64:
		dst = append(dst, markerNumber)
		return binary.BigEndian.AppendUint64(dst, math.Float64bits(v)), nil
	case int:
		return appendValue(dst, float64(v), depth)
	case uint32:
		return appendValue(dst, float64(v), depth)
	case bool:
		b := byte(0)
		if v {
			b = 1
		}
		return append(dst, markerBoolean, b), nil
	case string:
		if len(v) > math.MaxUint16 {
			dst = append(dst, markerLongString)
			dst = binary.BigEndian.AppendUint32(dst, uint32(len(v)))
			return append(dst, v...), nil
		}
		dst = append(dst, markerString)
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(v)))
		return append(dst, v...), nil
	case Object:
		return appendProperties(append(dst, markerObject), v, depth)
	case ECMAArray:
		dst = append(dst, markerECMAArray)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(v)))
		return appendProperties(dst, Object(v), depth)
	case []any:
		dst = append(dst, markerStrictArray)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(v)))
		for _, item := range v {
			var err error
			if dst, err = appendValue(dst, item, depth+1); err != nil {
				return dst, err
			}
		}
		return dst, nil
	default:
		return dst, fmt.Errorf("%w: a %T", ErrInvalid, v)
	}
}

func appendProperties(dst []byte, obj Object, depth int) ([]byte, error) {
	for _, p := range obj {
		if len(p.Name) > math.MaxUint16 {
			return dst, fmt.Errorf("%w: a %d-byte property name", ErrInvalid, len(p.Name))
		}
		dst = binary.BigEndian.AppendUint16(dst, uint16(len(p.Name)))
		dst = append(dst, p.Name...)
		var err error
		if dst, err = appendValue(dst, p.Value, depth+1); err != nil {
			return dst, err
		}
	}
	return append(dst, 0, 0, markerObjectEnd), nil
}

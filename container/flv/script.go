package flv

import (
	"encoding/binary"
	"math"
)

// AMF0 markers (the subset script tags use).
const (
	amfNumber      = 0x00
	amfBoolean     = 0x01
	amfString      = 0x02
	amfObject      = 0x03
	amfNull        = 0x05
	amfUndefined   = 0x06
	amfReference   = 0x07
	amfECMAArray   = 0x08
	amfObjectEnd   = 0x09
	amfStrictArray = 0x0a
	amfDate        = 0x0b
	amfLongString  = 0x0c
	amfXMLDocument = 0x0f
	amfTypedObject = 0x10
)

// maxAMFDepth bounds the nesting of objects and arrays a script tag may
// use; onMetaData nests two levels (a track map of track objects).
const maxAMFDepth = 16

// property is one name-value pair of an AMF0 object or ECMA array. Values
// are float64, bool, string, []property (an object or ECMA array), []any
// (a strict array), or nil.
type property struct {
	name  string
	value any
}

// decodeAMF reads one AMF0 value from r.
func decodeAMF(r *reader, depth int) any {
	if depth > maxAMFDepth {
		r.err = ErrInvalidData
	}
	marker := r.u8()
	if r.err != nil {
		return nil
	}
	switch marker {
	case amfNumber:
		if b := r.take(8); b != nil {
			return math.Float64frombits(binary.BigEndian.Uint64(b))
		}
	case amfBoolean:
		return r.u8() != 0
	case amfString:
		return string(r.take(r.u16()))
	case amfLongString, amfXMLDocument:
		if b := r.take(4); b != nil {
			return string(r.take(int(binary.BigEndian.Uint32(b))))
		}
	case amfObject:
		return decodeProperties(r, depth)
	case amfTypedObject:
		r.take(r.u16()) // class name
		return decodeProperties(r, depth)
	case amfECMAArray:
		r.take(4) // a count hint; the end marker ends the array
		return decodeProperties(r, depth)
	case amfStrictArray:
		b := r.take(4)
		if b == nil {
			return nil
		}
		count := int(binary.BigEndian.Uint32(b))
		if count > len(r.b) { // every value takes at least a byte
			r.err = ErrInvalidData
			return nil
		}
		values := make([]any, 0, count)
		for range count {
			values = append(values, decodeAMF(r, depth+1))
		}
		return values
	case amfDate:
		r.take(10) // milliseconds and a time zone
	case amfReference:
		r.take(2)
	case amfNull, amfUndefined:
	default:
		r.err = ErrInvalidData
	}
	return nil
}

// decodeProperties reads name-value pairs up to the object end marker.
func decodeProperties(r *reader, depth int) []property {
	var props []property
	for r.err == nil {
		name := string(r.take(r.u16()))
		if name == "" && len(r.b) > 0 && r.b[0] == amfObjectEnd {
			r.take(1)
			return props
		}
		props = append(props, property{name: name, value: decodeAMF(r, depth+1)})
	}
	return props
}

// lookup returns the value named name.
func lookup(props []property, name string) (any, bool) {
	for _, p := range props {
		if p.name == name {
			return p.value, true
		}
	}
	return nil, false
}

// number returns the number named name.
func number(props []property, name string) (float64, bool) {
	v, _ := lookup(props, name)
	n, ok := v.(float64)
	return n, ok && !math.IsNaN(n) && !math.IsInf(n, 0)
}

func appendAMFString(dst []byte, s string) []byte {
	dst = append(dst, amfString)
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(s)))
	return append(dst, s...)
}

func appendAMFNumber(dst []byte, v float64) []byte {
	dst = append(dst, amfNumber)
	return binary.BigEndian.AppendUint64(dst, math.Float64bits(v))
}

func appendAMFName(dst []byte, name string) []byte {
	dst = binary.BigEndian.AppendUint16(dst, uint16(len(name)))
	return append(dst, name...)
}

func appendAMFEnd(dst []byte) []byte { return append(dst, 0, 0, amfObjectEnd) }

package amf0

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	values := []any{
		"connect", 1.0, nil, true,
		Object{{Name: "app", Value: "live2"}, {Name: "fourCcList", Value: []any{"av01", "hvc1"}}, {Name: "nested", Value: Object{{Name: "n", Value: -2.5}}}},
		string(bytes.Repeat([]byte("x"), 70000)),
	}
	data, err := Append(nil, values...)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(data)
	if err != nil || !reflect.DeepEqual(got, values) {
		t.Fatalf("Decode = %#v, %v", got, err)
	}
	// An ECMA array reads back as an object.
	data, _ = Append(nil, ECMAArray{{Name: "width", Value: 1280}})
	got, err = Decode(data)
	if obj, ok := got[0].(Object); err != nil || !ok {
		t.Fatalf("ECMA array = %#v, %v", got, err)
	} else if w, ok := obj.Number("width"); !ok || w != 1280 {
		t.Fatalf("width = %v %v", w, ok)
	}
}

func TestDecodeRefusesMalformed(t *testing.T) {
	deep := bytes.Repeat([]byte{markerObject, 0, 1, 'a'}, maxDepth+2)
	for name, data := range map[string][]byte{
		"short number":      {markerNumber, 1, 2},
		"short string":      {markerString, 0, 9, 'a'},
		"unknown marker":    {0x11},
		"huge array":        {markerStrictArray, 0xff, 0xff, 0xff, 0xff, markerNull},
		"unended object":    {markerObject, 0, 1, 'a', markerNull},
		"nested too deep":   deep,
		"short long string": {markerLongString, 0, 0, 1, 0},
	} {
		if _, err := Decode(data); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", name, err)
		}
	}
	if _, err := Append(nil, struct{}{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("Append(struct) = %v", err)
	}
}

func FuzzDecode(f *testing.F) {
	seed, _ := Append(nil, "onStatus", 0.0, nil, Object{{Name: "code", Value: "NetStream.Publish.Start"}})
	f.Add(seed)
	f.Fuzz(func(t *testing.T, data []byte) {
		values, err := Decode(data)
		if err != nil {
			return
		}
		again, err := Append(nil, values...)
		if err != nil {
			t.Fatalf("decoded values do not encode: %v", err)
		}
		if _, err := Decode(again); err != nil {
			t.Fatalf("re-encoded values do not decode: %v", err)
		}
	})
}

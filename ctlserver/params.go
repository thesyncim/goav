package ctlserver

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/goav/internal/spelling"
)

// Param describes one parameter of a method, for capabilities and help.
type Param struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required,omitempty"`
	Help     string `json:"help,omitempty"`
}

type paramField struct {
	Param
	index int
	typ   reflect.Type
}

type paramSchema struct {
	typ    reflect.Type
	fields []paramField
}

var durationType = reflect.TypeFor[time.Duration]()

// schemaFor reads a params struct: `json:"name"`, `ctl:"required"`, and
// `help:"..."`. It runs once per method, when the server is built.
func schemaFor(typ reflect.Type) (paramSchema, error) {
	s := paramSchema{typ: typ}
	if typ.Kind() != reflect.Struct {
		return s, fmt.Errorf("ctlserver: method params must be a struct, not %s", typ)
	}
	for i := 0; i < typ.NumField(); i++ {
		sf := typ.Field(i)
		name, _, _ := strings.Cut(sf.Tag.Get("json"), ",")
		if name == "-" || !sf.IsExported() {
			continue
		}
		if name == "" {
			name = strings.ToLower(sf.Name)
		}
		label, ok := typeLabel(sf.Type)
		if !ok {
			return s, fmt.Errorf("ctlserver: %s.%s: unsupported param type %s", typ.Name(), sf.Name, sf.Type)
		}
		s.fields = append(s.fields, paramField{
			Param: Param{Name: name, Type: label, Required: sf.Tag.Get("ctl") == "required", Help: sf.Tag.Get("help")},
			index: i, typ: sf.Type,
		})
	}
	return s, nil
}

func typeLabel(t reflect.Type) (string, bool) {
	if t == durationType {
		return "duration", true
	}
	switch t.Kind() {
	case reflect.String:
		return "string", true
	case reflect.Bool:
		return "bool", true
	case reflect.Int, reflect.Int64, reflect.Uint32:
		return "integer", true
	case reflect.Float64:
		return "number", true
	case reflect.Slice:
		if t.Elem().Kind() == reflect.String {
			return "strings", true
		}
	}
	return "", false
}

func (s paramSchema) params() []Param {
	out := make([]Param, len(s.fields))
	for i := range s.fields {
		out[i] = s.fields[i].Param
	}
	return out
}

// bind fills a params value from a JSON object. A value may be JSON of the
// field's type or a string in the CLI's spelling: 2M, 20ms, a,b.
func (s paramSchema) bind(raw json.RawMessage) (reflect.Value, *Error) {
	v := reflect.New(s.typ).Elem()
	fields := map[string]json.RawMessage{}
	if len(raw) != 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &fields); err != nil {
			return reflect.Value{}, invalidParams("params must be an object of named values")
		}
	}
	for key := range fields {
		if _, ok := s.field(key); !ok {
			msg := fmt.Sprintf("unknown param %q", key)
			if hint := spelling.Nearest(key, s.names()); hint != "" {
				msg += fmt.Sprintf("; did you mean %q?", hint)
			} else if len(s.fields) > 0 {
				msg += "; params: " + strings.Join(s.names(), ", ")
			}
			return reflect.Value{}, invalidParams(msg)
		}
	}
	for _, f := range s.fields {
		value, ok := fields[f.Name]
		if !ok {
			if f.Required {
				return reflect.Value{}, invalidParams(fmt.Sprintf("missing param %q (%s)", f.Name, f.Type))
			}
			continue
		}
		if err := setParam(v.Field(f.index), f, value); err != nil {
			return reflect.Value{}, err
		}
	}
	return v, nil
}

func (s paramSchema) field(name string) (paramField, bool) {
	for _, f := range s.fields {
		if f.Name == name {
			return f, true
		}
	}
	return paramField{}, false
}

func (s paramSchema) names() []string {
	names := make([]string, len(s.fields))
	for i := range s.fields {
		names[i] = s.fields[i].Name
	}
	sort.Strings(names)
	return names
}

func setParam(dst reflect.Value, f paramField, raw json.RawMessage) *Error {
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		// Not a string: JSON of the field's own type.
		if f.typ == durationType {
			return invalidParams(fmt.Sprintf("param %q is a duration like \"2s\"", f.Name))
		}
		if err := json.Unmarshal(raw, dst.Addr().Interface()); err != nil {
			return invalidParams(fmt.Sprintf("param %q is %s", f.Name, f.Type))
		}
		return nil
	}
	bad := func(want string) *Error {
		return invalidParams(fmt.Sprintf("param %q: %q is not %s", f.Name, text, want))
	}
	switch {
	case f.typ == durationType:
		d, err := time.ParseDuration(text)
		if err != nil {
			return bad("a duration like 2s or 20ms")
		}
		dst.SetInt(int64(d))
		return nil
	}
	switch f.typ.Kind() {
	case reflect.String:
		dst.SetString(text)
	case reflect.Bool:
		b, ok := spelling.Bool(text)
		if !ok {
			return bad("true or false")
		}
		dst.SetBool(b)
	case reflect.Int, reflect.Int64:
		n, ok := spelling.Quantity(text)
		if !ok {
			return bad("a whole number (k and M multiply by 1000)")
		}
		dst.SetInt(int64(n))
	case reflect.Uint32:
		n, ok := spelling.Quantity(text)
		if !ok || n < 0 || n > math.MaxUint32 {
			return bad("a non-negative whole number")
		}
		dst.SetUint(uint64(n))
	case reflect.Float64:
		n, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return bad("a number")
		}
		dst.SetFloat(n)
	case reflect.Slice:
		var parts []string
		for _, p := range strings.Split(text, ",") {
			if p = strings.TrimSpace(p); p != "" {
				parts = append(parts, p)
			}
		}
		dst.Set(reflect.ValueOf(parts))
	}
	return nil
}

package launch

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/goav/internal/spelling"
)

// Size is a width and height, written WxH: resize 1280x720.
type Size struct{ Width, Height int }

func parseSize(s string) (Size, bool) {
	w, h, ok := strings.Cut(strings.ToLower(s), "x")
	if !ok {
		return Size{}, false
	}
	width, err1 := strconv.Atoi(w)
	height, err2 := strconv.Atoi(h)
	if err1 != nil || err2 != nil || width <= 0 || height <= 0 {
		return Size{}, false
	}
	return Size{width, height}, true
}

var (
	sizeType     = reflect.TypeFor[Size]()
	durationType = reflect.TypeFor[time.Duration]()
	restType     = reflect.TypeFor[map[string]string]()
)

// argField is one bindable field of an element's arguments struct, from its
// `launch:"name,positional,required,flag,rest"` tag and `help:"..."`.
type argField struct {
	name       string
	positional bool
	required   bool
	flag       bool // a bare word equal to name sets a bool
	rest       bool // map[string]string collecting unknown keys
	help       string
	index      int
	typ        reflect.Type
}

type argSchema struct {
	typ    reflect.Type
	fields []argField
}

// schemaOf reads an arguments struct's fields. It is called once, when an
// element is defined, so binding costs reflection only at compile time.
func schemaOf(typ reflect.Type) (argSchema, error) {
	schema := argSchema{typ: typ}
	if typ == nil {
		return schema, nil
	}
	if typ.Kind() != reflect.Struct {
		return schema, fmt.Errorf("launch: element arguments must be a struct, not %s", typ)
	}
	for i := 0; i < typ.NumField(); i++ {
		sf := typ.Field(i)
		tag, ok := sf.Tag.Lookup("launch")
		if !ok || !sf.IsExported() {
			continue
		}
		parts := strings.Split(tag, ",")
		f := argField{name: parts[0], index: i, typ: sf.Type, help: sf.Tag.Get("help")}
		for _, opt := range parts[1:] {
			switch opt {
			case "positional":
				f.positional = true
			case "required":
				f.required = true
			case "flag":
				f.flag = true
			case "rest":
				f.rest = true
			default:
				return schema, fmt.Errorf("launch: %s.%s: unknown tag option %q", typ.Name(), sf.Name, opt)
			}
		}
		if f.name == "" && !f.positional && !f.rest {
			f.name = strings.ToLower(sf.Name)
		}
		if f.rest && sf.Type != restType {
			return schema, fmt.Errorf("launch: %s.%s: a rest field must be map[string]string", typ.Name(), sf.Name)
		}
		if f.flag && sf.Type.Kind() != reflect.Bool {
			return schema, fmt.Errorf("launch: %s.%s: a flag field must be bool", typ.Name(), sf.Name)
		}
		if !f.rest && !supportedArgType(sf.Type) {
			return schema, fmt.Errorf("launch: %s.%s: unsupported argument type %s", typ.Name(), sf.Name, sf.Type)
		}
		schema.fields = append(schema.fields, f)
	}
	return schema, nil
}

func supportedArgType(t reflect.Type) bool {
	if t == sizeType || t == durationType {
		return true
	}
	switch t.Kind() {
	case reflect.String, reflect.Bool, reflect.Int, reflect.Int64, reflect.Uint, reflect.Uint32, reflect.Uint64, reflect.Float64:
		return true
	}
	return false
}

// bind fills a new arguments value from a term's arguments.
func (s argSchema) bind(element string, args []argNode) (reflect.Value, *Error) {
	if s.typ == nil {
		if len(args) > 0 {
			a := args[0]
			return reflect.Value{}, &Error{Pos: argPos(a), End: a.end, Msg: element + " takes no settings", Hint: "write " + element + " on its own; join steps with ! and separate a join's inputs with ,"}
		}
		return reflect.Value{}, nil
	}
	v := reflect.New(s.typ).Elem()
	seen := map[string]bool{}
	positional := 0
	var rest reflect.Value
	for _, f := range s.fields {
		if f.rest {
			rest = v.Field(f.index)
		}
	}
	for _, a := range args {
		if a.key == "" {
			if f, ok := s.flagNamed(a.value); ok {
				if seen[f.name] {
					return reflect.Value{}, &Error{Pos: a.pos, End: a.end, Msg: element + ": " + f.name + " is set twice"}
				}
				seen[f.name] = true
				v.Field(f.index).SetBool(true)
				continue
			}
			f, ok := s.positionalAt(positional)
			if !ok {
				return reflect.Value{}, &Error{Pos: a.pos, End: a.end, Msg: fmt.Sprintf("%s: unexpected value %q", element, a.value), Hint: s.usageHint(element)}
			}
			positional++
			if err := setArg(v.Field(f.index), f, a); err != nil {
				return reflect.Value{}, err
			}
			seen[s.fieldKey(f)] = true
			continue
		}
		f, ok := s.fieldNamed(a.key)
		if !ok {
			// A key one or two edits from a known setting is a typo, not a
			// native setting to pass through.
			if hint := suggest(a.key, s.names()); rest.IsValid() && hint != "" {
				return reflect.Value{}, &Error{Pos: a.keyPos, End: a.keyPos + len(a.key), Msg: fmt.Sprintf("%s has no setting %q", element, a.key), Hint: hint}
			}
			if rest.IsValid() {
				if rest.IsNil() {
					rest.Set(reflect.MakeMap(restType))
				}
				rest.SetMapIndex(reflect.ValueOf(a.key), reflect.ValueOf(a.value))
				continue
			}
			hint := suggest(a.key, s.names())
			if hint == "" {
				hint = s.usageHint(element)
			}
			return reflect.Value{}, &Error{Pos: a.keyPos, End: a.keyPos + len(a.key), Msg: fmt.Sprintf("%s has no setting %q", element, a.key), Hint: hint}
		}
		if seen[s.fieldKey(f)] {
			return reflect.Value{}, &Error{Pos: a.keyPos, End: a.end, Msg: element + ": " + a.key + " is set twice"}
		}
		seen[s.fieldKey(f)] = true
		if err := setArg(v.Field(f.index), f, a); err != nil {
			return reflect.Value{}, err
		}
	}
	for _, f := range s.fields {
		if f.required && !seen[s.fieldKey(f)] {
			return reflect.Value{}, &Error{Pos: -1, Msg: element + " needs " + s.fieldLabel(f), Hint: s.usageHint(element)}
		}
	}
	return v, nil
}

func argPos(a argNode) int {
	if a.key != "" {
		return a.keyPos
	}
	return a.pos
}

func (s argSchema) fieldKey(f argField) string {
	if f.name != "" {
		return f.name
	}
	return "#" + strconv.Itoa(f.index)
}

func (s argSchema) fieldLabel(f argField) string {
	if f.name != "" {
		return f.name
	}
	return "a " + f.help
}

func (s argSchema) flagNamed(word string) (argField, bool) {
	for _, f := range s.fields {
		if f.flag && f.name == word {
			return f, true
		}
	}
	return argField{}, false
}

func (s argSchema) positionalAt(n int) (argField, bool) {
	for _, f := range s.fields {
		if f.positional {
			if n == 0 {
				return f, true
			}
			n--
		}
	}
	return argField{}, false
}

func (s argSchema) fieldNamed(name string) (argField, bool) {
	for _, f := range s.fields {
		if f.name == name && !f.rest {
			return f, true
		}
	}
	return argField{}, false
}

func (s argSchema) names() []string {
	var names []string
	for _, f := range s.fields {
		if f.name != "" && !f.rest {
			names = append(names, f.name)
		}
	}
	return names
}

// usage renders the element's argument form: name <positional> key=<type>.
func (s argSchema) usage(element string) string {
	parts := []string{element}
	for _, f := range s.fields {
		switch {
		case f.rest:
			parts = append(parts, "[key=value...]")
		case f.flag:
			parts = append(parts, "["+f.name+"]")
		case f.positional:
			label := "<" + firstNonEmpty(f.name, f.help, "value") + ">"
			if !f.required {
				label = "[" + label + "]"
			}
			parts = append(parts, label)
		default:
			label := f.name + "=<" + firstNonEmpty(f.help, typeLabel(f.typ)) + ">"
			if !f.required {
				label = "[" + label + "]"
			}
			parts = append(parts, label)
		}
	}
	return strings.Join(parts, " ")
}

func (s argSchema) usageHint(element string) string {
	return "usage: " + s.usage(element)
}

func typeLabel(t reflect.Type) string {
	switch {
	case t == sizeType:
		return "WxH"
	case t == durationType:
		return "duration"
	}
	switch t.Kind() {
	case reflect.Bool:
		return "bool"
	case reflect.String:
		return "text"
	case reflect.Float64:
		return "number"
	default:
		return "n"
	}
}

func setArg(dst reflect.Value, f argField, a argNode) *Error {
	bad := func(want string) *Error {
		return &Error{Pos: a.pos, End: a.end, Msg: fmt.Sprintf("%s: %q is not %s", firstNonEmpty(f.name, f.help, "value"), a.value, want)}
	}
	switch {
	case f.typ == sizeType:
		size, ok := parseSize(a.value)
		if !ok {
			return bad("a size like 1280x720")
		}
		dst.Set(reflect.ValueOf(size))
		return nil
	case f.typ == durationType:
		d, err := time.ParseDuration(a.value)
		if err != nil {
			return bad("a duration like 2s or 20ms")
		}
		dst.SetInt(int64(d))
		return nil
	}
	switch f.typ.Kind() {
	case reflect.String:
		dst.SetString(a.value)
	case reflect.Bool:
		b, ok := spelling.Bool(a.value)
		if !ok {
			return bad("true or false")
		}
		dst.SetBool(b)
	case reflect.Int, reflect.Int64:
		n, ok := spelling.Quantity(a.value)
		if !ok {
			return bad("a whole number (k and M suffixes multiply by 1000)")
		}
		dst.SetInt(int64(n))
	case reflect.Uint, reflect.Uint32, reflect.Uint64:
		n, ok := spelling.Quantity(a.value)
		if !ok || n < 0 || dst.OverflowUint(uint64(n)) {
			return bad("a non-negative whole number")
		}
		dst.SetUint(uint64(n))
	case reflect.Float64:
		n, err := strconv.ParseFloat(a.value, 64)
		if err != nil {
			return bad("a number")
		}
		dst.SetFloat(n)
	}
	return nil
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

// rate is a frame rate as a fraction: 30/1, 30000/1001.
type rate struct{ num, den int }

// parseRate reads a frame rate written as 30, 29.97, or 30000/1001.
func parseRate(s string) (rate, bool) {
	if left, right, ok := strings.Cut(s, "/"); ok {
		num, err1 := strconv.Atoi(left)
		den, err2 := strconv.Atoi(right)
		if err1 != nil || err2 != nil || num <= 0 || den <= 0 {
			return rate{}, false
		}
		return reduced(rate{num, den}), true
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f <= 0 || math.IsInf(f, 0) {
		return rate{}, false
	}
	if f == math.Trunc(f) {
		return rate{int(f), 1}, true
	}
	return reduced(rate{int(math.Round(f * 1000)), 1000}), true
}

func reduced(r rate) rate {
	a, b := r.num, r.den
	for b != 0 {
		a, b = b, a%b
	}
	return rate{r.num / a, r.den / a}
}

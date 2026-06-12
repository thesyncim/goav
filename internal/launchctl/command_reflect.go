package launchctl

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/goav/av"
)

var (
	streamIDType  = reflect.TypeOf(av.StreamID(""))
	eventTypeType = reflect.TypeOf(av.EventType(""))
	metadataType  = reflect.TypeOf(av.Metadata{})
	durationType  = reflect.TypeOf(time.Duration(0))
)

type fieldSpec struct {
	name     string
	required bool
	parser   string
	usage    string
	help     string
	index    int
	typ      reflect.Type
}

type bindContext struct {
	name        string
	operation   string
	argsType    reflect.Type
	usage       string
	suggestions []string
}

// BindArgs reflects over a known command args struct and fills it from
// key=value CLI arguments. The caller supplies the CommandSpec from the
// manifest; no user-provided method or type name is ever invoked.
func BindArgs(spec CommandSpec, args []string) (any, error) {
	return bindKnownStruct(bindContext{
		name:        spec.Name,
		operation:   "control " + spec.Name,
		argsType:    spec.ArgsType,
		usage:       CommandUsage(spec),
		suggestions: []string{"use `goav ctl help control " + spec.Name + "`"},
	}, args)
}

func bindKnownStruct(ctx bindContext, args []string) (any, error) {
	if ctx.argsType == nil || ctx.argsType.Kind() != reflect.Struct {
		return nil, commandError("invalid_command", ctx.operation, "", "command args type must be a struct", nil, nil, nil)
	}
	fields := commandFields(ctx.argsType)
	target := reflect.New(ctx.argsType).Elem()
	seen := make(map[string]struct{})
	for _, arg := range args {
		if arg == "" {
			continue
		}
		if strings.HasPrefix(arg, "--") {
			name := strings.TrimPrefix(arg, "--")
			field, ok := fields[name]
			if !ok || field.typ.Kind() != reflect.Bool {
				return nil, unknownFieldError(ctx, name, fields)
			}
			target.Field(field.index).SetBool(true)
			seen[field.name] = struct{}{}
			continue
		}
		if strings.Contains(arg, "..") && !strings.Contains(arg, "=") {
			if err := bindRange(ctx, target, fields, arg, seen); err != nil {
				return nil, err
			}
			continue
		}
		key, value, ok := strings.Cut(arg, "=")
		if !ok {
			return nil, commandError(
				"invalid_argument",
				ctx.operation,
				arg,
				"arguments must use key=value form",
				nil,
				ctx.suggestions,
				nil,
			)
		}
		if strings.HasPrefix(key, "metadata.") {
			field, ok := fields["metadata"]
			if !ok || field.typ != metadataType {
				return nil, unknownFieldError(ctx, key, fields)
			}
			setMetadata(target.Field(field.index), strings.TrimPrefix(key, "metadata."), value)
			seen[field.name] = struct{}{}
			continue
		}
		field, ok := fields[key]
		if !ok {
			return nil, unknownFieldError(ctx, key, fields)
		}
		if err := setFieldValue(target.Field(field.index), field, value, ctx); err != nil {
			return nil, err
		}
		seen[field.name] = struct{}{}
	}
	for _, field := range orderedFields(fields) {
		if field.required {
			if _, ok := seen[field.name]; !ok {
				return nil, commandError(
					"missing_required",
					ctx.operation,
					field.name,
					fmt.Sprintf("missing required field %s", field.name),
					[]string{"usage=" + ctx.usage},
					ctx.suggestions,
					nil,
				)
			}
		}
	}
	return target.Interface(), nil
}

// BindJSON fills a known command args struct from a JSON object. It uses the
// same field metadata as BindArgs so JSON and CLI parsing cannot drift.
func BindJSON(spec CommandSpec, data []byte) (any, error) {
	return bindKnownJSON(bindContext{
		name:        spec.Name,
		operation:   "control " + spec.Name,
		argsType:    spec.ArgsType,
		usage:       CommandUsage(spec),
		suggestions: []string{"use `goav ctl help control " + spec.Name + "`"},
	}, data)
}

func bindKnownJSON(ctx bindContext, data []byte) (any, error) {
	raw, err := decodeObject(data)
	if err != nil {
		return nil, commandError("invalid_json", ctx.operation, "", err.Error(), nil, nil, err)
	}
	args := make([]string, 0, len(raw))
	for key, value := range raw {
		if key == "metadata" {
			obj, ok := value.(map[string]any)
			if !ok {
				return nil, commandError("invalid_value", ctx.operation, key, "metadata must be an object", nil, nil, nil)
			}
			for metadataKey, metadataValue := range obj {
				text, ok := metadataScalarString(metadataValue)
				if !ok {
					return nil, commandError("invalid_value", ctx.operation, "metadata."+metadataKey, "metadata values must be scalar", nil, nil, nil)
				}
				args = append(args, "metadata."+metadataKey+"="+text)
			}
			continue
		}
		text, ok := metadataScalarString(value)
		if !ok {
			return nil, commandError("invalid_value", ctx.operation, key, "value must be scalar", nil, nil, nil)
		}
		args = append(args, key+"="+text)
	}
	return bindKnownStruct(ctx, args)
}

func commandFields(argsType reflect.Type) map[string]fieldSpec {
	fields := make(map[string]fieldSpec)
	for i := 0; i < argsType.NumField(); i++ {
		structField := argsType.Field(i)
		if structField.PkgPath != "" {
			continue
		}
		tag := structField.Tag.Get("goavctl")
		if tag == "-" {
			continue
		}
		field := fieldSpec{
			name:  strings.ToLower(structField.Name),
			usage: structField.Tag.Get("usage"),
			help:  structField.Tag.Get("help"),
			index: i,
			typ:   structField.Type,
		}
		if tag != "" {
			parts := strings.Split(tag, ",")
			if parts[0] != "" {
				field.name = parts[0]
			}
			for _, option := range parts[1:] {
				switch option {
				case "required":
					field.required = true
				case "rate", "duration":
					field.parser = option
				}
			}
		}
		fields[field.name] = field
	}
	return fields
}

func orderedFields(fields map[string]fieldSpec) []fieldSpec {
	out := make([]fieldSpec, 0, len(fields))
	for _, field := range fields {
		out = append(out, field)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].index > out[j].index; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func setFieldValue(field reflect.Value, spec fieldSpec, value string, ctx bindContext) error {
	if spec.typ == streamIDType || spec.typ == eventTypeType {
		field.SetString(value)
		return nil
	}
	if spec.typ == durationType || spec.parser == "duration" {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return commandError(
				"invalid_value",
				ctx.operation,
				spec.name,
				fmt.Sprintf("cannot parse %s.%s: expected duration like 12.5s or 1m30s", ctx.name, spec.name),
				[]string{"value=" + value},
				[]string{"use " + spec.name + "=12.5s", "use " + spec.name + "=1m30s"},
				err,
			)
		}
		field.SetInt(int64(duration))
		return nil
	}
	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
	case reflect.Bool:
		parsed, err := parseBool(value)
		if err != nil {
			return commandError("invalid_value", ctx.operation, spec.name, fmt.Sprintf("cannot parse %s.%s: expected true or false", ctx.name, spec.name), []string{"value=" + value}, []string{"use " + spec.name + "=true", "use " + spec.name + "=false"}, err)
		}
		field.SetBool(parsed)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		var parsed int
		var err error
		if spec.parser == "rate" {
			parsed, err = parseRate(value)
			if err != nil {
				return commandError(
					"invalid_value",
					ctx.operation,
					spec.name,
					fmt.Sprintf("cannot parse %s.%s: expected bitrate like 1200k, 2M, or integer bits per second", ctx.name, spec.name),
					[]string{"value=" + value},
					[]string{"use value=1200k", "use value=2000000"},
					err,
				)
			}
		} else {
			parsed64, parseErr := strconv.ParseInt(value, 10, 64)
			parsed, err = int(parsed64), parseErr
		}
		if err != nil {
			return commandError("invalid_value", ctx.operation, spec.name, fmt.Sprintf("cannot parse %s.%s: expected integer", ctx.name, spec.name), []string{"value=" + value}, nil, err)
		}
		field.SetInt(int64(parsed))
	case reflect.Float64:
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(parsed) {
			return commandError("invalid_value", ctx.operation, spec.name, fmt.Sprintf("cannot parse %s.%s: expected float", ctx.name, spec.name), []string{"value=" + value}, []string{"use value=0.5"}, err)
		}
		field.SetFloat(parsed)
	default:
		if spec.typ == metadataType {
			return commandError("invalid_argument", ctx.operation, spec.name, "metadata must be set as metadata.<key>=<value>", nil, []string{"use metadata.source=cli"}, nil)
		}
		return commandError("invalid_command", ctx.operation, spec.name, "unsupported field type "+spec.typ.String(), nil, nil, nil)
	}
	return nil
}

func bindRange(ctx bindContext, target reflect.Value, fields map[string]fieldSpec, arg string, seen map[string]struct{}) error {
	startText, endText, ok := strings.Cut(arg, "..")
	if !ok {
		return nil
	}
	start, okStart := fields["start"]
	end, okEnd := fields["end"]
	if !okStart || !okEnd {
		return commandError("invalid_argument", ctx.operation, arg, "range syntax is only supported by segment", nil, []string{"use start=10s end=20s"}, nil)
	}
	if err := setFieldValue(target.Field(start.index), start, startText, ctx); err != nil {
		return err
	}
	if err := setFieldValue(target.Field(end.index), end, endText, ctx); err != nil {
		return err
	}
	seen["start"] = struct{}{}
	seen["end"] = struct{}{}
	return nil
}

func setMetadata(field reflect.Value, key string, value string) {
	if field.IsNil() {
		field.Set(reflect.MakeMap(field.Type()))
	}
	field.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(value))
}

func parseBool(value string) (bool, error) {
	switch value {
	case "true":
		return true, nil
	case "false":
		return false, nil
	default:
		return false, fmt.Errorf("expected true or false")
	}
}

func parseRate(value string) (int, error) {
	if value == "" {
		return 0, fmt.Errorf("empty rate")
	}
	multiplier := 1.0
	number := value
	suffix := value[len(value)-1]
	switch suffix {
	case 'k', 'K':
		multiplier = 1000
		number = value[:len(value)-1]
	case 'm', 'M':
		multiplier = 1000 * 1000
		number = value[:len(value)-1]
	}
	parsed, err := strconv.ParseFloat(number, 64)
	if err != nil || parsed <= 0 || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
		return 0, fmt.Errorf("invalid rate %q", value)
	}
	bps := parsed * multiplier
	if bps > float64(math.MaxInt) {
		return 0, fmt.Errorf("rate overflows int")
	}
	return int(math.Round(bps)), nil
}

func unknownFieldError(ctx bindContext, name string, fields map[string]fieldSpec) error {
	names := make([]string, 0, len(fields))
	for fieldName := range fields {
		names = append(names, fieldName)
	}
	suggestions := cloneStrings(ctx.suggestions)
	if nearest := closest(name, names); nearest != "" {
		suggestions = append([]string{"use " + nearest + "="}, suggestions...)
	}
	return commandError(
		"unknown_field",
		ctx.operation,
		name,
		fmt.Sprintf("unknown field %q for %s", name, ctx.name),
		[]string{"known_fields=" + strings.Join(names, ",")},
		suggestions,
		nil,
	)
}

func closest(input string, candidates []string) string {
	if len(candidates) == 0 {
		return ""
	}
	best := ""
	bestDistance := 1000
	for _, candidate := range candidates {
		distance := levenshtein(input, candidate)
		if distance < bestDistance {
			bestDistance = distance
			best = candidate
		}
	}
	if bestDistance > 3 {
		return ""
	}
	return best
}

func levenshtein(a, b string) int {
	if a == b {
		return 0
	}
	if a == "" {
		return len(b)
	}
	if b == "" {
		return len(a)
	}
	prev := make([]int, len(b)+1)
	curr := make([]int, len(b)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(a); i++ {
		curr[0] = i
		for j := 1; j <= len(b); j++ {
			cost := 0
			if a[i-1] != b[j-1] {
				cost = 1
			}
			curr[j] = minInt(curr[j-1]+1, prev[j]+1, prev[j-1]+cost)
		}
		prev, curr = curr, prev
	}
	return prev[len(b)]
}

func minInt(values ...int) int {
	minimum := values[0]
	for _, value := range values[1:] {
		if value < minimum {
			minimum = value
		}
	}
	return minimum
}

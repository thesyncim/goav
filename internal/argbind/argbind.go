package argbind

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/cliargs"
)

var (
	streamIDType    = reflect.TypeOf(av.StreamID(""))
	eventTypeType   = reflect.TypeOf(av.EventType(""))
	metadataType    = reflect.TypeOf(av.Metadata{})
	durationType    = reflect.TypeOf(time.Duration(0))
	avDurationType  = reflect.TypeOf(av.Duration{})
	errInterfaceTyp = reflect.TypeOf((*error)(nil)).Elem()
)

// Field describes one exported struct field bindable from key=value args.
type Field struct {
	Name     string
	Required bool
	Parser   string
	Usage    string
	Help     string
	Index    int
	Type     reflect.Type
	Unknown  bool
	Positive bool
}

// Context configures one bind operation over an allowlisted struct type.
type Context struct {
	Name                 string
	Operation            string
	ArgsType             reflect.Type
	Usage                string
	Suggestions          []string
	IgnoreFields         map[string]struct{}
	UnknownMetadataField string
}

// Result is the reflected value plus the canonical field names seen while
// binding. Callers use Seen for cold-path normalization that cannot live in the
// generic binder, such as deriving codec structural parameters.
type Result struct {
	Value any
	Seen  map[string]struct{}
}

// TypeOf returns the struct type used by generic helper constructors without
// forcing each caller to import reflect directly.
func TypeOf[T any]() reflect.Type {
	var zero T
	if typ := reflect.TypeOf(zero); typ != nil {
		return typ
	}
	return reflect.TypeOf((*T)(nil)).Elem()
}

// Error describes one binding refusal without depending on any outer command
// protocol. Callers may wrap it in their own structured error type.
type Error struct {
	Code        string
	Operation   string
	Node        string
	Message     string
	Details     []string
	Suggestions []string
	Cause       error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Bind fills the allowlisted struct type from key=value command-line args.
func Bind(ctx Context, args []string) (Result, error) {
	if ctx.ArgsType == nil || ctx.ArgsType.Kind() != reflect.Struct {
		return Result{}, bindError("invalid_command", ctx.Operation, "", "args type must be a struct", nil, nil, nil)
	}
	fields := Fields(ctx.ArgsType)
	target := reflect.New(ctx.ArgsType).Elem()
	seen := make(map[string]struct{})
	for _, arg := range args {
		if arg == "" {
			continue
		}
		if strings.HasPrefix(arg, "--") {
			name := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(arg, "--")))
			if ignored(ctx, name) {
				continue
			}
			field, ok := fields[name]
			if !ok || field.Type.Kind() != reflect.Bool {
				return Result{}, unknownFieldError(ctx, name, fields)
			}
			if _, ok := seen[field.Name]; ok {
				return Result{}, duplicateFieldError(ctx, field.Name)
			}
			target.Field(field.Index).SetBool(true)
			seen[field.Name] = struct{}{}
			continue
		}
		if strings.Contains(arg, "..") && !strings.Contains(arg, "=") {
			if err := bindRange(ctx, target, fields, arg, seen); err != nil {
				return Result{}, err
			}
			continue
		}
		key, value, ok := strings.Cut(arg, "=")
		if !ok {
			return Result{}, bindError(
				"invalid_argument",
				ctx.Operation,
				arg,
				"arguments must use key=value form",
				nil,
				ctx.Suggestions,
				nil,
			)
		}
		key = strings.ToLower(strings.TrimSpace(key))
		if ignored(ctx, key) {
			continue
		}
		if err := bindKeyValue(ctx, target, fields, key, value, seen); err != nil {
			return Result{}, err
		}
	}
	for _, field := range OrderedFields(fields) {
		if field.Required {
			if _, ok := seen[field.Name]; !ok {
				return Result{}, bindError(
					"missing_required",
					ctx.Operation,
					field.Name,
					fmt.Sprintf("missing required field %s", field.Name),
					[]string{"usage=" + ctx.Usage},
					ctx.Suggestions,
					nil,
				)
			}
		}
	}
	return Result{Value: target.Interface(), Seen: seen}, nil
}

func bindKeyValue(ctx Context, target reflect.Value, fields map[string]Field, key string, value string, seen map[string]struct{}) error {
	if field, metadataKey, ok := metadataPrefixField(fields, key); ok {
		if metadataKey == "" {
			return bindError("invalid_argument", ctx.Operation, key, field.Name+" key cannot be empty", nil, []string{"use " + field.Name + ".<key>=<value>"}, nil)
		}
		seenKey := field.Name + "." + metadataKey
		if _, ok := seen[seenKey]; ok {
			return duplicateFieldError(ctx, seenKey)
		}
		setMetadata(target.Field(field.Index), metadataKey, value)
		seen[seenKey] = struct{}{}
		seen[field.Name] = struct{}{}
		return nil
	}
	field, ok := fields[key]
	if !ok {
		if unknown, ok := unknownMetadataField(ctx, fields); ok {
			if _, exists := seen[key]; exists {
				return duplicateFieldError(ctx, key)
			}
			setMetadata(target.Field(unknown.Index), key, value)
			seen[key] = struct{}{}
			seen[unknown.Name] = struct{}{}
			return nil
		}
		return unknownFieldError(ctx, key, fields)
	}
	if field.Unknown {
		return bindError("invalid_argument", ctx.Operation, field.Name, field.Name+" must be set as "+field.Name+".<key>=<value>", nil, []string{"use " + field.Name + ".source=cli"}, nil)
	}
	if _, ok := seen[field.Name]; ok {
		return duplicateFieldError(ctx, field.Name)
	}
	if err := setFieldValue(target.Field(field.Index), field, value, ctx); err != nil {
		return err
	}
	seen[field.Name] = struct{}{}
	return nil
}

// BindJSON fills the allowlisted struct type from a JSON object using the same
// metadata as Bind.
func BindJSON(ctx Context, data []byte) (Result, error) {
	raw, err := decodeObject(data)
	if err != nil {
		return Result{}, bindError("invalid_json", ctx.Operation, "", err.Error(), nil, nil, err)
	}
	fields := Fields(ctx.ArgsType)
	args := make([]string, 0, len(raw))
	for key, value := range raw {
		key = strings.ToLower(strings.TrimSpace(key))
		if ignored(ctx, key) {
			continue
		}
		if field, ok := fields[key]; ok && field.Type == metadataType {
			obj, ok := value.(map[string]any)
			if !ok {
				return Result{}, bindError("invalid_value", ctx.Operation, key, field.Name+" must be an object", nil, nil, nil)
			}
			for metadataKey, metadataValue := range obj {
				text, ok := metadataScalarString(metadataValue)
				if !ok {
					return Result{}, bindError("invalid_value", ctx.Operation, field.Name+"."+metadataKey, field.Name+" values must be scalar", nil, nil, nil)
				}
				args = append(args, field.Name+"."+metadataKey+"="+text)
			}
			continue
		}
		text, ok := metadataScalarString(value)
		if !ok {
			return Result{}, bindError("invalid_value", ctx.Operation, key, "value must be scalar", nil, nil, nil)
		}
		args = append(args, key+"="+text)
	}
	return Bind(ctx, args)
}

// ArgsFromMap renders key/value maps into stable argv order.
func ArgsFromMap(args map[string]string) []string {
	if len(args) == 0 {
		return nil
	}
	keys := make([]string, 0, len(args))
	for key := range args {
		keys = append(keys, key)
	}
	sortStrings(keys)
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if args[key] == "" {
			out = append(out, "--"+key)
			continue
		}
		out = append(out, key+"="+args[key])
	}
	return out
}

// ArgsUsage renders the key=value usage fragment for a typed args struct.
func ArgsUsage(argsType reflect.Type) string {
	if argsType == nil || argsType.Kind() != reflect.Struct {
		return ""
	}
	fields := OrderedFields(Fields(argsType))
	parts := make([]string, 0, len(fields))
	for _, field := range fields {
		if field.Unknown {
			if field.Usage != "" {
				parts = append(parts, field.Usage)
			}
			continue
		}
		if field.Usage != "" {
			parts = append(parts, field.Usage)
			continue
		}
		text := field.Name + "=<value>"
		if !field.Required {
			text = "[" + text + "]"
		}
		parts = append(parts, text)
	}
	return strings.Join(parts, " ")
}

// Fields reads bindable exported fields from argsType.
func Fields(argsType reflect.Type) map[string]Field {
	fields := make(map[string]Field)
	if argsType == nil || argsType.Kind() != reflect.Struct {
		return fields
	}
	for i := 0; i < argsType.NumField(); i++ {
		structField := argsType.Field(i)
		if structField.PkgPath != "" {
			continue
		}
		tag := structField.Tag.Get("goavctl")
		if tag == "-" {
			continue
		}
		field := Field{
			Name:  strings.ToLower(structField.Name),
			Usage: structField.Tag.Get("usage"),
			Help:  structField.Tag.Get("help"),
			Index: i,
			Type:  structField.Type,
		}
		if tag != "" {
			parts := strings.Split(tag, ",")
			if parts[0] != "" {
				field.Name = parts[0]
			}
			for _, option := range parts[1:] {
				switch option {
				case "required":
					field.Required = true
				case "rate", "duration", "fps":
					field.Parser = option
				case "positive":
					field.Positive = true
				case "unknown":
					field.Unknown = true
				}
			}
		}
		fields[field.Name] = field
	}
	return fields
}

// OrderedFields sorts fields by their struct field order.
func OrderedFields(fields map[string]Field) []Field {
	out := make([]Field, 0, len(fields))
	for _, field := range fields {
		out = append(out, field)
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].Index > out[j].Index; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

func setFieldValue(field reflect.Value, spec Field, value string, ctx Context) error {
	if spec.Type == streamIDType || spec.Type == eventTypeType {
		field.SetString(value)
		return nil
	}
	if spec.Type == durationType || spec.Parser == "duration" {
		duration, err := time.ParseDuration(value)
		if err != nil {
			return bindError(
				"invalid_value",
				ctx.Operation,
				spec.Name,
				fmt.Sprintf("cannot parse %s.%s: expected duration like 12.5s or 1m30s", ctx.Name, spec.Name),
				[]string{"value=" + value},
				[]string{"use " + spec.Name + "=12.5s", "use " + spec.Name + "=1m30s"},
				err,
			)
		}
		field.SetInt(int64(duration))
		return nil
	}
	if spec.Parser == "fps" {
		fps, err := cliargs.ParseFPS(value)
		if err != nil {
			return bindError(
				"invalid_value",
				ctx.Operation,
				spec.Name,
				fmt.Sprintf("cannot parse %s.%s: expected fps like 30, 29.97, or 30000/1001", ctx.Name, spec.Name),
				[]string{"value=" + value},
				[]string{"use " + spec.Name + "=30", "use " + spec.Name + "=30000/1001"},
				err,
			)
		}
		if spec.Type != avDurationType {
			return bindError("invalid_command", ctx.Operation, spec.Name, "fps parser requires av.Duration field, got "+spec.Type.String(), nil, nil, nil)
		}
		field.Set(reflect.ValueOf(av.Duration{Value: int64(fps.Den), Base: av.TimeBase{Num: 1, Den: int64(fps.Num)}}))
		return nil
	}
	switch field.Kind() {
	case reflect.String:
		field.SetString(value)
	case reflect.Bool:
		parsed, err := parseBool(value)
		if err != nil {
			return bindError("invalid_value", ctx.Operation, spec.Name, fmt.Sprintf("cannot parse %s.%s: expected true or false", ctx.Name, spec.Name), []string{"value=" + value}, []string{"use " + spec.Name + "=true", "use " + spec.Name + "=false"}, err)
		}
		field.SetBool(parsed)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		parsed, err := parseIntField(spec, value)
		if err != nil {
			return bindError("invalid_value", ctx.Operation, spec.Name, intParseMessage(ctx, spec), []string{"value=" + value}, intSuggestions(spec), err)
		}
		field.SetInt(parsed)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		parsed, err := parseUintField(spec, value)
		if err != nil {
			return bindError("invalid_value", ctx.Operation, spec.Name, fmt.Sprintf("cannot parse %s.%s: expected positive integer", ctx.Name, spec.Name), []string{"value=" + value}, nil, err)
		}
		field.SetUint(parsed)
	case reflect.Float64:
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(parsed) {
			return bindError("invalid_value", ctx.Operation, spec.Name, fmt.Sprintf("cannot parse %s.%s: expected float", ctx.Name, spec.Name), []string{"value=" + value}, []string{"use value=0.5"}, err)
		}
		field.SetFloat(parsed)
	default:
		if spec.Type == metadataType {
			return bindError("invalid_argument", ctx.Operation, spec.Name, spec.Name+" must be set as "+spec.Name+".<key>=<value>", nil, []string{"use " + spec.Name + ".source=cli"}, nil)
		}
		if spec.Type.Kind() == reflect.Func || spec.Type.Implements(errInterfaceTyp) {
			return bindError("invalid_command", ctx.Operation, spec.Name, "unsupported field type "+spec.Type.String(), nil, nil, nil)
		}
		return bindError("invalid_command", ctx.Operation, spec.Name, "unsupported field type "+spec.Type.String(), nil, nil, nil)
	}
	return nil
}

func parseIntField(spec Field, value string) (int64, error) {
	if spec.Parser == "rate" {
		parsed, err := cliargs.ParseRate(value)
		return int64(parsed), err
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return 0, err
	}
	if spec.Positive && parsed <= 0 {
		return 0, fmt.Errorf("%s must be positive", spec.Name)
	}
	return parsed, nil
}

func parseUintField(spec Field, value string) (uint64, error) {
	parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, spec.Type.Bits())
	if err != nil {
		return 0, err
	}
	if spec.Positive && parsed == 0 {
		return 0, fmt.Errorf("%s must be positive", spec.Name)
	}
	return parsed, nil
}

func intParseMessage(ctx Context, spec Field) string {
	if spec.Parser == "rate" {
		return fmt.Sprintf("cannot parse %s.%s: expected bitrate like 1200k, 2M, or integer bits per second", ctx.Name, spec.Name)
	}
	if spec.Positive {
		return fmt.Sprintf("cannot parse %s.%s: expected positive integer", ctx.Name, spec.Name)
	}
	return fmt.Sprintf("cannot parse %s.%s: expected integer", ctx.Name, spec.Name)
}

func intSuggestions(spec Field) []string {
	if spec.Parser == "rate" {
		return []string{"use " + spec.Name + "=1200k", "use " + spec.Name + "=2000000"}
	}
	return nil
}

func bindRange(ctx Context, target reflect.Value, fields map[string]Field, arg string, seen map[string]struct{}) error {
	startText, endText, ok := strings.Cut(arg, "..")
	if !ok {
		return nil
	}
	start, okStart := fields["start"]
	end, okEnd := fields["end"]
	if !okStart || !okEnd {
		return bindError("invalid_argument", ctx.Operation, arg, "range syntax is only supported by segment", nil, []string{"use start=10s end=20s"}, nil)
	}
	if _, ok := seen[start.Name]; ok {
		return duplicateFieldError(ctx, start.Name)
	}
	if _, ok := seen[end.Name]; ok {
		return duplicateFieldError(ctx, end.Name)
	}
	if err := setFieldValue(target.Field(start.Index), start, startText, ctx); err != nil {
		return err
	}
	if err := setFieldValue(target.Field(end.Index), end, endText, ctx); err != nil {
		return err
	}
	seen["start"] = struct{}{}
	seen["end"] = struct{}{}
	return nil
}

func duplicateFieldError(ctx Context, name string) error {
	return bindError(
		"invalid_argument",
		ctx.Operation,
		name,
		fmt.Sprintf("%s field %s was provided more than once", ctx.Name, name),
		nil,
		append([]string{"keep only one " + name + "=... value"}, ctx.Suggestions...),
		nil,
	)
}

func unknownFieldError(ctx Context, name string, fields map[string]Field) error {
	names := make([]string, 0, len(fields))
	for fieldName, field := range fields {
		if field.Unknown {
			continue
		}
		names = append(names, fieldName)
	}
	sortStrings(names)
	suggestions := cloneStrings(ctx.Suggestions)
	if nearest := Closest(name, names); nearest != "" {
		suggestions = append([]string{"use " + nearest + "="}, suggestions...)
	}
	return bindError(
		"unknown_field",
		ctx.Operation,
		name,
		fmt.Sprintf("unknown field %q for %s", name, ctx.Name),
		[]string{"known_fields=" + strings.Join(names, ",")},
		suggestions,
		nil,
	)
}

func metadataPrefixField(fields map[string]Field, key string) (Field, string, bool) {
	for _, field := range fields {
		if field.Type != metadataType {
			continue
		}
		prefix := field.Name + "."
		if strings.HasPrefix(key, prefix) {
			return field, strings.TrimPrefix(key, prefix), true
		}
	}
	return Field{}, "", false
}

func unknownMetadataField(ctx Context, fields map[string]Field) (Field, bool) {
	if ctx.UnknownMetadataField != "" {
		field, ok := fields[ctx.UnknownMetadataField]
		return field, ok && field.Type == metadataType
	}
	for _, field := range fields {
		if field.Unknown && field.Type == metadataType {
			return field, true
		}
	}
	return Field{}, false
}

func setMetadata(field reflect.Value, key string, value string) {
	if field.IsNil() {
		field.Set(reflect.MakeMap(field.Type()))
	}
	field.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(value))
}

func ignored(ctx Context, key string) bool {
	_, ok := ctx.IgnoreFields[key]
	return ok
}

func bindError(code string, operation string, node string, message string, details []string, suggestions []string, cause error) *Error {
	return &Error{
		Code:        code,
		Operation:   operation,
		Node:        node,
		Message:     message,
		Details:     cloneStrings(details),
		Suggestions: cloneStrings(suggestions),
		Cause:       cause,
	}
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

func decodeObject(data []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	value, err := decodeJSONValue(decoder)
	if err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("raw JSON must contain one object")
	}
	obj, ok := value.(map[string]any)
	if !ok || obj == nil {
		return nil, fmt.Errorf("raw JSON must contain an object")
	}
	return obj, nil
}

func decodeJSONValue(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return token, nil
	}
	switch delimiter {
	case '{':
		return decodeJSONObject(decoder)
	case '[':
		return decodeJSONArray(decoder)
	default:
		return nil, fmt.Errorf("unexpected JSON delimiter %q", delimiter)
	}
}

func decodeJSONObject(decoder *json.Decoder) (map[string]any, error) {
	obj := make(map[string]any)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("raw JSON object key must be a string")
		}
		if _, exists := obj[key]; exists {
			return nil, fmt.Errorf("duplicate JSON field %q", key)
		}
		value, err := decodeJSONValue(decoder)
		if err != nil {
			return nil, err
		}
		obj[key] = value
	}
	end, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := end.(json.Delim); !ok || delimiter != '}' {
		return nil, fmt.Errorf("raw JSON object is not closed")
	}
	return obj, nil
}

func decodeJSONArray(decoder *json.Decoder) ([]any, error) {
	var values []any
	for decoder.More() {
		value, err := decodeJSONValue(decoder)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	end, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	if delimiter, ok := end.(json.Delim); !ok || delimiter != ']' {
		return nil, fmt.Errorf("raw JSON array is not closed")
	}
	return values, nil
}

func metadataScalarString(value any) (string, bool) {
	switch typed := value.(type) {
	case nil:
		return "", true
	case string:
		return typed, true
	case json.Number:
		return typed.String(), true
	case bool:
		return strconv.FormatBool(typed), true
	default:
		return "", false
	}
}

// Closest returns the nearest small edit-distance candidate.
func Closest(input string, candidates []string) string {
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

func cloneStrings(values []string) []string {
	return append([]string(nil), values...)
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j-1] > values[j]; j-- {
			values[j-1], values[j] = values[j], values[j-1]
		}
	}
}

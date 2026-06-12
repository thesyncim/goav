package launchctl

import (
	"errors"
	"reflect"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/internal/argbind"
	"github.com/thesyncim/goav/internal/cliargs"
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
	unknown  bool
	positive bool
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
	result, err := argbind.Bind(argbind.Context{
		Name:        ctx.name,
		Operation:   ctx.operation,
		ArgsType:    ctx.argsType,
		Usage:       ctx.usage,
		Suggestions: ctx.suggestions,
	}, args)
	if err != nil {
		return nil, bindStructuredError(err)
	}
	return result.Value, nil
}

// BindJSON fills a known command args struct from a JSON object. It uses the
// same field metadata as BindArgs so JSON and CLI parsing cannot drift.
func BindJSON(spec CommandSpec, data []byte) (any, error) {
	result, err := argbind.BindJSON(argbind.Context{
		Name:        spec.Name,
		Operation:   "control " + spec.Name,
		ArgsType:    spec.ArgsType,
		Usage:       CommandUsage(spec),
		Suggestions: []string{"use `goav ctl help control " + spec.Name + "`"},
	}, data)
	if err != nil {
		return nil, bindStructuredError(err)
	}
	return result.Value, nil
}

func bindStructuredError(err error) error {
	var bindErr *argbind.Error
	if errors.As(err, &bindErr) {
		return commandError(
			bindErr.Code,
			bindErr.Operation,
			bindErr.Node,
			bindErr.Message,
			bindErr.Details,
			bindErr.Suggestions,
			bindErr.Cause,
		)
	}
	return err
}

func commandFields(argsType reflect.Type) map[string]fieldSpec {
	fields := argbind.Fields(argsType)
	out := make(map[string]fieldSpec, len(fields))
	for name, field := range fields {
		out[name] = fieldSpec{
			name:     field.Name,
			required: field.Required,
			parser:   field.Parser,
			usage:    field.Usage,
			help:     field.Help,
			index:    field.Index,
			typ:      field.Type,
			unknown:  field.Unknown,
			positive: field.Positive,
		}
	}
	return out
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

func parseRate(value string) (int, error) {
	return cliargs.ParseRate(value)
}

func closest(input string, candidates []string) string {
	return argbind.Closest(input, candidates)
}

package launchctl

import (
	"context"
	"reflect"
	"strings"

	goav "github.com/thesyncim/goav"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/internal/argbind"
	"github.com/thesyncim/goav/internal/codecargs"
)

// CapabilitySet groups every host-owned extension one control server exposes.
// It is a convenience layer over the existing command and pipeline allowlists.
type CapabilitySet struct {
	Commands []CommandSpec
	Pipeline PipelineRegistry
}

// ValidateCapabilities checks a host-owned capability set against the built-in
// control and branch-pipeline namespaces before a server is started.
func ValidateCapabilities(caps CapabilitySet) error {
	manifest := append(ControlManifest(), caps.Commands...)
	return validateControlRegistry(manifest, caps.Pipeline)
}

// Merge returns a new set containing both capability sets.
func (c CapabilitySet) Merge(next CapabilitySet) CapabilitySet {
	out := CapabilitySet{
		Commands: append([]CommandSpec(nil), c.Commands...),
		Pipeline: PipelineRegistry{
			Steps:    append([]BranchPipelineStepSpec(nil), c.Pipeline.Steps...),
			Encoders: append([]EncoderSpec(nil), c.Pipeline.Encoders...),
		},
	}
	out.Commands = append(out.Commands, next.Commands...)
	out.Pipeline.Steps = append(out.Pipeline.Steps, next.Pipeline.Steps...)
	out.Pipeline.Encoders = append(out.Pipeline.Encoders, next.Pipeline.Encoders...)
	return out
}

type capabilityOptions struct {
	aliases []string
	usage   string
}

// CapabilityOption configures typed helper constructors.
type CapabilityOption func(*capabilityOptions)

// Aliases adds alternate CLI spellings for one command, branch step, or
// encoder spelling. Names and aliases remain per-server allowlisted.
func Aliases(values ...string) CapabilityOption {
	return func(options *capabilityOptions) {
		options.aliases = append(options.aliases, values...)
	}
}

// Usage overrides the generated usage text for a typed branch step or encoder.
func Usage(value string) CapabilityOption {
	return func(options *capabilityOptions) {
		options.usage = value
	}
}

// NewCommand builds a typed, allowlisted control command. The args struct tags
// drive CLI binding, JSON binding, validation, and generated help.
func NewCommand[T any](name string, summary string, apply func(context.Context, goav.Task, T) (ControlResponse, error), options ...CapabilityOption) CommandSpec {
	opts := collectCapabilityOptions(options)
	argsType := typedArgsType[T]()
	spec := CommandSpec{
		Name:     name,
		Aliases:  cloneStrings(opts.aliases),
		Summary:  summary,
		ArgsType: argsType,
	}
	if apply != nil {
		spec.Apply = func(ctx context.Context, task goav.Task, args any) (ControlResponse, error) {
			typed, ok := args.(T)
			if !ok {
				return ControlResponse{}, commandError("invalid_command", "control "+name, "", "bound command args have unexpected type", nil, nil, nil)
			}
			return apply(ctx, task, typed)
		}
	}
	return spec
}

// NewBranchStep builds a typed, allowlisted branch-pipeline step. The host
// receives normal Go values while the CLI still passes key=value settings.
func NewBranchStep[T any](name string, summary string, apply func(*BranchPipeline, T) error, options ...CapabilityOption) BranchPipelineStepSpec {
	opts := collectCapabilityOptions(options)
	argsType := typedArgsType[T]()
	usage := firstNonEmpty(opts.usage, ArgsUsage(argsType))
	spec := BranchPipelineStepSpec{
		Name:     name,
		Aliases:  cloneStrings(opts.aliases),
		Summary:  summary,
		Usage:    usage,
		ArgsType: argsType,
	}
	if apply != nil {
		spec.Apply = func(branch *BranchPipeline, args StepArgs) error {
			bound, err := bindStepArgs(name, argsType, args, usage)
			if err != nil {
				return err
			}
			typed, ok := bound.(T)
			if !ok {
				return commandError("invalid_pipeline_step", "parse branch pipeline", name, "bound step args have unexpected type", nil, nil, nil)
			}
			return apply(branch, typed)
		}
	}
	return spec
}

// NewEncoderSpec builds a typed custom encoder spelling for native adapter
// settings. Prefer generic `encode codec=<id> ...` when pass-through custom
// settings are enough; use this when host code must apply codec.Control or
// richer validation.
func NewEncoderSpec[T any](name string, summary string, apply func(T) (codec.CodecSpec, error), options ...CapabilityOption) EncoderSpec {
	opts := collectCapabilityOptions(options)
	argsType := typedArgsType[T]()
	usage := firstNonEmpty(opts.usage, ArgsUsage(argsType))
	spec := EncoderSpec{
		Name:     name,
		Aliases:  cloneStrings(opts.aliases),
		Summary:  summary,
		Usage:    usage,
		ArgsType: argsType,
	}
	if apply != nil {
		spec.Apply = func(args StepArgs) (codec.CodecSpec, error) {
			bound, err := bindStepArgs(name, argsType, args, usage)
			if err != nil {
				return codec.CodecSpec{}, err
			}
			typed, ok := bound.(T)
			if !ok {
				return codec.CodecSpec{}, commandError("invalid_pipeline_step", "parse branch pipeline", name, "bound encoder args have unexpected type", nil, nil, nil)
			}
			return apply(typed)
		}
	}
	return spec
}

func collectCapabilityOptions(options []CapabilityOption) capabilityOptions {
	var out capabilityOptions
	for _, option := range options {
		if option != nil {
			option(&out)
		}
	}
	return out
}

func typedArgsType[T any]() reflect.Type {
	var zero T
	if typ := reflect.TypeOf(zero); typ != nil {
		return typ
	}
	return reflect.TypeOf((*T)(nil)).Elem()
}

func bindStepArgs(name string, argsType reflect.Type, args StepArgs, usage string) (any, error) {
	return bindKnownStruct(bindContext{
		name:        name,
		operation:   "parse branch pipeline",
		argsType:    argsType,
		usage:       strings.TrimSpace(name + " " + usage),
		suggestions: []string{"run `goav ctl help attach`"},
	}, argbind.ArgsFromMap(args))
}

// ArgsUsage renders the key=value usage fragment for a typed args struct.
func ArgsUsage(argsType reflect.Type) string {
	return argbind.ArgsUsage(argsType)
}

// CapabilityReport is the machine-readable form of server-aware help.
type CapabilityReport struct {
	Controls           []CapabilityEntry   `json:"controls,omitempty"`
	BuiltInBranchSteps []CapabilityEntry   `json:"built_in_branch_steps,omitempty"`
	CustomBranchSteps  []CapabilityEntry   `json:"custom_branch_steps,omitempty"`
	CustomEncoders     []CapabilityEntry   `json:"custom_encoders,omitempty"`
	RuntimeEncoders    []codec.Descriptor  `json:"runtime_encoders,omitempty"`
	RuntimeMuxers      []format.Descriptor `json:"runtime_muxers,omitempty"`
}

// CapabilityEntry describes one callable command or branch-pipeline spelling.
type CapabilityEntry struct {
	Name    string            `json:"name,omitempty"`
	Aliases []string          `json:"aliases,omitempty"`
	Summary string            `json:"summary,omitempty"`
	Usage   string            `json:"usage,omitempty"`
	Fields  []CapabilityField `json:"fields,omitempty"`
}

// CapabilityField describes one reflect-bound key=value field.
type CapabilityField struct {
	Name     string `json:"name,omitempty"`
	Required bool   `json:"required,omitempty"`
	Type     string `json:"type,omitempty"`
	Usage    string `json:"usage,omitempty"`
	Help     string `json:"help,omitempty"`
}

func capabilityReport(manifest []CommandSpec, registry PipelineRegistry, task goav.Task) (CapabilityReport, error) {
	if err := validateControlRegistry(manifest, registry); err != nil {
		return CapabilityReport{}, err
	}
	report := CapabilityReport{
		Controls:           commandCapabilityEntries(manifest),
		BuiltInBranchSteps: builtinBranchCapabilityEntries(),
		CustomBranchSteps:  branchStepCapabilityEntries(registry.Steps),
		CustomEncoders:     encoderCapabilityEntries(registry.Encoders),
	}
	caps := runtimeCapabilities(task)
	report.RuntimeEncoders = caps.encoders
	report.RuntimeMuxers = caps.muxers
	return report, nil
}

func commandCapabilityEntries(manifest []CommandSpec) []CapabilityEntry {
	out := make([]CapabilityEntry, 0, len(manifest))
	for _, spec := range manifest {
		out = append(out, CapabilityEntry{
			Name:    spec.Name,
			Aliases: cloneStrings(spec.Aliases),
			Summary: spec.Summary,
			Usage:   CommandUsage(spec),
			Fields:  fieldCapabilities(spec.ArgsType),
		})
	}
	return out
}

func builtinBranchCapabilityEntries() []CapabilityEntry {
	rows := builtinPipelineHelpRows()
	out := make([]CapabilityEntry, 0, len(rows))
	for _, row := range rows {
		entry := CapabilityEntry{Name: row.name, Summary: row.summary, Usage: row.usage}
		if row.name == "encode" {
			entry.Fields = append([]CapabilityField{
				{Name: "codec", Required: true, Type: "codec-id", Usage: "codec=<id>", Help: "codec id registered on the task runtime"},
				{Name: "media", Required: true, Type: "media-type", Usage: "media=<audio|video|subtitle>", Help: "media kind accepted by the encoder"},
			}, fieldCapabilitiesFromFields(codecargs.SettingsFields())...)
		}
		out = append(out, entry)
	}
	return out
}

func branchStepCapabilityEntries(steps []BranchPipelineStepSpec) []CapabilityEntry {
	out := make([]CapabilityEntry, 0, len(steps))
	for _, step := range steps {
		out = append(out, CapabilityEntry{
			Name:    step.Name,
			Aliases: cloneStrings(step.Aliases),
			Summary: step.Summary,
			Usage:   firstNonEmpty(step.Usage, ArgsUsage(step.ArgsType)),
			Fields:  fieldCapabilities(step.ArgsType),
		})
	}
	return out
}

func encoderCapabilityEntries(encoders []EncoderSpec) []CapabilityEntry {
	out := make([]CapabilityEntry, 0, len(encoders))
	for _, encoder := range encoders {
		out = append(out, CapabilityEntry{
			Name:    encoder.Name,
			Aliases: cloneStrings(encoder.Aliases),
			Summary: encoder.Summary,
			Usage:   firstNonEmpty(encoder.Usage, ArgsUsage(encoder.ArgsType)),
			Fields:  fieldCapabilities(encoder.ArgsType),
		})
	}
	return out
}

func fieldCapabilities(argsType reflect.Type) []CapabilityField {
	if argsType == nil || argsType.Kind() != reflect.Struct {
		return nil
	}
	fields := orderedFields(commandFields(argsType))
	out := make([]CapabilityField, 0, len(fields))
	for _, field := range fields {
		out = append(out, CapabilityField{
			Name:     field.name,
			Required: field.required,
			Type:     fieldTypeLabel(field),
			Usage:    field.usage,
			Help:     field.help,
		})
	}
	return out
}

func fieldCapabilitiesFromFields(fields []argbind.Field) []CapabilityField {
	out := make([]CapabilityField, 0, len(fields))
	for _, field := range fields {
		if field.Unknown {
			out = append(out, CapabilityField{
				Name:     field.Name,
				Required: field.Required,
				Type:     "metadata",
				Usage:    field.Usage,
				Help:     field.Help,
			})
			continue
		}
		out = append(out, CapabilityField{
			Name:     field.Name,
			Required: field.Required,
			Type:     fieldTypeLabelFromArgbind(field),
			Usage:    field.Usage,
			Help:     field.Help,
		})
	}
	return out
}

func fieldTypeLabel(field fieldSpec) string {
	if field.parser != "" {
		return field.parser
	}
	if field.typ == streamIDType {
		return "stream-id"
	}
	if field.typ == eventTypeType {
		return "event-type"
	}
	if field.typ == durationType {
		return "duration"
	}
	if field.typ == metadataType {
		return "metadata"
	}
	switch field.typ.Kind() {
	case reflect.Bool:
		return "bool"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "int"
	case reflect.Float32, reflect.Float64:
		return "float"
	case reflect.String:
		return "string"
	default:
		return field.typ.String()
	}
}

func fieldTypeLabelFromArgbind(field argbind.Field) string {
	if field.Parser != "" {
		return field.Parser
	}
	converted := fieldSpec{
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
	return fieldTypeLabel(converted)
}

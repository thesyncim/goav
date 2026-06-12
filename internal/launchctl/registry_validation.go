package launchctl

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

type registryNameOwner struct {
	kind string
	name string
}

func validateControlRegistry(manifest []CommandSpec, registry PipelineRegistry) error {
	if err := validateCommandManifest(manifest); err != nil {
		return err
	}
	return validatePipelineRegistry(registry)
}

func validateCommandManifest(manifest []CommandSpec) error {
	owners := make(map[string]registryNameOwner)
	for _, spec := range manifest {
		if err := addRegistryName(owners, "control command", spec.Name, spec.Name, "configure control server"); err != nil {
			return err
		}
		if err := validateArgsType("control command", spec.Name, spec.ArgsType, true, "configure control server"); err != nil {
			return err
		}
		for _, alias := range spec.Aliases {
			if err := addRegistryName(owners, "control command alias", alias, spec.Name, "configure control server"); err != nil {
				return err
			}
		}
	}
	return nil
}

func validatePipelineRegistry(registry PipelineRegistry) error {
	owners := make(map[string]registryNameOwner)
	for _, name := range builtinPipelineNames() {
		owners[name] = registryNameOwner{kind: "built-in branch-pipeline step", name: name}
	}
	for _, spec := range registry.Steps {
		if err := addRegistryName(owners, "custom branch-pipeline step", spec.Name, spec.Name, "configure branch pipeline"); err != nil {
			return err
		}
		if err := validateArgsType("custom branch-pipeline step", spec.Name, spec.ArgsType, false, "configure branch pipeline"); err != nil {
			return err
		}
		for _, alias := range spec.Aliases {
			if err := addRegistryName(owners, "custom branch-pipeline step alias", alias, spec.Name, "configure branch pipeline"); err != nil {
				return err
			}
		}
	}
	for _, spec := range registry.Encoders {
		if err := addRegistryName(owners, "custom encoder", spec.Name, spec.Name, "configure branch pipeline"); err != nil {
			return err
		}
		if err := validateArgsType("custom encoder", spec.Name, spec.ArgsType, false, "configure branch pipeline"); err != nil {
			return err
		}
		for _, alias := range spec.Aliases {
			if err := addRegistryName(owners, "custom encoder alias", alias, spec.Name, "configure branch pipeline"); err != nil {
				return err
			}
		}
	}
	return nil
}

func addRegistryName(owners map[string]registryNameOwner, kind string, value string, specName string, operation string) error {
	name := normalizeRegistryName(value)
	if name == "" {
		return invalidRegistryShape(operation, registryNameNode(kind), fmt.Sprintf("%s needs a non-empty name", kind), kind, specName)
	}
	owner := registryNameOwner{kind: kind, name: firstNonEmpty(specName, value)}
	if previous, ok := owners[name]; ok {
		return commandError(
			"invalid_registry",
			operation,
			name,
			fmt.Sprintf("%s %q conflicts with %s %q", kind, name, previous.kind, previous.name),
			[]string{
				"first=" + previous.kind + ":" + previous.name,
				"second=" + owner.kind + ":" + owner.name,
			},
			[]string{
				"choose unique custom command, branch step, encoder, and alias names",
				"avoid reserved branch-pipeline names: " + strings.Join(builtinPipelineNames(), ","),
			},
			nil,
		)
	}
	owners[name] = owner
	return nil
}

func validateArgsType(kind string, name string, argsType reflect.Type, required bool, operation string) error {
	if argsType == nil {
		if !required {
			return nil
		}
		return invalidRegistryShape(operation, firstNonEmpty(name, "args"), fmt.Sprintf("%s %q needs a struct ArgsType", kind, name), kind, name)
	}
	if argsType.Kind() != reflect.Struct {
		return invalidRegistryShape(operation, firstNonEmpty(name, "args"), fmt.Sprintf("%s %q ArgsType must be a struct, got %s", kind, name, argsType), kind, name)
	}
	return nil
}

func invalidRegistryShape(operation string, node string, message string, kind string, owner string) error {
	details := []string{"kind=" + kind}
	if owner != "" {
		details = append(details, "owner="+owner)
	}
	return commandError(
		"invalid_registry",
		operation,
		node,
		message,
		details,
		[]string{"choose non-empty names and struct settings types for custom control capabilities"},
		nil,
	)
}

func registryNameNode(kind string) string {
	if strings.Contains(kind, "alias") {
		return "alias"
	}
	return "name"
}

func normalizeRegistryName(value string) string {
	return strings.ToLower(strings.TrimSpace(value))
}

func builtinPipelineNames() []string {
	names := []string{
		"copy",
		"decode",
		"resize",
		"resample",
		"encode",
		"encoder",
		"vp8enc",
		"vp8",
		"vp9enc",
		"vp9",
		"h264enc",
		"h264",
		"av1enc",
		"av1",
		"opusenc",
		"opus",
		"filesink",
	}
	sort.Strings(names)
	return names
}

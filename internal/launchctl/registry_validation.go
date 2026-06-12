package launchctl

import (
	"fmt"
	"sort"
	"strings"
)

type registryNameOwner struct {
	kind string
	name string
}

func validateCommandManifest(manifest []CommandSpec) error {
	owners := make(map[string]registryNameOwner)
	for _, spec := range manifest {
		if err := addRegistryName(owners, "control command", spec.Name, spec.Name, "configure control server"); err != nil {
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
		return nil
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
		"file",
	}
	sort.Strings(names)
	return names
}

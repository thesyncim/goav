package launchctl

import (
	"context"
	"reflect"
	"sort"

	goav "github.com/thesyncim/goav"
)

// ControlResponse is the structured result returned by one control-plane
// command. Result deliberately stays cold-path and JSON-friendly.
type ControlResponse struct {
	Operation string `json:"operation,omitempty"`
	Result    any    `json:"result,omitempty"`
}

// CommandSpec is the explicit allowlist entry for one callable control verb.
// ArgsType must be a struct type; reflection is used only to bind CLI/JSON
// fields into that known struct before Apply lowers it into goav APIs.
type CommandSpec struct {
	Name     string
	Aliases  []string
	Summary  string
	ArgsType reflect.Type
	Apply    func(context.Context, goav.LiveTask, any) (ControlResponse, error)
}

// ControlManifest is the complete built-in allowlist for goav ctl control
// verbs. There is no init-time registration or global mutable registry.
func ControlManifest() []CommandSpec {
	return []CommandSpec{
		{
			Name:     "keyframe",
			Summary:  "request a keyframe for a stream",
			ArgsType: reflect.TypeOf(KeyframeCommand{}),
			Apply:    applyKeyframe,
		},
		{
			Name:     "bitrate",
			Summary:  "retarget live encoders to a bitrate",
			ArgsType: reflect.TypeOf(BitrateCommand{}),
			Apply:    applyBitrate,
		},
		{
			Name:     "seek",
			Summary:  "seek task sources to a media position",
			ArgsType: reflect.TypeOf(SeekCommand{}),
			Apply:    applySeek,
		},
		{
			Name:     "rate",
			Summary:  "change source playback rate",
			ArgsType: reflect.TypeOf(RateCommand{}),
			Apply:    applyRate,
		},
		{
			Name:     "segment",
			Summary:  "play one source time window",
			ArgsType: reflect.TypeOf(SegmentCommand{}),
			Apply:    applySegment,
		},
		{
			Name:     "select",
			Summary:  "switch a live select arm",
			ArgsType: reflect.TypeOf(SelectCommand{}),
			Apply:    applySelect,
		},
		{
			Name:     "deliver",
			Summary:  "deliver an allowlisted event control at a tap",
			ArgsType: reflect.TypeOf(DeliverCommand{}),
			Apply:    applyDeliver,
		},
	}
}

// LookupControlCommand finds a manifest command by name or alias.
func LookupControlCommand(name string) (CommandSpec, bool) {
	return LookupCommand(ControlManifest(), name)
}

// LookupCommand finds a command in an explicit manifest by name or alias.
func LookupCommand(manifest []CommandSpec, name string) (CommandSpec, bool) {
	for _, spec := range manifest {
		if spec.Name == name {
			return spec, true
		}
		for _, alias := range spec.Aliases {
			if alias == name {
				return spec, true
			}
		}
	}
	return CommandSpec{}, false
}

func controlCommandNames() []string {
	return commandNames(ControlManifest())
}

func commandNames(manifest []CommandSpec) []string {
	names := make([]string, 0, len(manifest))
	for _, spec := range manifest {
		names = append(names, spec.Name)
	}
	sort.Strings(names)
	return names
}

package compile

import (
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/builderr"
	"github.com/thesyncim/goav/pipeline"
)

type EncodeRequest struct {
	Name     string
	Selector av.StreamSelector
	Config   codec.EncodeConfig
}

type DecodeRequest struct {
	Selector av.StreamSelector
	Config   codec.Spec
}

type MediaTransform struct {
	Name    string
	Factory string
	Stage   pipeline.Stage
	Video   *filter.ResizeConfig
	Audio   *filter.ResampleConfig
}

func branchComposeSelectorKey(selector av.StreamSelector) string {
	return strings.Join([]string{
		string(selector.ID),
		strconv.FormatInt(int64(selector.Index), 10),
		strconv.FormatBool(selector.UseIndex),
		string(selector.Type),
		string(selector.Codec),
		selector.Name,
	}, "\x00")
}

// branchComposeSelectorGroupKey keys input nodes by selector AND input
// narrowing so two same-selector chains narrowed to different inputs get their
// own select/decode nodes.
func branchComposeSelectorGroupKey(selector av.StreamSelector, input string) string {
	return branchComposeSelectorKey(selector) + "\x00@" + input
}

// branchComposeInputNodeName suffixes a select/decode node name with the input
// narrowing so same-selector chains on different inputs stay distinct nodes.
func branchComposeInputNodeName(name string, input string) string {
	if input == "" {
		return name
	}
	return name + "@" + input
}

func codecSpecHasDecodeIntent(spec codec.Spec) bool {
	return spec.ID != "" ||
		spec.Type != "" ||
		CodecSpecHasParameters(spec) ||
		spec.Settings.Bitrate != 0 ||
		spec.Settings.Framerate != (av.Duration{}) ||
		spec.Settings.KeyframeInterval != 0 ||
		spec.Settings.Profile != "" ||
		spec.Settings.Level != "" ||
		spec.Settings.Control != nil
}

func branchComposeDecodeConfigConflictError(first string, second string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.DecodeConfigConflictCode),
		Code:      builderr.DecodeConfigConflictCode,
		Operation: "build recipe",
		Node:      second,
		Reason:    "branches that share one decoder declared different decode configs",
		Details:   builderr.Details(builderr.Note("first branch: "+first), builderr.Note("conflicting branch: "+second)),
		Fixes: builderr.Fixes([]string{
			"move shared decode config to the stream chain with .Decode(...)",
			"use the same decode config for branches that share a decoder",
		}),
	}
}

func branchChainStepError(name string, reason string) error {
	return &builderr.Refusal{
		Phase:     builderr.PhaseBuild,
		Family:    errcode.FamilyForCode(builderr.BranchOperationChainUnsupportedCode),
		Code:      builderr.BranchOperationChainUnsupportedCode,
		Operation: "build branch composition",
		Node:      name,
		Reason:    reason,
		Fixes: builderr.Fixes([]string{
			"use one operation per branch call",
			"use resize on video branches and resample on audio branches",
			"use v.Do(stage).Resize(...).Encode(codec.VP9(...)).To(output) for recipe stream operations",
		}),
		Cause: builderr.ErrUnsupportedBuild,
	}
}

package goav

import (
	"context"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/pipeline"
)

type mediaTransform struct {
	name    string
	factory string
	stage   pipeline.Stage
	video   *filter.ResizeConfig
	audio   *filter.ResampleConfig
}

func (b *builder) newBranchComposeStepStageNamed(ctx context.Context, name string, transform mediaTransform, stream av.Stream, realtime bool) (pipeline.Stage, av.Stream, error) {
	if transform.stage != nil {
		// Playout gates pace on the task timeline the runtime clone carries
		// as its clock; binding here keeps the gate internal to the lowering.
		// The QoS reporter rides the same clone so late admits reach Watch.
		bindGateDeps(transform.stage, b.taskClock(), b.runtime.qosReportFunc())
		if name != "" && name != transform.stage.Name() {
			return namedStage{name: name, stage: transform.stage}, stream, nil
		}
		return transform.stage, stream, nil
	}
	return b.newMediaTransformStageNamed(ctx, name, transform, stream, realtime)
}

func (b *builder) newMediaTransformStageNamed(ctx context.Context, name string, transform mediaTransform, stream av.Stream, realtime bool) (*filter.Stage, av.Stream, error) {
	outputStream, err := applyMediaTransformToStream(stream, transform)
	if err != nil {
		return nil, av.Stream{}, err
	}
	factory, err := b.runtime.filters.Factory(transform.factory)
	if err != nil {
		return nil, av.Stream{}, err
	}
	config := filter.Config{
		Stream:   stream,
		Realtime: realtime,
		Video:    transform.video,
		Audio:    transform.audio,
	}
	frameFilter, err := factory.NewFilter(ctx, config)
	if err != nil {
		return nil, av.Stream{}, err
	}
	name = firstNonEmpty(name, transform.name)
	stage, err := filter.NewStage(filter.StageConfig{
		Name:   name,
		Detail: mediaTransformDetail(transform),
		Filter: frameFilter,
		Result: filterResultForStream(outputStream),
	})
	if err != nil {
		frameFilter.Close()
		return nil, av.Stream{}, err
	}
	return stage, outputStream, nil
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
		codecSpecHasParameters(spec) ||
		spec.Settings.Bitrate != 0 ||
		spec.Settings.Framerate != (av.Duration{}) ||
		spec.Settings.KeyframeInterval != 0 ||
		spec.Settings.Profile != "" ||
		spec.Settings.Level != "" ||
		spec.Settings.Control != nil
}

func branchComposeDecodeConfigConflictError(first string, second string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(decodeConfigConflictCode),
		Code:      decodeConfigConflictCode,
		Operation: "build recipe",
		Node:      second,
		Reason:    "branches that share one decoder declared different decode configs",
		fields:    errDetails(errNote("first branch: "+first), errNote("conflicting branch: "+second)),
		fixes: buildErrorFixes([]string{
			"move shared decode config to the stream chain with .Decode(...)",
			"use the same decode config for branches that share a decoder",
		}),
	}
}

func branchChainStepError(name string, reason string) error {
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(branchOperationChainUnsupportedCode),
		Code:      branchOperationChainUnsupportedCode,
		Operation: "build branch composition",
		Node:      name,
		Reason:    reason,
		fixes: buildErrorFixes([]string{
			"use one operation per branch call",
			"use resize on video branches and resample on audio branches",
			"use v.Do(stage).Resize(...).Encode(codec.VP9(...)).To(output) for recipe stream operations",
		}),
		cause: errUnsupportedBuild,
	}
}

func applyMediaTransformToStream(stream av.Stream, transform mediaTransform) (av.Stream, error) {
	out := stream
	switch {
	case transform.audio != nil:
		if stream.Type != av.MediaAudio && stream.Codec.Type != av.MediaAudio {
			return av.Stream{}, mediaTransformMismatchError(transform, stream, "resample", "audio")
		}
		out.Type = av.MediaAudio
		out.Codec.Type = av.MediaAudio
		if transform.audio.SampleRate != 0 {
			out.Codec.SampleRate = transform.audio.SampleRate
			out.Codec.ClockRate = uint32(transform.audio.SampleRate)
			out.TimeBase = av.TimeBase{Num: 1, Den: int64(transform.audio.SampleRate)}
		}
		if transform.audio.Channels != 0 {
			out.Codec.Channels = transform.audio.Channels
		}
		if transform.audio.ChannelLayout != "" {
			out.Codec.ChannelLayout = transform.audio.ChannelLayout
		}
		if transform.audio.SampleFormat != "" {
			out.Codec.SampleFormat = transform.audio.SampleFormat
		}
	case transform.video != nil:
		if stream.Type != av.MediaVideo && stream.Codec.Type != av.MediaVideo {
			return av.Stream{}, mediaTransformMismatchError(transform, stream, "resize", "video")
		}
		out.Type = av.MediaVideo
		out.Codec.Type = av.MediaVideo
		if err := applyResizeConfigToStream(&out, *transform.video); err != nil {
			return av.Stream{}, err
		}
		if transform.video.PixelFormat != "" {
			out.Codec.PixelFormat = transform.video.PixelFormat
		}
	}
	return out, nil
}

func mediaTransformMismatchError(transform mediaTransform, stream av.Stream, operation string, media string) error {
	details := []string{
		"stream id: " + string(stream.ID),
		"stream type: " + string(stream.Type),
		"codec type: " + string(stream.Codec.Type),
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(branchTransformMediaMismatchCode),
		Code:      branchTransformMediaMismatchCode,
		Operation: "build branch composition",
		Node:      transform.name,
		Reason:    operation + " applies to " + media + " streams",
		fields:    errDetailLines(details),
		fixes: buildErrorFixes([]string{
			"use resize on video branches",
			"use resample on audio branches",
			"check the branch selector",
		}),
		cause: errUnsupportedBuild,
	}
}

func filterResultForStream(stream av.Stream) filter.Result {
	frame := av.Frame{}
	if stream.Type == av.MediaAudio || stream.Codec.Type == av.MediaAudio {
		frame.Planes = []av.Plane{{Buffer: av.Buffer{Bytes: make([]byte, 0, audioDecodeBufferSize(stream))}}}
	}
	if stream.Type == av.MediaVideo || stream.Codec.Type == av.MediaVideo {
		// Plane slots only: video filters publish new pooled planes.
		frame = av.Frame{Planes: make([]av.Plane, 3)}
	}
	return filter.Result{
		Frames: []av.Frame{frame}[:0],
		Events: make([]av.Event, 0, 1),
	}
}

func applyResizeConfigToStream(stream *av.Stream, config filter.ResizeConfig) error {
	mode := config.Mode
	if mode == "" {
		mode = filter.ResizeExact
	}
	switch mode {
	case filter.ResizePassthrough:
		return nil
	case filter.ResizeExact:
		if config.Width != 0 {
			stream.Codec.Width = config.Width
		}
		if config.Height != 0 {
			stream.Codec.Height = config.Height
		}
		return nil
	case filter.ResizeFit:
		if config.Width <= 0 || config.Height <= 0 {
			return transcodeResizeConfigError(*stream, mode, config, "resize fit requires positive target width and height")
		}
		if stream.Codec.Width <= 0 || stream.Codec.Height <= 0 {
			return transcodeResizeConfigError(*stream, mode, config, "resize fit requires known positive input width and height")
		}
		stream.Codec.Width, stream.Codec.Height = resizeFitStreamDimensions(stream.Codec.Width, stream.Codec.Height, config.Width, config.Height)
		if stream.Codec.Width == 0 || stream.Codec.Height == 0 {
			return transcodeResizeConfigError(*stream, mode, config, "resize fit produced empty output geometry")
		}
		return nil
	case filter.ResizeFill:
		if config.Width <= 0 || config.Height <= 0 {
			return transcodeResizeConfigError(*stream, mode, config, "resize fill requires positive target width and height")
		}
		stream.Codec.Width = config.Width
		stream.Codec.Height = config.Height
		return nil
	default:
		return transcodeResizeConfigError(*stream, mode, config, "unsupported resize mode")
	}
}

func transcodeResizeConfigError(stream av.Stream, mode filter.ResizeMode, config filter.ResizeConfig, reason string) error {
	node := "resize"
	if stream.ID != "" {
		node += "-" + string(stream.ID)
	}
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(transcodeResizeInvalidCode),
		Code:      transcodeResizeInvalidCode,
		Operation: "build transcode",
		Node:      node,
		Reason:    reason,
		fields:    errDetails(errNote("mode: "+string(mode)), errNote("stream id: "+string(stream.ID)), errNote("input width: "+strconv.Itoa(stream.Codec.Width)), errNote("input height: "+strconv.Itoa(stream.Codec.Height)), errNote("target width: "+strconv.Itoa(config.Width)), errNote("target height: "+strconv.Itoa(config.Height))),
		fixes: buildErrorFixes([]string{
			"use resize mode exact, fit, fill, or passthrough",
			"provide positive target dimensions for fit and fill",
			"use exact resize when input dimensions are not known before filtering",
		}),
		cause: errUnsupportedBuild,
	}
}

func resizeFitStreamDimensions(inputWidth int, inputHeight int, targetWidth int, targetHeight int) (int, int) {
	if targetWidth*inputHeight <= targetHeight*inputWidth {
		return evenStreamDimension(targetWidth), evenStreamDimension((inputHeight*targetWidth + inputWidth/2) / inputWidth)
	}
	return evenStreamDimension((inputWidth*targetHeight + inputHeight/2) / inputHeight), evenStreamDimension(targetHeight)
}

func evenStreamDimension(value int) int {
	if value < 2 {
		return 0
	}
	return value &^ 1
}

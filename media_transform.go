package goav

import (
	"context"
	"strconv"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/internal/compile"
	"github.com/thesyncim/goav/pipeline"
)

func (b *builder) newBranchComposeStepStageNamed(ctx context.Context, name string, transform compile.MediaTransform, stream av.Stream, realtime bool) (pipeline.Stage, av.Stream, error) {
	if transform.Stage != nil {
		// Playout gates pace on the task timeline the runtime clone carries
		// as its clock; binding here keeps the gate internal to the lowering.
		// The QoS reporter rides the same clone so late admits reach Watch.
		bindGateDeps(transform.Stage, b.taskClock(), b.runtime.qosReportFunc())
		if name != "" && name != transform.Stage.Name() {
			return namedStage{name: name, stage: transform.Stage}, stream, nil
		}
		return transform.Stage, stream, nil
	}
	return b.newMediaTransformStageNamed(ctx, name, transform, stream, realtime)
}

func (b *builder) newMediaTransformStageNamed(ctx context.Context, name string, transform compile.MediaTransform, stream av.Stream, realtime bool) (*filter.Stage, av.Stream, error) {
	outputStream, err := applyMediaTransformToStream(stream, transform)
	if err != nil {
		return nil, av.Stream{}, err
	}
	factory, err := b.runtime.filters.Factory(transform.Factory)
	if err != nil {
		return nil, av.Stream{}, err
	}
	config := filter.Config{
		Stream:   stream,
		Realtime: realtime,
		Video:    transform.Video,
		Audio:    transform.Audio,
	}
	frameFilter, err := factory.NewFilter(ctx, config)
	if err != nil {
		return nil, av.Stream{}, err
	}
	name = firstNonEmpty(name, transform.Name)
	stage, err := filter.NewStage(filter.StageConfig{
		Name:   name,
		Detail: compile.MediaTransformDetail(transform),
		Filter: frameFilter,
		Result: filterResultForStream(outputStream),
	})
	if err != nil {
		frameFilter.Close()
		return nil, av.Stream{}, err
	}
	return stage, outputStream, nil
}

func applyMediaTransformToStream(stream av.Stream, transform compile.MediaTransform) (av.Stream, error) {
	out := stream
	switch {
	case transform.Audio != nil:
		if stream.Type != av.MediaAudio && stream.Codec.Type != av.MediaAudio {
			return av.Stream{}, mediaTransformMismatchError(transform, stream, "resample", "audio")
		}
		out.Type = av.MediaAudio
		out.Codec.Type = av.MediaAudio
		if transform.Audio.SampleRate != 0 {
			out.Codec.SampleRate = transform.Audio.SampleRate
			out.Codec.ClockRate = uint32(transform.Audio.SampleRate)
			out.TimeBase = av.TimeBase{Num: 1, Den: int64(transform.Audio.SampleRate)}
		}
		if transform.Audio.Channels != 0 {
			out.Codec.Channels = transform.Audio.Channels
		}
		if transform.Audio.ChannelLayout != "" {
			out.Codec.ChannelLayout = transform.Audio.ChannelLayout
		}
		if transform.Audio.SampleFormat != "" {
			out.Codec.SampleFormat = transform.Audio.SampleFormat
		}
	case transform.Video != nil:
		if stream.Type != av.MediaVideo && stream.Codec.Type != av.MediaVideo {
			return av.Stream{}, mediaTransformMismatchError(transform, stream, "resize", "video")
		}
		out.Type = av.MediaVideo
		out.Codec.Type = av.MediaVideo
		if err := applyResizeConfigToStream(&out, *transform.Video); err != nil {
			return av.Stream{}, err
		}
		if transform.Video.PixelFormat != "" {
			out.Codec.PixelFormat = transform.Video.PixelFormat
		}
	}
	return out, nil
}

func mediaTransformMismatchError(transform compile.MediaTransform, stream av.Stream, operation string, media string) error {
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
		Node:      transform.Name,
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

package goav

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/errcode"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/shape"
)

const realtimeRecipeBufferCapacity = 32

func realtimeRecipeBufferPolicy(runtimeBuffer pipeline.BufferPolicy) pipeline.BufferPolicy {
	if !runtimeBuffer.IsDirect() {
		return runtimeBuffer
	}
	return pipeline.BufferPolicy{
		Capacity: realtimeRecipeBufferCapacity,
		Drop:     pipeline.DropBlock,
	}
}

func recipeGraphConfig(rt *runtime, name string, work workPlan, bufferedWhenRealtime bool) (pipeline.GraphConfig, error) {
	if rt == nil {
		return pipeline.GraphConfig{}, recipeGraphUnsupportedError("configure graph", intent{})
	}
	buffer := rt.buffer
	if bufferedWhenRealtime && rt.realtime && buffer.IsDirect() {
		buffer = realtimeRecipeBufferPolicy(buffer)
	}
	if !buffer.IsDirect() {
		var err error
		buffer, err = bufferPolicyWithShapeBudgets(buffer, work)
		if err != nil {
			return pipeline.GraphConfig{}, err
		}
	}
	return pipeline.GraphConfig{
		Name:             firstNonEmpty(name, work.Name, "goav"),
		Realtime:         rt.realtime,
		Buffer:           buffer,
		EventCapacity:    rt.eventCapacity,
		CloseWaitTimeout: rt.closeWaitTimeout,
	}, nil
}

func bufferPolicyWithShapeBudgets(policy pipeline.BufferPolicy, work workPlan) (pipeline.BufferPolicy, error) {
	return bufferPolicyWithShapeBudgetsForOperations(policy, work.Operations)
}

func bufferPolicyWithShapeBudgetsForOperations(policy pipeline.BufferPolicy, operations []workOperation) (pipeline.BufferPolicy, error) {
	needPackets := policy.CopyPacketBytes <= 0
	needFrames := policy.CopyFrameBytes <= 0
	if !needPackets && !needFrames {
		return policy, nil
	}
	budget, err := copyBudgetForOperations(operations, needPackets, needFrames)
	if err != nil {
		return pipeline.BufferPolicy{}, err
	}
	if needPackets {
		policy.CopyPacketBytes = budget.packetBytes
	}
	if needFrames {
		policy.CopyFrameBytes = budget.frameBytes
	}
	return policy, nil
}

func bufferPolicyWithShapeBudgetsForSteps(policy pipeline.BufferPolicy, steps []attachStep) (pipeline.BufferPolicy, error) {
	operations := make([]workOperation, 0, len(steps))
	for i := range steps {
		operations = append(operations, steps[i].operation)
	}
	return bufferPolicyWithShapeBudgetsForOperations(policy, operations)
}

func mergeBufferCopyBounds(base pipeline.BufferPolicy, next pipeline.BufferPolicy) pipeline.BufferPolicy {
	if base.IsDirect() && !next.IsDirect() {
		base = next
	}
	if next.CopyPacketBytes > base.CopyPacketBytes {
		base.CopyPacketBytes = next.CopyPacketBytes
	}
	if next.CopyFrameBytes > base.CopyFrameBytes {
		base.CopyFrameBytes = next.CopyFrameBytes
	}
	if next.CopyAlways {
		base.CopyAlways = true
	}
	return base
}

type copyBudget struct {
	packetBytes int
	frameBytes  int
}

func copyBudgetForOperations(operations []workOperation, needPackets bool, needFrames bool) (copyBudget, error) {
	var budget copyBudget
	for i := range operations {
		operation := operations[i]
		if operation.Node == "" || operation.Kind == plan.OpShape || operation.Kind == plan.OpTap {
			continue
		}
		next, err := copyBudgetForNodeInput(operation, needPackets, needFrames)
		if err != nil {
			return copyBudget{}, err
		}
		if next.packetBytes > budget.packetBytes {
			budget.packetBytes = next.packetBytes
		}
		if next.frameBytes > budget.frameBytes {
			budget.frameBytes = next.frameBytes
		}
	}
	return budget, nil
}

func workOperationForNodeKind(operations []workOperation, node pipeline.NodeRef, kind plan.OperationKind) (workOperation, bool) {
	for i := range operations {
		if operations[i].Node == node && operations[i].Kind == kind {
			return operations[i], true
		}
	}
	return workOperation{}, false
}

func copyBudgetForNodeInput(operation workOperation, needPackets bool, needFrames bool) (copyBudget, error) {
	spec := normalizeTapShape(operation.ShapeIn)
	switch spec.Domain {
	case shape.DomainPacket:
		if !needPackets {
			return copyBudget{}, nil
		}
		return copyBudget{packetBytes: packetCopyBudget(spec)}, nil
	case shape.DomainFrame:
		if !needFrames {
			return copyBudget{}, nil
		}
		frameBytes, err := frameCopyBudget(spec)
		if err != nil {
			return copyBudget{}, bufferBudgetMissingError(operation, err)
		}
		return copyBudget{frameBytes: frameBytes}, nil
	default:
		return copyBudget{}, nil
	}
}

func packetCopyBudget(spec shape.Spec) int {
	media := firstNonEmptyMedia(spec.MediaKind, codecMedia(spec.Codec))
	stream := av.Stream{
		Type: media,
		Codec: av.CodecParameters{
			ID:           spec.Codec,
			Type:         media,
			Width:        spec.Width,
			Height:       spec.Height,
			SampleRate:   spec.SampleRate,
			Channels:     spec.Channels,
			PixelFormat:  spec.PixelFormat,
			SampleFormat: spec.SampleFormat,
		},
	}
	return encodePacketBufferSize(stream)
}

func frameCopyBudget(spec shape.Spec) (int, error) {
	media := firstNonEmptyMedia(spec.MediaKind, codecMedia(spec.Codec))
	switch media {
	case av.MediaVideo:
		return videoFrameCopyBudget(spec)
	case av.MediaAudio:
		return audioFrameCopyBudget(spec)
	default:
		return 0, nil
	}
}

func videoFrameCopyBudget(spec shape.Spec) (int, error) {
	if spec.Width <= 0 {
		return 0, fmt.Errorf("missing width")
	}
	if spec.Height <= 0 {
		return 0, fmt.Errorf("missing height")
	}
	if spec.PixelFormat == "" {
		return 0, fmt.Errorf("missing pixel_format")
	}
	width := alignedVideoCopyDimension(spec.Width)
	height := alignedVideoCopyDimension(spec.Height)
	switch spec.PixelFormat {
	case av.PixelFormatI420, av.PixelFormatYUV420P:
		chromaWidth := alignedVideoCopyDimension((spec.Width + 1) / 2)
		chromaHeight := alignedVideoCopyDimension((spec.Height + 1) / 2)
		return width*height + 2*chromaWidth*chromaHeight, nil
	case av.PixelFormatGray8:
		return width * height, nil
	case av.PixelFormatI422, av.PixelFormatYUV422P:
		chromaWidth := alignedVideoCopyDimension((spec.Width + 1) / 2)
		return width*height + 2*chromaWidth*height, nil
	case av.PixelFormatI444, av.PixelFormatYUV444P:
		return width * height * 3, nil
	default:
		return 0, fmt.Errorf("unsupported pixel_format %q", spec.PixelFormat)
	}
}

func alignedVideoCopyDimension(value int) int {
	const alignment = 64
	if value <= 0 {
		return 0
	}
	return ((value + alignment - 1) / alignment) * alignment
}

func audioFrameCopyBudget(spec shape.Spec) (int, error) {
	sampleRate := spec.SampleRate
	if sampleRate <= 0 {
		return 0, fmt.Errorf("missing sample_rate")
	}
	channels := spec.Channels
	if channels <= 0 {
		return 0, fmt.Errorf("missing channels")
	}
	if spec.SampleFormat == "" {
		return 0, fmt.Errorf("missing sample_format")
	}
	bytesPerSample, ok := audioSampleBytes(spec.SampleFormat)
	if !ok {
		return 0, fmt.Errorf("unsupported sample_format %q", spec.SampleFormat)
	}
	samples := sampleRate * 120 / 1000
	if samples < 960 {
		samples = 960
	}
	return samples * channels * bytesPerSample, nil
}

func audioSampleBytes(sampleFormat string) (int, bool) {
	switch sampleFormat {
	case av.SampleFormatS16:
		return 2, true
	case av.SampleFormatF32:
		return 4, true
	default:
		return 0, false
	}
}

func bufferBudgetMissingError(operation workOperation, err error) error {
	fact := err.Error()
	node := firstNonEmpty(operation.Node.String(), operation.Name, operation.Branch, "node")
	return &BuildError{
		Phase:     phaseBuild,
		Family:    errcode.FamilyForCode(bufferBudgetMissingCode),
		Code:      bufferBudgetMissingCode,
		Operation: "configure graph buffer",
		Node:      node,
		Reason:    "buffered recipe graph cannot derive a copy budget: " + fact,
		fields:    errDetails(errDetail("branch", operation.Branch), errDetail("kind", string(operation.Kind)), errDetail("shape", operation.ShapeIn.String())),
		fixes:     buildErrorFixes(bufferBudgetSuggestions(operation, fact)),
		cause:     errUnsupportedBuild,
	}
}

func bufferBudgetSuggestions(operation workOperation, fact string) []string {
	shapeText := operation.ShapeIn.String()
	switch {
	case fact == "missing width" || fact == "missing height":
		return []string{
			"declare video geometry on the source shape, for example shape.Frame(av.MediaVideo, shape.Video(width, height, av.PixelFormatI420))",
			"insert .Resize(width, height) before the buffered branch point or encoder",
			"if the maximum size is known only by an adapter, expose it through stream codec parameters before Build",
		}
	case fact == "missing pixel_format":
		return []string{
			"declare the video pixel format on the source shape, for example shape.Video(width, height, av.PixelFormatI420)",
			"insert a format-converting filter before this buffered edge",
			"if the adapter fixes the pixel format, expose it through stream codec parameters before Build",
		}
	case fact == "missing sample_rate" || fact == "missing channels" || fact == "missing sample_format":
		return []string{
			"declare complete audio shape facts, for example shape.Audio(48000, codec.Stereo, av.SampleFormatS16)",
			"insert .Resample(rate, channels) before this buffered edge when the branch should normalize audio",
			"if the adapter fixes the audio format, expose it through stream codec parameters before Build",
		}
	case strings.HasPrefix(fact, "unsupported pixel_format"):
		return []string{
			"use an I420/YUV420P, I422/YUV422P, I444/YUV444P, or Gray8 frame shape",
			"insert a format-converting filter before this buffered edge",
			"declare the exact pixel format in the source shape; current shape: " + shapeText,
		}
	case strings.HasPrefix(fact, "unsupported sample_format"):
		return []string{
			"use shape.Audio(rate, channels, av.SampleFormatS16) or av.SampleFormatF32",
			"insert .Resample(rate, channels) with a supported sample format before this buffered edge",
			"declare the exact sample format in the source shape; current shape: " + shapeText,
		}
	default:
		return []string{
			"declare complete shape facts on the input or transform before this buffered point",
			"set an explicit runtime buffer with copy bounds large enough for the stream: goavruntime.WithBufferPolicy(pipeline.BufferPolicy{Capacity: " + strconv.Itoa(realtimeRecipeBufferCapacity) + ", Drop: pipeline.DropBlock, CopyPacketBytes: ..., CopyFrameBytes: ...})",
		}
	}
}

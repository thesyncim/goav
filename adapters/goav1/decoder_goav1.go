package goav1

import (
	"context"
	"errors"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	backend "github.com/thesyncim/goav1"
)

type DecoderFactory struct{}

var _ codec.DecodeStateFactory = DecoderFactory{}

func NewDecoderFactory() DecoderFactory {
	return DecoderFactory{}
}

func Register(registry *codec.SimpleRegistry) {
	if registry == nil {
		return
	}
	registry.RegisterDecoder(activeDescriptor(), NewDecoderFactory())
}

func activeDescriptor() codec.Descriptor {
	desc := Descriptor()
	desc.Backend.Status = "active"
	return desc
}

func (DecoderFactory) NewDecoder(ctx context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	if config.Stream.Codec.ID != "" && config.Stream.Codec.ID != av.CodecAV1 {
		return nil, codec.ErrUnsupportedFormat
	}
	decoder := &Decoder{}
	if err := decoder.Open(ctx, config); err != nil {
		return nil, err
	}
	return decoder, nil
}

func (DecoderFactory) NewDecodeState(ctx context.Context, config codec.DecodeConfig) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if config.Stream.Codec.ID != "" && config.Stream.Codec.ID != av.CodecAV1 {
		return nil, codec.ErrUnsupportedFormat
	}
	bounds := config.Bounds
	if bounds.MaxWidth <= 0 {
		bounds.MaxWidth = config.Stream.Codec.Width
	}
	if bounds.MaxHeight <= 0 {
		bounds.MaxHeight = config.Stream.Codec.Height
	}
	format, err := DecoderFrameFormatFromStream(config.Stream, bounds)
	if err != nil {
		return nil, err
	}
	workerPool, err := backend.NewTileWorkerPool(defaultDecoderWorker)
	if err != nil {
		return nil, err
	}
	state, err := NewDecoderState(DecoderStateConfig{
		Bounds:        bounds,
		Format:        format,
		Scratch:       RuntimeDecoderScratchSizeFromBounds(bounds, defaultDecoderWorker),
		Workers:       defaultDecoderWorker,
		WorkerPool:    workerPool,
		OwnWorkerPool: true,
	})
	if err != nil {
		workerPool.Close()
		return nil, err
	}
	return state, nil
}

// Decoder consumes depacketized AV1 low-overhead OBU buffers through the
// generic codec.Decoder path. Low-level RTP callers that intentionally own raw
// AV1 RTP payload bytes can use DecodeRTPPayloadInto on this concrete type.
type Decoder struct {
	state            *DecoderState
	runner           *backend.DecoderFrameWorkResidualStreamRunner
	runnerSize       backend.DecoderFrameWorkResidualStreamScratchSize
	stream           av.Stream
	video            av.VideoFrame
	requestKeyframes bool
	dropDamagedVideo bool
	dropUntilSync    bool
	result           backend.DecoderFrameWorkResidualStreamResult
	closed           bool
}

func (d *Decoder) Descriptor() codec.Descriptor {
	return activeDescriptor()
}

func (d *Decoder) Open(ctx context.Context, config codec.DecodeConfig) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	state, ok := config.OpaqueState.(*DecoderState)
	if !ok || state == nil {
		return codec.ErrUnsupportedFormat
	}
	pixelFormat, _, err := backendFramePixelFormat(state.Format())
	if err != nil {
		return err
	}
	if requested := normalizeDecoderPixelFormat(config.Stream.Codec.PixelFormat); requested != "" && requested != pixelFormat {
		return codec.ErrUnsupportedFormat
	}

	state.Reset()
	d.state = state
	d.runner = nil
	d.runnerSize = backend.DecoderFrameWorkResidualStreamScratchSize{}
	d.stream = normalizeDecoderStream(config.Stream, state.Format(), pixelFormat)
	d.video = av.VideoFrame{
		Width:       d.stream.Codec.Width,
		Height:      d.stream.Codec.Height,
		PixelFormat: d.stream.Codec.PixelFormat,
	}
	d.requestKeyframes = config.Resilience.RequestKeyframes
	d.dropDamagedVideo = config.Resilience.DropDamagedVideo
	d.dropUntilSync = false
	d.result = backend.DecoderFrameWorkResidualStreamResult{}
	d.closed = false
	return nil
}

func (d *Decoder) DecodeInto(ctx context.Context, pkt *av.Packet, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.closed {
		return codec.ErrClosed
	}
	if d.state == nil {
		return codec.ErrUnsupportedFormat
	}
	if pkt == nil {
		d.resetAfterLoss()
		return d.appendKeyframeRequest(out, "av1 packet loss")
	}
	if pkt.LossBefore || pkt.Discontinuous {
		d.resetAfterLoss()
	}
	if pkt.Corrupt && d.dropDamagedVideo {
		d.resetAfterLoss()
		return d.appendKeyframeRequest(out, "av1 corrupt packet")
	}
	if len(pkt.Payload.Bytes) == 0 {
		return nil
	}

	plan, err := d.state.PlanLowOverhead(pkt.Payload.Bytes)
	if err != nil {
		if d.dropUntilSync {
			return d.appendKeyframeRequest(out, "av1 waiting for keyframe")
		}
		return mapGoav1Error(err)
	}
	sync := pkt.Keyframe || decoderPlanHasSync(plan, d.state.scratch.Events)
	if d.dropUntilSync && !sync {
		return d.appendKeyframeRequest(out, "av1 waiting for keyframe")
	}
	if len(out.Frames)+plan.Size.Event.Outputs > cap(out.Frames) {
		return codec.ErrResultFull
	}
	runner, err := d.runnerForPlan(plan)
	if err != nil {
		return mapGoav1Error(err)
	}
	d.result = backend.DecoderFrameWorkResidualStreamResult{}
	if err := runner.RunLowOverheadInto(&d.result, pkt.Payload.Bytes, nil); err != nil {
		return mapGoav1Error(err)
	}
	if sync {
		d.dropUntilSync = false
	}
	return d.appendDecodedFrames(pkt, &d.result, out)
}

func (d *Decoder) DecodeRTPPayloadInto(ctx context.Context, pkt *av.Packet, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.closed {
		return codec.ErrClosed
	}
	if d.state == nil {
		return codec.ErrUnsupportedFormat
	}
	if pkt == nil {
		if err := d.resetRTPAfterLoss(); err != nil {
			return mapGoav1Error(err)
		}
		return d.appendKeyframeRequest(out, "av1 rtp packet loss")
	}

	afterLoss := pkt.LossBefore || pkt.Discontinuous
	if afterLoss {
		if err := d.resetRTPAfterLoss(); err != nil {
			return mapGoav1Error(err)
		}
	}
	if pkt.Corrupt && d.dropDamagedVideo {
		if err := d.resetRTPAfterLoss(); err != nil {
			return mapGoav1Error(err)
		}
		return d.appendKeyframeRequest(out, "av1 corrupt rtp payload")
	}
	if len(pkt.Payload.Bytes) == 0 {
		return nil
	}

	plan, err := d.planRTPPayload(pkt.Payload.Bytes, afterLoss)
	if err != nil {
		if d.dropUntilSync {
			return d.appendKeyframeRequest(out, "av1 waiting for keyframe")
		}
		return mapGoav1Error(err)
	}
	sync := pkt.Keyframe ||
		decoderPlanHasSync(plan, d.state.scratch.Events) ||
		(d.state.HasSequenceHeader() && decoderPlanHasKeyFrame(plan, d.state.scratch.Events))
	if d.dropUntilSync && !sync && plan.Size.Events != 0 {
		return d.appendKeyframeRequest(out, "av1 waiting for keyframe")
	}
	if len(out.Frames)+plan.Size.Event.Outputs > cap(out.Frames) {
		return codec.ErrResultFull
	}
	runner, err := d.runnerForPlan(plan)
	if err != nil {
		return mapGoav1Error(err)
	}
	d.result = backend.DecoderFrameWorkResidualStreamResult{}
	if afterLoss {
		err = runner.RunRTPPayloadAfterLossInto(&d.result, pkt.Payload.Bytes, nil)
	} else {
		err = runner.RunRTPPayloadInto(&d.result, pkt.Payload.Bytes, nil)
	}
	if err != nil {
		if d.dropUntilSync {
			return d.appendKeyframeRequest(out, "av1 waiting for keyframe")
		}
		return mapGoav1Error(err)
	}
	if sync {
		d.dropUntilSync = false
	}
	return d.appendDecodedFrames(pkt, &d.result, out)
}

func (d *Decoder) FlushInto(ctx context.Context, out *codec.DecodeResult) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if out == nil {
		return codec.ErrNilResult
	}
	if d.closed {
		return codec.ErrClosed
	}
	return nil
}

func (d *Decoder) HandleEvent(ctx context.Context, event *av.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if event == nil {
		return nil
	}
	if d.closed {
		return codec.ErrClosed
	}
	switch event.Type {
	case av.EventPacketLoss:
		d.resetAfterLoss()
	case av.EventCodecChanged, av.EventDiscontinuity:
		d.applyEventStream(event)
		d.resetAfterLoss()
	}
	return nil
}

func (d *Decoder) Close() error {
	if d.closed {
		return nil
	}
	d.closed = true
	if d.state != nil {
		d.state.Close()
	}
	d.runner = nil
	d.runnerSize = backend.DecoderFrameWorkResidualStreamScratchSize{}
	d.dropUntilSync = false
	return nil
}

func (d *Decoder) resetAfterLoss() {
	if d.state != nil {
		d.state.Reset()
	}
	d.runner = nil
	d.runnerSize = backend.DecoderFrameWorkResidualStreamScratchSize{}
	d.dropUntilSync = true
	d.result = backend.DecoderFrameWorkResidualStreamResult{}
}

func (d *Decoder) resetRTPAfterLoss() error {
	if d.runner != nil {
		if err := d.runner.ResetRTP(); err != nil {
			return err
		}
	}
	d.dropUntilSync = true
	d.result = backend.DecoderFrameWorkResidualStreamResult{}
	return nil
}

func (d *Decoder) planRTPPayload(payload []byte, afterLoss bool) (backend.DecoderFrameWorkResidualStreamPlan, error) {
	if afterLoss {
		return d.state.PlanRTPPayloadAfterLoss(payload)
	}
	return d.state.PlanRTPPayload(payload)
}

func (d *Decoder) runnerForPlan(plan backend.DecoderFrameWorkResidualStreamPlan) (*backend.DecoderFrameWorkResidualStreamRunner, error) {
	if d.runner != nil && decoderStreamScratchFits(d.runnerSize, plan.Size) {
		return d.runner, nil
	}
	retained := 0
	if d.runner != nil {
		retained = d.runner.RTPUsed
	}
	runner, err := d.state.BindRunner(plan)
	if err != nil {
		return nil, err
	}
	if retained > 0 {
		if retained > len(runner.RTPBuffer) {
			return nil, backend.ErrRTPShortBuffer
		}
		copy(runner.RTPBuffer[:retained], d.state.scratch.RTPBuffer[:retained])
		runner.RTPUsed = retained
	}
	d.runner = runner
	d.runnerSize = plan.Size
	return runner, nil
}

func (d *Decoder) applyEventStream(event *av.Event) {
	if event.Stream != nil {
		d.stream = normalizeDecoderStream(*event.Stream, d.state.Format(), d.video.PixelFormat)
		if event.Codec == nil {
			return
		}
	}
	if event.StreamID != "" {
		d.stream.ID = event.StreamID
	}
	if event.Epoch != 0 {
		d.stream.Epoch = event.Epoch
	}
	if event.Codec != nil {
		d.stream.Codec = *event.Codec
	}
	d.stream = normalizeDecoderStream(d.stream, d.state.Format(), d.video.PixelFormat)
	d.video.Width = d.stream.Codec.Width
	d.video.Height = d.stream.Codec.Height
	d.video.PixelFormat = d.stream.Codec.PixelFormat
}

func normalizeDecoderStream(stream av.Stream, format backend.FrameFormat, pixelFormat string) av.Stream {
	stream.Type = av.MediaVideo
	stream.Codec.ID = av.CodecAV1
	stream.Codec.Type = av.MediaVideo
	if stream.Codec.ClockRate == 0 {
		stream.Codec.ClockRate = 90000
	}
	if stream.Codec.Width <= 0 {
		stream.Codec.Width = format.Width
	}
	if stream.Codec.Height <= 0 {
		stream.Codec.Height = format.Height
	}
	stream.Codec.PixelFormat = normalizeDecoderPixelFormat(stream.Codec.PixelFormat)
	if stream.Codec.PixelFormat == "" {
		stream.Codec.PixelFormat = pixelFormat
	}
	if stream.TimeBase == (av.TimeBase{}) {
		stream.TimeBase = av.RTPTimeBase(stream.Codec.ClockRate)
	}
	return stream
}

func normalizeDecoderPixelFormat(pixelFormat string) string {
	switch pixelFormat {
	case av.PixelFormatYUV420P:
		return av.PixelFormatI420
	case av.PixelFormatYUV422P:
		return av.PixelFormatI422
	case av.PixelFormatYUV444P:
		return av.PixelFormatI444
	default:
		return pixelFormat
	}
}

func (d *Decoder) appendDecodedFrames(pkt *av.Packet, result *backend.DecoderFrameWorkResidualStreamResult, out *codec.DecodeResult) error {
	if result == nil || result.Run.OutputCount == 0 {
		return nil
	}
	if result.Run.OutputCount > len(result.Run.Outputs) {
		return codec.ErrResultFull
	}
	needed := 0
	outputs := result.Run.Outputs[:result.Run.OutputCount]
	for i := range outputs {
		if outputs[i] != nil {
			needed++
		}
	}
	if len(out.Frames)+needed > cap(out.Frames) {
		return codec.ErrResultFull
	}
	for i := range outputs {
		if outputs[i] == nil {
			continue
		}
		if err := d.appendDecodedFrame(pkt, outputs[i], out); err != nil {
			return err
		}
	}
	return nil
}

func (d *Decoder) appendDecodedFrame(pkt *av.Packet, src *backend.Frame, out *codec.DecodeResult) error {
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	if err := d.fillDecodedFrame(pkt, src, frame); err != nil {
		out.Frames = out.Frames[:index]
		return err
	}
	return nil
}

func (d *Decoder) fillDecodedFrame(pkt *av.Packet, src *backend.Frame, frame *av.Frame) error {
	pixelFormat, planeCount, err := backendFramePixelFormat(src.Format)
	if err != nil {
		return err
	}
	if cap(frame.Planes) < planeCount {
		return codec.ErrOutputBufferTooSmall
	}

	frame.Planes = frame.Planes[:planeCount]
	frame.Planes[0] = av.Plane{
		Buffer: av.Buffer{Bytes: src.Y.Pix, Ownership: av.BufferBorrowed},
		Stride: src.Y.Stride,
	}
	if planeCount == 3 {
		frame.Planes[1] = av.Plane{
			Buffer: av.Buffer{Bytes: src.U.Pix, Ownership: av.BufferBorrowed},
			Stride: src.U.Stride,
		}
		frame.Planes[2] = av.Plane{
			Buffer: av.Buffer{Bytes: src.V.Pix, Ownership: av.BufferBorrowed},
			Stride: src.V.Stride,
		}
	}

	width, height := src.Format.Width, src.Format.Height
	if src.Y.Width > 0 {
		width = src.Y.Width
	}
	if src.Y.Height > 0 {
		height = src.Y.Height
	}
	d.video.Width = width
	d.video.Height = height
	d.video.PixelFormat = pixelFormat
	d.stream.Codec.Width = width
	d.stream.Codec.Height = height
	d.stream.Codec.PixelFormat = pixelFormat

	frame.StreamID = d.stream.ID
	frame.CodecEpoch = d.stream.Epoch
	frame.Type = av.MediaVideo
	frame.Video = &d.video
	if pkt != nil {
		if pkt.StreamID != "" {
			frame.StreamID = pkt.StreamID
		}
		if pkt.CodecEpoch != 0 {
			frame.CodecEpoch = pkt.CodecEpoch
		}
		frame.PTS = pkt.PTS
		frame.Duration = pkt.Duration
		frame.Metadata = pkt.Metadata
	}
	return nil
}

func backendFramePixelFormat(format backend.FrameFormat) (string, int, error) {
	if format.Width <= 0 || format.Height <= 0 || format.BitDepth != 8 {
		return "", 0, codec.ErrUnsupportedFormat
	}
	if format.MonoChrome {
		return av.PixelFormatGray8, 1, nil
	}
	if format.SubsamplingX && format.SubsamplingY {
		return av.PixelFormatI420, 3, nil
	}
	if format.SubsamplingX && !format.SubsamplingY {
		return av.PixelFormatI422, 3, nil
	}
	if !format.SubsamplingX && !format.SubsamplingY {
		return av.PixelFormatI444, 3, nil
	}
	return "", 0, codec.ErrUnsupportedFormat
}

func decoderStreamScratchFits(have, need backend.DecoderFrameWorkResidualStreamScratchSize) bool {
	return have.Events >= need.Events &&
		have.RTPBuffer >= need.RTPBuffer &&
		have.RTPSpans >= need.RTPSpans &&
		decoderEventScratchFits(have.Event, need.Event)
}

func decoderEventScratchFits(have, need backend.DecoderFrameWorkResidualEventScratchSize) bool {
	return have.Outputs >= need.Outputs &&
		have.Plan.SpanCount >= need.Plan.SpanCount &&
		have.Plan.JobCount >= need.Plan.JobCount &&
		have.Plan.BatchCount >= need.Plan.BatchCount &&
		decoderRunnerScratchFits(have.Runner, need.Runner) &&
		decoderSideDataScratchFits(have.SideData, need.SideData)
}

func decoderPlanHasSync(plan backend.DecoderFrameWorkResidualStreamPlan, events []backend.DecoderEvent) bool {
	eventCount := plan.Size.Events
	if eventCount > len(events) {
		eventCount = len(events)
	}
	haveSequence := false
	for i := 0; i < eventCount; i++ {
		event := &events[i]
		switch event.Kind {
		case backend.DecoderEventSequenceHeader:
			haveSequence = true
		case backend.DecoderEventFrameHeader,
			backend.DecoderEventRedundantFrameHeader,
			backend.DecoderEventFrame:
			if haveSequence && event.FrameHeader.FrameType == backend.FrameTypeKey {
				return true
			}
		}
	}
	return false
}

func decoderPlanHasKeyFrame(plan backend.DecoderFrameWorkResidualStreamPlan, events []backend.DecoderEvent) bool {
	eventCount := plan.Size.Events
	if eventCount > len(events) {
		eventCount = len(events)
	}
	for i := 0; i < eventCount; i++ {
		event := &events[i]
		switch event.Kind {
		case backend.DecoderEventFrameHeader,
			backend.DecoderEventRedundantFrameHeader,
			backend.DecoderEventFrame:
			if event.FrameHeader.FrameType == backend.FrameTypeKey {
				return true
			}
		}
	}
	return false
}

func decoderRunnerScratchFits(have, need backend.DecoderFrameWorkBatchResidualRunnerScratchSize) bool {
	return have.Workers >= need.Workers &&
		have.RestorationRequests >= need.RestorationRequests &&
		have.Int32Scratch >= need.Int32Scratch &&
		have.ResidualScratch >= need.ResidualScratch &&
		have.LoopContextAbove >= need.LoopContextAbove &&
		have.Batch.Int32Scratch >= need.Batch.Int32Scratch &&
		have.Batch.ResidualScratch >= need.Batch.ResidualScratch &&
		have.Batch.LoopContextAbove >= need.Batch.LoopContextAbove
}

func decoderSideDataScratchFits(have, need backend.DecoderFrameWorkSideDataScratchSize) bool {
	return have.CDEFIndexMap >= need.CDEFIndexMap &&
		have.CDEFReadMap >= need.CDEFReadMap &&
		have.LoopFilterMap >= need.LoopFilterMap &&
		have.RestorationRecords >= need.RestorationRecords &&
		have.RestorationBoundaryAbove >= need.RestorationBoundaryAbove &&
		have.RestorationBoundaryBelow >= need.RestorationBoundaryBelow
}

func (d *Decoder) appendKeyframeRequest(out *codec.DecodeResult, reason string) error {
	if !d.requestKeyframes {
		return nil
	}
	if len(out.Requests) == cap(out.Requests) {
		return codec.ErrResultFull
	}
	out.Requests = append(out.Requests, codec.ControlRequest{
		Type:     codec.ControlRequestKeyframe,
		StreamID: d.stream.ID,
		Reason:   reason,
	})
	return nil
}

func mapGoav1Error(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, backend.ErrFrameInvalidFormat):
		return codec.ErrUnsupportedFormat
	case errors.Is(err, backend.ErrDecoderInvalidFrameWorkState):
		return codec.ErrUnsupportedFormat
	default:
		return err
	}
}

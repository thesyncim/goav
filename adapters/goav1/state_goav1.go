//go:build goav_goav1

package goav1

import (
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	backend "github.com/thesyncim/goav1"
)

const (
	defaultAlign         = 64
	defaultSurfaceCount  = backend.RefFrames + 1
	defaultMaxFrames     = 1
	defaultMaxEvents     = 1
	defaultRuntimeEvents = 8
	defaultMaxPayload    = 1200
	defaultMaxRetained   = 4096
	defaultDecoderWorker = 1
)

// DecoderStateConfig describes caller-owned AV1 decoder state for the tagged
// realtime adapter path. Format is an exact coded frame format, not merely a
// maximum envelope; AV1 frame pools must match the stream format accepted by
// the backend runner.
type DecoderStateConfig struct {
	// Bounds supplies generic per-input result, payload, retained-fragment, and
	// geometry limits from codec.DecodeConfig.
	Bounds codec.DecodeBounds
	// Format is the exact backend frame-pool format for the accepted coded
	// stream. If empty, Bounds.MaxWidth/MaxHeight are used for first-slice
	// 8-bit 4:2:0 output.
	Format backend.FrameFormat
	// Scratch augments the generic bounds with backend event/tile scratch sizes
	// learned from probing or application policy.
	Scratch backend.DecoderFrameWorkResidualStreamScratchSize
	// SurfaceCount defaults to the AV1 reference set plus one in-flight output.
	SurfaceCount int
	// Workers defaults to one and is retained for runner planning.
	Workers int
	// WorkerPool is caller-owned and required before running tile-bearing plans.
	WorkerPool *backend.TileWorkerPool
	// OwnWorkerPool closes WorkerPool when DecoderState.Close is called.
	OwnWorkerPool bool
}

// DecoderState owns the reusable backend state and scratch required by a
// packet-by-packet AV1 decoder. It is intended to be passed as
// codec.DecodeConfig.OpaqueState once the concrete decoder factory is enabled.
type DecoderState struct {
	bounds       codec.DecodeBounds
	format       backend.FrameFormat
	scratchSize  backend.DecoderFrameWorkResidualStreamScratchSize
	surfaceCount int
	workers      int

	stream backend.DecoderStream
	work   backend.DecoderFrameWorkState
	refs   backend.DecoderSurfaceReferences

	frameBacking []byte
	frames       []backend.Frame
	free         []int
	used         []bool
	pool         backend.FramePool

	referenceSurfaces []int
	referenceFrames   []*backend.Frame
	releases          []int

	sideData      backend.DecoderFrameWorkSideData
	stats         backend.DecoderFrameWorkTileResidualStats
	batch         backend.DecoderFrameWorkBatchResidualRunner
	scratch       backend.DecoderFrameWorkResidualStreamScratch
	planRTPBuffer []byte
	planRTPSpans  []backend.RTPObuSpan
	runner        backend.DecoderFrameWorkResidualStreamRunner
	bound         bool

	workerPool    *backend.TileWorkerPool
	ownWorkerPool bool
}

// DecoderFrameFormatFromBounds converts DecodeBounds into the exact default
// 8-bit 4:2:0 coded frame format used by the first AV1 adapter slice.
func DecoderFrameFormatFromBounds(bounds codec.DecodeBounds) (backend.FrameFormat, error) {
	bounds = normalizeDecoderBounds(bounds)
	if bounds.MaxWidth <= 0 || bounds.MaxHeight <= 0 {
		return backend.FrameFormat{}, codec.ErrUnsupportedFormat
	}
	format := backend.FrameFormat{
		Width:        bounds.MaxWidth,
		Height:       bounds.MaxHeight,
		BitDepth:     8,
		SubsamplingX: true,
		SubsamplingY: true,
		Align:        defaultAlign,
	}
	if _, err := backend.FrameRequiredSize(format); err != nil {
		return backend.FrameFormat{}, err
	}
	return format, nil
}

// DecoderFrameFormatFromStream converts stream metadata and bounds into the
// exact frame-pool format used by the first AV1 adapter slice.
func DecoderFrameFormatFromStream(stream av.Stream, bounds codec.DecodeBounds) (backend.FrameFormat, error) {
	bounds = normalizeDecoderBounds(bounds)
	if bounds.MaxWidth <= 0 {
		bounds.MaxWidth = stream.Codec.Width
	}
	if bounds.MaxHeight <= 0 {
		bounds.MaxHeight = stream.Codec.Height
	}
	format, err := DecoderFrameFormatFromBounds(bounds)
	if err != nil {
		return backend.FrameFormat{}, err
	}
	switch stream.Codec.PixelFormat {
	case "", av.PixelFormatI420, av.PixelFormatYUV420P:
		format.MonoChrome = false
		format.SubsamplingX = true
		format.SubsamplingY = true
	case av.PixelFormatGray8:
		format.MonoChrome = true
		format.SubsamplingX = true
		format.SubsamplingY = true
	default:
		return backend.FrameFormat{}, codec.ErrUnsupportedFormat
	}
	if _, err := backend.FrameRequiredSize(format); err != nil {
		return backend.FrameFormat{}, err
	}
	return format, nil
}

// DecoderScratchSizeFromBounds reserves parser/RTP/output slots from the
// generic DecodeBounds. Event-runner scratch remains caller-overridable through
// DecoderStateConfig.Scratch because the backend sizes tile residual scratch
// from parsed sequence and frame events.
func DecoderScratchSizeFromBounds(bounds codec.DecodeBounds) backend.DecoderFrameWorkResidualStreamScratchSize {
	bounds = normalizeDecoderBounds(bounds)
	retainedAndPayload := bounds.MaxRetainedBytes + bounds.MaxPayloadBytes
	if retainedAndPayload < bounds.MaxRetainedBytes {
		retainedAndPayload = bounds.MaxRetainedBytes
	}
	return backend.DecoderFrameWorkResidualStreamScratchSize{
		Events:    bounds.MaxEventsPerInput,
		RTPBuffer: retainedAndPayload,
		RTPSpans:  bounds.MaxEventsPerInput,
		Event: backend.DecoderFrameWorkResidualEventScratchSize{
			Outputs: bounds.MaxFramesPerInput,
		},
	}
}

// RuntimeDecoderScratchSizeFromBounds returns a conservative first-slice
// scratch shape for high-level realtime runtimes. Applications with exact
// stream knowledge can still pass a tuned DecoderState through OpaqueState.
func RuntimeDecoderScratchSizeFromBounds(bounds codec.DecodeBounds, workers int) backend.DecoderFrameWorkResidualStreamScratchSize {
	bounds = normalizeDecoderBounds(bounds)
	if bounds.MaxEventsPerInput < defaultRuntimeEvents {
		bounds.MaxEventsPerInput = defaultRuntimeEvents
	}
	if workers <= 0 {
		workers = defaultDecoderWorker
	}
	size := DecoderScratchSizeFromBounds(bounds)
	tileScratch := backend.MaxTiles
	if tileScratch < 1 {
		tileScratch = 1
	}
	size.Event.Plan = backend.DecoderTileWorkPlan{
		SpanCount:  tileScratch,
		JobCount:   tileScratch,
		BatchCount: tileScratch,
	}
	framePixels := bounds.MaxWidth * bounds.MaxHeight
	if framePixels < 4096 {
		framePixels = 4096
	}
	sideEntries := framePixels / 16
	if sideEntries < 64 {
		sideEntries = 64
	}
	size.Event.SideData = backend.DecoderFrameWorkSideDataScratchSize{
		CDEFIndexMap:             sideEntries,
		CDEFReadMap:              sideEntries,
		LoopFilterMap:            sideEntries,
		RestorationRecords:       sideEntries,
		RestorationBoundaryAbove: bounds.MaxWidth * 3,
		RestorationBoundaryBelow: bounds.MaxWidth * 3,
	}
	batchScratch := backend.DecoderFrameWorkBatchResidualScratchSize{
		Int32Scratch:     32768,
		ResidualScratch:  32768,
		LoopContextAbove: maxInt(bounds.MaxWidth*4, 1024),
	}
	size.Event.Runner = backend.DecoderFrameWorkBatchResidualRunnerScratchSize{
		Workers:             workers,
		Batch:               batchScratch,
		RestorationRequests: workers,
		Int32Scratch:        batchScratch.Int32Scratch * workers,
		ResidualScratch:     batchScratch.ResidualScratch * workers,
		LoopContextAbove:    batchScratch.LoopContextAbove * workers,
	}
	return size
}

// NewDecoderState allocates and binds reusable AV1 decoder state from explicit
// bounds and exact format. All slices retained by the returned state are owned
// by the state and reused across packet decode calls.
func NewDecoderState(config DecoderStateConfig) (*DecoderState, error) {
	bounds := normalizeDecoderBounds(config.Bounds)
	format := config.Format
	if format.Width == 0 || format.Height == 0 {
		var err error
		format, err = DecoderFrameFormatFromBounds(bounds)
		if err != nil {
			return nil, err
		}
	}
	if _, err := backend.FrameRequiredSize(format); err != nil {
		return nil, err
	}

	surfaceCount := config.SurfaceCount
	if surfaceCount <= 0 {
		surfaceCount = defaultSurfaceCount
	}
	workers := config.Workers
	if workers <= 0 {
		workers = defaultDecoderWorker
	}

	scratchSize := DecoderScratchSizeFromBounds(bounds).Max(config.Scratch)
	_, backingSize, err := backend.FramePoolRequiredSize(format, surfaceCount)
	if err != nil {
		return nil, err
	}

	state := &DecoderState{
		bounds:            bounds,
		format:            format,
		scratchSize:       scratchSize,
		surfaceCount:      surfaceCount,
		workers:           workers,
		frameBacking:      make([]byte, backingSize),
		frames:            make([]backend.Frame, surfaceCount),
		free:              make([]int, surfaceCount),
		used:              make([]bool, surfaceCount),
		referenceSurfaces: make([]int, backend.InterRefsPerFrame),
		referenceFrames:   make([]*backend.Frame, backend.InterRefsPerFrame),
		releases:          make([]int, backend.RefFrames),
		workerPool:        config.WorkerPool,
		ownWorkerPool:     config.OwnWorkerPool,
	}
	state.pool, err = backend.BindFramePool(state.frameBacking[:backingSize], format, state.frames, state.free, state.used)
	if err != nil {
		return nil, err
	}
	state.scratch = newDecoderStreamScratch(scratchSize)
	if err := state.scratch.Check(scratchSize); err != nil {
		return nil, err
	}
	state.planRTPBuffer = make([]byte, scratchSize.RTPBuffer)
	state.planRTPSpans = make([]backend.RTPObuSpan, scratchSize.RTPSpans)
	return state, nil
}

func normalizeDecoderBounds(bounds codec.DecodeBounds) codec.DecodeBounds {
	return bounds.WithDefaults(codec.DecodeBounds{
		MaxFramesPerInput: defaultMaxFrames,
		MaxEventsPerInput: defaultMaxEvents,
		MaxPayloadBytes:   defaultMaxPayload,
		MaxRetainedBytes:  defaultMaxRetained,
	})
}

func newDecoderStreamScratch(size backend.DecoderFrameWorkResidualStreamScratchSize) backend.DecoderFrameWorkResidualStreamScratch {
	return backend.DecoderFrameWorkResidualStreamScratch{
		Events:    make([]backend.DecoderEvent, size.Events),
		Event:     newDecoderEventScratch(size.Event),
		SideData:  newDecoderSideDataScratch(size.Event.SideData),
		Outputs:   make([]*backend.Frame, size.Event.Outputs),
		RTPBuffer: make([]byte, size.RTPBuffer),
		RTPSpans:  make([]backend.RTPObuSpan, size.RTPSpans),
	}
}

func newDecoderEventScratch(size backend.DecoderFrameWorkResidualEventScratchSize) backend.DecoderFrameWorkResidualEventScratch {
	return backend.DecoderFrameWorkResidualEventScratch{
		Runner:   newDecoderBatchRunnerScratch(size.Runner),
		SideData: newDecoderSideDataScratch(size.SideData),
		Spans:    make([]backend.TileSpan, size.Plan.SpanCount),
		Jobs:     make([]backend.TileJob, size.Plan.JobCount),
		Batches:  make([]backend.TileBatch, size.Plan.BatchCount),
	}
}

func newDecoderBatchRunnerScratch(size backend.DecoderFrameWorkBatchResidualRunnerScratchSize) backend.DecoderFrameWorkBatchResidualRunnerScratch {
	return backend.DecoderFrameWorkBatchResidualRunnerScratch{
		States:                  make([]backend.TileDecodeState, size.Workers),
		Storages:                make([]backend.DecoderFrameWorkTileResidualCDFStorage, size.Workers),
		TileScratch:             make([]backend.DecoderFrameWorkTileResidualScratch, size.Workers),
		RestorationRequests:     make([]backend.DecoderFrameWorkTileRestorationRequest, size.RestorationRequests),
		PredictionScratch:       make([]backend.DecoderFrameWorkPredictionScratch, size.Workers),
		InterPredictionScratch:  make([]backend.DecoderFrameWorkInterPredictionScratch, size.Workers),
		Stats:                   make([]backend.DecoderFrameWorkTileResidualStats, size.Workers),
		Int32Scratch:            make([]int32, size.Int32Scratch),
		ResidualScratch:         make([]int16, size.ResidualScratch),
		LoopContextAboveScratch: make([]backend.TileBlockLoopRootAboveContext, size.LoopContextAbove),
	}
}

func newDecoderSideDataScratch(size backend.DecoderFrameWorkSideDataScratchSize) backend.DecoderFrameWorkSideDataScratch {
	return backend.DecoderFrameWorkSideDataScratch{
		CDEFIndexMap:             make([]uint8, size.CDEFIndexMap),
		CDEFReadMap:              make([]bool, size.CDEFReadMap),
		LoopFilterMap:            make([]backend.DecoderFrameWorkLoopFilterBlockRecord, size.LoopFilterMap),
		RestorationRecords:       make([]backend.TileRestorationUnitRecord, size.RestorationRecords),
		RestorationBoundaryAbove: make([]uint16, size.RestorationBoundaryAbove),
		RestorationBoundaryBelow: make([]uint16, size.RestorationBoundaryBelow),
	}
}

func (s *DecoderState) Bounds() codec.DecodeBounds {
	if s == nil {
		return codec.DecodeBounds{}
	}
	return s.bounds
}

func (s *DecoderState) Format() backend.FrameFormat {
	if s == nil {
		return backend.FrameFormat{}
	}
	return s.format
}

func (s *DecoderState) ScratchSize() backend.DecoderFrameWorkResidualStreamScratchSize {
	if s == nil {
		return backend.DecoderFrameWorkResidualStreamScratchSize{}
	}
	return s.scratchSize
}

func (s *DecoderState) Stream() *backend.DecoderStream {
	if s == nil {
		return nil
	}
	return &s.stream
}

func (s *DecoderState) Scratch() backend.DecoderFrameWorkResidualStreamScratch {
	if s == nil {
		return backend.DecoderFrameWorkResidualStreamScratch{}
	}
	return s.scratch
}

func (s *DecoderState) Runtime() backend.DecoderFrameWorkResidualEventRuntime {
	if s == nil {
		return backend.DecoderFrameWorkResidualEventRuntime{}
	}
	return backend.DecoderFrameWorkResidualEventRuntime{
		State:             &s.work,
		Refs:              &s.refs,
		FramePool:         &s.pool,
		Align:             s.format.Align,
		ReferenceSurfaces: s.referenceSurfaces,
		ReferenceFrames:   s.referenceFrames,
		Releases:          s.releases,
		WorkerPool:        s.workerPool,
		SideData:          &s.sideData,
		Stats:             &s.stats,
		Outputs:           s.scratch.Outputs,
	}
}

func (s *DecoderState) BatchRunner() *backend.DecoderFrameWorkBatchResidualRunner {
	if s == nil {
		return nil
	}
	return &s.batch
}

// PlanLowOverhead validates one depacketized AV1 low-overhead OBU buffer
// against a copy of the current stream state and reports the scratch/bind shape
// required by BindRunner.
func (s *DecoderState) PlanLowOverhead(payload []byte) (backend.DecoderFrameWorkResidualStreamPlan, error) {
	if s == nil {
		return backend.DecoderFrameWorkResidualStreamPlan{}, backend.ErrDecoderInvalidFrameWorkState
	}
	return backend.DecoderFrameWorkResidualLowOverheadStreamPlan(
		s.stream,
		payload,
		s.workers,
		s.scratch.Events,
		s.scratch.Event.Spans,
		s.scratch.Event.Jobs,
		s.scratch.Event.Batches,
	)
}

// PlanLowOverheads validates an ordered batch of depacketized AV1
// low-overhead OBU buffers against a copy of the current stream state and
// returns a reusable max plan for BindRunner.
func (s *DecoderState) PlanLowOverheads(payloads [][]byte) (backend.DecoderFrameWorkResidualStreamPlan, error) {
	if s == nil {
		return backend.DecoderFrameWorkResidualStreamPlan{}, backend.ErrDecoderInvalidFrameWorkState
	}
	return backend.DecoderFrameWorkResidualLowOverheadStreamsPlan(
		s.stream,
		payloads,
		s.workers,
		s.scratch.Events,
		s.scratch.Event.Spans,
		s.scratch.Event.Jobs,
		s.scratch.Event.Batches,
	)
}

// PlanRTPPayload validates one raw AV1 RTP payload against a copy of the
// current stream state and retained fragment bytes.
func (s *DecoderState) PlanRTPPayload(payload []byte) (backend.DecoderFrameWorkResidualStreamPlan, error) {
	return s.planRTPPayload(payload, false)
}

// PlanRTPPayloadAfterLoss validates one raw AV1 RTP payload as the first
// payload after loss. It clears retained RTP fragment state in the planning
// copy while preserving sequence/reference parser state.
func (s *DecoderState) PlanRTPPayloadAfterLoss(payload []byte) (backend.DecoderFrameWorkResidualStreamPlan, error) {
	return s.planRTPPayload(payload, true)
}

func (s *DecoderState) planRTPPayload(payload []byte, afterLoss bool) (backend.DecoderFrameWorkResidualStreamPlan, error) {
	if s == nil {
		return backend.DecoderFrameWorkResidualStreamPlan{}, backend.ErrDecoderInvalidFrameWorkState
	}
	stream := s.stream
	used := 0
	if afterLoss {
		stream.ResetRTP()
	} else if s.bound {
		used = s.runner.RTPUsed
	}
	if used > len(s.planRTPBuffer) {
		return backend.DecoderFrameWorkResidualStreamPlan{}, backend.ErrRTPShortBuffer
	}
	if used > 0 {
		copy(s.planRTPBuffer, s.runner.RTPBuffer[:used])
	}
	return backend.DecoderFrameWorkResidualRTPPayloadStreamPlan(
		stream,
		used,
		payload,
		s.workers,
		s.planRTPBuffer,
		s.planRTPSpans,
		s.scratch.Events,
		s.scratch.Event.Spans,
		s.scratch.Event.Jobs,
		s.scratch.Event.Batches,
	)
}

func (s *DecoderState) HasSequenceHeader() bool {
	if s == nil {
		return false
	}
	return s.stream.HasSequenceHeader()
}

// BindRunner binds the reusable backend runner for a previously planned stream
// shape. Callers should re-plan and re-bind when codec parameters or accepted
// sequence shape change.
func (s *DecoderState) BindRunner(plan backend.DecoderFrameWorkResidualStreamPlan) (*backend.DecoderFrameWorkResidualStreamRunner, error) {
	if s == nil {
		return nil, backend.ErrDecoderInvalidFrameWorkState
	}
	if plan.Size.Event.Runner.Workers > 0 && s.workerPool == nil {
		return nil, backend.ErrThreadingInvalidWorkerCount
	}
	if err := s.scratch.Check(plan.Size); err != nil {
		return nil, err
	}
	runner, _, err := backend.BindDecoderFrameWorkResidualStreamPlanRunner(
		plan,
		&s.stream,
		s.Runtime(),
		s.scratch,
		&s.batch,
	)
	if err != nil {
		return nil, err
	}
	s.runner = runner
	s.bound = true
	return &s.runner, nil
}

// Runner returns the currently bound backend runner, when BindRunner has
// succeeded and Reset has not cleared it.
func (s *DecoderState) Runner() (*backend.DecoderFrameWorkResidualStreamRunner, bool) {
	if s == nil || !s.bound {
		return nil, false
	}
	return &s.runner, true
}

func (s *DecoderState) Reset() {
	if s == nil {
		return
	}
	s.stream.Reset()
	s.work.Reset()
	s.refs.Reset()
	s.pool.Reset()
	s.sideData = backend.DecoderFrameWorkSideData{}
	s.stats = backend.DecoderFrameWorkTileResidualStats{}
	s.batch = backend.DecoderFrameWorkBatchResidualRunner{}
	s.runner = backend.DecoderFrameWorkResidualStreamRunner{}
	s.bound = false
}

func (s *DecoderState) Close() {
	if s == nil {
		return
	}
	s.Reset()
	if s.ownWorkerPool {
		s.workerPool.Close()
		s.workerPool = nil
		s.ownWorkerPool = false
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

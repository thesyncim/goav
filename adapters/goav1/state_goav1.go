//go:build goav_goav1

package goav1

import (
	"github.com/thesyncim/goav/codec"
	backend "github.com/thesyncim/goav1"
)

const (
	defaultAlign         = 64
	defaultSurfaceCount  = backend.RefFrames + 1
	defaultMaxFrames     = 1
	defaultMaxEvents     = 1
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
	// WorkerPool is caller-owned and is threaded into the backend runtime.
	WorkerPool *backend.TileWorkerPool
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

	sideData backend.DecoderFrameWorkSideData
	stats    backend.DecoderFrameWorkTileResidualStats
	batch    backend.DecoderFrameWorkBatchResidualRunner
	scratch  backend.DecoderFrameWorkResidualStreamScratch

	workerPool *backend.TileWorkerPool
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
	}
	state.pool, err = backend.BindFramePool(state.frameBacking[:backingSize], format, state.frames, state.free, state.used)
	if err != nil {
		return nil, err
	}
	state.scratch = newDecoderStreamScratch(scratchSize)
	if err := state.scratch.Check(scratchSize); err != nil {
		return nil, err
	}
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
}

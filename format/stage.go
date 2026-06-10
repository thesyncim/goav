package format

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/pipeline"
)

type DemuxSourceConfig struct {
	Name string
	// Detail is optional graph-rendering context for humans.
	Detail string
	// Demuxer reads container input into caller-owned Result scratch.
	Demuxer Demuxer
	// Result owns the packet pointer and event slice reused for every read.
	Result ReadResult
	// Realtime paces the pump on a clock: each packet is delivered when its
	// media time is due (PTS-paced playback), and av.EventRate scales the pace
	// live. False (offline) pumps as fast as the sink drains — a transcode job
	// must not run at 1x — and rejects rate controls with ErrRateUnsupported.
	Realtime bool
	// Clock is the time source realtime pacing runs on; nil defaults to
	// av.MonotonicClock(). Tests inject a fake so nothing sleeps for real.
	// Ignored when Realtime is false.
	Clock av.Clock
}

type MuxStageConfig struct {
	Name string
	// Detail is optional graph-rendering context for humans.
	Detail string
	// Muxer writes packet messages to the configured output.
	Muxer Muxer
	// Result owns the event slice reused for every write.
	Result WriteResult
}

type DemuxSource struct {
	name    string
	detail  string
	demuxer Demuxer
	seeker  Seeker
	pending atomic.Pointer[timeControlRequest]
	window  demuxWindow
	pacer   *demuxPacer
	result  ReadResult
	message pipeline.Message
	eos     av.Event
	disc    av.Event
	streams []av.Stream
	events  []av.Event
	closed  bool
}

// timeControlRequest is one recorded reposition: a seek position, plus an
// exclusive segment end when the request is a window (end > 0 means segment).
// Control swaps it into DemuxSource.pending; the Start loop applies it between
// reads, per the pipeline.ControllableSource concurrency contract.
type timeControlRequest struct {
	position time.Duration
	end      time.Duration
}

// demuxWindow is the Start-loop-local segment state: the exclusive end bound
// and the streams whose packets already passed it. Only the Start loop touches
// it — Control communicates exclusively through the pending pointer.
type demuxWindow struct {
	active bool
	end    time.Duration
	done   []av.StreamID
}

type MuxStage struct {
	name    string
	detail  string
	muxer   Muxer
	result  WriteResult
	message pipeline.Message
	closed  bool
}

var _ pipeline.Source = (*DemuxSource)(nil)
var _ pipeline.Stage = (*MuxStage)(nil)
var _ pipeline.NodeDescriber = (*DemuxSource)(nil)
var _ pipeline.NodeDescriber = (*MuxStage)(nil)
var _ pipeline.ControllableSource = (*controllableDemuxSource)(nil)

// NewDemuxSource builds the pump that drives Demuxer.ReadInto into a pipeline.
// When the demuxer implements Seeker the returned source also implements
// pipeline.ControllableSource, honouring av.EventSeek and av.EventSegment by
// repositioning through the demuxer (Control records the request; the Start
// loop applies it and emits av.EventDiscontinuity before the first message at
// the new position). A demuxer without Seeker yields a plain source — time
// controls report the standard not-controllable error at the graph seam, never
// a silent ignore.
//
// With Realtime set the pump paces delivery on the configured clock — each
// packet emits when its media time is due, av.EventRate scales the pace — and
// every reposition (seek, segment start, demuxer discontinuity) re-anchors the
// pacing timeline so playback resumes paced at the new position.
func NewDemuxSource(config DemuxSourceConfig) (pipeline.Source, error) {
	if config.Demuxer == nil {
		return nil, ErrNilDemuxer
	}
	if config.Result.Packet == nil {
		return nil, ErrNilPacket
	}
	name := config.Name
	if name == "" {
		name = "demux"
	}

	source := &DemuxSource{
		name:    name,
		detail:  config.Detail,
		demuxer: config.Demuxer,
		result:  config.Result,
	}
	if config.Realtime {
		clock := config.Clock
		if clock == nil {
			clock = av.MonotonicClock()
		}
		source.pacer = newDemuxPacer(clock)
	}
	source.initStreamEvents(config.Demuxer.Streams())
	if seeker, ok := config.Demuxer.(Seeker); ok {
		source.seeker = seeker
		return &controllableDemuxSource{DemuxSource: source}, nil
	}
	return source, nil
}

// controllableDemuxSource is the pipeline.ControllableSource face of a
// DemuxSource whose demuxer implements Seeker. The split keeps the capability
// honest: a source over a demuxer that cannot reposition does not expose
// Control at all, so capability discovery by type assertion never lies.
type controllableDemuxSource struct {
	*DemuxSource
}

// Control records a time-axis request for the Start loop to apply between
// reads (the ControllableSource contract: record, don't touch loop state).
// av.EventSeek and av.EventSegment are honoured through the demuxer's Seeker.
// av.EventRate is honoured when the pump paces delivery (Realtime): the rate
// multiplier swaps atomically and the loop re-anchors its pacing timeline at
// the next packet — pure pacing, no discontinuity. An offline pump runs
// unpaced and rejects rate controls with ErrRateUnsupported.
func (s *controllableDemuxSource) Control(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessageEvent || msg.Event == nil {
		return fmt.Errorf("format: %s: only event controls are supported", s.name)
	}
	switch msg.Event.Type {
	case av.EventSeek:
		position, ok := msg.Event.Timestamp.ToDuration()
		if !ok || position < 0 {
			return fmt.Errorf("format: %s: seek needs a non-negative position on Event.Timestamp", s.name)
		}
		s.pending.Store(&timeControlRequest{position: position})
		return nil
	case av.EventSegment:
		start, ok := msg.Event.Timestamp.ToDuration()
		if !ok || start < 0 {
			return fmt.Errorf("format: %s: segment needs a non-negative start on Event.Timestamp", s.name)
		}
		end, ok := av.EventSegmentEnd(msg.Event)
		if !ok || end <= start {
			return fmt.Errorf("format: %s: segment needs an exclusive end after start (build it with av.SegmentEndMetadata)", s.name)
		}
		s.pending.Store(&timeControlRequest{position: start, end: end})
		return nil
	case av.EventRate:
		if s.pacer == nil {
			return fmt.Errorf("%s: %w", s.name, ErrRateUnsupported)
		}
		rate, ok := av.EventRateValue(msg.Event)
		if !ok {
			return fmt.Errorf("format: %s: rate needs a positive, finite value on Event.Metadata (build it with av.RateMetadata)", s.name)
		}
		s.pacer.setRate(rate)
		return nil
	default:
		return fmt.Errorf("format: %s: unsupported source control %q", s.name, msg.Event.Type)
	}
}

func NewMuxStage(config MuxStageConfig) (*MuxStage, error) {
	if config.Muxer == nil {
		return nil, ErrNilMuxer
	}
	name := config.Name
	if name == "" {
		name = "mux"
	}
	return &MuxStage{
		name:   name,
		detail: config.Detail,
		muxer:  config.Muxer,
		result: config.Result,
	}, nil
}

func (s *DemuxSource) Name() string {
	return s.name
}

func (s *MuxStage) Name() string {
	return s.name
}

func (s *DemuxSource) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeSource, Detail: s.detail}
}

func (s *MuxStage) DescribeNode() pipeline.NodeSpec {
	return pipeline.NodeSpec{Name: s.name, Kind: pipeline.NodeStage, Detail: s.detail}
}

func (s *DemuxSource) Start(ctx context.Context, emitter pipeline.Emitter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return pipeline.ErrClosed
	}
	if err := s.emitStreamEvents(ctx, emitter); err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.applyPendingControl(ctx, emitter); err != nil {
			return err
		}
		s.result.Reset()
		if err := s.demuxer.ReadInto(ctx, &s.result); err != nil {
			if errors.Is(err, io.EOF) {
				return s.emitEndOfStream(ctx, emitter)
			}
			return err
		}
		if err := s.emitEvents(ctx, emitter, s.result.Events); err != nil {
			return err
		}
		s.reanchorOnDiscontinuity(s.result.Events)
		if s.result.PacketReady {
			emit, finished := s.windowAdmits(s.result.Packet)
			if finished {
				return s.emitEndOfStream(ctx, emitter)
			}
			if emit {
				if err := s.pacePacket(ctx, s.result.Packet); err != nil {
					return err
				}
				if err := s.emitPacket(ctx, emitter, s.result.Packet); err != nil {
					return err
				}
			}
		}
	}
}

// applyPendingControl applies a reposition recorded by Control: it seeks the
// demuxer, arms or disarms the segment window, and emits av.EventDiscontinuity
// before the first message at the new position (the repositioning half of the
// pipeline.ControllableSource contract). Only the Start loop calls it, so the
// seek never races ReadInto. A failed seek stops the pump with the demuxer's
// error — including format.ErrNotSeekable input — rather than playing on from
// the wrong position.
func (s *DemuxSource) applyPendingControl(ctx context.Context, emitter pipeline.Emitter) error {
	request := s.pending.Swap(nil)
	if request == nil {
		return nil
	}
	if err := s.seeker.Seek(ctx, request.position); err != nil {
		return fmt.Errorf("format: %s: seek to %v: %w", s.name, request.position, err)
	}
	s.window.active = request.end > 0
	s.window.end = request.end
	s.window.done = s.window.done[:0]
	s.disc.Reset()
	s.disc.Type = av.EventDiscontinuity
	if s.window.active {
		s.disc.Reason = "segment"
	} else {
		s.disc.Reason = "seek"
	}
	if len(s.streams) == 1 {
		s.disc.StreamID = s.streams[0].ID
		s.disc.Epoch = s.streams[0].Epoch
	}
	s.message.Kind = pipeline.MessageEvent
	s.message.Packet = nil
	s.message.Frame = nil
	s.message.Event = &s.disc
	if s.pacer != nil {
		// The reposition moved media time; re-anchor so playback resumes paced
		// at the new position — no giant sleep forward, no burst backward.
		s.pacer.reset()
	}
	return emitter.Emit(ctx, &s.message)
}

// pacePacket holds the packet until its media time is due when the pump is
// realtime (paced); offline pumps deliver as fast as the sink drains.
func (s *DemuxSource) pacePacket(ctx context.Context, packet *av.Packet) error {
	if s.pacer == nil {
		return nil
	}
	return s.pacer.pace(ctx, packet)
}

// reanchorOnDiscontinuity resets the pacing anchor when the demuxer itself
// reported a timeline break in this read's events, so pacing re-anchors at the
// first packet after the break exactly as it does after a seek.
func (s *DemuxSource) reanchorOnDiscontinuity(events []av.Event) {
	if s.pacer == nil {
		return
	}
	for i := range events {
		if events[i].Type == av.EventDiscontinuity {
			s.pacer.reset()
			return
		}
	}
}

// windowAdmits applies the segment window to one demuxed packet: packets
// before the exclusive end pass through; a packet at or past the end marks its
// stream finished and is dropped. finished reports that every declared stream
// passed the bound, which ends the source exactly like the end of the media.
// A packet without a comparable time passes through — bounding cannot justify
// silently dropping media.
func (s *DemuxSource) windowAdmits(packet *av.Packet) (emit bool, finished bool) {
	if !s.window.active || packet == nil {
		return true, false
	}
	pts, ok := packet.PTS.ToDuration()
	if !ok {
		return true, false
	}
	if pts < s.window.end {
		return !s.windowStreamDone(packet.StreamID), false
	}
	s.markWindowStreamDone(packet.StreamID)
	return false, s.windowFinished()
}

func (s *DemuxSource) windowStreamDone(id av.StreamID) bool {
	for i := range s.window.done {
		if s.window.done[i] == id {
			return true
		}
	}
	return false
}

func (s *DemuxSource) markWindowStreamDone(id av.StreamID) {
	if !s.windowStreamDone(id) {
		s.window.done = append(s.window.done, id)
	}
}

// windowFinished reports whether every declared stream has passed the segment
// end. With no declared streams the first crossing finishes the window — there
// is no stream set to wait for.
func (s *DemuxSource) windowFinished() bool {
	if len(s.streams) == 0 {
		return true
	}
	for i := range s.streams {
		if !s.windowStreamDone(s.streams[i].ID) {
			return false
		}
	}
	return true
}

func (s *MuxStage) Handle(ctx context.Context, msg *pipeline.Message, emitter pipeline.Emitter) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return pipeline.ErrClosed
	}
	if msg == nil {
		return nil
	}
	switch msg.Kind {
	case pipeline.MessagePacket:
		if msg.Packet == nil {
			return nil
		}
		s.result.Reset()
		if err := s.muxer.Write(ctx, msg.Packet, &s.result); err != nil {
			return err
		}
		if err := s.emitEvents(ctx, emitter); err != nil {
			return err
		}
		return nil
	case pipeline.MessageEvent:
		return nil
	case pipeline.MessageFrame:
		return emitter.Emit(ctx, msg)
	default:
		return nil
	}
}

func (s *DemuxSource) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.demuxer.Close()
}

func (s *MuxStage) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	return s.muxer.Close()
}

func (s *MuxStage) MarkFailed() {
	if marker, ok := s.muxer.(interface{ MarkFailed() }); ok {
		marker.MarkFailed()
	}
}

func (s *DemuxSource) initStreamEvents(streams []av.Stream) {
	if len(streams) == 0 {
		return
	}
	s.streams = make([]av.Stream, len(streams))
	copy(s.streams, streams)
	s.events = make([]av.Event, len(streams))
	for i := range s.streams {
		stream := &s.streams[i]
		s.events[i] = av.Event{
			Type:     av.EventStreamAdded,
			StreamID: stream.ID,
			Epoch:    stream.Epoch,
			Stream:   stream,
		}
	}
}

func (s *DemuxSource) emitStreamEvents(ctx context.Context, emitter pipeline.Emitter) error {
	return s.emitEvents(ctx, emitter, s.events)
}

func (s *DemuxSource) emitEvents(ctx context.Context, emitter pipeline.Emitter, events []av.Event) error {
	for i := range events {
		s.message.Kind = pipeline.MessageEvent
		s.message.Packet = nil
		s.message.Frame = nil
		s.message.Event = &events[i]
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	return nil
}

func (s *DemuxSource) emitPacket(ctx context.Context, emitter pipeline.Emitter, packet *av.Packet) error {
	if packet == nil {
		return nil
	}
	s.message.Kind = pipeline.MessagePacket
	s.message.Packet = packet
	s.message.Frame = nil
	s.message.Event = nil
	return emitter.Emit(ctx, &s.message)
}

func (s *DemuxSource) emitEndOfStream(ctx context.Context, emitter pipeline.Emitter) error {
	s.eos.Reset()
	s.eos.Type = av.EventEndOfStream
	if len(s.streams) == 1 {
		s.eos.StreamID = s.streams[0].ID
		s.eos.Epoch = s.streams[0].Epoch
	}
	s.message.Kind = pipeline.MessageEvent
	s.message.Packet = nil
	s.message.Frame = nil
	s.message.Event = &s.eos
	return emitter.Emit(ctx, &s.message)
}

func (s *MuxStage) emitEvents(ctx context.Context, emitter pipeline.Emitter) error {
	for i := range s.result.Events {
		s.message.Kind = pipeline.MessageEvent
		s.message.Packet = nil
		s.message.Frame = nil
		s.message.Event = &s.result.Events[i]
		if err := emitter.Emit(ctx, &s.message); err != nil {
			return err
		}
	}
	return nil
}

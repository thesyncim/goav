package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/thesyncim/goav/control"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/webrtcav"
)

const (
	videoTapName          = "video.decoded"
	audioTapName          = "audio.decoded"
	decodedVideoMaxWidth  = 1920
	decodedVideoMaxHeight = 1080
	videoTapWidth         = 640
	videoTapHeight        = 360
	maxEvents             = 80
)

type session struct {
	id         string
	pc         *webrtc.PeerConnection
	runtime    *goav.Runtime
	browserURL string
	ctx        context.Context
	cancel     context.CancelFunc
	created    time.Time
	updated    time.Time

	signalMu sync.Mutex
	mu       sync.Mutex

	branches         map[string]*branch
	listeners        map[string]chan stateResponse
	events           []debugEvent
	revision         uint64
	eventSeq         uint64
	renegotiateSeq   uint64
	renegotiateKinds []string
	scenarios        []scenarioResult

	videoTask  goav.LiveTask
	audioTask  goav.LiveTask
	syncPolicy goav.SyncPolicy
	videoCodec string
	videoSSRC  uint32
	audioCodec string
	video      *videoAnalyzer
	audio      *audioAnalyzer
	native     nativeAudio
	lastError  string

	writeRTCP          func([]rtcp.Packet) error
	lastInboundPLI     time.Time
	inboundPLIInterval time.Duration
}

func (s *session) acceptTrack(track *webrtc.TrackRemote) {
	switch track.Kind() {
	case webrtc.RTPCodecTypeVideo:
		s.startVideoTrack(track)
	case webrtc.RTPCodecTypeAudio:
		s.startAudioTrack(track)
	}
}

func (s *session) startVideoTrack(track *webrtc.TrackRemote) {
	s.mu.Lock()
	if s.videoTask != nil {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	task, err := goav.From(goav.Input(webrtcav.Track(track, rtpav.WithFeedback(peerFeedback{s.pc})))).
		UseRuntime(s.runtime).
		Video().
		Decode().
		Shape(shape.Frame(av.MediaVideo, shape.Video(decodedVideoMaxWidth, decodedVideoMaxHeight, av.PixelFormatI420))).
		Resize(videoTapWidth, videoTapHeight).
		Shape(shape.Frame(av.MediaVideo, shape.Video(videoTapWidth, videoTapHeight, av.PixelFormatI420))).
		Tap(goav.FrameTap(videoTapName)).
		To(goav.Sink(s.video)).
		Build(s.ctx)
	if err != nil {
		s.setError(err.Error())
		return
	}

	s.mu.Lock()
	if s.videoTask != nil {
		s.mu.Unlock()
		_ = task.Close()
		return
	}
	s.videoTask = task
	s.videoCodec = codecName(track.Codec().MimeType)
	s.videoSSRC = uint32(track.SSRC())
	s.recordLocked("info", "track", "video decode task ready", "video", "", eventMeta("codec", s.videoCodec, "ssrc", fmt.Sprintf("%d", s.videoSSRC), "tap", videoTapName))
	for _, r := range s.sortedBranchesLocked("video") {
		if err := s.attachBranchLocked(s.ctx, r); err != nil {
			s.setErrorLocked(err.Error())
		}
	}
	s.mu.Unlock()

	go s.runTask("video", task)
}

func (s *session) startAudioTrack(track *webrtc.TrackRemote) {
	s.mu.Lock()
	if s.audioTask != nil {
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	task, err := goav.From(goav.Input(webrtcav.Track(track, rtpav.WithFeedback(peerFeedback{s.pc})))).
		UseRuntime(s.runtime).
		Audio().
		Decode().
		Shape(shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16))).
		Resample(48_000, codec.Stereo).
		Shape(shape.Frame(av.MediaAudio, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16))).
		Tap(goav.FrameTap(audioTapName)).
		To(goav.Sink(newAudioMonitorSink(s.audio, s.native))).
		Build(s.ctx)
	if err != nil {
		s.setError(err.Error())
		return
	}

	s.mu.Lock()
	if s.audioTask != nil {
		s.mu.Unlock()
		_ = task.Close()
		return
	}
	s.audioTask = task
	s.audioCodec = codecName(track.Codec().MimeType)
	s.recordLocked("info", "track", "audio decode task ready", "audio", "", eventMeta("codec", s.audioCodec, "tap", audioTapName))
	for _, r := range s.sortedBranchesLocked("audio") {
		if err := s.attachBranchLocked(s.ctx, r); err != nil {
			s.setErrorLocked(err.Error())
		}
	}
	s.mu.Unlock()

	go s.runTask("audio", task)
}

func (s *session) runTask(kind string, task goav.LiveTask) {
	s.record("info", "task", kind+" task running", kind, "", nil)
	go s.drainTaskEvents(kind, task)
	if err := task.Run(s.ctx); err != nil && !errors.Is(err, context.Canceled) {
		s.setError(kind + ": " + err.Error())
		return
	}
	s.record("info", "task", kind+" task stopped", kind, "", nil)
}

func (s *session) drainTaskEvents(kind string, task goav.Observable) {
	for {
		select {
		case event, ok := <-task.Events():
			if !ok {
				return
			}
			s.handleTaskEvent(kind, event)
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *session) handleTaskEvent(kind string, event av.Event) {
	if kind == "audio" && s.audio != nil {
		s.audio.observeEvent(&event)
	}
	if kind == "video" && event.Type == av.EventKeyframeRequired {
		s.requestInboundVideoKeyframe(event.Reason)
	}
	switch event.Type {
	case av.EventStats:
		return
	case av.EventPacketLoss, av.EventBitrateChanged, av.EventKeyframeRequired, av.EventDiscontinuity, av.EventStreamAdded, av.EventStreamRemoved, av.EventAttachError, av.EventEndOfStream:
		meta := eventMeta("type", string(event.Type), "stream", string(event.StreamID), "reason", event.Reason)
		if event.Cause != nil {
			meta["cause"] = event.Cause.Error()
		}
		level := "debug"
		if event.Type == av.EventAttachError {
			level = "error"
		}
		s.record(level, "media-event", string(event.Type), kind, "", meta)
	}
}

func (s *session) requestInboundVideoKeyframe(reason string) {
	s.mu.Lock()
	ssrc := s.videoSSRC
	interval := s.inboundPLIInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	now := time.Now()
	if ssrc == 0 || (!s.lastInboundPLI.IsZero() && now.Sub(s.lastInboundPLI) < interval) {
		s.mu.Unlock()
		return
	}
	s.lastInboundPLI = now
	writeRTCP := s.writeRTCP
	pc := s.pc
	s.mu.Unlock()

	packet := &rtcp.PictureLossIndication{MediaSSRC: ssrc}
	var err error
	if writeRTCP != nil {
		err = writeRTCP([]rtcp.Packet{packet})
	} else if pc != nil {
		err = pc.WriteRTCP([]rtcp.Packet{packet})
	}
	meta := eventMeta("ssrc", fmt.Sprintf("%d", ssrc), "rtcp", "pli")
	if reason != "" {
		meta["reason"] = reason
	}
	if err != nil {
		meta["cause"] = err.Error()
		s.record("error", "feedback", "browser keyframe request failed", "video", "", meta)
		return
	}
	s.record("debug", "feedback", "browser keyframe requested", "video", "", meta)
}

func (s *session) requestOutputVideoKeyframe(ctx context.Context, branchID, feedback string) {
	s.mu.Lock()
	r := s.branches[branchID]
	task := s.videoTask
	interval := s.inboundPLIInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	now := time.Now()
	if r == nil || r.Spec.Kind != "video" || task == nil || (!r.lastRTCPKeyframe.IsZero() && now.Sub(r.lastRTCPKeyframe) < interval) {
		s.mu.Unlock()
		return
	}
	r.lastRTCPKeyframe = now
	meta := branchMeta(r.Spec)
	meta["rtcp"] = feedback
	s.mu.Unlock()

	control := control.Keyframe(av.StreamID("")).AtTap(videoTapName)
	if err := task.Control(ctx, control); err != nil {
		meta["cause"] = err.Error()
		s.record("error", "feedback", "browser output keyframe failed", "video", branchID, meta)
		return
	}
	s.record("debug", "feedback", "browser output keyframe requested", "video", branchID, meta)
}

type peerFeedback struct {
	pc *webrtc.PeerConnection
}

func (f peerFeedback) WriteRTCP(ctx context.Context, packets []rtcp.Packet) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if f.pc == nil || len(packets) == 0 {
		return nil
	}
	return f.pc.WriteRTCP(packets)
}

func (s *session) State() stateResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stateLocked()
}

func (s *session) stateLocked() stateResponse {
	out := stateResponse{
		ID:         s.id,
		Revision:   s.revision,
		Created:    s.created,
		Updated:    s.updated,
		VideoCodec: s.videoCodec,
		AudioCodec: s.audioCodec,
		LastError:  s.lastError,
		BrowserURL: s.browserURL,
		Renegotiate: renegotiateCommand{
			Seq:   s.renegotiateSeq,
			Kinds: append([]string(nil), s.renegotiateKinds...),
		},
		Scenarios: append([]scenarioResult(nil), s.scenarios...),
	}
	for _, r := range s.sortedBranchesLocked("") {
		view := branchView{branchSpec: r.Spec, Bound: r.Attachment != nil, Paused: r.Paused, State: "waiting"}
		if r.Attachment != nil {
			view.State = fmt.Sprint(r.Attachment.Snapshot().State)
		}
		if r.Sink != nil {
			view.Packets = r.Sink.packets.Load()
			view.Bytes = r.Sink.bytes.Load()
		}
		out.Branches = append(out.Branches, view)
	}
	if s.videoTask != nil {
		out.VideoGraph = graphFromSpec("video", s.videoTask.Describe())
	}
	if s.audioTask != nil {
		out.AudioGraph = graphFromSpec("audio", s.audioTask.Describe())
	}
	out.Debug = s.debugLocked(out.VideoGraph, out.AudioGraph)
	if s.audio != nil {
		out.Audio = s.audio.view()
	}
	if s.video != nil {
		out.Video = s.video.view()
	}
	if s.native != nil {
		out.NativeAudio = s.native.Status()
	}
	out.Events = append([]debugEvent(nil), s.events...)
	return out
}

func (s *session) debugLocked(videoGraph, audioGraph graphView) runtimeDebugView {
	debug := runtimeDebugView{Revision: s.revision}
	video := s.taskDebugLocked("video", s.videoCodec, s.videoTask != nil, videoGraph, s.sortedBranchesLocked("video"))
	audio := s.taskDebugLocked("audio", s.audioCodec, s.audioTask != nil, audioGraph, s.sortedBranchesLocked("audio"))
	debug.Tasks = []taskDebugView{video, audio}
	for _, task := range debug.Tasks {
		debug.Totals.Packets += task.Packets
		debug.Totals.Bytes += task.Bytes
	}
	return debug
}

func (s *session) taskDebugLocked(kind, codec string, running bool, graph graphView, branches []*branch) taskDebugView {
	task := taskDebugView{
		Kind:  kind,
		Codec: codec,
		State: "waiting",
		Graph: graph.Stats,
	}
	if running {
		task.State = "running"
	}
	for _, r := range branches {
		if r.Attachment != nil {
			task.Attached = append(task.Attached, r.Spec.ID)
		} else {
			task.Waiting = append(task.Waiting, r.Spec.ID)
		}
		if r.Sink != nil {
			task.Packets += r.Sink.packets.Load()
			task.Bytes += r.Sink.bytes.Load()
		}
	}
	return task
}

func (s *session) subscribe() (<-chan stateResponse, func()) {
	id := randomID()
	ch := make(chan stateResponse, 4)

	s.mu.Lock()
	s.listeners[id] = ch
	snapshot := s.stateLocked()
	s.mu.Unlock()

	ch <- snapshot
	return ch, func() {
		s.mu.Lock()
		delete(s.listeners, id)
		close(ch)
		s.mu.Unlock()
	}
}

func (s *session) sortedBranchesLocked(kind string) []*branch {
	out := make([]*branch, 0, len(s.branches))
	for _, r := range s.branches {
		if kind == "" || r.Spec.Kind == kind {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Spec.Kind != out[j].Spec.Kind {
			return out[i].Spec.Kind < out[j].Spec.Kind
		}
		return out[i].Created.Before(out[j].Created)
	})
	return out
}

func (s *session) taskForKindLocked(kind string) goav.LiveTask {
	switch kind {
	case "video":
		return s.videoTask
	case "audio":
		return s.audioTask
	default:
		return nil
	}
}

func (s *session) queueRenegotiateLocked(kinds ...string) {
	seen := make(map[string]struct{}, len(s.renegotiateKinds)+len(kinds))
	next := make([]string, 0, len(s.renegotiateKinds)+len(kinds))
	for _, kind := range s.renegotiateKinds {
		if kind == "" {
			continue
		}
		if _, ok := seen[kind]; ok {
			continue
		}
		seen[kind] = struct{}{}
		next = append(next, kind)
	}
	for _, kind := range kinds {
		if kind == "" {
			continue
		}
		if _, ok := seen[kind]; ok {
			continue
		}
		seen[kind] = struct{}{}
		next = append(next, kind)
	}
	s.renegotiateSeq++
	s.renegotiateKinds = next
}

func (s *session) clearRenegotiate() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renegotiateKinds = nil
}

func (s *session) requestKeyframe(ctx context.Context, kind string) error {
	s.mu.Lock()
	task := s.taskForKindLocked(kind)
	s.mu.Unlock()
	if task == nil {
		return fmt.Errorf("%s task is not running", kind)
	}
	if err := task.Control(ctx, control.Keyframe(av.StreamID(""))); err != nil {
		return err
	}
	s.record("info", "control", "keyframe requested", kind, "", nil)
	return nil
}

func (s *session) setScenarios(results []scenarioResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scenarios = append([]scenarioResult(nil), results...)
	s.recordLocked("info", "scenario", "planner scenarios refreshed", "", "", eventMeta("count", fmt.Sprintf("%d", len(results))))
}

func (s *session) setError(message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setErrorLocked(message)
}

func (s *session) setErrorLocked(message string) {
	s.lastError = message
	log.Printf("session %s: %s", s.id, message)
	s.recordLocked("error", "error", message, "", "", nil)
}

func (s *session) record(level, kind, message, stream, branch string, meta map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recordLocked(level, kind, message, stream, branch, meta)
}

func (s *session) recordLocked(level, kind, message, stream, branch string, meta map[string]string) {
	now := time.Now()
	s.revision++
	s.eventSeq++
	s.updated = now
	if level == "" {
		level = "info"
	}
	event := debugEvent{
		Seq:     s.eventSeq,
		Time:    now,
		Level:   level,
		Kind:    kind,
		Message: message,
		Stream:  stream,
		Branch:  branch,
		Meta:    meta,
	}
	s.events = append(s.events, event)
	if len(s.events) > maxEvents {
		s.events = append([]debugEvent(nil), s.events[len(s.events)-maxEvents:]...)
	}
	s.publishLocked()
}

func (s *session) publishLocked() {
	if len(s.listeners) == 0 {
		return
	}
	snapshot := s.stateLocked()
	for _, ch := range s.listeners {
		select {
		case ch <- snapshot:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- snapshot:
			default:
			}
		}
	}
}

func (s *session) Close() {
	s.cancel()
	s.mu.Lock()
	if s.videoTask != nil {
		_ = s.videoTask.Close()
	}
	if s.audioTask != nil {
		_ = s.audioTask.Close()
	}
	if s.native != nil {
		_ = s.native.Close()
	}
	s.recordLocked("info", "session", "session closed", "", "", nil)
	s.mu.Unlock()
	_ = s.pc.Close()
}

func codecName(mime string) string {
	parts := strings.Split(mime, "/")
	if len(parts) == 2 {
		return strings.ToLower(parts[1])
	}
	return strings.ToLower(mime)
}

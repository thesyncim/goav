package main

import (
	"context"
	"errors"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav"
)

const (
	videoTapName = "video.decoded"
	audioTapName = "audio.decoded"
	maxEvents    = 80
)

type session struct {
	id      string
	pc      *webrtc.PeerConnection
	runtime goav.Runtime
	ctx     context.Context
	cancel  context.CancelFunc
	created time.Time
	updated time.Time

	signalMu sync.Mutex
	mu       sync.Mutex

	branches  map[string]*branch
	listeners map[string]chan stateResponse
	events    []debugEvent
	revision  uint64
	eventSeq  uint64

	videoTask  goav.Task
	audioTask  goav.Task
	videoCodec string
	audioCodec string
	lastError  string
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

	task, err := goav.From(goav.WebRTCTrack(track).Feedback(peerFeedback{s.pc})).
		UseRuntime(s.runtime).
		Video().
		Decode().
		Tap(goav.FrameTap(videoTapName)).
		To(goav.Sink(discardSink("video-decoded"))).
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
	s.recordLocked("info", "track", "video decode task ready", "video", "", eventMeta("codec", s.videoCodec, "tap", videoTapName))
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

	task, err := goav.From(goav.WebRTCTrack(track).Feedback(peerFeedback{s.pc})).
		UseRuntime(s.runtime).
		Audio().
		Decode().
		Tap(goav.FrameTap(audioTapName)).
		To(goav.Sink(discardSink("audio-decoded"))).
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

func (s *session) runTask(kind string, task goav.Task) {
	s.record("info", "task", kind+" task running", kind, "", nil)
	go s.drainTaskEvents(task)
	if err := task.Run(s.ctx); err != nil && !errors.Is(err, context.Canceled) {
		s.setError(kind + ": " + err.Error())
		return
	}
	s.record("info", "task", kind+" task stopped", kind, "", nil)
}

func (s *session) drainTaskEvents(task goav.Task) {
	for {
		select {
		case _, ok := <-task.Events():
			if !ok {
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
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
	}
	for _, r := range s.sortedBranchesLocked("") {
		view := branchView{branchSpec: r.Spec, Bound: r.Attachment != nil}
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

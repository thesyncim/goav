package main

import (
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/thesyncim/goav/graphrender"
)

// --- State, Graph, Devices & Events ---

func (s *studio) Snapshot() studioState {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

func (s *studio) snapshotLocked() studioState {
	branches := make([]branchView, 0, len(s.branches))
	for _, b := range s.sortedBranchesLocked() {
		branches = append(branches, branchView{
			branchSpec: b.spec,
			Packets:    b.sink.packets.Load(),
			Bytes:      b.sink.bytes.Load(),
			Kbps:       b.sink.kbps(),
			Bound:      b.attachment != nil,
			Paused:     b.paused,
		})
	}

	var graph graphView
	mermaid := "flowchart LR\n  idle[Studio Idle]"
	if s.task != nil {
		snap := s.task.Snapshot()
		for _, n := range snap.Spec.Nodes {
			graph.Nodes = append(graph.Nodes, nodeView{Name: n.Name, Kind: string(n.Kind), Detail: n.Detail})
		}
		for _, e := range snap.Spec.Edges {
			graph.Edges = append(graph.Edges, edgeView{From: string(e.From), To: string(e.To)})
		}
		if rendered, err := graphrender.RenderSnapshotFlowchart(snap); err == nil {
			mermaid = rendered
		}
	}

	rec := s.recordingStatusLocked()
	hlsSt := hlsStatus{
		Active:  s.hls.active,
		Bitrate: s.hls.bitrate,
	}
	if s.hls.active {
		hlsSt.URL = s.baseURL + "/hls/live.m3u8"
	}
	whipSt := whipStatus{
		Active:  s.whip.active,
		URL:     s.whip.url,
		Codec:   s.whip.codec,
		Bitrate: s.whip.bitrate,
	}

	devs := devicesView{
		Cameras:      s.cachedCams,
		Displays:     s.cachedDisps,
		CameraAccess: s.cameraAccess,
		ScreenAccess: s.cachedScreen,
		Platform:     runtime.GOOS + "/" + runtime.GOARCH,
	}

	events := append([]studioEvent(nil), s.events...)
	return studioState{
		Revision:   s.revision,
		BaseURL:    s.baseURL,
		WatchURL:   s.baseURL + "/watch",
		Config:     s.cfg,
		Program:    s.monitor.snapshot(),
		Devices:    devs,
		Branches:   branches,
		Viewers:    len(s.viewers),
		Recording:  rec,
		HLS:        hlsSt,
		WHIP:       whipSt,
		Graph:      graph,
		Code:       generateCode(s.cfg, branches, rec, hlsSt, whipSt, mermaid),
		LastError:  s.lastError,
		Events:     events,
		RenegSeq:   s.renegSeq,
		TaskActive: s.task != nil,
	}
}

func (s *studio) recordingStatusLocked() recordingStatus {
	if !s.recording.active {
		return recordingStatus{}
	}
	var size int64
	if info, err := os.Stat(s.recording.path); err == nil {
		size = info.Size()
	}
	return recordingStatus{
		Active:    true,
		Format:    s.recording.format,
		Codec:     s.recording.codec,
		Bitrate:   s.recording.bitrate,
		Filename:  s.recording.filename,
		Path:      s.recording.path,
		Bytes:     size,
		StartedAt: s.recording.startedAt.Format(time.RFC3339),
	}
}

func (s *studio) sortedBranchesLocked() []*studioBranch {
	out := make([]*studioBranch, 0, len(s.branches))
	for _, b := range s.branches {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].spec.ID < out[j].spec.ID })
	return out
}

func (s *studio) recordEventLocked(level, kind, msg string) {
	s.eventSeq++
	s.events = append(s.events, studioEvent{
		Seq:     s.eventSeq,
		Time:    time.Now().Format("15:04:05"),
		Level:   level,
		Kind:    kind,
		Message: msg,
	})
	if len(s.events) > maxStudioEvents {
		s.events = s.events[len(s.events)-maxStudioEvents:]
	}
}

func (s *studio) subscribeState() (chan studioState, func()) {
	ch := make(chan studioState, 4)
	s.mu.Lock()
	s.listeners[ch] = struct{}{}
	snap := s.snapshotLocked()
	s.mu.Unlock()
	ch <- snap
	return ch, func() {
		s.mu.Lock()
		delete(s.listeners, ch)
		s.mu.Unlock()
	}
}

func (s *studio) notifyLocked() {
	s.revision++
	if len(s.listeners) == 0 {
		return
	}
	snap := s.snapshotLocked()
	for ch := range s.listeners {
		select {
		case ch <- snap:
		default:
			select {
			case <-ch:
			default:
			}
			select {
			case ch <- snap:
			default:
			}
		}
	}
}

// --- Helpers ---

func normalizeBranchSpec(spec *branchSpec, seq *atomic.Uint64) error {
	spec.Codec = strings.ToLower(strings.TrimSpace(spec.Codec))
	if spec.Codec == "" {
		spec.Codec = "vp8"
	}
	switch spec.Codec {
	case "vp8", "vp9", "av1":
	default:
		return fmt.Errorf("unsupported codec %q (pure-Go studio supports vp8, vp9, av1)", spec.Codec)
	}
	if spec.Width <= 0 {
		spec.Width = 640
	}
	if spec.Height <= 0 {
		spec.Height = 360
	}
	spec.Width &^= 1
	spec.Height &^= 1
	if spec.FPS <= 0 {
		spec.FPS = 30
	}
	if spec.Bitrate <= 0 {
		spec.Bitrate = 700_000
	}
	if strings.TrimSpace(spec.ID) == "" {
		spec.ID = fmt.Sprintf("%s-%dp-%d", spec.Codec, spec.Height, seq.Add(1))
	}
	return nil
}

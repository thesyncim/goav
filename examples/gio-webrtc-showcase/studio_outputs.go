package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/hls"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/flow"
)

// --- WebRTC Simulcast / Multi-Codec Branches ---

func (s *studio) AddBranch(ctx context.Context, spec branchSpec) (branchSpec, error) {
	if err := normalizeBranchSpec(&spec, &s.branchSeq); err != nil {
		return spec, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.branches[spec.ID]; exists {
		return spec, fmt.Errorf("branch %q already exists", spec.ID)
	}
	track, err := webrtc.NewTrackLocalStaticSample(codecCapability(spec.Codec), spec.ID, spec.ID)
	if err != nil {
		return spec, err
	}
	b := &studioBranch{
		spec:  spec,
		track: track,
		sink:  newWebRTCTrackSink(spec, track),
	}
	s.branches[spec.ID] = b
	if err := s.attachBranchLocked(ctx, b); err != nil {
		delete(s.branches, spec.ID)
		return spec, err
	}
	s.syncViewerTracksLocked()
	s.recordEventLocked("info", "branch", fmt.Sprintf("hot-attached %s (%s %dx%d @ %dkbps)", spec.ID, strings.ToUpper(spec.Codec), spec.Width, spec.Height, spec.Bitrate/1000))
	s.notifyLocked()
	return spec, nil
}

func (s *studio) RemoveBranch(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.branches[id]
	if !ok {
		return fmt.Errorf("branch %q not found", id)
	}
	if b.attachment != nil && s.task != nil {
		_ = s.task.Detach(ctx, b.attachment)
		b.attachment = nil
	}
	delete(s.branches, id)
	s.syncViewerTracksLocked()
	s.recordEventLocked("info", "branch", fmt.Sprintf("detached branch %s", id))
	s.notifyLocked()
	return nil
}

func (s *studio) PauseBranch(ctx context.Context, id string, pause bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.branches[id]
	if !ok {
		return fmt.Errorf("branch %q not found", id)
	}
	if b.attachment == nil {
		return fmt.Errorf("branch %q not attached", id)
	}
	var err error
	if pause {
		err = b.attachment.Pause(ctx)
	} else {
		err = b.attachment.Resume(ctx)
	}
	if err != nil {
		return err
	}
	b.paused = pause
	action := "resumed"
	if pause {
		action = "paused"
	}
	s.recordEventLocked("info", "branch", fmt.Sprintf("%s %s", action, id))
	s.notifyLocked()
	return nil
}

func (s *studio) Rebranch(ctx context.Context, id string, width, height, bitrate int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.branches[id]
	if !ok {
		return fmt.Errorf("branch %q not found", id)
	}
	if s.task == nil || b.attachment == nil {
		return fmt.Errorf("branch %q not attached", id)
	}
	old := b.spec
	oldSink := b.sink
	oldGen := b.gen
	if width >= 160 && height >= 90 {
		b.spec.Width = width &^ 1
		b.spec.Height = height &^ 1
	}
	if bitrate >= 100_000 {
		b.spec.Bitrate = bitrate
	}
	b.gen++
	b.sink = newWebRTCTrackSink(b.spec, b.track)
	b.sink.name = fmt.Sprintf("webrtc-%s-g%d", b.spec.ID, b.gen)
	out, err := buildBranchOutput(s.task, b)
	if err != nil {
		b.spec = old
		b.sink = oldSink
		b.gen = oldGen
		return err
	}
	next, err := b.attachment.Rebranch(ctx, out, goav.SwitchAt(goav.AtMediaTime(0)), goav.DrainBranch())
	if err != nil {
		b.spec = old
		b.sink = oldSink
		b.gen = oldGen
		return err
	}
	b.attachment = next
	b.paused = false
	s.recordEventLocked("info", "branch", fmt.Sprintf("rebranched %s -> %dx%d @ %dkbps", id, b.spec.Width, b.spec.Height, b.spec.Bitrate/1000))
	s.notifyLocked()
	return nil
}

func (s *studio) SetBitrate(ctx context.Context, id string, bitrate int) error {
	if bitrate < 50_000 {
		return fmt.Errorf("bitrate must be >= 50000 bps")
	}
	s.mu.Lock()
	b, ok := s.branches[id]
	task := s.task
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("branch %q not found", id)
	}
	b.spec.Bitrate = bitrate
	s.mu.Unlock()
	if task == nil {
		return fmt.Errorf("pipeline is not running")
	}
	ctrl, err := control.SetBitrate(av.StreamID(""), bitrate)
	if err != nil {
		return err
	}
	if err := task.Control(ctx, ctrl.AtTap(programTapName)); err != nil {
		return err
	}
	s.mu.Lock()
	s.recordEventLocked("info", "control", fmt.Sprintf("bitrate %s -> %dkbps", id, bitrate/1000))
	s.notifyLocked()
	s.mu.Unlock()
	return nil
}

func (s *studio) attachBranchLocked(ctx context.Context, b *studioBranch) error {
	if s.task == nil {
		return nil
	}
	out, err := buildBranchOutput(s.task, b)
	if err != nil {
		return err
	}
	att, err := s.task.Attach(ctx, out)
	if err != nil {
		return err
	}
	b.attachment = att
	b.paused = false
	return nil
}

func buildBranchOutput(task *goav.Task, b *studioBranch) (goav.Output, error) {
	enc, err := buildCodecSpec(b.spec.Codec, b.spec.Bitrate, b.spec.FPS)
	if err != nil {
		return goav.Output{}, err
	}
	nodeLabel := b.spec.ID
	if b.gen > 0 {
		nodeLabel = fmt.Sprintf("%s-g%d", b.spec.ID, b.gen)
	}
	return task.Tap(programTapName).
		Buffer(flow.DropOldest(branchBufferDepth)).
		Resize(b.spec.Width, b.spec.Height).
		Encode(enc).
		As(nodeLabel).
		To(goav.Sink(b.sink)), nil
}

// --- Simultaneous Local Recording ---

func (s *studio) StartRecording(ctx context.Context, format, codecName string, bitrate int) (recordingStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.recording.active {
		return s.recordingStatusLocked(), fmt.Errorf("recording already active")
	}
	if s.task == nil {
		return recordingStatus{}, fmt.Errorf("pipeline not running")
	}
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "webm":
		format = "webm"
	case "mkv":
		format = "mkv"
	case "mp4":
		format = "mp4"
	default:
		return recordingStatus{}, fmt.Errorf("unsupported recording format %q (expected webm, mkv, or mp4)", format)
	}
	if codecName == "" {
		codecName = "vp9"
		if format == "mp4" {
			codecName = "vp9"
		}
	}
	if bitrate <= 0 {
		bitrate = 1_800_000
	}
	enc, err := buildCodecSpec(codecName, bitrate, s.cfg.FPS)
	if err != nil {
		return recordingStatus{}, err
	}
	recDir := recordingDir()
	if err := os.MkdirAll(recDir, 0o755); err != nil {
		return recordingStatus{}, err
	}
	filename := fmt.Sprintf("goav-studio-%s.%s", time.Now().Format("20060102-150405"), format)
	fullPath := filepath.Join(recDir, filename)
	f, err := os.Create(fullPath)
	if err != nil {
		return recordingStatus{}, err
	}

	s.recGen++
	out := s.task.Tap(programTapName).
		Buffer(flow.DropOldest(branchBufferDepth)).
		Encode(enc).
		As(fmt.Sprintf("recording-%d", s.recGen)).
		To(goav.Write(filename, f))
	att, err := s.task.Attach(ctx, out)
	if err != nil {
		_ = f.Close()
		_ = os.Remove(fullPath)
		return recordingStatus{}, err
	}
	s.recording = recordingState{
		active:     true,
		format:     format,
		codec:      codecName,
		bitrate:    bitrate,
		filename:   filename,
		path:       fullPath,
		file:       f,
		attachment: att,
		startedAt:  time.Now(),
	}
	s.recordEventLocked("info", "record", fmt.Sprintf("recording started: %s (%s)", fullPath, strings.ToUpper(codecName)))
	s.notifyLocked()
	return s.recordingStatusLocked(), nil
}

func recordingDir() string {
	if dir := os.Getenv("GOAV_STUDIO_RECORDINGS_DIR"); dir != "" {
		return dir
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, "Movies", "goav-studio")
	}
	return filepath.Join(os.TempDir(), "goav-studio-recordings")
}

func (s *studio) StopRecording(ctx context.Context) (recordingStatus, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.recording.active {
		return recordingStatus{}, nil
	}
	final := s.recordingStatusLocked()
	if s.task != nil && s.recording.attachment != nil {
		_ = s.task.Detach(ctx, s.recording.attachment)
	}
	if s.recording.file != nil {
		_ = s.recording.file.Close()
	}
	if info, err := os.Stat(final.Path); err == nil {
		final.Bytes = info.Size()
	}
	final.Active = false
	s.recording = recordingState{}
	s.recordEventLocked("info", "record", fmt.Sprintf("recording saved: %s (%d KB)", final.Path, final.Bytes/1024))
	s.notifyLocked()
	return final, nil
}

// --- Live LL-HLS Origin ---

func (s *studio) ToggleHLS(ctx context.Context, enable bool, bitrate int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !enable {
		if s.hls.active && s.task != nil && s.hls.attachment != nil {
			_ = s.task.Detach(ctx, s.hls.attachment)
		}
		s.hls.active = false
		s.hls.attachment = nil
		s.recordEventLocked("info", "hls", "LL-HLS origin stopped")
		s.notifyLocked()
		return nil
	}
	if s.hls.active {
		return nil
	}
	if bitrate <= 0 {
		bitrate = 1_200_000
	}
	s.hls.bitrate = bitrate
	s.hls.server = s.hlsSrv
	if err := s.attachHLSLocked(ctx); err != nil {
		return err
	}
	s.hls.active = true
	s.recordEventLocked("info", "hls", fmt.Sprintf("LL-HLS live at %s/hls/live.m3u8 (VP9, 250ms parts)", s.baseURL))
	s.notifyLocked()
	return nil
}

func (s *studio) attachHLSLocked(ctx context.Context) error {
	if s.task == nil {
		return nil
	}
	s.hlsGen++
	out := s.task.Tap(programTapName).
		Buffer(flow.DropOldest(branchBufferDepth)).
		Encode(codec.VP9(codec.Bitrate(s.hls.bitrate), codec.FPS(s.cfg.FPS), codec.KeyframeInterval(max(15, s.cfg.FPS)))).
		As(fmt.Sprintf("ll-hls-%d", s.hlsGen)).
		To(goav.Custom(hls.New("live.m3u8", s.hlsSrv, hls.Parts(250*time.Millisecond), hls.Window(6))))
	att, err := s.task.Attach(ctx, out)
	if err != nil {
		return err
	}
	s.hls.attachment = att
	return nil
}

// --- WHIP Live Push ---

func (s *studio) StartWHIP(ctx context.Context, endpoint, token, codecName string, bitrate int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.whip.active {
		return fmt.Errorf("WHIP publish already active")
	}
	if strings.TrimSpace(endpoint) == "" {
		return fmt.Errorf("WHIP URL is required (e.g. whips://... or whip+http://...)")
	}
	if codecName == "" {
		codecName = "vp8"
	}
	if bitrate <= 0 {
		bitrate = 1_200_000
	}
	s.whip = whipState{
		active:  true,
		url:     strings.TrimSpace(endpoint),
		token:   strings.TrimSpace(token),
		codec:   codecName,
		bitrate: bitrate,
	}
	if err := s.attachWHIPLocked(ctx); err != nil {
		s.whip = whipState{}
		return err
	}
	s.recordEventLocked("info", "whip", fmt.Sprintf("WHIP publishing to %s (%s)", s.whip.url, strings.ToUpper(codecName)))
	s.notifyLocked()
	return nil
}

func (s *studio) StopWHIP(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.whip.active {
		return nil
	}
	if s.task != nil && s.whip.attachment != nil {
		_ = s.task.Detach(ctx, s.whip.attachment)
	}
	s.whip = whipState{}
	s.recordEventLocked("info", "whip", "WHIP publish stopped")
	s.notifyLocked()
	return nil
}

func (s *studio) attachWHIPLocked(ctx context.Context) error {
	if s.task == nil {
		return nil
	}
	enc, err := buildCodecSpec(s.whip.codec, s.whip.bitrate, s.cfg.FPS)
	if err != nil {
		return err
	}
	s.whipGen++
	out := s.task.Tap(programTapName).
		Buffer(flow.DropOldest(branchBufferDepth)).
		Encode(enc).
		As(fmt.Sprintf("whip-push-%d", s.whipGen)).
		To(goav.ToURI(s.whip.url))
	att, err := s.task.Attach(ctx, out)
	if err != nil {
		return err
	}
	s.whip.attachment = att
	return nil
}

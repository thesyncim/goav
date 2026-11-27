package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/cameraav"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/container/hls"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/examples/gio-webrtc-showcase/screenav"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/graphrender"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/runconfig"
	"github.com/thesyncim/goav/webrtcav"
)

const (
	programTapName    = "program"
	branchBufferDepth = 8
	maxStudioEvents   = 60
)

type studioConfig struct {
	Scene         string `json:"scene"`         // pattern | camera | screen | pip | sidebyside
	PiPCorner     string `json:"pipCorner"`     // br | bl | tr | tl
	Width         int    `json:"width"`         // canvas width (default 960)
	Height        int    `json:"height"`        // canvas height (default 540)
	FPS           int    `json:"fps"`           // default 30
	CameraID      string `json:"cameraId"`      // empty = default camera
	CameraWidth   int    `json:"cameraWidth"`   // default 640
	CameraHeight  int    `json:"cameraHeight"`  // default 480
	DisplayID     uint32 `json:"displayId"`     // 0 = main display
	CaptureCursor bool   `json:"captureCursor"` // default true
}

type branchSpec struct {
	ID      string `json:"id"`
	Codec   string `json:"codec"` // vp8 | vp9 | av1 (pure Go, no H.264)
	Width   int    `json:"width"`
	Height  int    `json:"height"`
	FPS     int    `json:"fps"`
	Bitrate int    `json:"bitrate"`
}

type branchView struct {
	branchSpec
	Packets uint64  `json:"packets"`
	Bytes   uint64  `json:"bytes"`
	Kbps    float64 `json:"kbps"`
	Bound   bool    `json:"bound"`
	Paused  bool    `json:"paused"`
}

type recordingStatus struct {
	Active    bool   `json:"active"`
	Format    string `json:"format,omitempty"`
	Codec     string `json:"codec,omitempty"`
	Bitrate   int    `json:"bitrate,omitempty"`
	Filename  string `json:"filename,omitempty"`
	Path      string `json:"path,omitempty"`
	Bytes     int64  `json:"bytes,omitempty"`
	StartedAt string `json:"startedAt,omitempty"`
}

type hlsStatus struct {
	Active  bool   `json:"active"`
	URL     string `json:"url,omitempty"`
	Bitrate int    `json:"bitrate,omitempty"`
}

type whipStatus struct {
	Active  bool   `json:"active"`
	URL     string `json:"url,omitempty"`
	Codec   string `json:"codec,omitempty"`
	Bitrate int    `json:"bitrate,omitempty"`
}

type studioEvent struct {
	Seq     uint64 `json:"seq"`
	Time    string `json:"time"`
	Level   string `json:"level"`
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

type nodeView struct {
	Name   string `json:"name"`
	Kind   string `json:"kind"`
	Detail string `json:"detail,omitempty"`
}

type edgeView struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type graphView struct {
	Nodes []nodeView `json:"nodes"`
	Edges []edgeView `json:"edges"`
}

type devicesView struct {
	Cameras      []cameraav.Device  `json:"cameras"`
	Displays     []screenav.Display `json:"displays"`
	CameraAccess bool               `json:"cameraAccess"`
	ScreenAccess bool               `json:"screenAccess"`
	Platform     string             `json:"platform"`
}

type studioState struct {
	Revision   uint64          `json:"revision"`
	BaseURL    string          `json:"baseUrl"`
	WatchURL   string          `json:"watchUrl"`
	Config     studioConfig    `json:"config"`
	Program    programMetrics  `json:"program"`
	Devices    devicesView     `json:"devices"`
	Branches   []branchView    `json:"branches"`
	Viewers    int             `json:"viewers"`
	Recording  recordingStatus `json:"recording"`
	HLS        hlsStatus       `json:"hls"`
	WHIP       whipStatus      `json:"whip"`
	Graph      graphView       `json:"graph"`
	Code       codeBundle      `json:"code"`
	LastError  string          `json:"lastError,omitempty"`
	Events     []studioEvent   `json:"events"`
	RenegSeq   uint64          `json:"renegSeq"`
	TaskActive bool            `json:"taskActive"`
}

type studioBranch struct {
	spec       branchSpec
	track      *webrtc.TrackLocalStaticSample
	attachment goav.Attachment
	sink       *webrtcTrackSink
	paused     bool
	gen        uint64
}

type recordingState struct {
	active     bool
	format     string
	codec      string
	bitrate    int
	filename   string
	path       string
	file       *os.File
	attachment goav.Attachment
	startedAt  time.Time
}

type hlsState struct {
	active     bool
	bitrate    int
	server     *hls.Server
	attachment goav.Attachment
}

type whipState struct {
	active     bool
	url        string
	token      string
	codec      string
	bitrate    int
	attachment goav.Attachment
}

type viewerPeer struct {
	id      string
	pc      *webrtc.PeerConnection
	senders map[string]*webrtc.RTPSender // branchID -> sender
}

type studio struct {
	runtime *goav.Runtime
	api     *webrtc.API
	baseURL string
	monitor *programMonitor
	hlsSrv  *hls.Server

	mu         sync.Mutex
	cfg        studioConfig
	task       *goav.Task
	taskCancel context.CancelFunc
	taskDone   chan struct{}

	branches  map[string]*studioBranch
	viewers   map[string]*viewerPeer
	recording recordingState
	hls       hlsState
	whip      whipState

	revision     uint64
	renegSeq     uint64
	eventSeq     uint64
	events       []studioEvent
	lastError    string
	cameraAccess bool
	builtCamOK   bool
	builtScrOK   bool
	cachedCams   []cameraav.Device
	cachedDisps  []screenav.Display
	cachedScreen bool
	camModes     map[string][3]int
	listeners    map[chan studioState]struct{}
	branchSeq    atomic.Uint64
	recGen       uint64
	hlsGen       uint64
	whipGen      uint64
}

func newStudio(baseURL string) *studio {
	rt := bundle.MustNew(
		runconfig.WithEventCapacity(2048),
		runconfig.WithScheme("whip", webrtcav.WHIP()),
		runconfig.WithScheme("whips", webrtcav.WHIP()),
		runconfig.WithScheme("whip+http", webrtcav.WHIP()),
	)

	me := &webrtc.MediaEngine{}
	for _, cap := range []webrtc.RTPCodecParameters{
		{
			RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000},
			PayloadType:        96,
		},
		{
			RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP9, ClockRate: 90000},
			PayloadType:        98,
		},
		{
			RTPCodecCapability: webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeAV1, ClockRate: 90000},
			PayloadType:        45,
		},
	} {
		_ = me.RegisterCodec(cap, webrtc.RTPCodecTypeVideo)
	}
	ir := &interceptor.Registry{}
	_ = webrtc.RegisterDefaultInterceptors(me, ir)
	api := webrtc.NewAPI(webrtc.WithMediaEngine(me), webrtc.WithInterceptorRegistry(ir))

	st := &studio{
		runtime: rt,
		api:     api,
		baseURL: baseURL,
		monitor: newProgramMonitor(),
		hlsSrv:  hls.NewServer(),
		cfg: studioConfig{
			Scene:         "pattern",
			PiPCorner:     "br",
			Width:         960,
			Height:        540,
			FPS:           30,
			CameraWidth:   1280,
			CameraHeight:  720,
			CaptureCursor: true,
		},
		branches:  make(map[string]*studioBranch),
		viewers:   make(map[string]*viewerPeer),
		camModes:  make(map[string][3]int),
		listeners: make(map[chan studioState]struct{}),
	}

	// Default starter WebRTC simulcast ladder: pure-Go VP8 HD + VP8 SD.
	st.addBranchInitial(branchSpec{ID: "vp8-hd", Codec: "vp8", Width: 960, Height: 540, FPS: 30, Bitrate: 900_000})
	st.addBranchInitial(branchSpec{ID: "vp8-sd", Codec: "vp8", Width: 480, Height: 270, FPS: 30, Bitrate: 350_000})
	st.refreshDevices()

	return st
}

func (s *studio) refreshDevices() {
	cams, _ := cameraav.Devices(context.Background())
	disps, _ := screenav.Displays(context.Background())
	scr := screenav.HasAccess()
	s.mu.Lock()
	s.cachedCams = cams
	s.cachedDisps = disps
	s.cachedScreen = scr
	s.mu.Unlock()
}

func (s *studio) addBranchInitial(spec branchSpec) {
	track, err := webrtc.NewTrackLocalStaticSample(codecCapability(spec.Codec), spec.ID, spec.ID)
	if err != nil {
		return
	}
	s.branches[spec.ID] = &studioBranch{
		spec:  spec,
		track: track,
		sink:  newWebRTCTrackSink(spec, track),
	}
}

// currentTask returns the active goav.Task pointer (for tests).
func (s *studio) currentTask() *goav.Task {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.task
}

// Start builds and starts the single unified goav.Select studio Task.
func (s *studio) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rebuildTaskLocked(ctx)
}

// Close shuts down the studio task, recording, and viewers cleanly.
func (s *studio) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopTaskLocked()
	for id, v := range s.viewers {
		_ = v.pc.Close()
		delete(s.viewers, id)
	}
}

func (s *studio) stopTaskLocked() {
	if s.recording.active {
		if s.task != nil && s.recording.attachment != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = s.task.Detach(ctx, s.recording.attachment)
			cancel()
		}
		if s.recording.file != nil {
			_ = s.recording.file.Close()
		}
		s.recording = recordingState{}
	}
	for _, b := range s.branches {
		b.attachment = nil
		b.paused = false
	}
	s.hls.attachment = nil
	s.whip.attachment = nil
	if s.taskCancel != nil {
		s.taskCancel()
	}
	done := s.taskDone
	task := s.task
	s.task = nil
	s.taskCancel = nil
	s.taskDone = nil
	if task != nil {
		s.mu.Unlock()
		if done != nil {
			select {
			case <-done:
			case <-time.After(3 * time.Second):
			}
		}
		_ = task.Close()
		s.mu.Lock()
	}
}

func (s *studio) rebuildTaskLocked(ctx context.Context) error {
	s.stopTaskLocked()
	s.monitor.reset()

	if s.cameraAccess {
		if mode, ok := s.camModes[s.cfg.CameraID]; ok {
			s.cfg.CameraWidth, s.cfg.CameraHeight = mode[0], mode[1]
		} else if w, h, fps, ok := detectCameraMode(context.Background(), s.cfg.CameraID); ok {
			s.camModes[s.cfg.CameraID] = [3]int{w, h, fps}
			s.cfg.CameraWidth, s.cfg.CameraHeight = w, h
		}
	}

	output, err := s.buildStudioOutputLocked()
	if err != nil {
		s.lastError = err.Error()
		s.recordEventLocked("error", "build", err.Error())
		s.notifyLocked()
		return err
	}

	taskCtx, cancel := context.WithCancel(context.Background())
	task, err := s.runtime.Build(taskCtx, output)
	if err != nil {
		cancel()
		s.lastError = err.Error()
		s.recordEventLocked("error", "build", err.Error())
		s.notifyLocked()
		return err
	}

	s.task = task
	s.taskCancel = cancel
	s.taskDone = make(chan struct{})
	s.builtCamOK = s.cameraAccess
	s.builtScrOK = s.cachedScreen
	s.lastError = ""

	go func(t *goav.Task, c context.Context, done chan struct{}) {
		defer close(done)
		if err := t.Run(c); err != nil && !errors.Is(err, context.Canceled) {
			s.mu.Lock()
			if s.task == t {
				s.lastError = err.Error()
				s.recordEventLocked("error", "task", "pipeline stopped: "+err.Error())
				s.notifyLocked()
			}
			s.mu.Unlock()
		}
	}(task, taskCtx, s.taskDone)

	// Select the active arm immediately via control.SelectActive.
	if s.cfg.Scene != "" && s.cfg.Scene != "pattern" {
		s.selectArmAsync(task, s.cfg.Scene)
	}

	// Attach all WebRTC branches, HLS, and WHIP to the unified Tap("program").
	for _, b := range s.sortedBranchesLocked() {
		if err := s.attachBranchLocked(ctx, b); err != nil {
			s.recordEventLocked("error", "branch", fmt.Sprintf("attach %s: %v", b.spec.ID, err))
		}
	}
	if s.hls.active {
		if err := s.attachHLSLocked(ctx); err != nil {
			s.hls.active = false
			s.recordEventLocked("error", "hls", err.Error())
		}
	}
	if s.whip.active {
		if err := s.attachWHIPLocked(ctx); err != nil {
			s.whip.active = false
			s.recordEventLocked("error", "whip", err.Error())
		}
	}

	s.recordEventLocked("info", "scene", fmt.Sprintf("unified goav.Select task ready (active: %s, %dx%d @ %dfps)", s.cfg.Scene, s.cfg.Width, s.cfg.Height, s.cfg.FPS))
	s.notifyLocked()
	return nil
}

func sceneArmID(scene string) av.StreamID {
	switch scene {
	case "camera":
		return "camera"
	case "screen":
		return "screen"
	case "pip":
		return "composite"
	case "sidebyside":
		return "composite-2"
	default:
		return "pattern"
	}
}

func (s *studio) selectArmAsync(task *goav.Task, scene string) {
	arm := sceneArmID(scene)
	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			err := task.Control(ctx, control.SelectActive(arm).At("select"))
			cancel()
			if err == nil {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
	}()
}

// buildStudioOutputLocked constructs ONE unified goav.Select pipeline with all
// 5 studio scenes (pattern, camera, screen, pip, sidebyside) sharing a single
// camera source and single screen source via goav's fan-out-by-reuse.
func (s *studio) buildStudioOutputLocked() (goav.Output, error) {
	c := s.cfg
	pattern := goav.From(newTestPatternInput("studio-pattern", "pattern", c.Width, c.Height, c.FPS, 0)).Video().As("pattern")
	camRaw := s.cameraStreamLocked()
	scrRaw := s.screenStreamLocked()

	cam := camRaw.Resize(c.Width, c.Height).As("camera")
	screen := scrRaw.As("screen")

	pipW, pipH, pipX, pipY := pipGeometry(c.Width, c.Height, c.PiPCorner)
	pip := goav.Composite(
		scrRaw,
		goav.At(camRaw.Resize(pipW, pipH), pipX, pipY),
	).As("pip")

	halfW := (c.Width / 2) &^ 1
	sbs := goav.Composite(
		scrRaw.Resize(halfW, c.Height),
		goav.At(camRaw.Resize(halfW, c.Height), halfW, 0),
	).As("sidebyside")

	return goav.Select(pattern, cam, screen, pip, sbs).
		Do(newMonotonicPTSStage()).
		Tap(programTapName).
		Buffer(flow.DropOldest(branchBufferDepth)).
		To(goav.Sink(s.monitor)), nil
}

// monotonicPTSStage stamps frames leaving goav.Select onto one continuous
// monotonic wall-clock timeline so WebM/MKV/MP4/HLS/WebRTC muxers never see
// timestamp regressions when control.SelectActive cuts between live sources.
type monotonicPTSStage struct {
	start   time.Time
	lastPTS int64
}

func newMonotonicPTSStage() *monotonicPTSStage {
	return &monotonicPTSStage{start: time.Now()}
}

func (*monotonicPTSStage) Name() string { return "monotonic-pts" }
func (*monotonicPTSStage) Close() error { return nil }

func (m *monotonicPTSStage) Handle(ctx context.Context, msg *pipeline.Message, emit pipeline.Emitter) error {
	if msg != nil && msg.Kind == pipeline.MessageFrame && msg.Frame != nil {
		pts := time.Since(m.start).Nanoseconds()
		if pts <= m.lastPTS {
			pts = m.lastPTS + int64(time.Millisecond)
		}
		m.lastPTS = pts
		frameCopy := *msg.Frame
		frameCopy.PTS = av.Timestamp{Value: pts, Base: av.TimeBase{Num: 1, Den: int64(time.Second)}}
		out := *msg
		out.Frame = &frameCopy
		return emit.Emit(ctx, &out)
	}
	return emit.Emit(ctx, msg)
}

func (s *studio) screenStreamLocked() goav.Stream {
	c := s.cfg
	if s.cachedScreen {
		return goav.From(screenav.New(screenav.Config{
			DisplayID:     c.DisplayID,
			Width:         c.Width,
			Height:        c.Height,
			FPS:           c.FPS,
			CaptureCursor: c.CaptureCursor,
		})).Video()
	}
	return goav.From(newTestPatternInput("screen-placeholder", "screen", c.Width, c.Height, c.FPS, 0)).Video()
}

func (s *studio) cameraStreamLocked() goav.Stream {
	c := s.cfg
	if s.cameraAccess && len(s.cachedCams) > 0 {
		return goav.From(cameraav.New(cameraav.Config{
			Device: c.CameraID,
			Width:  c.CameraWidth,
			Height: c.CameraHeight,
			FPS:    c.FPS,
		})).Video()
	}
	return goav.From(newTestPatternInput("cam-placeholder", "camera", 640, 360, c.FPS, 1)).Video()
}

func (s *studio) UpdateConfig(ctx context.Context, next studioConfig) error {
	// Automatically request macOS permissions before switching to camera/screen scenes
	// (run outside s.mu so SSE heartbeats and UI stay responsive while the OS dialog is open).
	wantScene := next.Scene
	if wantScene == "camera" || wantScene == "pip" || wantScene == "sidebyside" {
		s.mu.Lock()
		alreadyGranted := s.cameraAccess
		s.mu.Unlock()
		if !alreadyGranted {
			permCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
			err := cameraav.RequestAccess(permCtx)
			cancel()
			s.mu.Lock()
			s.cameraAccess = (err == nil)
			s.mu.Unlock()
			if err != nil && wantScene == "camera" {
				s.mu.Lock()
				s.lastError = err.Error()
				s.recordEventLocked("error", "permission", err.Error())
				s.notifyLocked()
				s.mu.Unlock()
				return err
			}
		}
	}
	if wantScene == "screen" || wantScene == "pip" || wantScene == "sidebyside" {
		if !screenav.HasAccess() {
			screenav.RequestAccess()
		}
		s.refreshDevices()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	structuralChange := false
	if next.PiPCorner != "" && next.PiPCorner != s.cfg.PiPCorner {
		s.cfg.PiPCorner = next.PiPCorner
		structuralChange = true
	}
	if next.Width >= 320 && next.Height >= 180 {
		w, h := next.Width&^1, next.Height&^1
		if w != s.cfg.Width || h != s.cfg.Height {
			s.cfg.Width, s.cfg.Height = w, h
			structuralChange = true
		}
	}
	if next.FPS >= 10 && next.FPS <= 60 && next.FPS != s.cfg.FPS {
		s.cfg.FPS = next.FPS
		structuralChange = true
	}
	if next.CameraID != s.cfg.CameraID && (next.CameraID != "" || next.Scene == "camera") {
		s.cfg.CameraID = next.CameraID
		structuralChange = true
	}
	if next.DisplayID != s.cfg.DisplayID {
		s.cfg.DisplayID = next.DisplayID
		structuralChange = true
	}
	if next.CaptureCursor != s.cfg.CaptureCursor {
		s.cfg.CaptureCursor = next.CaptureCursor
		structuralChange = true
	}
	if s.cameraAccess != s.builtCamOK || s.cachedScreen != s.builtScrOK {
		structuralChange = true
	}
	if next.Scene != "" {
		s.cfg.Scene = next.Scene
	}

	// Fast path: switch active scene via control.SelectActive on the SAME running
	// goav.Task — 0ms cut, ZERO restart, recording/WebRTC/LL-HLS/WHIP stay live!
	if !structuralChange && s.task != nil {
		task := s.task
		scene := s.cfg.Scene
		arm := sceneArmID(scene)
		s.mu.Unlock()
		err := task.Control(ctx, control.SelectActive(arm).At("select"))
		s.mu.Lock()
		if err != nil {
			s.lastError = err.Error()
			s.recordEventLocked("error", "select", err.Error())
			s.notifyLocked()
			return err
		}
		s.lastError = ""
		s.recordEventLocked("info", "select", fmt.Sprintf("control.SelectActive(%q) — zero restart", scene))
		s.notifyLocked()
		return nil
	}

	return s.rebuildTaskLocked(ctx)
}

func detectCameraMode(ctx context.Context, deviceID string) (w, h, fps int, ok bool) {
	candidates := [][3]int{
		{1280, 720, 30},
		{1920, 1080, 30},
		{640, 480, 30},
		{960, 540, 30},
		{1280, 720, 24},
		{1920, 1080, 24},
		{1280, 720, 25},
		{640, 480, 24},
	}
	for _, c := range candidates {
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		src, _, err := cameraav.New(cameraav.Config{
			Device: deviceID,
			Width:  c[0],
			Height: c[1],
			FPS:    c[2],
		}).OpenSource(probeCtx)
		cancel()
		if err == nil {
			_ = src.Close()
			return c[0], c[1], c[2], true
		}
	}
	return 0, 0, 0, false
}

func (s *studio) RequestCameraAccess(ctx context.Context) error {
	permCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	err := cameraav.RequestAccess(permCtx)
	s.mu.Lock()
	s.cameraAccess = (err == nil)
	if err != nil {
		s.recordEventLocked("info", "permission", "camera access not yet granted: "+err.Error())
	} else {
		s.lastError = ""
		s.recordEventLocked("info", "permission", "macOS camera access granted")
	}
	s.notifyLocked()
	s.mu.Unlock()
	return err
}

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
	if format == "" {
		format = "webm"
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

// --- Multi-Viewer WebRTC Broadcast Signaling ---

func (s *studio) OfferViewer(viewerID string) (string, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if viewerID == "" {
		viewerID = fmt.Sprintf("v-%d", time.Now().UnixNano())
	}
	v, ok := s.viewers[viewerID]
	if !ok {
		pc, err := s.api.NewPeerConnection(webrtc.Configuration{})
		if err != nil {
			return "", "", err
		}
		v = &viewerPeer{
			id:      viewerID,
			pc:      pc,
			senders: make(map[string]*webrtc.RTPSender),
		}
		pc.OnConnectionStateChange(func(st webrtc.PeerConnectionState) {
			if st == webrtc.PeerConnectionStateConnected {
				s.requestKeyframe()
			}
			if st == webrtc.PeerConnectionStateClosed || st == webrtc.PeerConnectionStateFailed {
				s.mu.Lock()
				delete(s.viewers, viewerID)
				s.notifyLocked()
				s.mu.Unlock()
			}
		})
		s.viewers[viewerID] = v
	}

	// Ensure viewer has senders for all current branches.
	for _, b := range s.sortedBranchesLocked() {
		if _, exists := v.senders[b.spec.ID]; exists {
			continue
		}
		sender, err := v.pc.AddTrack(b.track)
		if err != nil {
			continue
		}
		v.senders[b.spec.ID] = sender
		go s.drainRTCP(sender)
	}
	// Remove senders for deleted branches.
	for id, sender := range v.senders {
		if _, exists := s.branches[id]; !exists {
			_ = v.pc.RemoveTrack(sender)
			delete(v.senders, id)
		}
	}

	offer, err := v.pc.CreateOffer(nil)
	if err != nil {
		return "", "", err
	}
	gather := webrtc.GatheringCompletePromise(v.pc)
	if err := v.pc.SetLocalDescription(offer); err != nil {
		return "", "", err
	}
	pc := v.pc
	s.mu.Unlock()
	select {
	case <-gather:
	case <-time.After(400 * time.Millisecond):
	}
	var sdp string
	if ld := pc.LocalDescription(); ld != nil {
		sdp = ld.SDP
	}
	s.mu.Lock()
	s.notifyLocked()
	return viewerID, sdp, nil
}

func (s *studio) AnswerViewer(viewerID, sdp string) error {
	s.mu.Lock()
	v, ok := s.viewers[viewerID]
	s.mu.Unlock()
	if !ok {
		return fmt.Errorf("viewer %q not found", viewerID)
	}
	return v.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  sdp,
	})
}

func (s *studio) syncViewerTracksLocked() {
	s.renegSeq++
}

func (s *studio) requestKeyframe() {
	s.mu.Lock()
	task := s.task
	s.mu.Unlock()
	if task == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = task.Control(ctx, control.Keyframe(av.StreamID("")).AtTap(programTapName))
}

func (s *studio) drainRTCP(sender *webrtc.RTPSender) {
	for {
		pkts, _, err := sender.ReadRTCP()
		if err != nil {
			return
		}
		for _, pkt := range pkts {
			switch pkt.(type) {
			case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
				s.requestKeyframe()
			}
		}
	}
}

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

func buildCodecSpec(name string, bitrate, fps int) (codec.Spec, error) {
	kf := max(15, fps)
	switch strings.ToLower(name) {
	case "vp8":
		return codec.VP8(codec.Bitrate(bitrate), codec.FPS(fps), codec.KeyframeInterval(kf)), nil
	case "vp9":
		return codec.VP9(codec.Bitrate(bitrate), codec.FPS(fps), codec.KeyframeInterval(kf)), nil
	case "av1":
		return codec.AV1(codec.Bitrate(bitrate), codec.FPS(fps), codec.KeyframeInterval(kf)), nil
	default:
		return codec.Spec{}, fmt.Errorf("unsupported codec %q (use vp8, vp9, or av1)", name)
	}
}

func codecCapability(name string) webrtc.RTPCodecCapability {
	switch strings.ToLower(name) {
	case "vp9":
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP9, ClockRate: 90000}
	case "av1":
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeAV1, ClockRate: 90000}
	default:
		return webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8, ClockRate: 90000}
	}
}

type webrtcTrackSink struct {
	name     string
	track    *webrtc.TrackLocalStaticSample
	fallback time.Duration
	packets  atomic.Uint64
	bytes    atomic.Uint64

	mu          sync.Mutex
	windowStart time.Time
	windowBytes uint64
	rateKbps    float64
	prevPTS     time.Duration
	havePrevPTS bool
}

func newWebRTCTrackSink(spec branchSpec, track *webrtc.TrackLocalStaticSample) *webrtcTrackSink {
	fb := 33 * time.Millisecond
	if spec.FPS > 0 {
		fb = time.Second / time.Duration(spec.FPS)
	}
	return &webrtcTrackSink{
		name:     "webrtc-" + spec.ID,
		track:    track,
		fallback: fb,
	}
}

func (s *webrtcTrackSink) Name() string { return s.name }
func (s *webrtcTrackSink) Close() error { return nil }

func (s *webrtcTrackSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg == nil || msg.Kind != pipeline.MessagePacket || msg.Packet == nil {
		return nil
	}
	dur := s.fallback
	if cur, ok := msg.Packet.PTS.ToDuration(); ok {
		if s.havePrevPTS {
			if gap := cur - s.prevPTS; gap > 0 && gap <= 2*time.Second {
				dur = gap
			}
		}
		s.prevPTS = cur
		s.havePrevPTS = true
	}
	if err := s.track.WriteSample(media.Sample{
		Data:     msg.Packet.Payload.Bytes,
		Duration: dur,
	}); err != nil {
		return err
	}
	n := uint64(len(msg.Packet.Payload.Bytes))
	s.packets.Add(1)
	s.bytes.Add(n)

	now := time.Now()
	s.mu.Lock()
	if s.windowStart.IsZero() {
		s.windowStart = now
	}
	s.windowBytes += n
	if elapsed := now.Sub(s.windowStart); elapsed >= 500*time.Millisecond {
		s.rateKbps = (float64(s.windowBytes) * 8.0 / 1000.0) / elapsed.Seconds()
		s.windowStart = now
		s.windowBytes = 0
	}
	s.mu.Unlock()
	return nil
}

func (s *webrtcTrackSink) kbps() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rateKbps
}

// unused import guard if rtcp needed for future PLI hooks
var _ rtcp.Packet

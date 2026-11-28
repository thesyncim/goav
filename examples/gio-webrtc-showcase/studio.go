package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/bundle"
	"github.com/thesyncim/goav/cameraav"
	"github.com/thesyncim/goav/container/hls"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/examples/gio-webrtc-showcase/screenav"
	"github.com/thesyncim/goav/flow"
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
	s.builtCamOK = s.cameraAccess && len(s.cachedCams) > 0
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
	wantCamOK := s.cameraAccess && len(s.cachedCams) > 0
	if wantCamOK != s.builtCamOK || s.cachedScreen != s.builtScrOK {
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

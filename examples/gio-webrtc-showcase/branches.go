package main

import (
	"context"
	"fmt"
	"github.com/thesyncim/goav/control"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/flow"
	"github.com/thesyncim/goav/pipeline"
)

// videoBranchBufferDepth bounds how many decoded frames a video output branch
// may queue before the encoder before drop-oldest sheds the stalest one — a
// few frames of jitter smoothing without letting a slow encoder build latency
// or backpressure the live decode tap.
const videoBranchBufferDepth = 12

var branchSeq atomic.Uint64

type branch struct {
	Spec       branchSpec                     `json:"spec"`
	Track      *webrtc.TrackLocalStaticSample `json:"-"`
	Sender     *webrtc.RTPSender              `json:"-"`
	Attachment goav.Attachment                `json:"-"`
	Sink       *trackSampleSink               `json:"-"`
	Paused     bool                           `json:"paused"`
	Created    time.Time                      `json:"created"`

	lastRTCPKeyframe time.Time
}

type branchSpec struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Codec   string `json:"codec"`
	Width   int    `json:"width,omitempty"`
	Height  int    `json:"height,omitempty"`
	FPS     int    `json:"fps,omitempty"`
	Bitrate int    `json:"bitrate"`
}

type trackSampleSink struct {
	name     string
	track    *webrtc.TrackLocalStaticSample
	fallback time.Duration
	analyzer *audioAnalyzer
	packets  atomic.Uint64
	bytes    atomic.Uint64

	// prevPTS tracks the last sample's presentation time so the WebRTC sample
	// duration reflects the real gap between delivered packets. A dropping output
	// branch sheds frames under load, so consecutive encoded packets can span more
	// than one nominal frame; pacing RTP by the PTS delta keeps the timestamp clock
	// aligned with wall time instead of drifting behind. Handle runs on the node's
	// single worker, so these need no lock.
	prevPTS     time.Duration
	havePrevPTS bool
}

// maxSampleGap caps the per-sample RTP advance so a PTS discontinuity (clock
// reset, seek) cannot translate into an absurd timestamp jump; a genuine slow
// stream stays well under it.
const maxSampleGap = 2 * time.Second

// sampleDuration paces one WebRTC sample by the real time between presentation
// timestamps when both are known, falling back to the packet's declared duration
// and then a fixed cadence. This is what keeps RTP timing correct when a
// dropping branch sheds frames between delivered packets.
func sampleDuration(prevPTS time.Duration, havePrev bool, packet *av.Packet, fallback time.Duration) (time.Duration, time.Duration, bool) {
	cur, haveCur := packet.PTS.ToDuration()
	if haveCur && havePrev {
		if gap := cur - prevPTS; gap > 0 && gap <= maxSampleGap {
			return gap, cur, true
		}
	}
	duration := fallback
	if d, ok := packet.Duration.ToDuration(); ok && d > 0 {
		duration = d
	}
	if haveCur {
		return duration, cur, true
	}
	return duration, prevPTS, havePrev
}

func defaultBranches() []branchSpec {
	return []branchSpec{
		{Kind: "video", Codec: "vp8", Width: 640, Height: 360, Bitrate: 700_000},
		{Kind: "video", Codec: "vp8", Width: 320, Height: 180, Bitrate: 320_000},
		{Kind: "audio", Codec: "opus", Bitrate: 96_000},
		{Kind: "audio", Codec: "opus", Bitrate: 32_000},
	}
}

func (s *session) addBranch(ctx context.Context, spec branchSpec) (*branch, error) {
	if err := normalizeBranch(&spec); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.branches[spec.ID]; exists {
		return nil, fmt.Errorf("branch %q already exists", spec.ID)
	}
	track, err := webrtc.NewTrackLocalStaticSample(codecCapability(spec), spec.ID, "goav-"+s.id)
	if err != nil {
		return nil, err
	}
	sender, err := s.pc.AddTrack(track)
	if err != nil {
		return nil, err
	}
	r := &branch{
		Spec:    spec,
		Track:   track,
		Sender:  sender,
		Sink:    newTrackSampleSink(spec, track, s.audio),
		Created: time.Now(),
	}
	go s.drainBranchRTCP(r)
	s.branches[spec.ID] = r
	s.queueRenegotiateLocked(spec.Kind)
	s.recordLocked("info", "branch", "branch track added", spec.Kind, spec.ID, branchMeta(spec))
	if err := s.attachBranchLocked(ctx, r); err != nil {
		s.setErrorLocked(err.Error())
	}
	return r, nil
}

func (s *session) updateBranch(ctx context.Context, spec branchSpec) (*branch, error) {
	if err := normalizeBranch(&spec); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.branches[spec.ID]
	if !ok {
		return nil, fmt.Errorf("branch %q not found", spec.ID)
	}
	if r.Spec.Kind != spec.Kind || r.Spec.Codec != spec.Codec {
		return nil, fmt.Errorf("kind and codec changes create a new track; add a new branch instead")
	}
	if r.Attachment != nil {
		_ = s.detachLocked(ctx, r)
	}
	r.Spec.Width = spec.Width
	r.Spec.Height = spec.Height
	r.Spec.FPS = spec.FPS
	r.Spec.Bitrate = spec.Bitrate
	s.recordLocked("info", "branch", "branch parameters updated", spec.Kind, spec.ID, branchMeta(r.Spec))
	if err := s.attachBranchLocked(ctx, r); err != nil {
		return nil, err
	}
	return r, nil
}

func (s *session) pauseBranch(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.branches[id]
	if !ok {
		return fmt.Errorf("branch %q not found", id)
	}
	if r.Attachment == nil {
		return fmt.Errorf("branch %q is not attached yet", id)
	}
	if err := r.Attachment.Pause(ctx); err != nil {
		return err
	}
	r.Paused = true
	s.recordLocked("info", "branch", "runtime branch paused", r.Spec.Kind, id, branchMeta(r.Spec))
	return nil
}

func (s *session) resumeBranch(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.branches[id]
	if !ok {
		return fmt.Errorf("branch %q not found", id)
	}
	if r.Attachment == nil {
		return fmt.Errorf("branch %q is not attached yet", id)
	}
	if err := r.Attachment.Resume(ctx); err != nil {
		return err
	}
	r.Paused = false
	s.recordLocked("info", "branch", "runtime branch resumed", r.Spec.Kind, id, branchMeta(r.Spec))
	return nil
}

func (s *session) rebranch(ctx context.Context, id string, spec branchSpec) (*branch, error) {
	if spec.ID == "" {
		spec.ID = id
	}
	if spec.ID != id {
		return nil, fmt.Errorf("rebranch id mismatch: %q != %q", spec.ID, id)
	}
	if err := normalizeBranch(&spec); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.branches[id]
	if !ok {
		return nil, fmt.Errorf("branch %q not found", id)
	}
	if r.Spec.Kind != spec.Kind || r.Spec.Codec != spec.Codec {
		return nil, fmt.Errorf("kind and codec changes create a new track; add a new branch instead")
	}
	if r.Attachment == nil {
		r.Spec.Width = spec.Width
		r.Spec.Height = spec.Height
		r.Spec.FPS = spec.FPS
		r.Spec.Bitrate = spec.Bitrate
		if err := s.attachBranchLocked(ctx, r); err != nil {
			return nil, err
		}
		return r, nil
	}
	oldSpec := r.Spec
	r.Spec.Width = spec.Width
	r.Spec.Height = spec.Height
	r.Spec.FPS = spec.FPS
	r.Spec.Bitrate = spec.Bitrate
	replacement, err := branchRuntimeSpec(r, s.syncPolicy)
	if err != nil {
		r.Spec = oldSpec
		return nil, err
	}
	next, err := r.Attachment.Rebranch(ctx,
		replacement,
		goav.SwitchAt(goav.AtMediaTime(0)),
		goav.DrainOldBranch(),
		goav.KeepOldOnFailure(),
	)
	if err != nil {
		r.Spec = oldSpec
		return nil, err
	}
	r.Attachment = next
	r.Paused = false
	s.recordLocked("info", "branch", "runtime branch rebranched", r.Spec.Kind, id, branchMeta(r.Spec))
	return r, nil
}

func (s *session) setBranchBitrate(ctx context.Context, id string, bitrate int) error {
	if bitrate <= 0 {
		return fmt.Errorf("bitrate must be positive")
	}
	s.mu.Lock()
	r, ok := s.branches[id]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("branch %q not found", id)
	}
	kind := r.Spec.Kind
	task := s.taskForKindLocked(kind)
	r.Spec.Bitrate = bitrate
	meta := branchMeta(r.Spec)
	s.mu.Unlock()
	if task == nil {
		return fmt.Errorf("%s task is not running", kind)
	}
	if err := task.Control(ctx, control.SetBitrate(av.StreamID(""), bitrate)); err != nil {
		return err
	}
	s.record("info", "control", "live bitrate retargeted", kind, id, meta)
	return nil
}

func (s *session) deleteBranch(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.branches[id]
	if !ok {
		return fmt.Errorf("branch %q not found", id)
	}
	_ = s.detachLocked(ctx, r)
	if r.Sender != nil {
		_ = s.pc.RemoveTrack(r.Sender)
	}
	delete(s.branches, id)
	s.queueRenegotiateLocked(r.Spec.Kind)
	s.recordLocked("info", "branch", "branch track removed", r.Spec.Kind, id, branchMeta(r.Spec))
	return nil
}

func (s *session) attachBranchLocked(ctx context.Context, r *branch) error {
	if r.Attachment != nil {
		return nil
	}
	spec, err := branchRuntimeSpec(r, s.syncPolicy)
	if err != nil {
		return err
	}
	switch r.Spec.Kind {
	case "video":
		if s.videoTask == nil {
			s.recordLocked("debug", "branch", "video branch waiting for input track", "video", r.Spec.ID, branchMeta(r.Spec))
			return nil
		}
		attachment, err := s.videoTask.Attach(ctx, spec)
		if err != nil {
			return err
		}
		r.Attachment = attachment
	case "audio":
		if s.audioTask == nil {
			s.recordLocked("debug", "branch", "audio branch waiting for input track", "audio", r.Spec.ID, branchMeta(r.Spec))
			return nil
		}
		attachment, err := s.audioTask.Attach(ctx, spec)
		if err != nil {
			return err
		}
		r.Attachment = attachment
	default:
		return fmt.Errorf("unsupported branch kind %q", r.Spec.Kind)
	}
	s.recordLocked("info", "branch", "runtime branch attached", r.Spec.Kind, r.Spec.ID, branchMeta(r.Spec))
	return nil
}

func branchRuntimeSpec(r *branch, syncPolicy goav.SyncPolicy) (goav.BranchSpec, error) {
	if r == nil || r.Sink == nil {
		return goav.BranchSpec{}, fmt.Errorf("branch has no output sink")
	}
	switch r.Spec.Kind {
	case "video":
		// A video output branch must not pace the live decode tap: software
		// VP8/VP9 encoders can fall behind the source, and the default blocking
		// branch buffer would backpressure the tap fan-out, dragging the decoded
		// FPS (and every sibling branch) down to the slowest encoder. A realtime
		// drop-oldest buffer lets a slow encoder shed frames for itself while the
		// decode path and faster branches keep full rate.
		branch := goav.Branch(r.Spec.ID).
			From(goav.FrameTap(videoTapName)).
			Buffer(flow.DropOldest(videoBranchBufferDepth)).
			Sync(syncPolicy).
			Resize(r.Spec.Width, r.Spec.Height)
		switch r.Spec.Codec {
		case "vp8":
			return branch.Encode(codec.VP8(codec.Bitrate(r.Spec.Bitrate), codec.FPS(r.Spec.FPS))).To(goav.Sink(r.Sink)), nil
		case "vp9":
			return branch.Encode(codec.VP9(codec.Bitrate(r.Spec.Bitrate), codec.FPS(r.Spec.FPS))).To(goav.Sink(r.Sink)), nil
		default:
			return goav.BranchSpec{}, fmt.Errorf("unsupported video encoder %q", r.Spec.Codec)
		}
	case "audio":
		if r.Spec.Codec != "opus" {
			return goav.BranchSpec{}, fmt.Errorf("unsupported audio encoder %q", r.Spec.Codec)
		}
		return goav.Branch(r.Spec.ID).
			From(goav.FrameTap(audioTapName)).
			Sync(syncPolicy).
			Resample(48_000, codec.Stereo).
			Encode(codec.Opus(codec.Bitrate(r.Spec.Bitrate), codec.Channels(codec.Stereo), codec.SampleRate(48_000))).
			To(goav.Sink(r.Sink)), nil
	default:
		return goav.BranchSpec{}, fmt.Errorf("unsupported branch kind %q", r.Spec.Kind)
	}
}

func (s *session) detachLocked(ctx context.Context, r *branch) error {
	if r.Attachment == nil {
		return nil
	}
	var task goav.Mutable
	if r.Spec.Kind == "video" {
		task = s.videoTask
	} else {
		task = s.audioTask
	}
	if task == nil {
		r.Attachment = nil
		return nil
	}
	err := task.Detach(ctx, r.Attachment)
	r.Attachment = nil
	r.Paused = false
	s.recordLocked("info", "branch", "runtime branch detached", r.Spec.Kind, r.Spec.ID, branchMeta(r.Spec))
	return err
}

func newTrackSampleSink(spec branchSpec, track *webrtc.TrackLocalStaticSample, analyzer *audioAnalyzer) *trackSampleSink {
	fallback := 33 * time.Millisecond
	if spec.Kind == "audio" {
		fallback = 20 * time.Millisecond
	}
	return &trackSampleSink{name: "browser-" + spec.ID, track: track, fallback: fallback, analyzer: analyzer}
}

func (s *trackSampleSink) Name() string {
	return s.name
}

func (s *trackSampleSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg.Kind != "packet" || msg.Packet == nil {
		return nil
	}
	duration, prevPTS, havePrev := sampleDuration(s.prevPTS, s.havePrevPTS, msg.Packet, s.fallback)
	s.prevPTS, s.havePrevPTS = prevPTS, havePrev
	if err := s.track.WriteSample(media.Sample{
		Data:     msg.Packet.Payload.Bytes,
		Duration: duration,
	}); err != nil {
		return err
	}
	s.packets.Add(1)
	s.bytes.Add(uint64(len(msg.Packet.Payload.Bytes)))
	if s.analyzer != nil {
		s.analyzer.observePacket(msg.Packet)
	}
	return nil
}

func (s *trackSampleSink) Close() error {
	return nil
}

func discardSink(name string) pipeline.Sink {
	return goav.SinkFunc(name, func(context.Context, goav.Message) error {
		return nil
	})
}

func normalizeBranch(spec *branchSpec) error {
	spec.Kind = strings.ToLower(strings.TrimSpace(spec.Kind))
	spec.Codec = strings.ToLower(strings.TrimSpace(spec.Codec))
	if spec.ID == "" {
		spec.ID = fmt.Sprintf("%s-%s-%d", spec.Kind, spec.Codec, branchSeq.Add(1))
	}
	if spec.Bitrate <= 0 {
		if spec.Kind == "audio" {
			spec.Bitrate = 64_000
		} else {
			spec.Bitrate = 600_000
		}
	}
	switch spec.Kind {
	case "video":
		if spec.Codec != "vp8" && spec.Codec != "vp9" {
			return fmt.Errorf("video output supports vp8 or vp9")
		}
		if spec.Width <= 0 {
			spec.Width = 640
		}
		if spec.Height <= 0 {
			spec.Height = 360
		}
		// VP8/VP9 require even dimensions; round down so an odd value typed in the
		// UI cannot fail the branch's encoder reattach.
		spec.Width -= spec.Width % 2
		spec.Height -= spec.Height % 2
		if spec.FPS <= 0 {
			spec.FPS = 30
		}
		if spec.FPS > 60 {
			spec.FPS = 60
		}
	case "audio":
		if spec.Codec != "opus" {
			return fmt.Errorf("audio output supports opus")
		}
	default:
		return fmt.Errorf("kind must be audio or video")
	}
	return nil
}

func codecCapability(spec branchSpec) webrtc.RTPCodecCapability {
	switch spec.Codec {
	case "opus":
		return webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeOpus,
			ClockRate:    48_000,
			Channels:     2,
			SDPFmtpLine:  "minptime=10;useinbandfec=1",
			RTCPFeedback: nil,
		}
	case "vp9":
		return webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeVP9,
			ClockRate:   90_000,
			SDPFmtpLine: "profile-id=0",
		}
	default:
		return webrtc.RTPCodecCapability{
			MimeType:  webrtc.MimeTypeVP8,
			ClockRate: 90_000,
		}
	}
}

func (s *session) drainBranchRTCP(r *branch) {
	if r == nil || r.Sender == nil {
		return
	}
	for {
		packets, _, err := r.Sender.ReadRTCP()
		if err != nil {
			return
		}
		for _, packet := range packets {
			if feedback, ok := rtcpKeyframeFeedback(packet); ok {
				s.requestOutputVideoKeyframe(s.ctx, r.Spec.ID, feedback)
			}
		}
	}
}

func rtcpKeyframeFeedback(packet rtcp.Packet) (string, bool) {
	switch packet.(type) {
	case *rtcp.PictureLossIndication:
		return "pli", true
	case *rtcp.FullIntraRequest:
		return "fir", true
	default:
		return "", false
	}
}

func branchMeta(spec branchSpec) map[string]string {
	meta := eventMeta("codec", spec.Codec, "bitrate", fmt.Sprintf("%d", spec.Bitrate))
	if spec.Kind == "video" {
		meta["size"] = fmt.Sprintf("%dx%d", spec.Width, spec.Height)
		meta["fps"] = fmt.Sprintf("%d", spec.FPS)
	}
	return meta
}

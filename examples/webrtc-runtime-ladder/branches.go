package main

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/thesyncim/goav"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/pipeline"
)

var branchSeq atomic.Uint64

type branch struct {
	Spec       branchSpec                     `json:"spec"`
	Track      *webrtc.TrackLocalStaticSample `json:"-"`
	Sender     *webrtc.RTPSender              `json:"-"`
	Attachment goav.Attachment                `json:"-"`
	Sink       *trackSampleSink               `json:"-"`
	Created    time.Time                      `json:"created"`
}

type branchSpec struct {
	ID      string `json:"id"`
	Kind    string `json:"kind"`
	Codec   string `json:"codec"`
	Width   int    `json:"width,omitempty"`
	Height  int    `json:"height,omitempty"`
	Bitrate int    `json:"bitrate"`
}

type trackSampleSink struct {
	name     string
	track    *webrtc.TrackLocalStaticSample
	fallback time.Duration
	packets  atomic.Uint64
	bytes    atomic.Uint64
}

func defaultBranches() []branchSpec {
	return []branchSpec{
		{Kind: "video", Codec: "vp8", Width: 640, Height: 360, Bitrate: 700_000},
		{Kind: "video", Codec: "vp9", Width: 320, Height: 180, Bitrate: 320_000},
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
	go drainRTCP(sender)
	r := &branch{
		Spec:    spec,
		Track:   track,
		Sender:  sender,
		Sink:    newTrackSampleSink(spec, track),
		Created: time.Now(),
	}
	s.branches[spec.ID] = r
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
	r.Spec.Bitrate = spec.Bitrate
	s.recordLocked("info", "branch", "branch parameters updated", spec.Kind, spec.ID, branchMeta(r.Spec))
	if err := s.attachBranchLocked(ctx, r); err != nil {
		return nil, err
	}
	return r, nil
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
	s.recordLocked("info", "branch", "branch track removed", r.Spec.Kind, id, branchMeta(r.Spec))
	return nil
}

func (s *session) attachBranchLocked(ctx context.Context, r *branch) error {
	if r.Attachment != nil {
		return nil
	}
	switch r.Spec.Kind {
	case "video":
		if s.videoTask == nil {
			s.recordLocked("debug", "branch", "video branch waiting for input track", "video", r.Spec.ID, branchMeta(r.Spec))
			return nil
		}
		branch := goav.Branch(r.Spec.ID).
			From(goav.FrameTap(videoTapName)).
			Resize(r.Spec.Width, r.Spec.Height)
		var spec goav.BranchSpec
		switch r.Spec.Codec {
		case "vp8":
			spec = branch.Encode(codec.VP8(codec.Bitrate(r.Spec.Bitrate))).To(goav.Sink(r.Sink))
		case "vp9":
			spec = branch.Encode(codec.VP9(codec.Bitrate(r.Spec.Bitrate))).To(goav.Sink(r.Sink))
		default:
			return fmt.Errorf("unsupported video encoder %q", r.Spec.Codec)
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
		spec := goav.Branch(r.Spec.ID).
			From(goav.FrameTap(audioTapName)).
			Resample(48_000, goav.Stereo).
			Encode(codec.Opus(codec.Bitrate(r.Spec.Bitrate), codec.Channels(goav.Stereo))).
			To(goav.Sink(r.Sink))
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

func (s *session) detachLocked(ctx context.Context, r *branch) error {
	if r.Attachment == nil {
		return nil
	}
	var task goav.Task
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
	s.recordLocked("info", "branch", "runtime branch detached", r.Spec.Kind, r.Spec.ID, branchMeta(r.Spec))
	return err
}

func newTrackSampleSink(spec branchSpec, track *webrtc.TrackLocalStaticSample) *trackSampleSink {
	fallback := 33 * time.Millisecond
	if spec.Kind == "audio" {
		fallback = 20 * time.Millisecond
	}
	return &trackSampleSink{name: "browser-" + spec.ID, track: track, fallback: fallback}
}

func (s *trackSampleSink) Name() string {
	return s.name
}

func (s *trackSampleSink) Handle(_ context.Context, msg *pipeline.Message) error {
	if msg.Kind != "packet" || msg.Packet == nil {
		return nil
	}
	duration := s.fallback
	if d, ok := msg.Packet.Duration.ToDuration(); ok && d > 0 {
		duration = d
	}
	if err := s.track.WriteSample(media.Sample{
		Data:     msg.Packet.Payload.Bytes,
		Duration: duration,
	}); err != nil {
		return err
	}
	s.packets.Add(1)
	s.bytes.Add(uint64(len(msg.Packet.Payload.Bytes)))
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

func drainRTCP(sender *webrtc.RTPSender) {
	for {
		if _, _, err := sender.ReadRTCP(); err != nil {
			return
		}
	}
}

func branchMeta(spec branchSpec) map[string]string {
	meta := eventMeta("codec", spec.Codec, "bitrate", fmt.Sprintf("%d", spec.Bitrate))
	if spec.Kind == "video" {
		meta["size"] = fmt.Sprintf("%dx%d", spec.Width, spec.Height)
	}
	return meta
}

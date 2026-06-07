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
	"github.com/thesyncim/goav/pipeline"
)

var renditionSeq atomic.Uint64

type rendition struct {
	Spec       renditionSpec                  `json:"spec"`
	Track      *webrtc.TrackLocalStaticSample `json:"-"`
	Sender     *webrtc.RTPSender              `json:"-"`
	Attachment goav.Attachment                `json:"-"`
	Sink       *trackSampleSink               `json:"-"`
	Created    time.Time                      `json:"created"`
}

type renditionSpec struct {
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

func defaultRenditions() []renditionSpec {
	return []renditionSpec{
		{Kind: "video", Codec: "vp8", Width: 640, Height: 360, Bitrate: 700_000},
		{Kind: "video", Codec: "vp9", Width: 320, Height: 180, Bitrate: 320_000},
	}
}

func (s *session) addRendition(ctx context.Context, spec renditionSpec) (*rendition, error) {
	if err := normalizeRendition(&spec); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.renditions[spec.ID]; exists {
		return nil, fmt.Errorf("rendition %q already exists", spec.ID)
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
	r := &rendition{
		Spec:    spec,
		Track:   track,
		Sender:  sender,
		Sink:    newTrackSampleSink(spec, track),
		Created: time.Now(),
	}
	s.renditions[spec.ID] = r
	s.recordLocked("info", "rendition", "rendition track added", spec.Kind, spec.ID, renditionMeta(spec))
	if err := s.attachRenditionLocked(ctx, r); err != nil {
		s.setErrorLocked(err.Error())
	}
	return r, nil
}

func (s *session) updateRendition(ctx context.Context, spec renditionSpec) (*rendition, error) {
	if err := normalizeRendition(&spec); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.renditions[spec.ID]
	if !ok {
		return nil, fmt.Errorf("rendition %q not found", spec.ID)
	}
	if r.Spec.Kind != spec.Kind || r.Spec.Codec != spec.Codec {
		return nil, fmt.Errorf("kind and codec changes create a new track; add a new rendition instead")
	}
	if r.Attachment != nil {
		_ = s.detachLocked(ctx, r)
	}
	r.Spec.Width = spec.Width
	r.Spec.Height = spec.Height
	r.Spec.Bitrate = spec.Bitrate
	s.recordLocked("info", "rendition", "rendition parameters updated", spec.Kind, spec.ID, renditionMeta(r.Spec))
	if err := s.attachRenditionLocked(ctx, r); err != nil {
		return nil, err
	}
	return r, nil
}

func (s *session) deleteRendition(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.renditions[id]
	if !ok {
		return fmt.Errorf("rendition %q not found", id)
	}
	_ = s.detachLocked(ctx, r)
	if r.Sender != nil {
		_ = s.pc.RemoveTrack(r.Sender)
	}
	delete(s.renditions, id)
	s.recordLocked("info", "rendition", "rendition track removed", r.Spec.Kind, id, renditionMeta(r.Spec))
	return nil
}

func (s *session) attachRenditionLocked(ctx context.Context, r *rendition) error {
	if r.Attachment != nil {
		return nil
	}
	switch r.Spec.Kind {
	case "video":
		if s.videoTask == nil {
			s.recordLocked("debug", "branch", "video branch waiting for input track", "video", r.Spec.ID, renditionMeta(r.Spec))
			return nil
		}
		branch := goav.Branch(r.Spec.ID).
			From(goav.FrameTap(videoTapName)).
			Resize(r.Spec.Width, r.Spec.Height)
		var spec goav.BranchSpec
		switch r.Spec.Codec {
		case "vp8":
			spec = branch.VP8(r.Spec.Bitrate).To(goav.Sink(r.Sink))
		case "vp9":
			spec = branch.VP9(r.Spec.Bitrate).To(goav.Sink(r.Sink))
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
			s.recordLocked("debug", "branch", "audio branch waiting for input track", "audio", r.Spec.ID, renditionMeta(r.Spec))
			return nil
		}
		spec := goav.Branch(r.Spec.ID).
			From(goav.FrameTap(audioTapName)).
			Resample(48_000, goav.Stereo).
			Opus(r.Spec.Bitrate, goav.Channels(goav.Stereo)).
			To(goav.Sink(r.Sink))
		attachment, err := s.audioTask.Attach(ctx, spec)
		if err != nil {
			return err
		}
		r.Attachment = attachment
	default:
		return fmt.Errorf("unsupported rendition kind %q", r.Spec.Kind)
	}
	s.recordLocked("info", "branch", "runtime branch attached", r.Spec.Kind, r.Spec.ID, renditionMeta(r.Spec))
	return nil
}

func (s *session) detachLocked(ctx context.Context, r *rendition) error {
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
	s.recordLocked("info", "branch", "runtime branch detached", r.Spec.Kind, r.Spec.ID, renditionMeta(r.Spec))
	return err
}

func newTrackSampleSink(spec renditionSpec, track *webrtc.TrackLocalStaticSample) *trackSampleSink {
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

func normalizeRendition(spec *renditionSpec) error {
	spec.Kind = strings.ToLower(strings.TrimSpace(spec.Kind))
	spec.Codec = strings.ToLower(strings.TrimSpace(spec.Codec))
	if spec.ID == "" {
		spec.ID = fmt.Sprintf("%s-%s-%d", spec.Kind, spec.Codec, renditionSeq.Add(1))
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

func codecCapability(spec renditionSpec) webrtc.RTPCodecCapability {
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

func renditionMeta(spec renditionSpec) map[string]string {
	meta := eventMeta("codec", spec.Codec, "bitrate", fmt.Sprintf("%d", spec.Bitrate))
	if spec.Kind == "video" {
		meta["size"] = fmt.Sprintf("%dx%d", spec.Width, spec.Height)
	}
	return meta
}

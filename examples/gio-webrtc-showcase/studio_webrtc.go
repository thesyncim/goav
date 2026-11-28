package main

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/control"
	"github.com/thesyncim/goav/pipeline"
)

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

package webrtcav

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/interceptor/pkg/cc"
	"github.com/pion/interceptor/pkg/gcc"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/provider"
	"github.com/thesyncim/goav/rtpav"
)

var (
	// ErrWHIPDisconnected reports a WHIP session whose peer connection
	// failed, or never connected, while it published.
	ErrWHIPDisconnected = errors.New("webrtcav: whip session disconnected")
	// ErrWHIPCodecRefused reports a WHIP answer that refuses a stream's
	// codec; encode the stream to one the server takes.
	ErrWHIPCodecRefused = errors.New("webrtcav: whip server refused the codec")
)

// WHIP is the whip scheme, a WebRTC-HTTP Ingestion Protocol client (RFC
// 9725), for goav.URI: whip://host/path and whips://host/path publish over
// https, whip+http://host/path over plain http. Register it under each name
// used:
//
//	runconfig.Scheme("whip", webrtcav.WHIP(webrtcav.WithToken(token)))
//
// The destination takes Opus, VP8, H264, VP9, and AV1 packets, so the
// planner encodes frames to the first of their media it can encode. When
// the run opens it, it offers one send-only track per stream routed to it,
// in that stream's codec, POSTs the offer, applies the answer, and waits
// for the peer connection to connect. ICE candidates found after the offer
// go out by PATCH when the server accepts trickle, and STUN/TURN servers
// the answer names (Link rel="ice-server") configure the session as they do
// for WHEP. Keyframe requests (PLI, FIR) and bandwidth estimates (REMB, and
// TWCC through Pion's send-side estimator when WHIP builds the peer
// connection's API) reach the encoders feeding the streams. Closing it
// DELETEs the session's resource. A refused request is a *StatusError, a
// refused codec ErrWHIPCodecRefused, and a dropped peer connection fails the
// run with ErrWHIPDisconnected. WHIP is a destination scheme: WHEP plays.
func WHIP(options ...Option) provider.DestinationScheme {
	return whipScheme{config: newSignalConfig(options), servers: newICEServers()}
}

type whipScheme struct {
	config  signalConfig
	servers *iceServers
	// settings, for tests, configure the API WHIP builds.
	settings *webrtc.SettingEngine
}

func (s whipScheme) Destination(u *url.URL) (provider.Output, error) {
	endpoint, err := endpointURL(u, "whip")
	if err != nil {
		return nil, err
	}
	e := &egress{name: "whip", detail: "whip publish", accepts: egressCodecs.Clone()}
	e.open = func(ctx context.Context, streams []av.Stream) (*trackWriter, []rtpav.SendOption, error) {
		session, err := negotiate(ctx, s.servers, endpoint.Host, s.config.session.Configuration, func(ctx context.Context, config webrtc.Configuration) (*whipSession, error) {
			return s.offer(ctx, endpoint, config, streams)
		})
		if err != nil {
			return nil, nil, err
		}
		if err := session.awaitConnected(ctx); err != nil {
			session.Close()
			return nil, nil, err
		}
		return session.writer, session.options, nil
	}
	return e, nil
}

// whipSession is one negotiated WHIP session: the peer connection, its
// signaling, and its tracks' writer.
type whipSession struct {
	config    signalConfig
	pc        *webrtc.PeerConnection
	sig       *signaling
	cancel    context.CancelFunc
	writer    *trackWriter
	options   []rtpav.SendOption
	connected chan struct{}
	failed    chan struct{}
	failOnce  sync.Once
	connOnce  sync.Once
	closeOnce sync.Once
	closeErr  error
}

// offer opens a peer connection under config with one send-only track per
// stream, in the stream's codec, and negotiates it.
func (s whipScheme) offer(ctx context.Context, endpoint *url.URL, config webrtc.Configuration, streams []av.Stream) (*whipSession, error) {
	if len(streams) == 0 {
		return nil, errors.New("whip: no stream to publish")
	}
	// targets carries the latest send-side estimate; a stale one is replaced.
	targets := make(chan int, 1)
	estimating := false
	api := s.config.session.API
	if api == nil {
		var err error
		if api, err = egressAPI(s.settings, targets); err != nil {
			return nil, err
		}
		estimating = true
	}
	pc, err := api.NewPeerConnection(config)
	if err != nil {
		return nil, err
	}
	sessionCtx, cancel := context.WithCancel(context.Background())
	session := &whipSession{
		config:    s.config,
		pc:        pc,
		sig:       newSignaling(sessionCtx, "whip", s.config, pc),
		cancel:    cancel,
		connected: make(chan struct{}),
		failed:    make(chan struct{}),
	}
	session.writer = newTrackWriter(pc, session.failed)
	session.writer.onClose = session.Close
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected:
			session.connOnce.Do(func() { close(session.connected) })
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			session.failOnce.Do(func() { close(session.failed) })
		}
	})
	mids := make([]*webrtc.RTPTransceiver, len(streams))
	var ssrcs []uint32
	for i, stream := range streams {
		codec, ok := streamCapability(stream)
		if !ok {
			session.Close()
			return nil, fmt.Errorf("whip: stream %s: %w: WebRTC does not carry %s", stream.ID, ErrWHIPCodecRefused, stream.Codec.ID)
		}
		track, err := webrtc.NewTrackLocalStaticRTP(codec.RTPCodecCapability, string(stream.ID), "goav")
		if err == nil {
			mids[i], err = pc.AddTransceiverFromTrack(track, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendonly})
		}
		if err == nil {
			err = mids[i].SetCodecPreferences([]webrtc.RTPCodecParameters{codec})
		}
		if err != nil {
			session.Close()
			return nil, fmt.Errorf("whip: stream %s: %w", stream.ID, err)
		}
		ssrc := senderSSRC(mids[i].Sender())
		ssrcs = append(ssrcs, ssrc)
		session.writer.add(ssrc, track, mids[i].Sender())
		session.options = append(session.options, rtpav.WithSSRC(stream.ID, ssrc))
	}
	_, err = session.sig.exchange(ctx, endpoint, func(answer string) error { return refusedCodecs(answer, mids, streams) })
	if err != nil {
		session.Close()
		return nil, err
	}
	if estimating {
		go session.followEstimate(sessionCtx, targets, streams, ssrcs)
	}
	return session, nil
}

func (s *whipSession) linkedServers() []webrtc.ICEServer { return s.sig.linkedServers() }

// awaitConnected waits until the peer connection connects, so the first
// packets, a keyframe among them, reach the server.
func (s *whipSession) awaitConnected(ctx context.Context) error {
	deadline := time.NewTimer(s.config.connect)
	defer deadline.Stop()
	select {
	case <-s.connected:
		return nil
	case <-s.failed:
		return ErrWHIPDisconnected
	case <-deadline.C:
		return fmt.Errorf("whip: not connected within %s: %w", s.config.connect, ErrWHIPDisconnected)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// followEstimate hands the send-side bandwidth estimate (TWCC feedback) to
// the sender as the REMB a receiver would send for the video streams: what
// the audio leaves, which the sender splits, bounds by the configured
// bitrates, and turns into bitrate controls, as it does a receiver's REMB.
func (s *whipSession) followEstimate(ctx context.Context, targets <-chan int, streams []av.Stream, ssrcs []uint32) {
	const audioShare = 64_000
	var video []uint32
	audio := 0
	for i, stream := range streams {
		if stream.Type == av.MediaVideo || stream.Codec.Type == av.MediaVideo {
			video = append(video, ssrcs[i])
		} else {
			audio++
		}
	}
	if len(video) == 0 {
		return
	}
	for {
		select {
		case <-ctx.Done():
			return
		case bitrate := <-targets:
			remb := &rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: float32(max(bitrate-audio*audioShare, audioShare)), SSRCs: video}
			select {
			case s.writer.feedback <- []rtcp.Packet{remb}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Close DELETEs the session's resource and closes the peer connection;
// the writer calls it when the sender closes.
func (s *whipSession) Close() error {
	s.closeOnce.Do(func() {
		s.closeErr = s.sig.close()
		s.cancel()
		// Closing the peer connection stops its senders, which ends the
		// writer's RTCP readers.
		if err := s.pc.Close(); s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

// refusedCodecs reports a stream whose media section the answer rejects or
// answers without the stream's codec.
func refusedCodecs(answer string, transceivers []*webrtc.RTPTransceiver, streams []av.Stream) error {
	parsed, err := (&webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}).Unmarshal()
	if err != nil {
		return err
	}
	for i, transceiver := range transceivers {
		codec, _ := streamCapability(streams[i])
		name := mimeName(codec.MimeType)
		accepted := false
		for _, media := range parsed.MediaDescriptions {
			if mid, _ := media.Attribute("mid"); mid != transceiver.Mid() {
				continue
			}
			if media.MediaName.Port.Value == 0 {
				break
			}
			for _, attribute := range media.Attributes {
				if attribute.Key == "rtpmap" && strings.Contains(strings.ToLower(attribute.Value), " "+strings.ToLower(name)+"/") {
					accepted = true
				}
			}
		}
		if !accepted {
			return fmt.Errorf("whip: stream %s: %w: %s; encode it to one the server takes", streams[i].ID, ErrWHIPCodecRefused, codec.MimeType)
		}
	}
	return nil
}

// egressAPI is the API WHIP builds when the caller configures none: Pion's
// default codecs and interceptors, the TWCC header extension, and a
// send-side bandwidth estimator, unpaced, whose estimate the session follows.
func egressAPI(settings *webrtc.SettingEngine, targets chan int) (*webrtc.API, error) {
	media := &webrtc.MediaEngine{}
	if err := media.RegisterDefaultCodecs(); err != nil {
		return nil, err
	}
	registry := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(media, registry); err != nil {
		return nil, err
	}
	estimator, err := cc.NewInterceptor(func() (cc.BandwidthEstimator, error) {
		return gcc.NewSendSideBWE(gcc.SendSideBWEInitialBitrate(1_000_000), gcc.SendSideBWEPacer(gcc.NewNoOpPacer()))
	})
	if err != nil {
		return nil, err
	}
	// The estimate callback is set as the peer connection is made, before
	// any feedback can move the estimate.
	estimator.OnNewPeerConnection(func(_ string, e cc.BandwidthEstimator) {
		e.OnTargetBitrateChange(func(bitrate int) { latest(targets, bitrate) })
	})
	registry.Add(estimator)
	if err := webrtc.ConfigureTWCCHeaderExtensionSender(media, registry); err != nil {
		return nil, err
	}
	options := []func(*webrtc.API){webrtc.WithMediaEngine(media), webrtc.WithInterceptorRegistry(registry)}
	if settings != nil {
		options = append(options, webrtc.WithSettingEngine(*settings))
	}
	return webrtc.NewAPI(options...), nil
}

// latest puts v in the one-slot channel c, replacing what it holds.
func latest(c chan int, v int) {
	for {
		select {
		case c <- v:
			return
		default:
		}
		select {
		case <-c:
		default:
		}
	}
}

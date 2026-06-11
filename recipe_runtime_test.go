package goav

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/thesyncim/goav/av"
	"github.com/thesyncim/goav/codec"
	"github.com/thesyncim/goav/filter"
	"github.com/thesyncim/goav/format"
	"github.com/thesyncim/goav/pipeline"
	"github.com/thesyncim/goav/plan"
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/shape"
	"github.com/thesyncim/goav/snapshot"
	"github.com/thesyncim/goav/webrtcav"
)

// webRTCRemote adapts a fixture RemoteTrack into a provider input the way
// webrtcav.Track does for live tracks: the source name and codec intent are
// derived from the adapted track stream, then handed to rtpav.Receive.
func webRTCRemote(remote webrtcav.RemoteTrack) InputSpec {
	reader, err := webrtcav.NewTrackRemoteAdapter().AdaptTrack(context.Background(), remote)
	if err != nil {
		return InputSpec{err: err}
	}
	streams, err := reader.Streams(context.Background())
	if err != nil {
		return InputSpec{err: err}
	}
	if len(streams) == 0 {
		return InputSpec{err: webrtcav.ErrUnknownStream}
	}
	stream := streams[0]
	options := make([]rtpav.ReceiveOption, 0, 2)
	if name := firstNonEmpty(string(stream.ID), stream.Name); name != "" {
		options = append(options, rtpav.WithName(name))
	}
	if stream.Codec.ID != "" {
		spec := codec.CodecSpec{
			ID:         stream.Codec.ID,
			Type:       stream.Codec.Type,
			Parameters: stream.Codec,
		}
		if spec.Type == "" {
			spec.Type = stream.Type
		}
		if spec.Parameters.Type == "" {
			spec.Parameters.Type = spec.Type
		}
		options = append(options, rtpav.WithCodec(spec))
	}
	return Input(rtpav.Receive(reader, options...))
}

// TestProviderRTPDescribePinsLegacyConstructorStrings pins the Describe()
// node name and detail strings the deleted goav.RTP constructor path produced,
// so an RTP job through the provider seam stays byte-identical: the node is
// named by rtpav.WithName and the detail is the rtpav receive summary.
func TestProviderRTPDescribePinsLegacyConstructorStrings(t *testing.T) {
	job := From(Input(rtpav.Receive(&runtimeRTPReceiver{
		streams: []av.Stream{{
			ID:   "audio",
			Type: av.MediaAudio,
			Codec: av.CodecParameters{
				ID:   av.CodecOpus,
				Type: av.MediaAudio,
			},
		}},
	},
		rtpav.WithName("audio"),
		rtpav.WithCodec(codec.Opus()),
		rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}),
		rtpav.WithMaxTimestampGap(av.SamplesDuration(960, 48000)),
	))).Copy().To(File("recording.ogg", io.Discard, Format(av.FormatOgg)))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	var source *pipeline.NodeSpec
	for i := range spec.Nodes {
		if spec.Nodes[i].Kind == pipeline.NodeSource {
			source = &spec.Nodes[i]
		}
	}
	if source == nil {
		t.Fatalf("spec has no source node:\n%s", specText(spec))
	}
	if source.Name != "audio" {
		t.Fatalf("source name = %q, want %q (legacy goav.RTP naming)", source.Name, "audio")
	}
	if source.Detail != "rtp receive, codec=opus, timestamp gap" {
		t.Fatalf("source detail = %q, want %q", source.Detail, "rtp receive, codec=opus, timestamp gap")
	}
}

func TestWebRTCTrackRecordRecipeUsesCodecIntent(t *testing.T) {
	job := From(
		webRTCRemote(webrtcav.RemoteTrack{
			Track: &webrtc.TrackRemote{},
			Codec: webrtc.RTPCodecParameters{
				RTPCodecCapability: webrtc.RTPCodecCapability{
					MimeType:  webrtc.MimeTypeVP8,
					ClockRate: 90000,
				},
				PayloadType: 96,
			},
			Stream: av.Stream{
				ID:   "video",
				Type: av.MediaVideo,
			},
		}),
	).Copy().To(File("recording.ivf", io.Discard))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "video -> recording.ivf") ||
		!strings.Contains(text, "rtp receive, codec=vp8") ||
		strings.Contains(text, "depacketizers=") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.plan()
	if len(intent.Inputs) != 1 ||
		!intent.Inputs[0].Realtime ||
		intent.Inputs[0].Codec.ID != av.CodecVP8 {
		t.Fatalf("intent: %+v", intent)
	}

	task, err := job.Build(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(spec, built) {
		t.Fatalf("planned = %+v, built = %+v", spec, built)
	}
}

func TestWebRTCTrackUnknownCodecMetadataYieldsNoCodecIntent(t *testing.T) {
	// Unknown track codecs are the provider's concern now: the derived input
	// carries no codec intent, and depacketization fails inside the provider
	// at run time (rtpav.ErrDepacketizerNotFound) instead of at the root.
	input := webRTCRemote(webrtcav.RemoteTrack{
		Track: &webrtc.TrackRemote{},
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  "audio/telephone-event",
				ClockRate: 8000,
			},
			PayloadType: 101,
		},
		Stream: av.Stream{
			ID:   "dtmf",
			Type: av.MediaAudio,
		},
	})
	intent := input.intent()
	if intent.Codec.ID != "" {
		t.Fatalf("codec = %q, want no codec intent for an unknown track codec", intent.Codec.ID)
	}
	if !intent.Realtime || intent.Name != "dtmf" {
		t.Fatalf("intent = %+v, want realtime input named after the track stream", intent)
	}
}

func TestWebRTCTrackRecordMultiInputRecipeUsesCodecIntent(t *testing.T) {
	job := From(webRTCRemote(webrtcav.RemoteTrack{
		Track: &webrtc.TrackRemote{},
		Codec: webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  webrtc.MimeTypeOpus,
				ClockRate: 48000,
				Channels:  2,
			},
			PayloadType: 111,
		},
		Stream: av.Stream{
			ID:   "audio",
			Type: av.MediaAudio,
		},
	})).
		And(webRTCRemote(webrtcav.RemoteTrack{
			Track: &webrtc.TrackRemote{},
			Codec: webrtc.RTPCodecParameters{
				RTPCodecCapability: webrtc.RTPCodecCapability{
					MimeType:  webrtc.MimeTypeVP8,
					ClockRate: 90000,
				},
				PayloadType: 96,
			},
			Stream: av.Stream{
				ID:   "video",
				Type: av.MediaVideo,
			},
		})).
		To(File("recording.webm", io.Discard))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "audio -> recording.webm") ||
		!strings.Contains(text, "video -> recording.webm") ||
		!strings.Contains(text, "rtp receive, codec=opus") ||
		!strings.Contains(text, "rtp receive, codec=vp8") ||
		strings.Contains(text, "depacketizers=") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.plan()
	if len(intent.Inputs) != 2 ||
		intent.Inputs[0].Codec.ID != av.CodecOpus ||
		intent.Inputs[1].Codec.ID != av.CodecVP8 {
		t.Fatalf("intent: %+v", intent)
	}
}

func TestRecordRecipeRTPAutoCodecRuns(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	runtime := New(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	))

	task, err := From(
		Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(File("recording.ogg", io.Discard)).UseRuntime(runtime).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 {
		t.Fatalf("muxers=%d, want 1", len(muxers.muxers))
	}
	if muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "audio" {
		t.Fatalf("writes=%d stream=%s", muxers.muxers[0].writes, muxers.muxers[0].lastStream)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRecordRecipeCopyToTypedDestinationRuns(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	runtime := New(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	))
	job := From(
		Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(File("recording.ogg", io.Discard, Format(av.FormatOgg))).UseRuntime(runtime)

	intent := job.plan()
	if len(intent.Destinations) != 1 || intent.Destinations[0].Name != "recording.ogg" {
		t.Fatalf("intent: %+v", intent)
	}
	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(planned)
	if !strings.Contains(text, "audio -> recording.ogg") {
		t.Fatalf("planned:\n%s", text)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 {
		t.Fatalf("muxers=%d, want 1", len(muxers.muxers))
	}
	if muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "audio" {
		t.Fatalf("writes=%d stream=%s", muxers.muxers[0].writes, muxers.muxers[0].lastStream)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestCustomPacketSourceRunsThroughRecipe(t *testing.T) {
	ctx := context.Background()
	input := Source("generated",
		shape.Packet(av.MediaAudio, av.CodecOpus,
			shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
		),
		func(_ context.Context, push SourcePush) error {
			packet := av.Packet{
				Payload: av.Buffer{
					Bytes:     []byte{1},
					Ownership: av.BufferImmutable,
				},
			}
			if _, err := push.Packet(&packet); err != nil {
				return err
			}
			return push.EOS()
		},
	)
	var packets int
	var events int
	var got av.Packet
	job := From(input).Audio().Copy().To(Sink(SinkFunc("packets", func(_ context.Context, msg Message) error {
		switch msg.Kind {
		case pipeline.MessagePacket:
			packets++
			if msg.Packet != nil {
				got = *msg.Packet
			}
		case pipeline.MessageEvent:
			events++
		}
		return nil
	})))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "custom source") ||
		!strings.Contains(text, "generated -> select-audio") {
		t.Fatalf("spec:\n%s", text)
	}
	intent := job.plan()
	if len(intent.Inputs) != 1 ||
		intent.Inputs[0].Protocol != av.ProtocolCustom ||
		intent.Inputs[0].Codec.ID != av.CodecOpus {
		t.Fatalf("intent: %+v", intent)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if packets != 1 ||
		got.StreamID != "generated" ||
		len(got.Payload.Bytes) != 1 ||
		got.Payload.Bytes[0] != 1 {
		t.Fatalf("packets=%d got=%+v, want one generated opus packet", packets, got)
	}
	if events != 1 {
		t.Fatalf("events=%d, want source EOS event", events)
	}
}

// TestFrontDoorFlowControlSentinelsClassifyRuntimeErrors proves a front-door
// source/stage author can classify runtime flow-control errors with the public
// goav sentinels (errors.Is) without importing the pipeline package: a real
// runtime shutdown surfaces as ErrClosed, and ErrBackpressure is the same value
// the buffered runner returns when a downstream is full.
func TestFrontDoorFlowControlSentinelsClassifyRuntimeErrors(t *testing.T) {
	ctx := context.Background()
	input := Source("gen",
		shape.Packet(av.MediaAudio, av.CodecOpus, shape.Audio(48_000, codec.Stereo, av.SampleFormatS16)),
		func(_ context.Context, push SourcePush) error {
			packet := av.Packet{Payload: av.Buffer{Bytes: []byte{1}, Ownership: av.BufferImmutable}}
			if _, err := push.Packet(&packet); err != nil {
				return err
			}
			return push.EOS()
		},
	)
	task, err := From(input).Audio().Copy().
		To(Sink(SinkFunc("packets", func(context.Context, Message) error { return nil }))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	task.Close()
	if runErr := task.Run(ctx); !errors.Is(runErr, ErrClosed) {
		t.Fatalf("run after close = %v, want errors.Is(ErrClosed)", runErr)
	}
	if !errors.Is(ErrBackpressure, ErrBackpressure) || ErrBackpressure == nil {
		t.Fatal("ErrBackpressure must be a usable front-door sentinel")
	}
}

func TestCustomFrameSourceRunsThroughRecipeWithoutDecode(t *testing.T) {
	ctx := context.Background()
	input := Source("pcm",
		shape.Frame(av.MediaAudio,
			shape.Audio(48_000, codec.Stereo, av.SampleFormatS16),
		),
		func(_ context.Context, push SourcePush) error {
			frame := av.Frame{
				Type: av.MediaAudio,
				Audio: &av.AudioFrame{
					SampleRate:   48_000,
					Channels:     codec.Stereo,
					SampleFormat: av.SampleFormatS16,
					Samples:      480,
				},
			}
			if _, err := push.Frame(&frame); err != nil {
				return err
			}
			return push.EOS()
		},
	)
	var frames int
	var events int
	var got av.Frame
	job := From(input).Audio().To(Sink(SinkFunc("frames", func(_ context.Context, msg Message) error {
		switch msg.Kind {
		case pipeline.MessageFrame:
			frames++
			if msg.Frame != nil {
				got = *msg.Frame
			}
		case pipeline.MessageEvent:
			events++
		}
		return nil
	})))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "custom source") ||
		!strings.Contains(text, "domain=frame") ||
		!strings.Contains(text, "pcm -> select-audio") ||
		strings.Contains(text, "decode-audio") {
		t.Fatalf("spec:\n%s", text)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if frames != 1 ||
		got.StreamID != "pcm" ||
		got.Audio == nil ||
		got.Audio.SampleRate != 48_000 ||
		got.Audio.Channels != codec.Stereo {
		t.Fatalf("frames=%d got=%+v, want one generated PCM frame", frames, got)
	}
	if events != 1 {
		t.Fatalf("events=%d, want source EOS event", events)
	}
}

func TestCustomEventSourceRunsThroughRecipeToSink(t *testing.T) {
	ctx := context.Background()
	input := Source("diagnostics",
		shape.Event(),
		func(_ context.Context, push SourcePush) error {
			if _, err := push.Event(av.Event{Type: av.EventStats}); err != nil {
				return err
			}
			return push.EOS()
		},
	)
	var events []av.Event
	job := From(input).To(Sink(SinkFunc("events", func(_ context.Context, msg Message) error {
		if msg.Kind == pipeline.MessageEvent && msg.Event != nil {
			events = append(events, *msg.Event)
		}
		return nil
	})))

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "custom source") ||
		!strings.Contains(text, "domain=event") ||
		!strings.Contains(text, "diagnostics -> events") ||
		strings.Contains(text, "packet-copy") ||
		strings.Contains(text, "select-") {
		t.Fatalf("spec:\n%s", text)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 ||
		events[0].Type != av.EventStats ||
		events[0].StreamID != "diagnostics" ||
		events[1].Type != av.EventEndOfStream ||
		events[1].StreamID != "diagnostics" {
		t.Fatalf("events=%+v, want stats and EOS from diagnostics", events)
	}
}

func TestCustomEventSourceRejectsMuxDestination(t *testing.T) {
	input := Source("diagnostics",
		shape.Event(),
		func(context.Context, SourcePush) error {
			return nil
		},
	)

	_, err := From(input).To(File("events.ivf", io.Discard, Format(av.FormatIVF))).Describe()
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "graph_plan_invalid" ||
		!strings.Contains(err.Error(), "event source destination must be a sink") {
		t.Fatalf("err = %v, want event source sink-only diagnostic", err)
	}
}

func TestRecordRecipeCopyToCustomWriterDestinationRuns(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{7, 8, 9},
		}},
		events: make(chan av.Event),
	}
	state := &writerDestinationState{}
	muxer := &writerDestinationMuxer{}
	runtime := New(withTestFormats(
		testFormatMuxer(av.FormatOgg, writerDestinationMuxerFactory{muxer: muxer}),
	))
	target := Writer("s3://bucket/call.ogg", func(_ context.Context, info DestinationInfo) (io.WriteCloser, error) {
		state.opens++
		state.info = info
		return &writerDestinationWriteCloser{state: state}, nil
	}, Format(av.FormatOgg), MIME("audio/ogg"))

	task, err := From(
		Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(target).UseRuntime(runtime).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.opens != 1 ||
		state.info.Name != "s3://bucket/call.ogg" ||
		state.info.Format != av.FormatOgg ||
		state.info.MIMEType != "audio/ogg" ||
		len(state.info.Streams) != 1 ||
		state.info.Streams[0].ID != "audio" {
		t.Fatalf("destination info: opens=%d info=%+v", state.opens, state.info)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state.bytes.Bytes(), []byte{7, 8, 9}) {
		t.Fatalf("written bytes = %v", state.bytes.Bytes())
	}
	if muxer.writes != 1 {
		t.Fatalf("muxer writes = %d, want 1", muxer.writes)
	}
	if state.closes != 0 {
		t.Fatalf("writer closed before task close: %d", state.closes)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if state.closes != 1 {
		t.Fatalf("writer closes = %d, want 1", state.closes)
	}
	if state.commits != 1 || state.aborts != 0 {
		t.Fatalf("commits=%d aborts=%d, want commit only", state.commits, state.aborts)
	}
}

// TestRecordRecipeCopyToTransactionalWriterDestinationRuns pins the object-store
// capability through the one Writer constructor: an opened writer that
// implements TransactionalDestinationWriter commits after success and closes
// exactly once.
func TestRecordRecipeCopyToTransactionalWriterDestinationRuns(t *testing.T) {
	ctx := context.Background()
	stream := audioOpusTestStream()
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{9, 8, 7},
		}},
		events: make(chan av.Event),
	}
	state := &writerDestinationState{}
	muxer := &writerDestinationMuxer{}
	runtime := New(withTestFormats(
		testFormatMuxer(av.FormatOgg, writerDestinationMuxerFactory{muxer: muxer}),
	))
	metadata := av.Metadata{"storage": "hot"}
	target := Writer("s3://bucket/object.ogg", func(_ context.Context, info DestinationInfo) (io.WriteCloser, error) {
		state.opens++
		state.info = info
		return &writerDestinationWriteCloser{state: state}, nil
	}, Format(av.FormatOgg), MIME("audio/ogg"), Metadata(metadata))

	task, err := From(
		Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(target).UseRuntime(runtime).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if state.opens != 1 ||
		state.info.Name != "s3://bucket/object.ogg" ||
		state.info.Format != av.FormatOgg ||
		state.info.MIMEType != "audio/ogg" ||
		state.info.Metadata["storage"] != "hot" ||
		len(state.info.Streams) != 1 ||
		state.info.Streams[0].ID != "audio" {
		t.Fatalf("destination info: opens=%d info=%+v", state.opens, state.info)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state.bytes.Bytes(), []byte{9, 8, 7}) {
		t.Fatalf("written bytes = %v", state.bytes.Bytes())
	}
	if muxer.writes != 1 {
		t.Fatalf("muxer writes = %d, want 1", muxer.writes)
	}
	if state.closes != 1 || state.commits != 1 || state.aborts != 0 {
		t.Fatalf("closes=%d commits=%d aborts=%d, want one close and commit", state.closes, state.commits, state.aborts)
	}
}

// TestFileDestinationClosesCloserWriterOnce pins the goav.File close contract:
// a writer that also implements io.Closer is closed exactly once when the
// destination finalizes, while plain writers stay the caller's to close.
func TestFileDestinationClosesCloserWriterOnce(t *testing.T) {
	ctx := context.Background()
	stream := audioOpusTestStream()
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{4, 5, 6},
		}},
		events: make(chan av.Event),
	}
	muxer := &writerDestinationMuxer{}
	runtime := New(withTestFormats(
		testFormatMuxer(av.FormatOgg, writerDestinationMuxerFactory{muxer: muxer}),
	))
	writer := &fileDestinationWriteCloser{}

	task, err := From(
		Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(File("call.ogg", writer, Format(av.FormatOgg))).UseRuntime(runtime).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if writer.closes != 0 {
		t.Fatalf("writer closed before task close: %d", writer.closes)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(writer.bytes.Bytes(), []byte{4, 5, 6}) {
		t.Fatalf("written bytes = %v", writer.bytes.Bytes())
	}
	if writer.closes != 1 {
		t.Fatalf("writer closes = %d, want exactly one close", writer.closes)
	}
}

type fileDestinationWriteCloser struct {
	bytes  bytes.Buffer
	closes int
}

func (w *fileDestinationWriteCloser) Write(p []byte) (int, error) {
	return w.bytes.Write(p)
}

func (w *fileDestinationWriteCloser) Close() error {
	w.closes++
	return nil
}

func TestRecordRecipeCustomWriterDestinationAbortsOnRunError(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	runErr := errors.New("rtp read failed")
	receiver := &runtimeRTPErrorReceiver{
		runtimeRTPReceiver: runtimeRTPReceiver{
			streams: []av.Stream{stream},
			payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
				PayloadType: 111,
				Parameters:  stream.Codec,
				MIMEType:    rtpav.MIMEOpus,
				ClockRate:   48000,
				Channels:    codec.Stereo,
			}}),
			packets: []*rtp.Packet{{
				Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
				Payload: []byte{7, 8, 9},
			}},
			events: make(chan av.Event),
		},
		err: runErr,
	}
	state := &writerDestinationState{}
	muxer := &writerDestinationMuxer{}
	runtime := New(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, writerDestinationMuxerFactory{muxer: muxer}),
	))

	task, err := From(
		Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(Writer(
		"s3://bucket/call.ogg",
		func(context.Context, DestinationInfo) (io.WriteCloser, error) {
			state.opens++
			return &writerDestinationWriteCloser{state: state}, nil
		},
		Format(av.FormatOgg),
		MIME("audio/ogg"),
	)).UseRuntime(runtime).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = task.Run(ctx)
	if !errors.Is(err, runErr) {
		t.Fatalf("run err = %v, want %v", err, runErr)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if state.opens != 1 || state.closes != 1 {
		t.Fatalf("opens=%d closes=%d, want one open and close", state.opens, state.closes)
	}
	if state.commits != 0 || state.aborts != 1 {
		t.Fatalf("commits=%d aborts=%d, want abort only", state.commits, state.aborts)
	}
}

func TestTaskAttachCustomWriterDestinationRuns(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{4, 5, 6},
		}},
		events: make(chan av.Event),
	}
	state := &writerDestinationState{}
	muxer := &writerDestinationMuxer{}
	runtime := New(withTestFormats(
		testFormatMuxer(av.FormatOgg, writerDestinationMuxerFactory{muxer: muxer}),
	))
	task, err := From(Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2})))).
		UseRuntime(runtime).
		Audio().
		Copy().
		Tap(PacketTap("audio.packets")).
		To(Sink(SinkFunc("base", func(context.Context, Message) error { return nil }))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	writer := Writer(
		"s3://bucket/late.ogg",
		func(_ context.Context, info DestinationInfo) (io.WriteCloser, error) {
			state.opens++
			state.info = info
			return &writerDestinationWriteCloser{state: state}, nil
		},
		Format(av.FormatOgg),
		MIME("audio/ogg"),
	)
	attachment, err := task.Attach(ctx,
		Branch("late").
			From(PacketTap("audio.packets")).
			Copy().
			To(writer),
	)
	if err != nil {
		_ = task.Close()
		t.Fatal(err)
	}
	if attachment == nil {
		_ = task.Close()
		t.Fatal("attachment = nil, want runtime attachment")
	}
	if state.opens != 1 ||
		state.info.Name != "s3://bucket/late.ogg" ||
		state.info.Format != av.FormatOgg ||
		state.info.MIMEType != "audio/ogg" ||
		len(state.info.Streams) != 1 ||
		state.info.Streams[0].ID != "audio" {
		_ = task.Close()
		t.Fatalf("destination info: opens=%d info=%+v", state.opens, state.info)
	}
	if err := task.Run(ctx); err != nil {
		_ = task.Close()
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(state.bytes.Bytes(), []byte{4, 5, 6}) {
		t.Fatalf("written bytes = %v", state.bytes.Bytes())
	}
	if muxer.writes != 1 {
		t.Fatalf("muxer writes = %d, want 1", muxer.writes)
	}
	if state.closes != 1 || state.commits != 1 || state.aborts != 0 {
		t.Fatalf("closes=%d commits=%d aborts=%d, want one close and commit", state.closes, state.commits, state.aborts)
	}
}

func TestTaskAttachCustomWriterDestinationAbortsOnPatchFailure(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		events: make(chan av.Event),
	}
	state := &writerDestinationState{}
	muxer := &writerDestinationMuxer{}
	runtime := New(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, writerDestinationMuxerFactory{muxer: muxer}),
	))
	task, err := From(Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus())))).
		UseRuntime(runtime).
		Audio().
		Copy().
		Tap(PacketTap("audio.packets")).
		To(Sink(SinkFunc("base", func(context.Context, Message) error { return nil }))).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	writer := Writer(
		"s3://bucket/late.ogg",
		func(context.Context, DestinationInfo) (io.WriteCloser, error) {
			state.opens++
			return &writerDestinationWriteCloser{state: state}, nil
		},
		Format(av.FormatOgg),
		MIME("audio/ogg"),
	)
	_, err = task.Attach(ctx,
		Branch("late").
			From(PacketTap("audio.packets")).
			Do(PacketFunc("meter", func(_ context.Context, packet *av.Packet, emit Emit) error {
				return emit.Packet(packet)
			})).
			Copy().
			To(Sink(SinkFunc("one", func(context.Context, Message) error { return nil }))),
		Branch("late").
			From(PacketTap("audio.packets")).
			Do(PacketFunc("meter", func(_ context.Context, packet *av.Packet, emit Emit) error {
				return emit.Packet(packet)
			})).
			Copy().
			To(writer),
	)
	if err == nil {
		t.Fatal("Attach succeeded, want duplicate node failure")
	}
	if !errors.Is(err, pipeline.ErrNodeExists) {
		t.Fatalf("attach err = %v, want node duplicate", err)
	}
	if state.opens != 1 || state.closes != 1 {
		t.Fatalf("opens=%d closes=%d, want one open and close", state.opens, state.closes)
	}
	if state.commits != 0 || state.aborts != 1 {
		t.Fatalf("commits=%d aborts=%d, want abort only", state.commits, state.aborts)
	}
}

type writerDestinationState struct {
	bytes   bytes.Buffer
	info    DestinationInfo
	opens   int
	closes  int
	commits int
	aborts  int
}

type writerDestinationWriteCloser struct {
	state *writerDestinationState
}

func (w *writerDestinationWriteCloser) Write(p []byte) (int, error) {
	return w.state.bytes.Write(p)
}

func (w *writerDestinationWriteCloser) Close() error {
	w.state.closes++
	return nil
}

func (w *writerDestinationWriteCloser) Commit(context.Context) error {
	w.state.commits++
	return nil
}

func (w *writerDestinationWriteCloser) Abort(context.Context) error {
	w.state.aborts++
	return nil
}

type writerDestinationMuxerFactory struct {
	muxer *writerDestinationMuxer
}

func (f writerDestinationMuxerFactory) NewMuxer(context.Context, av.FormatID) (format.Muxer, error) {
	return f.muxer, nil
}

type writerDestinationMuxer struct {
	writer io.Writer
	writes int
	closed bool
}

func (m *writerDestinationMuxer) Format() av.FormatID {
	return av.FormatOgg
}

func (m *writerDestinationMuxer) Open(_ context.Context, output format.Output, _ []av.Stream, _ format.OpenOptions) error {
	m.writer = output.Writer
	return nil
}

func (m *writerDestinationMuxer) Write(_ context.Context, packet *av.Packet, _ *format.WriteResult) error {
	m.writes++
	_, err := m.writer.Write(packet.Payload.Bytes)
	return err
}

func (m *writerDestinationMuxer) Close() error {
	m.closed = true
	return nil
}

type runtimeRTPErrorReceiver struct {
	runtimeRTPReceiver
	err error
}

func (r *runtimeRTPErrorReceiver) ReadRTP(ctx context.Context) (*rtp.Packet, error) {
	if r.index >= len(r.packets) {
		return nil, r.err
	}
	return r.runtimeRTPReceiver.ReadRTP(ctx)
}

func TestRecordRecipeRTPCodecUsesReaderStreamWhenUnnamed(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	runtime := New(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	))

	task, err := From(
		Input(rtpav.Receive(receiver, rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(File("recording.ogg", io.Discard)).UseRuntime(runtime).Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 {
		t.Fatalf("muxers=%d, want 1", len(muxers.muxers))
	}
	if muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "audio" {
		t.Fatalf("writes=%d stream=%s", muxers.muxers[0].writes, muxers.muxers[0].lastStream)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultRecordRecipeRTPVP8Runs(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     16,
			Height:    16,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90},
			Payload: []byte{0x10, 0x00, 0x11, 0x22},
		}},
		events: make(chan av.Event),
	}
	var out bytes.Buffer
	job := From(
		Input(rtpav.Receive(receiver, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).Copy().To(File("recording.ivf", &out))

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	stats := task.Stats()
	if stats.Messages < 2 ||
		stats.Packets != 1 ||
		stats.Events == 0 ||
		stats.EventsByType[av.EventEndOfStream] != 1 ||
		stats.Delivered < 2 ||
		stats.Dropped != 0 ||
		!stats.LastEventPresent ||
		stats.LastEvent.Type != av.EventEndOfStream {
		t.Fatalf("stats = %+v", stats)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if out.Len() <= 32 {
		t.Fatalf("output bytes=%d, want IVF header and frame", out.Len())
	}
}

func TestRecordRecipeInputMIMEDrivesFormatProbe(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{{ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}}}
	demuxer := &remuxTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	runtime := New(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatDemuxer(av.FormatOgg, remuxTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	))
	job := From(
		FileInput("", strings.NewReader(""), MIME("audio/ogg")),
	).Copy().To(File("recording.ogg", io.Discard)).UseRuntime(runtime)
	intent := job.plan()
	if len(intent.Inputs) != 1 || intent.Inputs[0].MIMEType != "audio/ogg" {
		t.Fatalf("intent: %+v", intent)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.opened || len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 {
		t.Fatalf("opened=%v muxers=%d", demuxer.opened, len(muxers.muxers))
	}
}

func TestRecordRecipeOutputMIMEDrivesFormatProbe(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{{ID: "audio", Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}}}
	demuxer := &remuxTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	runtime := New(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatDemuxer(av.FormatOgg, remuxTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	))
	job := From(
		FileInput("input.ogg", strings.NewReader("")),
	).Copy().To(File("", io.Discard, MIME("audio/ogg"))).UseRuntime(runtime)
	intent := job.plan()
	if len(intent.Destinations) != 1 || intent.Destinations[0].MIMEType != "audio/ogg" {
		t.Fatalf("intent: %+v", intent)
	}

	spec, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(spec)
	if !strings.Contains(text, "input.ogg -> output") ||
		!strings.Contains(text, "mime=audio/ogg") {
		t.Fatalf("spec:\n%s", text)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 {
		t.Fatalf("muxers=%d", len(muxers.muxers))
	}
}

func TestFromAndRecordRecipeMultipleRTPInputsRuns(t *testing.T) {
	ctx := context.Background()
	audio := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	video := av.Stream{
		ID:       "video",
		Type:     av.MediaVideo,
		TimeBase: av.TimeBase{Num: 1, Den: 90000},
		Codec: av.CodecParameters{
			ID:        av.CodecVP8,
			Type:      av.MediaVideo,
			ClockRate: 90000,
			Width:     16,
			Height:    16,
		},
	}
	audioReceiver := &runtimeRTPReceiver{
		streams: []av.Stream{audio},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  audio.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	videoReceiver := &runtimeRTPReceiver{
		streams: []av.Stream{video},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 96,
			Parameters:  video.Codec,
			MIMEType:    rtpav.MIMEVP8,
			ClockRate:   90000,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 96, Marker: true, Timestamp: 90},
			Payload: []byte{0x10, 0x00, 0x11, 0x22},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	runtime := New(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	))

	task, err := From(
		Input(rtpav.Receive(audioReceiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2}))),
	).UseRuntime(runtime).
		And(Input(rtpav.Receive(videoReceiver, rtpav.WithName("video"), rtpav.WithCodec(codec.VP8()), rtpav.WithBufferLimits(rtpav.BufferLimits{MaxPackets: 2})))).
		To(File("recording.ogg", io.Discard)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 {
		t.Fatalf("muxers=%d, want 1", len(muxers.muxers))
	}
	muxer := muxers.muxers[0]
	if muxer.streamCount != 2 || muxer.writes != 2 {
		t.Fatalf("streamCount=%d writes=%d", muxer.streamCount, muxer.writes)
	}
	if len(muxer.writtenStreams) != 2 || muxer.writtenStreams[0] != "audio" || muxer.writtenStreams[1] != "video" {
		t.Fatalf("written streams=%v", muxer.writtenStreams)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !audioReceiver.closed || !videoReceiver.closed || !muxer.closed {
		t.Fatalf("closed audio=%v video=%v mux=%v", audioReceiver.closed, videoReceiver.closed, muxer.closed)
	}
}

func TestStreamRecipeReportsAmbiguousStreams(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{
		{ID: "audio-main", Index: 0, Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
		{ID: "audio-alt", Index: 1, Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
	}
	demuxer := &decodeTestDemuxer{streams: streams}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	codecs := withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))

	_, err := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		To(Sink(&runtimeTestSink{name: "frames"})).
		Build(ctx)

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "stream_ambiguous" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want stream_ambiguous wrapping ErrUnsupportedBuild", err)
	}
	text := err.Error()
	for _, want := range []string{
		"audio[0] id=audio-main codec=opus",
		"audio[1] id=audio-alt codec=opus",
		".Audio(goav.StreamID(\"audio-main\"))",
		".Audio(goav.StreamIndex(0))",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("err text missing %q:\n%s", want, text)
		}
	}
}

func TestStreamRecipeSelectsFirstStreamByIndex(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{
		{ID: "audio-main", Index: 0, Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
		{ID: "audio-alt", Index: 1, Type: av.MediaAudio, Codec: av.CodecParameters{ID: av.CodecOpus, Type: av.MediaAudio}},
	}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio-main",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoder := &decodeTestDecoder{}
	codecs := withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}))
	sink := &runtimeTestSink{name: "frames"}

	task, err := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio(StreamIndex(0)).
		To(Sink(sink)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || sink.frames != 1 || sink.lastFrame.StreamID != "audio-main" {
		t.Fatalf("decodes=%d frames=%d last=%s", decoder.decodes, sink.frames, sink.lastFrame.StreamID)
	}
	stats := task.Stats()
	if stats.Packets == 0 ||
		stats.Frames != 1 ||
		stats.EventsByType[av.EventStreamAdded] == 0 ||
		stats.EventsByType[av.EventEndOfStream] == 0 ||
		stats.Delivered == 0 ||
		stats.Dropped != 0 ||
		!stats.LastEventPresent ||
		stats.LastEvent.Type != av.EventEndOfStream {
		t.Fatalf("stats = %+v", stats)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStreamRecipeEncodeToSinkDestinationRuns(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoder := &decodeTestDecoder{}
	encoder := &encodeTestEncoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: encoder}),
	)
	sink := &runtimeTestSink{name: "packets"}

	task, err := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(Sink(sink)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || encoder.encodes != 1 || sink.lastPacket == nil || sink.frames != 0 {
		t.Fatalf("decodes=%d encodes=%d packet=%v frames=%d", decoder.decodes, encoder.encodes, sink.lastPacket, sink.frames)
	}
	if len(sink.lastPacketValue.Payload.Bytes) != 1 || sink.lastPacketValue.Payload.Bytes[0] != 7 {
		t.Fatalf("packet payload=%v", sink.lastPacketValue.Payload.Bytes)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !encoder.closed || !sink.closed {
		t.Fatalf("closed demux=%v decoder=%v encoder=%v sink=%v", demuxer.closed, decoder.closed, encoder.closed, sink.closed)
	}
}

func TestStreamRecipeEncodeFansOutToMuxAndSinkDestinations(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoder := &encodeTestEncoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: encoder}),
	)
	sink := &runtimeTestSink{name: "packets"}

	task, err := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(
			File("archive.ogg", io.Discard),
			Sink(sink),
		).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || encoder.encodes != 1 || len(muxers.muxers) != 1 || sink.lastPacket == nil || sink.frames != 0 {
		t.Fatalf("decodes=%d encodes=%d muxers=%d packet=%v frames=%d", decoder.decodes, encoder.encodes, len(muxers.muxers), sink.lastPacket, sink.frames)
	}
	if muxers.muxers[0].writes != 1 {
		t.Fatalf("mux writes=%d, want 1", muxers.muxers[0].writes)
	}
	if len(sink.lastPacketValue.Payload.Bytes) != 1 || sink.lastPacketValue.Payload.Bytes[0] != 7 {
		t.Fatalf("packet payload=%v", sink.lastPacketValue.Payload.Bytes)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !encoder.closed || !muxers.muxers[0].closed || !sink.closed {
		t.Fatalf("closed demux=%v decoder=%v encoder=%v mux=%v sink=%v", demuxer.closed, decoder.closed, encoder.closed, muxers.muxers[0].closed, sink.closed)
	}
}

func TestStreamRecipeEncodeToTypedDestinationRuns(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoder := &encodeTestEncoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: encoder}),
	)
	target := File("archive.ogg", io.Discard, Format(av.FormatOgg))
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(target)

	intent := job.plan()
	if len(intent.Streams) != 1 ||
		len(intent.Streams[0].Destinations) != 1 ||
		intent.Streams[0].Destinations[0] != "archive.ogg" ||
		len(intent.Destinations) != 1 ||
		intent.Destinations[0].Name != "archive.ogg" {
		t.Fatalf("intent: %+v", intent)
	}
	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(planned)
	if !strings.Contains(text, "encode-audio -> archive.ogg") {
		t.Fatalf("planned:\n%s", text)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(built))
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || encoder.encodes != 1 || len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 {
		t.Fatalf("decodes=%d encodes=%d muxers=%d first=%+v", decoder.decodes, encoder.encodes, len(muxers.muxers), muxers.muxers)
	}
	if muxers.muxers[0].lastStream != "audio" {
		t.Fatalf("mux stream=%s, want audio", muxers.muxers[0].lastStream)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !encoder.closed || !muxers.muxers[0].closed {
		t.Fatalf("closed demux=%v decoder=%v encoder=%v mux=%v", demuxer.closed, decoder.closed, encoder.closed, muxers.muxers[0].closed)
	}
}

func TestStreamRecipeCopyTapCanAttachRuntimeSink(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	base := &runtimeTestSink{name: "packets"}
	task, err := From(FileInput("input.ogg", nil)).UseRuntime(New(formats)).
		Audio().
		Copy().
		Tap(PacketTap("audio.packets")).
		To(Sink(base)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	taps := task.Taps()
	if len(taps) != 1 || taps[0].Name != "audio.packets" || taps[0].Domain != shape.DomainPacket || taps[0].Node.String() != "select-audio" {
		t.Fatalf("taps = %+v", taps)
	}
	late := &runtimeTestSink{name: "late-packets"}
	attachment, err := task.Attach(ctx, Branch("late").From(PacketTap("audio.packets")).Copy().To(Sink(late)))
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.Close(ctx)
	runtimeAttachment, ok := attachment.(*runtimeAttachment)
	if !ok {
		t.Fatalf("attachment = %T, want runtimeAttachment", attachment)
	}
	if got, want := workPatchOperationKindsForBranch(runtimeAttachment.work.Operations, "late"), []plan.OperationKind{plan.OpCopy, plan.OpSink}; !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime copy work operations = %+v, want %+v", got, want)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if base.lastPacket == nil || late.lastPacket == nil {
		t.Fatalf("base packet=%v late packet=%v", base.lastPacket, late.lastPacket)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStreamRecipeCopyFansOutToMuxAndSinkDestinations(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	sink := &runtimeTestSink{name: "packets"}
	task, err := From(FileInput("input.ogg", nil)).UseRuntime(New(formats)).
		Audio().
		Copy().
		To(
			File("archive.ogg", io.Discard),
			Sink(sink),
		).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 || sink.lastPacket == nil || sink.frames != 0 {
		t.Fatalf("muxers=%d packet=%v frames=%d", len(muxers.muxers), sink.lastPacket, sink.frames)
	}
	if muxers.muxers[0].writes != 1 {
		t.Fatalf("mux writes=%d, want 1", muxers.muxers[0].writes)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !muxers.muxers[0].closed || !sink.closed {
		t.Fatalf("closed demux=%v mux=%v sink=%v", demuxer.closed, muxers.muxers[0].closed, sink.closed)
	}
}

func TestTaskAttachRuntimeFlowCopyBranchFromPacketTap(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	base := &runtimeTestSink{name: "packets"}
	task, err := From(FileInput("input.ogg", nil)).UseRuntime(New(formats)).
		Audio().
		Copy().
		Tap(PacketTap("audio.packets")).
		To(Sink(base)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	late := &runtimeTestSink{name: "late-packets"}
	flow := Flow("late").
		Audio().
		Copy().
		Tap(PacketTap("audio.late.packets"))
	parent, err := task.Attach(ctx, Branch("late").
		From(PacketTap("audio.packets")).
		Apply(flow).
		To(Sink(late)))
	if err != nil {
		t.Fatal(err)
	}
	lateTap, ok := findTap(task.Taps(), "audio.late.packets")
	if !ok ||
		lateTap.Domain != shape.DomainPacket ||
		lateTap.MediaKind != av.MediaAudio ||
		lateTap.After != plan.OpCopy ||
		lateTap.Shape.Codec != av.CodecOpus ||
		lateTap.Node != "select-audio" {
		t.Fatalf("late tap = %+v ok=%v, want packet Opus tap on select-audio", lateTap, ok)
	}
	watcher := &runtimeTestSink{name: "watch-late"}
	child, err := task.Attach(ctx, Branch("watch").
		From(PacketTap("audio.late.packets")).
		Copy().
		To(Sink(watcher)))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if base.lastPacket == nil || late.lastPacket == nil || watcher.lastPacket == nil {
		t.Fatalf("base packet=%v late packet=%v watcher packet=%v", base.lastPacket, late.lastPacket, watcher.lastPacket)
	}
	if err := task.Detach(ctx, child); err != nil {
		t.Fatal(err)
	}
	if err := task.Detach(ctx, parent); err != nil {
		t.Fatal(err)
	}
}

func TestBranchCompositionCopyBranchesFanOutPackets(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &remuxTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, remuxTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	sink := &runtimeTestSink{name: "packets"}
	task, err := From(FileInput("input.ogg", nil)).UseRuntime(New(formats)).
		Audio().
		Copy().
		Tap(PacketTap("audio.packets")).
		Branches(
			Branch("archive").To(File("archive.ogg", io.Discard)),
			Branch("packets").To(Sink(sink)),
		).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	spec := task.Describe()
	text := specText(spec)
	for _, want := range []string{
		"input.ogg -> select-audio",
		"select-audio -> archive.ogg",
		"select-audio -> packets",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("spec missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, "decode-audio") || strings.Contains(text, "encode-archive") {
		t.Fatalf("packet copy branches should not decode or encode:\n%s", text)
	}

	var packetTap snapshot.Tap
	for _, tap := range task.Taps() {
		if tap.Name == "audio.packets" {
			packetTap = tap
			break
		}
	}
	if packetTap.Name == "" ||
		packetTap.Domain != shape.DomainPacket ||
		packetTap.MediaKind != av.MediaAudio ||
		packetTap.Shape.Codec != av.CodecOpus ||
		packetTap.Node != "select-audio" {
		t.Fatalf("packet tap = %+v, want Opus packet tap on select-audio", packetTap)
	}

	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 || sink.lastPacket == nil || sink.frames != 0 {
		t.Fatalf("muxers=%d writes=%d packet=%v frames=%d", len(muxers.muxers), firstMuxWrites(muxers), sink.lastPacket, sink.frames)
	}
	if muxers.muxers[0].lastStream != "audio" || sink.lastPacketValue.StreamID != "audio" {
		t.Fatalf("mux stream=%q sink stream=%q, want audio", muxers.muxers[0].lastStream, sink.lastPacketValue.StreamID)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !muxers.muxers[0].closed || !sink.closed {
		t.Fatalf("closed demux=%v mux=%v sink=%v", demuxer.closed, muxers.muxers[0].closed, sink.closed)
	}
}

func firstMuxWrites(factory *remuxTestMuxerFactory) int {
	if factory == nil || len(factory.muxers) == 0 || factory.muxers[0] == nil {
		return 0
	}
	return factory.muxers[0].writes
}

func TestStreamRecipeCopyTapCanAttachRuntimeMuxDestination(t *testing.T) {
	ctx := context.Background()
	stream := av.Stream{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:         av.CodecOpus,
			Type:       av.MediaAudio,
			ClockRate:  48000,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    codec.Stereo,
		}}),
		packets: []*rtp.Packet{{
			Header:  rtp.Header{PayloadType: 111, Timestamp: 960},
			Payload: []byte{1, 2, 3},
		}},
		events: make(chan av.Event),
	}
	muxers := &remuxTestMuxerFactory{}
	runtime := New(withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	))

	task, err := From(Input(rtpav.Receive(receiver, rtpav.WithName("audio"), rtpav.WithCodec(codec.Opus())))).
		UseRuntime(runtime).
		Audio().
		Copy().
		Tap(PacketTap("audio.copied")).
		To(File("archive.ogg", io.Discard)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	var copiedTap snapshot.Tap
	for _, tap := range task.Taps() {
		if tap.Name == "audio.copied" {
			copiedTap = tap
			break
		}
	}
	if copiedTap.Name == "" ||
		copiedTap.Domain != shape.DomainPacket ||
		copiedTap.MediaKind != av.MediaAudio ||
		copiedTap.Shape.Codec != av.CodecOpus ||
		copiedTap.Shape.StreamID != "audio" ||
		copiedTap.Node != "select-audio" {
		t.Fatalf("copied tap = %+v, want packet Opus audio tap on select-audio", copiedTap)
	}

	recording, err := task.Attach(ctx, Branch("record").
		From(PacketTap("audio.copied")).
		Copy().
		To(File("recording.ogg", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 2 ||
		muxers.muxers[0].writes != 1 ||
		muxers.muxers[1].writes != 1 ||
		muxers.muxers[0].lastStream != "audio" ||
		muxers.muxers[1].lastStream != "audio" {
		t.Fatalf("muxers=%d first=%+v second=%+v", len(muxers.muxers), muxers.muxers[0], muxers.muxers[1])
	}
	if err := task.Detach(ctx, recording); err != nil {
		t.Fatal(err)
	}
	if !muxers.muxers[1].closed {
		t.Fatal("late recording muxer was not closed by detach")
	}
}

func TestStreamRecipeDescribeMatchesBuiltGraph(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	codecs := withTestCodecs(testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}))
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		To(Sink(&runtimeTestSink{name: "frames"}))

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestFromAudioStreamRecipeDoEncodeRuns(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory),
	)
	meter := &runtimeTestStage{name: "meter"}

	task, err := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Do(meter).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(File("archive.ogg", io.Discard)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || decoder.flushes != 1 || meter.count != 3 || encoder.encodes != 1 || encoder.flushes != 1 {
		t.Fatalf("decodes=%d decoder flushes=%d meter=%d encodes=%d encoder flushes=%d", decoder.decodes, decoder.flushes, meter.count, encoder.encodes, encoder.flushes)
	}
	if encoderFactory.config.Parameters.ID != av.CodecOpus || encoderFactory.config.Settings.Bitrate != 96_000 {
		t.Fatalf("encode config: %+v", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "audio" {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !meter.closed || !encoder.closed || !muxers.muxers[0].closed {
		t.Fatalf("closed demux=%v decoder=%v meter=%v encoder=%v mux=%v", demuxer.closed, decoder.closed, meter.closed, encoder.closed, muxers.muxers[0].closed)
	}
}

func TestBranchCompositionRecipeDescribeMatchesBuiltGraph(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Decode().
		Tap(FrameTap("audio.decoded")).
		Branches(Branch("main").Encode(codec.Opus(codec.Bitrate(96_000))).To(File("archive.ogg", io.Discard)))

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned = %+v, built = %+v", planned, built)
	}
}

func TestBranchCompositionSharedParentOperationDescribeMatchesBuiltGraph(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{videoVP8TranscodeTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	resizeFactory := &transcodeTestFilterFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatIVF, muxers),
	)
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResize,
		Input:  av.MediaVideo,
		Output: av.MediaVideo,
	}, resizeFactory))
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs, filters)).
		Video().
		Decode().
		Resize(1280, 720).
		Tap(FrameTap("video.720p.frames")).
		Branches(
			Branch("web").
				Encode(codec.VP9(codec.Bitrate(2_000_000))).
				To(File("web.ivf", io.Discard, Format(av.FormatIVF))),
			Branch("thumb").
				Resize(320, 180).
				To(Sink(&runtimeTestSink{name: "thumbnail"})),
		)

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(planned)
	for _, want := range []string{
		"decode-video -> resize-video",
		"resize-video -> encode-web",
		"resize-video -> resize-thumb",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("planned spec missing %q:\n%s", want, text)
		}
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(built))
	}
}

func TestBranchCompositionCurrentPointDescribeMatchesBuiltGraph(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{videoVP8TranscodeTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	resizeFactory := &transcodeTestFilterFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatIVF, muxers),
	)
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResize,
		Input:  av.MediaVideo,
		Output: av.MediaVideo,
	}, resizeFactory))
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs, filters)).
		Video().
		Decode().
		Resize(1280, 720).
		Branches(
			Branch("web").
				Encode(codec.VP9(codec.Bitrate(2_000_000))).
				To(File("web.ivf", io.Discard, Format(av.FormatIVF))),
			Branch("thumb").
				Resize(320, 180).
				To(Sink(&runtimeTestSink{name: "thumbnail"})),
		)

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(planned)
	for _, want := range []string{
		"decode-video -> resize-video",
		"resize-video -> encode-web",
		"resize-video -> resize-thumb",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("planned spec missing %q:\n%s", want, text)
		}
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(built))
	}
}

func TestBranchCompositionEncodeFPSOptionSetsEncodeFramerate(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{videoVP8TranscodeTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	resizeFactory := &transcodeTestFilterFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatIVF, muxers),
	)
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResize,
		Input:  av.MediaVideo,
		Output: av.MediaVideo,
	}, resizeFactory))
	encoderFactory := &encodeTestEncoderFactory{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, encoderFactory),
	)
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs, filters)).
		Video().
		Decode().
		Branches(
			Branch("web").
				Resize(1280, 720).
				Encode(codec.VP9(codec.Bitrate(2_000_000), codec.FPS(30))).
				To(File("web.ivf", io.Discard, Format(av.FormatIVF))),
		)

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	want := av.Duration{Value: 1, Base: av.TimeBase{Num: 1, Den: 30}}
	if encoderFactory.config.Settings.Framerate != want {
		t.Fatalf("encoder framerate = %+v, want %+v", encoderFactory.config.Settings.Framerate, want)
	}
	if encoderFactory.config.Stream.Codec.Width != 1280 ||
		encoderFactory.config.Stream.Codec.Height != 720 ||
		encoderFactory.config.Stream.Codec.ID != av.CodecVP9 {
		t.Fatalf("encoder stream shape = %+v, want VP9 1280x720", encoderFactory.config.Stream.Codec)
	}
}

func TestBranchCompositionSharedResampleCurrentPointRuns(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	resampleFactory := &transcodeTestFilterFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResample,
		Input:  av.MediaAudio,
		Output: av.MediaAudio,
	}, resampleFactory))
	decoder := &decodeTestDecoder{}
	encoder := &encodeTestEncoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: encoder}),
	)
	levels := &runtimeTestSink{name: "levels"}
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs, filters)).
		Audio().
		Decode().
		Resample(16_000, codec.Mono).
		Branches(
			Branch("voice").
				Encode(codec.Opus(codec.Bitrate(64_000))).
				To(File("voice.ogg", io.Discard, Format(av.FormatOgg))),
			Branch("levels").
				To(Sink(levels)),
		)

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(planned)
	for _, want := range []string{
		"decode-audio -> resample-audio",
		"resample-audio -> encode-voice",
		"resample-audio -> levels",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("planned spec missing %q:\n%s", want, text)
		}
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(built))
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || resampleFactory.filter.frames != 1 || encoder.encodes != 1 || levels.frames != 1 {
		t.Fatalf("decodes=%d resampled=%d encodes=%d levels=%d", decoder.decodes, resampleFactory.filter.frames, encoder.encodes, levels.frames)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "voice" {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !resampleFactory.filter.closed || !encoder.closed || !levels.closed || !muxers.muxers[0].closed {
		t.Fatalf("closed demux=%v resample=%v encoder=%v levels=%v mux=%v", demuxer.closed, resampleFactory.filter.closed, encoder.closed, levels.closed, muxers.muxers[0].closed)
	}
}

func TestBranchCompositionFrameSinkDestinationRuns(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoder := &decodeTestDecoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
	)
	sink := &runtimeTestSink{name: "frames"}
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Decode().
		Branches(Branch("frames").To(Sink(sink)))

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(specText(planned), "encode-frames") ||
		!strings.Contains(specText(planned), "decode-audio -> frames") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || sink.frames != 1 || sink.lastFrame.StreamID != "audio" || sink.lastPacket != nil {
		t.Fatalf("decodes=%d frames=%d frame=%+v packet=%v", decoder.decodes, sink.frames, sink.lastFrame, sink.lastPacket)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !sink.closed {
		t.Fatalf("closed demux=%v decoder=%v sink=%v", demuxer.closed, decoder.closed, sink.closed)
	}
}

func TestBranchCompositionPacketBranchDecodeSinkRuns(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoder := &decodeTestDecoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
	)
	sink := &runtimeTestSink{name: "frames"}
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Copy().
		Branches(Branch("frames").Decode().To(Sink(sink)))

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(planned)
	if strings.Contains(text, "encode-frames") ||
		!strings.Contains(text, "input.ogg -> select-audio") ||
		!strings.Contains(text, "select-audio -> decode-audio") ||
		!strings.Contains(text, "decode-audio -> frames") {
		t.Fatalf("planned:\n%s", text)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(built))
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || sink.frames != 1 || sink.lastFrame.StreamID != "audio" || sink.lastPacket != nil {
		t.Fatalf("decodes=%d frames=%d frame=%+v packet=%v", decoder.decodes, sink.frames, sink.lastFrame, sink.lastPacket)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !sink.closed {
		t.Fatalf("closed demux=%v decoder=%v sink=%v", demuxer.closed, decoder.closed, sink.closed)
	}
}

func TestStreamRecipeFlowDecodeSinkRuns(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoder := &decodeTestDecoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
	)
	sink := &runtimeTestSink{name: "frames"}
	flow := Flow("preview").
		Audio().
		Decode().
		Tap(FrameTap("audio.flow.decoded"))
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Apply(flow).
		To(Sink(sink))

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(planned)
	if strings.Contains(text, "encode-preview") ||
		!strings.Contains(text, "input.ogg -> select-audio") ||
		!strings.Contains(text, "select-audio -> decode-audio") ||
		!strings.Contains(text, "decode-audio -> frames") {
		t.Fatalf("planned:\n%s", text)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(built))
	}
	decodedTap, ok := findTap(task.Taps(), "audio.flow.decoded")
	if !ok ||
		decodedTap.Domain != shape.DomainFrame ||
		decodedTap.MediaKind != av.MediaAudio ||
		decodedTap.After != plan.OpDecode ||
		decodedTap.Shape.Codec != av.CodecOpus ||
		decodedTap.Shape.SampleRate != 48000 ||
		decodedTap.Shape.Channels != codec.Stereo ||
		decodedTap.Node != "decode-audio" {
		t.Fatalf("decoded tap = %+v ok=%v, want frame Opus tap on stream decoder", decodedTap, ok)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || sink.frames != 1 || sink.lastFrame.StreamID != "audio" || sink.lastPacket != nil {
		t.Fatalf("decodes=%d frames=%d frame=%+v packet=%v", decoder.decodes, sink.frames, sink.lastFrame, sink.lastPacket)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !sink.closed {
		t.Fatalf("closed demux=%v decoder=%v sink=%v", demuxer.closed, decoder.closed, sink.closed)
	}
}

func TestBranchCompositionPacketBranchDecodeResampleEncodeMuxRuns(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	resampleFactory := &transcodeTestFilterFactory{}
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResample,
		Input:  av.MediaAudio,
		Output: av.MediaAudio,
	}, resampleFactory))
	decoder := &decodeTestDecoder{}
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory),
	)
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs, filters)).
		Audio().
		Copy().
		Branches(
			Branch("voice").
				Decode().
				Resample(16_000, codec.Mono).
				Encode(codec.Opus(codec.Bitrate(64_000))).
				To(File("voice.ogg", io.Discard, Format(av.FormatOgg))),
		)

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	text := specText(planned)
	for _, want := range []string{
		"select-audio -> decode-audio",
		"decode-audio -> resample-voice",
		"resample-voice -> encode-voice",
		"encode-voice -> voice.ogg",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("planned spec missing %q:\n%s", want, text)
		}
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if built := task.Describe(); !reflect.DeepEqual(planned, built) {
		t.Fatalf("planned:\n%s\nbuilt:\n%s", specText(planned), specText(built))
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || resampleFactory.filter.frames != 1 || encoder.encodes != 1 {
		t.Fatalf("decodes=%d resampled=%d encodes=%d", decoder.decodes, resampleFactory.filter.frames, encoder.encodes)
	}
	if encoderFactory.config.Stream.ID != "voice" ||
		encoderFactory.config.Parameters.ID != av.CodecOpus ||
		encoderFactory.config.Settings.Bitrate != 64_000 {
		t.Fatalf("encode config: %+v", encoderFactory.config)
	}
	if resampleFactory.config.Audio == nil ||
		resampleFactory.config.Audio.SampleRate != 16_000 ||
		resampleFactory.config.Audio.Channels != codec.Mono {
		t.Fatalf("resample config = %+v, want 16k mono", resampleFactory.config.Audio)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "voice" {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !resampleFactory.filter.closed || !encoder.closed || !muxers.muxers[0].closed {
		t.Fatalf("closed demux=%v decoder=%v resample=%v encoder=%v mux=%v",
			demuxer.closed, decoder.closed, resampleFactory.filter.closed, encoder.closed, muxers.muxers[0].closed)
	}
}

func TestBranchCompositionFrameSinkFanoutRuns(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoder := &decodeTestDecoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
	)
	analysis := &runtimeTestSink{name: "analysis"}
	preview := &runtimeTestSink{name: "preview"}
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Decode().
		Branches(
			Branch("frames").To(
				Sink(analysis),
				Sink(preview),
			),
		)

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	plannedText := specText(planned)
	if strings.Contains(plannedText, "encode-frames") ||
		!strings.Contains(plannedText, "decode-audio -> analysis") ||
		!strings.Contains(plannedText, "decode-audio -> preview") {
		t.Fatalf("planned:\n%s", plannedText)
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 ||
		analysis.frames != 1 ||
		preview.frames != 1 ||
		analysis.lastFrame.StreamID != "audio" ||
		preview.lastFrame.StreamID != "audio" {
		t.Fatalf("decodes=%d analysis=%d preview=%d", decoder.decodes, analysis.frames, preview.frames)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !analysis.closed || !preview.closed {
		t.Fatalf("closed demux=%v decoder=%v analysis=%v preview=%v", demuxer.closed, decoder.closed, analysis.closed, preview.closed)
	}
}

func TestBranchCompositionResizeSinkDestinationRuns(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{videoVP8TranscodeTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "video",
			Payload:  av.Buffer{Bytes: []byte{4, 5, 6}},
		}},
	}
	resizeFactory := &transcodeTestFilterFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResize,
		Input:  av.MediaVideo,
		Output: av.MediaVideo,
	}, resizeFactory))
	decoder := &decodeTestDecoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &decodeTestDecoderFactory{decoder: decoder}),
	)
	sink := &runtimeTestSink{name: "thumbnail"}
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs, filters)).
		Video().
		Decode().
		Branches(
			Branch("thumb").
				Resize(320, 180).
				To(Sink(sink)),
		)

	planned, err := job.Describe()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(planned), "decode-video -> resize-thumb") ||
		!strings.Contains(specText(planned), "resize-thumb -> thumbnail") ||
		strings.Contains(specText(planned), "encode-thumb") {
		t.Fatalf("planned:\n%s", specText(planned))
	}

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || resizeFactory.filter.frames != 1 || sink.frames != 1 {
		t.Fatalf("decodes=%d resized=%d frames=%d", decoder.decodes, resizeFactory.filter.frames, sink.frames)
	}
	if resizeFactory.config.Video == nil || resizeFactory.config.Video.Width != 320 || resizeFactory.config.Video.Height != 180 {
		t.Fatalf("resize config = %+v", resizeFactory.config.Video)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !resizeFactory.filter.closed || !sink.closed {
		t.Fatalf("closed demux=%v decoder=%v filter=%v sink=%v", demuxer.closed, decoder.closed, resizeFactory.filter.closed, sink.closed)
	}
}

func TestBranchCompositionEncodeSinkDestinationRuns(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoder := &decodeTestDecoder{}
	encoder := &encodeTestEncoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: encoder}),
	)
	sink := &runtimeTestSink{name: "packets"}
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Decode().
		Branches(Branch("packets").Encode(codec.Opus(codec.Bitrate(96_000))).To(Sink(sink)))

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || encoder.encodes != 1 || sink.lastPacket == nil || sink.frames != 0 {
		t.Fatalf("decodes=%d encodes=%d packet=%v frames=%d", decoder.decodes, encoder.encodes, sink.lastPacket, sink.frames)
	}
	if len(sink.lastPacketValue.Payload.Bytes) != 1 || sink.lastPacketValue.Payload.Bytes[0] != 7 {
		t.Fatalf("packet payload=%v", sink.lastPacketValue.Payload.Bytes)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
	if !demuxer.closed || !decoder.closed || !encoder.closed || !sink.closed {
		t.Fatalf("closed demux=%v decoder=%v encoder=%v sink=%v", demuxer.closed, decoder.closed, encoder.closed, sink.closed)
	}
}

func TestBranchCompositionTaskAttachesAfterEncodeTap(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	encoder := &encodeTestEncoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: encoder}),
	)
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Decode().
		Tap(FrameTap("audio.decoded")).
		Branches(
			Branch("archive").
				Encode(codec.Opus(codec.Bitrate(96_000))).
				Tap(PacketTap("audio.encoded")).
				To(File("archive.ogg", io.Discard)),
		)

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	var encodedTap snapshot.Tap
	for _, tap := range task.Taps() {
		if tap.Name == "audio.encoded" {
			encodedTap = tap
			break
		}
	}
	if encodedTap.Name == "" ||
		encodedTap.Domain != shape.DomainPacket ||
		encodedTap.MediaKind != av.MediaAudio ||
		encodedTap.Shape.Codec != av.CodecOpus ||
		encodedTap.Shape.StreamID != "archive" ||
		encodedTap.Node != "encode-archive" {
		t.Fatalf("encoded tap = %+v, want packet Opus archive tap on encode-archive", encodedTap)
	}

	recording, err := task.Attach(ctx, Branch("record").
		From(PacketTap("audio.encoded")).
		Copy().
		To(File("recording.ogg", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || encoder.encodes != 1 {
		t.Fatalf("decodes=%d encodes=%d", decoder.decodes, encoder.encodes)
	}
	if len(muxers.muxers) != 2 ||
		muxers.muxers[0].writes != 1 ||
		muxers.muxers[1].writes != 1 ||
		muxers.muxers[0].lastStream != "archive" ||
		muxers.muxers[1].lastStream != "archive" {
		t.Fatalf("muxers=%d first=%+v second=%+v", len(muxers.muxers), muxers.muxers[0], muxers.muxers[1])
	}
	if err := task.Detach(ctx, recording); err != nil {
		t.Fatal(err)
	}
	if !muxers.muxers[1].closed {
		t.Fatal("late recording muxer was not closed by detach")
	}
}

func TestBranchCompositionTaskExposesAndAttachesAfterResizeTap(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{videoVP8TranscodeTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	resizeFactory := &transcodeTestFilterFactory{}
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResize,
		Input:  av.MediaVideo,
		Output: av.MediaVideo,
	}, resizeFactory))
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecVP8, Type: av.MediaVideo}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecVP9, Type: av.MediaVideo}, &encodeTestEncoderFactory{}),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(New(formats, codecs, filters)).
		Video().
		Decode().
		Tap(FrameTap("video.decoded")).
		Branches(
			Branch("720p").
				Resize(1280, 720).
				Tap(FrameTap("video.720p.frames")).
				Encode(codec.VP9(codec.Bitrate(2_000_000))).
				To(File("web.ogg", io.Discard)),
		)

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	var resizeTap snapshot.Tap
	for _, tap := range task.Taps() {
		if tap.Name == "video.720p.frames" {
			resizeTap = tap
			break
		}
	}
	if resizeTap.Name == "" ||
		resizeTap.Domain != shape.DomainFrame ||
		resizeTap.MediaKind != av.MediaVideo ||
		resizeTap.After != plan.OpTransform ||
		resizeTap.Shape.Width != 1280 ||
		resizeTap.Shape.Height != 720 ||
		resizeTap.Shape.PixelFormat != av.PixelFormatYUV420P ||
		resizeTap.Node == "" {
		t.Fatalf("resize tap = %+v, want frame video 1280x720 tap with graph node", resizeTap)
	}

	attachment, err := task.Attach(ctx, Branch("screenshots").
		From(FrameTap("video.720p.frames")).
		Resize(320, 180).
		Tap(FrameTap("video.320.frames")).
		To(Sink(SinkFunc("screenshots", func(context.Context, Message) error {
			return nil
		}))))
	if err != nil {
		t.Fatal(err)
	}
	if resizeFactory.config.Video == nil || resizeFactory.config.Video.Width != 320 || resizeFactory.config.Video.Height != 180 {
		t.Fatalf("runtime resize config = %+v, want 320x180", resizeFactory.config.Video)
	}
	var resizedTap snapshot.Tap
	for _, tap := range task.Taps() {
		if tap.Name == "video.320.frames" {
			resizedTap = tap
			break
		}
	}
	if resizedTap.Name == "" ||
		resizedTap.Domain != shape.DomainFrame ||
		resizedTap.MediaKind != av.MediaVideo ||
		resizedTap.After != plan.OpTransform ||
		resizedTap.Shape.Width != 320 ||
		resizedTap.Shape.Height != 180 ||
		resizedTap.Node != "screenshots/resize-screenshots" {
		t.Fatalf("resized tap = %+v, want frame video 320x180 tap on screenshots/resize-screenshots", resizedTap)
	}
	nestedAttachment, err := task.Attach(ctx, Branch("preview").From(FrameTap("video.320.frames")).To(Sink(SinkFunc("preview", func(context.Context, Message) error {
		return nil
	}))))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Detach(ctx, nestedAttachment); err != nil {
		t.Fatal(err)
	}
	if err := task.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
}

func TestStreamRecipeTaskAttachesAfterCustomStageAndEncodeTaps(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)
	meter := &runtimeTestStage{name: "meter"}
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(New(formats, codecs)).
		Audio().
		Decode().
		Do(meter).
		Tap(FrameTap("audio.after-meter")).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		Tap(PacketTap("audio.encoded")).
		To(File("archive.ogg", io.Discard))

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	var customTap, encodedTap snapshot.Tap
	for _, tap := range task.Taps() {
		switch tap.Name {
		case "audio.after-meter":
			customTap = tap
		case "audio.encoded":
			encodedTap = tap
		}
	}
	if customTap.Name == "" || customTap.Domain != shape.DomainFrame || customTap.MediaKind != av.MediaAudio || customTap.After != plan.OpStage || customTap.Node != "meter" {
		t.Fatalf("custom tap = %+v, want frame audio tap on meter", customTap)
	}
	if encodedTap.Name == "" || encodedTap.Domain != shape.DomainPacket || encodedTap.MediaKind != av.MediaAudio || encodedTap.After != plan.OpEncode || encodedTap.Node != "encode-audio" {
		t.Fatalf("encoded tap = %+v, want packet audio tap on encode-audio", encodedTap)
	}

	frameAttachment, err := task.Attach(ctx, Branch("levels").From(FrameTap("audio.after-meter")).To(Sink(SinkFunc("levels", func(context.Context, Message) error {
		return nil
	}))))
	if err != nil {
		t.Fatal(err)
	}
	packetAttachment, err := task.Attach(ctx, Branch("packets").From(PacketTap("audio.encoded")).To(Sink(SinkFunc("packets", func(context.Context, Message) error {
		return nil
	}))))
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Detach(ctx, frameAttachment); err != nil {
		t.Fatal(err)
	}
	if err := task.Detach(ctx, packetAttachment); err != nil {
		t.Fatal(err)
	}
}

func tapInfoByName(taps []snapshot.Tap, name string) (snapshot.Tap, bool) {
	for i := range taps {
		if taps[i].Name == name {
			return taps[i], true
		}
	}
	return snapshot.Tap{}, false
}

func branchSnapshotByName(branches []snapshot.Branch, name string) (snapshot.Branch, bool) {
	for i := range branches {
		if branches[i].Name == name {
			return branches[i], true
		}
	}
	return snapshot.Branch{}, false
}

func destinationSnapshotByName(destinations []snapshot.Destination, name string) (snapshot.Destination, bool) {
	for i := range destinations {
		if destinations[i].Name == name {
			return destinations[i], true
		}
	}
	return snapshot.Destination{}, false
}

func TestTaskSnapshotReportsRuntimeBranchSnapshot(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(New(formats, codecs)).
		Audio().
		Decode().
		Tap(FrameTap("audio.frames")).
		To(Sink(SinkFunc("frames", func(context.Context, Message) error {
			return nil
		})))

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	initial := task.Snapshot()
	if len(initial.Branches) != 0 {
		t.Fatalf("initial branches = %+v, want none", initial.Branches)
	}
	if _, ok := tapInfoByName(initial.Taps, "audio.frames"); !ok {
		t.Fatalf("initial taps = %+v, want audio.frames", initial.Taps)
	}

	attachment, err := task.Attach(ctx, Branch("levels").
		From(FrameTap("audio.frames")).
		To(Sink(SinkFunc("levels", func(context.Context, Message) error {
			return nil
		}))))
	if err != nil {
		t.Fatal(err)
	}
	snap := task.Snapshot()
	branch, ok := branchSnapshotByName(snap.Branches, "levels")
	if !ok {
		t.Fatalf("branches = %+v, want levels branch", snap.Branches)
	}
	if branch.ID == "" || branch.State != "attached" {
		t.Fatalf("branch state = %+v, want attached branch with id", branch)
	}
	if len(branch.AnchorTaps) != 1 || branch.AnchorTaps[0] != "audio.frames" {
		t.Fatalf("branch anchors = %+v, want audio.frames", branch.AnchorTaps)
	}
	if len(branch.Nodes) == 0 || len(branch.Spec.Nodes) == 0 {
		t.Fatalf("branch snapshot = %+v, want scoped nodes and spec", branch)
	}
	destination, ok := destinationSnapshotByName(branch.Destinations, "levels")
	if !ok {
		t.Fatalf("branch destinations = %+v, want levels", branch.Destinations)
	}
	if destination.Operation != plan.OpSink || !destination.Open || !reflect.DeepEqual(destination.Branches, []string{"levels"}) {
		t.Fatalf("branch destination = %+v, want open levels sink destination", destination)
	}
	taskDestination, ok := destinationSnapshotByName(snap.Destinations, "levels")
	if !ok {
		t.Fatalf("task destinations = %+v, want levels", snap.Destinations)
	}
	if taskDestination.Operation != plan.OpSink || !taskDestination.Open {
		t.Fatalf("task destination = %+v, want open sink destination", taskDestination)
	}
	if _, ok := branch.Stats.Nodes[branch.Nodes[0].String()]; len(branch.Stats.Nodes) != 0 && !ok {
		t.Fatalf("branch stats = %+v, want node stats scoped to branch nodes %+v", branch.Stats, branch.Nodes)
	}
	handleSnapshot := attachment.Snapshot()
	if handleSnapshot.ID != branch.ID || handleSnapshot.Name != branch.Name || handleSnapshot.State != "attached" {
		t.Fatalf("attachment snapshot = %+v, task branch snapshot = %+v", handleSnapshot, branch)
	}
	if _, ok := destinationSnapshotByName(handleSnapshot.Destinations, "levels"); !ok {
		t.Fatalf("attachment destinations = %+v, want levels", handleSnapshot.Destinations)
	}

	if err := task.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	afterDetach := task.Snapshot()
	if _, ok := branchSnapshotByName(afterDetach.Branches, "levels"); ok {
		t.Fatalf("branches after detach = %+v, want levels removed", afterDetach.Branches)
	}
}

func TestRuntimeAttachShapeAnnotationCannotBreakOperationContract(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}),
	)
	job := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(New(formats, codecs)).
		Audio().
		Decode().
		Tap(FrameTap("audio.frames")).
		To(Sink(SinkFunc("frames", func(context.Context, Message) error {
			return nil
		})))

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	_, err = task.Attach(ctx, Branch("bad").
		From(FrameTap("audio.frames")).
		Shape(shape.New(shape.Domain(shape.DomainPacket), shape.Media(av.MediaAudio))).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(File("bad.ogg", io.Discard)))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "operation_shape_mismatch" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want operation_shape_mismatch wrapping ErrUnsupportedBuild", err)
	}
	for _, want := range []string{
		"opus cannot consume the current media shape",
		"expected_shape=domain=frame media=audio",
		"actual_shape=domain=packet media=audio",
		"keep .Shape(...) annotations in the frame domain before encoders",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
}

func TestStreamRecipeTaskAttachesRuntimeResampleBranch(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
	)
	resampleFactory := &transcodeTestFilterFactory{}
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResample,
		Input:  av.MediaAudio,
		Output: av.MediaAudio,
	}, resampleFactory))
	task, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(New(formats, codecs, filters)).
		Audio().
		Decode().
		Tap(FrameTap("audio.decoded")).
		To(Sink(&runtimeTestSink{name: "frames"})).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	meter := &runtimeTestStage{name: "meter"}
	voice := Flow("voice").
		Audio().
		Do(meter).
		Resample(16_000, codec.Mono).
		Tap(FrameTap("audio.16k"))
	attachment, err := task.Attach(ctx, Branch("voice").
		From(FrameTap("audio.decoded")).
		Apply(voice).
		To(Sink(SinkFunc("voice", func(context.Context, Message) error {
			return nil
		}))))
	if err != nil {
		t.Fatal(err)
	}
	attachmentText := specText(attachment.Spec())
	if !strings.Contains(attachmentText, "voice/meter -> voice/resample-voice") ||
		!strings.Contains(attachmentText, "voice/resample-voice -> voice/voice") {
		t.Fatalf("attachment spec:\n%s", attachmentText)
	}
	if resampleFactory.config.Audio == nil ||
		resampleFactory.config.Audio.SampleRate != 16_000 ||
		resampleFactory.config.Audio.Channels != codec.Mono {
		t.Fatalf("runtime resample config = %+v, want 16k mono", resampleFactory.config.Audio)
	}
	var resampledTap snapshot.Tap
	for _, tap := range task.Taps() {
		if tap.Name == "audio.16k" {
			resampledTap = tap
			break
		}
	}
	if resampledTap.Name == "" ||
		resampledTap.Domain != shape.DomainFrame ||
		resampledTap.MediaKind != av.MediaAudio ||
		resampledTap.Shape.SampleRate != 16_000 ||
		resampledTap.Shape.Channels != codec.Mono ||
		resampledTap.Node != "voice/resample-voice" {
		t.Fatalf("resampled tap = %+v, want frame audio 16k mono tap on voice/resample-voice", resampledTap)
	}
	if err := task.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeObservationBranchPublishesTapAndDetachesSubtree(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	decoder := &decodeTestDecoder{}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: decoder}),
	)
	base := &runtimeTestSink{name: "base"}
	task, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(New(formats, codecs)).
		Audio().
		Decode().
		Tap(FrameTap("audio.decoded")).
		To(Sink(base)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	observed := 0
	observer := FrameFunc("observer", func(_ context.Context, frame *av.Frame, emit Emit) error {
		observed++
		return emit.Frame(frame)
	})
	analysis := &runtimeTestSink{name: "analysis"}
	parent, err := task.Attach(ctx, Branch("analysis").
		From(FrameTap("audio.decoded")).
		Do(observer).
		Tap(FrameTap("audio.observed")).
		To(Sink(analysis)))
	if err != nil {
		t.Fatal(err)
	}
	parentText := specText(parent.Spec())
	if !strings.Contains(parentText, "analysis/observer -> analysis/analysis") {
		t.Fatalf("parent attachment spec:\n%s", parentText)
	}

	var observedTap snapshot.Tap
	for _, tap := range task.Taps() {
		if tap.Name == "audio.observed" {
			observedTap = tap
			break
		}
	}
	if observedTap.Name == "" ||
		observedTap.Domain != shape.DomainFrame ||
		observedTap.MediaKind != av.MediaAudio ||
		observedTap.Node != "analysis/observer" {
		t.Fatalf("observed tap = %+v, want frame audio tap on analysis/observer", observedTap)
	}

	dependent := &runtimeTestSink{name: "dependent"}
	child, err := task.Attach(ctx, Branch("dependent").
		From(FrameTap("audio.observed")).
		To(Sink(dependent)))
	if err != nil {
		t.Fatal(err)
	}
	if child.Name() != "dependent" {
		t.Fatalf("child name = %q, want dependent", child.Name())
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || observed != 1 || base.frames != 1 || analysis.frames != 1 || dependent.frames != 1 {
		t.Fatalf("decodes=%d observed=%d base=%d analysis=%d dependent=%d", decoder.decodes, observed, base.frames, analysis.frames, dependent.frames)
	}
	if err := task.Detach(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if !analysis.closed || !dependent.closed {
		t.Fatalf("closed analysis=%v dependent=%v", analysis.closed, dependent.closed)
	}
}

func TestTaskAttachRejectsRuntimeTransformDescriptorConfigBeforeMutation(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
	)
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
	)
	resampleFactory := &transcodeTestFilterFactory{}
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:          filter.FactoryResample,
		Input:         av.MediaAudio,
		Output:        av.MediaAudio,
		SampleFormats: []string{av.SampleFormatS16},
	}, resampleFactory))
	task, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(New(formats, codecs, filters)).
		Audio().
		Decode().
		Tap(FrameTap("audio.decoded")).
		To(Sink(&runtimeTestSink{name: "frames"})).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	before := task.Describe()

	branch := Branch("voice").
		From(FrameTap("audio.decoded")).
		Resample(16_000, codec.Mono).
		To(Sink(SinkFunc("voice", func(context.Context, Message) error {
			return nil
		})))
	branch.operations[0].Transform.Resample.SampleFormat = av.SampleFormatF32

	_, err = task.Attach(ctx, branch)
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "transform_adapter_incompatible" ||
		!strings.Contains(err.Error(), "field=sample_format") ||
		!strings.Contains(err.Error(), "requested=f32") ||
		!strings.Contains(err.Error(), "supported=s16") {
		t.Fatalf("err = %v, want runtime transform descriptor config error", err)
	}
	if resampleFactory.config.Audio != nil {
		t.Fatalf("runtime transform opened before descriptor preflight: %+v", resampleFactory.config.Audio)
	}
	if after := task.Describe(); !reflect.DeepEqual(before, after) {
		t.Fatalf("graph mutated after rejected attach:\nbefore:\n%s\nafter:\n%s", specText(before), specText(after))
	}
	for _, tap := range task.Taps() {
		if tap.Name == "audio.16k" || strings.Contains(tap.Name, "voice") {
			t.Fatalf("tap registered after rejected attach: %+v", tap)
		}
	}
}

func TestStreamRecipeTaskAttachesRuntimeEncodeMuxBranch(t *testing.T) {
	ctx := context.Background()
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory),
	)
	task, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(New(formats, codecs)).
		Audio().
		Decode().
		Tap(FrameTap("audio.decoded")).
		To(Sink(&runtimeTestSink{name: "frames"})).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	attachment, err := task.Attach(ctx, Branch("archive").
		From(FrameTap("audio.decoded")).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(File("archive.ogg", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	attachmentText := specText(attachment.Spec())
	if !strings.Contains(attachmentText, "archive/encode-archive -> archive/archive.ogg") {
		t.Fatalf("attachment spec:\n%s", attachmentText)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if encoder.encodes != 1 {
		t.Fatalf("encodes=%d", encoder.encodes)
	}
	if encoderFactory.config.Stream.ID != "archive" ||
		encoderFactory.config.Parameters.ID != av.CodecOpus ||
		encoderFactory.config.Settings.Bitrate != 96_000 {
		t.Fatalf("encode config: %+v", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 || !muxers.muxers[0].opened ||
		muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "archive" {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
}

func TestTaskAttachRejectsRuntimeMuxDescriptorBeforeMutation(t *testing.T) {
	ctx := context.Background()
	audioOnly := av.FormatID("audioonly")
	streams := []av.Stream{audioOpusTestStream()}
	demuxer := &decodeTestDemuxer{streams: streams}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		func(registry *format.SimpleRegistry) {
			registry.RegisterMuxerDescriptor(format.Descriptor{
				Format:     audioOnly,
				Media:      []av.MediaType{av.MediaAudio},
				Codecs:     []av.CodecID{av.CodecPCM},
				MaxStreams: 1,
				Metadata:   av.Metadata{"summary": "audioonly destinations accept PCM audio only"},
			}, muxers)
		},
	)
	encoderFactory := &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, &decodeTestDecoderFactory{decoder: &decodeTestDecoder{}}),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory),
	)
	task, err := From(FileInput("input.ogg", strings.NewReader(""))).UseRuntime(New(formats, codecs)).
		Audio().
		Decode().
		Tap(FrameTap("audio.decoded")).
		To(Sink(&runtimeTestSink{name: "frames"})).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	before := task.Describe()

	_, err = task.Attach(ctx, Branch("archive").
		From(FrameTap("audio.decoded")).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(File("archive.audioonly", io.Discard, Format(audioOnly))))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "destination_mux_incompatible" ||
		!strings.Contains(err.Error(), "audioonly destinations accept PCM audio only") ||
		!strings.Contains(err.Error(), "destination=archive") ||
		!strings.Contains(err.Error(), "branch=archive codec=opus media=audio") {
		t.Fatalf("err = %v, want descriptor-backed runtime mux incompatibility", err)
	}
	if len(muxers.muxers) != 0 {
		t.Fatalf("muxer opened before runtime mux preflight: %+v", muxers.muxers)
	}
	if after := task.Describe(); !reflect.DeepEqual(before, after) {
		t.Fatalf("graph mutated after rejected attach:\nbefore:\n%s\nafter:\n%s", specText(before), specText(after))
	}
}

func TestTaskAttachesRuntimePacketCopyMuxBranch(t *testing.T) {
	ctx := context.Background()
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	packet := av.Packet{StreamID: "audio", Payload: av.Buffer{Bytes: []byte{9}}}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	graph := Expert(New(formats)).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.packets",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainPacket,
		Shape: shape.Spec{
			Domain:     shape.DomainPacket,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	attachment, err := builtTask.Attach(ctx, Branch("record").
		From(PacketTap("audio.packets")).
		Copy().
		To(File("recording.ogg", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(attachment.Spec()), "source -> record/recording.ogg") {
		t.Fatalf("attachment spec:\n%s", specText(attachment.Spec()))
	}
	if err := builtTask.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 ||
		muxers.muxers[0].lastStream != "audio" || muxers.muxers[0].streamCount != 1 ||
		!streamIDsEqual(muxers.muxers[0].openedStreams, []av.StreamID{"audio"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
}

func TestTaskAttachRejectsDuplicateRuntimeBranchDestinationsBeforeMutation(t *testing.T) {
	ctx := context.Background()
	muxers := &remuxTestMuxerFactory{}
	encoderFactory := &encodeTestEncoderFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	codecs := withTestCodecs(
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory),
	)
	frame := av.Frame{StreamID: "audio", Type: av.MediaAudio}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	graph := Expert(New(formats, codecs)).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:     shape.DomainFrame,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()
	before := builtTask.Describe()
	tapsBefore := builtTask.Taps()

	archive := File("archive.ogg", io.Discard)
	_, err = builtTask.Attach(ctx, Branch("fanout").
		From(FrameTap("audio.frames")).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(archive, archive))

	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "destination_duplicate" ||
		buildErr.Operation != "attach runtime branch" ||
		!errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want runtime destination_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `branch routes to destination "archive.ogg" more than once`) ||
		!strings.Contains(err.Error(), "second destination index: 1") ||
		!strings.Contains(err.Error(), "list each destination once") {
		t.Fatalf("err = %v, want duplicate runtime destination guidance", err)
	}
	if len(encoderFactory.configs) != 0 {
		t.Fatalf("encoder opened before duplicate target validation: %+v", encoderFactory.configs)
	}
	if len(muxers.muxers) != 0 {
		t.Fatalf("muxer opened before duplicate target validation: %+v", muxers.muxers)
	}
	if after := builtTask.Describe(); !reflect.DeepEqual(before, after) {
		t.Fatalf("graph mutated after duplicate target attach:\nbefore:\n%s\nafter:\n%s", specText(before), specText(after))
	}
	if !reflect.DeepEqual(tapsBefore, builtTask.Taps()) {
		t.Fatalf("taps mutated after duplicate target attach: before=%+v after=%+v", tapsBefore, builtTask.Taps())
	}
}

func TestTaskAttachRuntimeMuxBranchRequiresCopyOrEncode(t *testing.T) {
	ctx := context.Background()
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	frame := av.Frame{StreamID: "audio", Type: av.MediaAudio}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	graph := Expert(New(formats)).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape:     shape.Spec{Domain: shape.DomainFrame, MediaKind: av.MediaAudio, StreamID: "audio", Codec: av.CodecOpus},
		Node:      "source",
	}}
	defer builtTask.Close()

	_, err = builtTask.Attach(ctx, Branch("archive").
		From(FrameTap("audio.frames")).
		To(File("archive.ogg", io.Discard)))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "destination_shape_mismatch" {
		t.Fatalf("err = %v, want destination_shape_mismatch", err)
	}
	for _, want := range []string{
		"byte or mux destination requires packet-domain media",
		"destination=archive",
		"actual_shape=domain=frame media=audio",
		"goav.Sink",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
	}
	if strings.Contains(specText(builtTask.Describe()), "archive/") {
		t.Fatalf("graph mutated after rejected attach:\n%s", specText(builtTask.Describe()))
	}
}

func TestTaskAttachRuntimeEncodeMuxBranchKeepsH264AV1WIPGuard(t *testing.T) {
	ctx := context.Background()
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatAnnexB, &remuxTestMuxerFactory{}),
		testFormatMuxer(av.FormatIVF, &remuxTestMuxerFactory{}),
	)
	frame := av.Frame{StreamID: "video", Type: av.MediaVideo}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	graph := Expert(New(formats)).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "video.frames",
		MediaKind: av.MediaVideo,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:      shape.DomainFrame,
			MediaKind:   av.MediaVideo,
			StreamID:    "video",
			Codec:       av.CodecVP8,
			Width:       1280,
			Height:      720,
			PixelFormat: av.PixelFormatI420,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	cases := []struct {
		name   string
		codec  codec.CodecSpec
		output Destination
	}{
		{name: "h264", codec: codec.H264(codec.Bitrate(2_000_000)), output: destinationHandle(fileDestination("archive.h264", io.Discard))},
		{name: "av1", codec: codec.AV1(codec.Bitrate(2_000_000)), output: destinationHandle(fileDestination("archive.ivf", io.Discard))},
	}
	for _, tc := range cases {
		_, err := builtTask.Attach(ctx, Branch(tc.name).
			From(FrameTap("video.frames")).
			Encode(tc.codec).
			To(tc.output))
		var buildErr *BuildError
		if !errors.As(err, &buildErr) || buildErr.Code != "encode_work_in_progress" {
			t.Fatalf("%s err = %v, want encode_work_in_progress", tc.name, err)
		}
	}
	if strings.Contains(specText(builtTask.Describe()), "h264/") || strings.Contains(specText(builtTask.Describe()), "av1/") {
		t.Fatalf("graph mutated after rejected attach:\n%s", specText(builtTask.Describe()))
	}
}

func TestTaskAttachRejectsRuntimeEncodeDescriptorBeforeMutation(t *testing.T) {
	ctx := context.Background()
	customPCM := av.CodecID("x_pcm_s16")
	encoderFactory := &encodeTestEncoderFactory{encoder: &encodeTestEncoder{}}
	codecs := withTestCodecs(
		testCodecEncoder(codec.Descriptor{
			ID:   customPCM,
			Type: av.MediaAudio,
			Capabilities: codec.Capabilities{
				SampleFormats: []string{av.SampleFormatS16},
			},
		}, encoderFactory),
	)
	frame := av.Frame{
		StreamID: "audio",
		Type:     av.MediaAudio,
		Audio: &av.AudioFrame{
			SampleRate:   48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatF32,
			Samples:      480,
		},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	graph := Expert(New(codecs)).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:       shape.DomainFrame,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecPCM,
			SampleRate:   48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatF32,
		},
		Node: "source",
	}}
	defer builtTask.Close()
	before := builtTask.Describe()

	_, err = builtTask.Attach(ctx, Branch("record").
		From(FrameTap("audio.frames")).
		Encode(codec.Codec(customPCM, av.MediaAudio)).
		To(Sink(SinkFunc("record", func(context.Context, Message) error {
			return nil
		}))))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "encode_adapter_incompatible" ||
		!strings.Contains(err.Error(), "field=sample_format") ||
		!strings.Contains(err.Error(), "requested=f32") ||
		!strings.Contains(err.Error(), "supported=s16") {
		t.Fatalf("err = %v, want runtime encode descriptor config error", err)
	}
	if len(encoderFactory.configs) != 0 {
		t.Fatalf("encoder opened before descriptor preflight: %+v", encoderFactory.configs)
	}
	if after := builtTask.Describe(); !reflect.DeepEqual(before, after) {
		t.Fatalf("graph mutated after rejected attach:\nbefore:\n%s\nafter:\n%s", specText(before), specText(after))
	}
}

func TestTaskAttachRuntimeCustomEncodeMuxBranch(t *testing.T) {
	ctx := context.Background()
	customPCM := av.CodecID("x_pcm_s16")
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	codecs := withTestCodecs(
		testCodecEncoder(codec.Descriptor{ID: customPCM, Type: av.MediaAudio}, encoderFactory),
	)
	frame := av.Frame{
		StreamID: "audio",
		Type:     av.MediaAudio,
		Audio: &av.AudioFrame{
			SampleRate:   48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
			Samples:      480,
		},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	graph := Expert(New(formats, codecs)).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:       shape.DomainFrame,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecPCM,
			SampleRate:   48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	attachment, err := builtTask.Attach(ctx, Branch("record").
		From(FrameTap("audio.frames")).
		Encode(codec.Codec(customPCM, av.MediaAudio, codec.SampleRate(16_000), codec.Channels(codec.Mono))).
		To(File("recording.ogg", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(attachment.Spec()), "record/encode-record -> record/recording.ogg") {
		t.Fatalf("attachment spec:\n%s", specText(attachment.Spec()))
	}
	if err := builtTask.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if encoder.encodes != 1 {
		t.Fatalf("encodes=%d", encoder.encodes)
	}
	if encoderFactory.config.Parameters.ID != customPCM ||
		encoderFactory.config.Stream.Codec.ID != customPCM ||
		encoderFactory.config.Stream.Codec.SampleRate != 16_000 ||
		encoderFactory.config.Stream.Codec.Channels != codec.Mono {
		t.Fatalf("custom runtime encode config: %+v", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "record" {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := builtTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
}

func TestTaskAttachRuntimeDecodeBranchFromPacketTap(t *testing.T) {
	ctx := context.Background()
	decoder := &decodeTestDecoder{}
	decoderFactory := &decodeTestDecoderFactory{decoder: decoder}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, decoderFactory),
	)
	packet := av.Packet{
		StreamID: "audio",
		Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	base := &runtimeTestSink{name: "base"}
	graph := Expert(New(codecs)).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.packets",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainPacket,
		Shape: shape.Spec{
			Domain:     shape.DomainPacket,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	decoded := &runtimeTestSink{name: "decoded"}
	parent, err := builtTask.Attach(ctx, Branch("preview").
		From(PacketTap("audio.packets")).
		Decode().
		Tap(FrameTap("audio.decoded.late")).
		To(Sink(decoded)))
	if err != nil {
		t.Fatal(err)
	}
	decodedTap, ok := findTap(builtTask.Taps(), "audio.decoded.late")
	if !ok ||
		decodedTap.Domain != shape.DomainFrame ||
		decodedTap.MediaKind != av.MediaAudio ||
		decodedTap.Shape.Codec != av.CodecOpus ||
		decodedTap.Shape.SampleRate != 48000 ||
		decodedTap.Node != "preview/decode-preview" {
		t.Fatalf("decoded tap = %+v ok=%v, want frame Opus tap on preview decoder", decodedTap, ok)
	}
	dependent := &runtimeTestSink{name: "dependent"}
	child, err := builtTask.Attach(ctx, Branch("dependent").
		From(FrameTap("audio.decoded.late")).
		To(Sink(dependent)))
	if err != nil {
		t.Fatal(err)
	}

	if err := builtTask.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || decoder.decodes != 1 || decoded.frames != 1 || dependent.frames != 1 {
		t.Fatalf("base=%d decodes=%d decodedFrames=%d dependentFrames=%d", base.count, decoder.decodes, decoded.frames, dependent.frames)
	}
	if decoderFactory.config.Stream.ID != "audio" ||
		decoderFactory.config.Stream.Codec.ID != av.CodecOpus ||
		decoderFactory.config.Stream.Codec.SampleRate != 48000 {
		t.Fatalf("runtime decode config: %+v", decoderFactory.config)
	}
	if err := builtTask.Detach(ctx, parent); err != nil {
		t.Fatal(err)
	}
	if !decoder.closed || !decoded.closed || !dependent.closed {
		t.Fatalf("closed decoder=%v decoded=%v dependent=%v", decoder.closed, decoded.closed, dependent.closed)
	}
	if err := child.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestTaskAttachRuntimeFlowDecodeBranchFromPacketTap(t *testing.T) {
	ctx := context.Background()
	decoder := &decodeTestDecoder{}
	decoderFactory := &decodeTestDecoderFactory{decoder: decoder}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, decoderFactory),
	)
	packet := av.Packet{
		StreamID: "audio",
		Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	base := &runtimeTestSink{name: "base"}
	graph := Expert(New(codecs)).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.packets",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainPacket,
		Shape: shape.Spec{
			Domain:     shape.DomainPacket,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	flow := Flow("preview").
		Audio().
		Decode().
		Tap(FrameTap("audio.flow.decoded"))
	decoded := &runtimeTestSink{name: "decoded"}
	attachment, err := builtTask.Attach(ctx, Branch("preview").
		From(PacketTap("audio.packets")).
		Apply(flow).
		To(Sink(decoded)))
	if err != nil {
		t.Fatal(err)
	}
	attachmentText := specText(attachment.Spec())
	if !strings.Contains(attachmentText, "preview/decode-preview -> preview/decoded") {
		t.Fatalf("attachment spec:\n%s", attachmentText)
	}
	decodedTap, ok := findTap(builtTask.Taps(), "audio.flow.decoded")
	if !ok ||
		decodedTap.Domain != shape.DomainFrame ||
		decodedTap.MediaKind != av.MediaAudio ||
		decodedTap.After != plan.OpDecode ||
		decodedTap.Node != "preview/decode-preview" {
		t.Fatalf("decoded tap = %+v ok=%v, want frame tap on flow decoder", decodedTap, ok)
	}

	if err := builtTask.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || decoder.decodes != 1 || decoded.frames != 1 {
		t.Fatalf("base=%d decodes=%d decodedFrames=%d", base.count, decoder.decodes, decoded.frames)
	}
	if decoderFactory.config.Stream.ID != "audio" ||
		decoderFactory.config.Stream.Codec.ID != av.CodecOpus {
		t.Fatalf("runtime decode config: %+v", decoderFactory.config)
	}
	if err := builtTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if !decoder.closed || !decoded.closed {
		t.Fatalf("closed decoder=%v decoded=%v", decoder.closed, decoded.closed)
	}
}

func TestTaskAttachRuntimeFlowMediaMismatchBeforeMutation(t *testing.T) {
	ctx := context.Background()
	frame := av.Frame{
		StreamID: "video",
		Type:     av.MediaVideo,
		Video: &av.VideoFrame{
			Width:       640,
			Height:      360,
			PixelFormat: av.PixelFormatYUV420P,
		},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	graph := Expert(New()).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "video.frames",
		MediaKind: av.MediaVideo,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:      shape.DomainFrame,
			MediaKind:   av.MediaVideo,
			StreamID:    "video",
			Codec:       av.CodecVP8,
			Width:       640,
			Height:      360,
			PixelFormat: av.PixelFormatYUV420P,
		},
		Node: "source",
	}}
	defer builtTask.Close()
	before := builtTask.Describe()

	_, err = builtTask.Attach(ctx, Branch("voice").
		From(FrameTap("video.frames")).
		Apply(Flow("voice").Audio().Resample(16_000, codec.Mono)).
		To(Sink(SinkFunc("voice", func(context.Context, Message) error {
			return nil
		}))))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_media_mismatch" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want flow_media_mismatch wrapping ErrUnsupportedBuild", err)
	}
	if buildErr.Operation != "attach runtime branch" ||
		!strings.Contains(err.Error(), "audio flow cannot be applied to video stream") ||
		!strings.Contains(err.Error(), "Flow(name).Audio") ||
		!strings.Contains(err.Error(), "Flow(name).Video") {
		t.Fatalf("err = %v, want runtime flow media guidance", err)
	}
	if after := builtTask.Describe(); !reflect.DeepEqual(before, after) {
		t.Fatalf("graph mutated after rejected runtime flow:\nbefore:\n%s\nafter:\n%s", specText(before), specText(after))
	}
	for _, tap := range builtTask.Taps() {
		if strings.Contains(tap.Node.String(), "voice") {
			t.Fatalf("runtime branch tap registered after rejected attach: %+v", tap)
		}
	}
}

func TestTaskAttachRuntimeDecodeResampleEncodeMuxBranchFromPacketTap(t *testing.T) {
	ctx := context.Background()
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &decodeTestDecoder{}
	decoderFactory := &decodeTestDecoderFactory{decoder: decoder}
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	codecs := withTestCodecs(
		testCodecDecoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, decoderFactory),
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory),
	)
	resampleFactory := &transcodeTestFilterFactory{}
	filters := withTestFilters(testFilterFactory(filter.Descriptor{
		Name:   filter.FactoryResample,
		Input:  av.MediaAudio,
		Output: av.MediaAudio,
	}, resampleFactory))
	packet := av.Packet{
		StreamID: "audio",
		Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessagePacket, Packet: &packet},
	}
	base := &runtimeTestSink{name: "base"}
	graph := Expert(New(formats, codecs, filters)).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.packets",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainPacket,
		Shape: shape.Spec{
			Domain:       shape.DomainPacket,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecOpus,
			SampleRate:   48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	attachment, err := builtTask.Attach(ctx, Branch("voice").
		From(PacketTap("audio.packets")).
		Decode().
		Resample(16_000, codec.Mono).
		Encode(codec.Opus(codec.Bitrate(64_000))).
		Tap(PacketTap("audio.voice.packets")).
		To(File("voice.ogg", io.Discard, Format(av.FormatOgg))))
	if err != nil {
		t.Fatal(err)
	}
	runtimeAttachment, ok := attachment.(*runtimeAttachment)
	if !ok {
		t.Fatalf("attachment = %T, want runtimeAttachment", attachment)
	}
	if got, want := workPatchOperationKindsForBranch(runtimeAttachment.work.Operations, "voice"), []plan.OperationKind{plan.OpDecode, plan.OpTransform, plan.OpEncode, plan.OpTap, plan.OpMux}; !reflect.DeepEqual(got, want) {
		t.Fatalf("runtime encode work operations = %+v, want %+v", got, want)
	}
	attachmentText := specText(attachment.Spec())
	for _, want := range []string{
		"voice/decode-voice -> voice/resample-voice",
		"voice/resample-voice -> voice/encode-voice",
		"voice/encode-voice -> voice/voice.ogg",
	} {
		if !strings.Contains(attachmentText, want) {
			t.Fatalf("attachment spec missing %q:\n%s", want, attachmentText)
		}
	}
	packetTap, ok := findTap(builtTask.Taps(), "audio.voice.packets")
	if !ok ||
		packetTap.Domain != shape.DomainPacket ||
		packetTap.MediaKind != av.MediaAudio ||
		packetTap.Shape.Codec != av.CodecOpus ||
		packetTap.Shape.SampleRate != 16_000 ||
		packetTap.Shape.Channels != codec.Mono ||
		packetTap.Node != "voice/encode-voice" {
		t.Fatalf("packet tap = %+v ok=%v, want Opus 16k mono packet tap on voice encoder", packetTap, ok)
	}

	if err := builtTask.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || decoder.decodes != 1 || resampleFactory.filter.frames != 1 || encoder.encodes != 1 {
		t.Fatalf("base=%d decodes=%d resampled=%d encodes=%d",
			base.count, decoder.decodes, resampleFactory.filter.frames, encoder.encodes)
	}
	if decoderFactory.config.Stream.ID != "audio" ||
		decoderFactory.config.Stream.Codec.ID != av.CodecOpus ||
		decoderFactory.config.Stream.Codec.SampleRate != 48000 {
		t.Fatalf("decode config: %+v", decoderFactory.config)
	}
	if resampleFactory.config.Audio == nil ||
		resampleFactory.config.Audio.SampleRate != 16_000 ||
		resampleFactory.config.Audio.Channels != codec.Mono {
		t.Fatalf("runtime resample config = %+v, want 16k mono", resampleFactory.config.Audio)
	}
	if encoderFactory.config.Stream.ID != "voice" ||
		encoderFactory.config.Stream.Codec.ID != av.CodecOpus ||
		encoderFactory.config.Stream.Codec.SampleRate != 16_000 ||
		encoderFactory.config.Stream.Codec.Channels != codec.Mono ||
		encoderFactory.config.Settings.Bitrate != 64_000 {
		t.Fatalf("encode config: %+v", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 ||
		muxers.muxers[0].writes != 1 ||
		muxers.muxers[0].lastStream != "voice" {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := builtTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if !decoder.closed || !resampleFactory.filter.closed || !encoder.closed || !muxers.muxers[0].closed {
		t.Fatalf("closed decoder=%v resample=%v encoder=%v mux=%v",
			decoder.closed, resampleFactory.filter.closed, encoder.closed, muxers.muxers[0].closed)
	}
}

func TestTaskAttachRuntimeFlowCustomEncodeMuxBranch(t *testing.T) {
	ctx := context.Background()
	customPCM := av.CodecID("x_pcm_s16")
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	codecs := withTestCodecs(
		testCodecEncoder(codec.Descriptor{ID: customPCM, Type: av.MediaAudio}, encoderFactory),
	)
	frame := av.Frame{
		StreamID: "audio",
		Type:     av.MediaAudio,
		Audio: &av.AudioFrame{
			SampleRate:   48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
			Samples:      480,
		},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	graph := Expert(New(formats, codecs)).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:       shape.DomainFrame,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecPCM,
			SampleRate:   48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	flow := Flow("voice").
		Audio().
		Encode(codec.Codec(customPCM, av.MediaAudio, codec.SampleRate(16_000), codec.Channels(codec.Mono))).
		Tap(PacketTap("audio.voice.packets"))
	attachment, err := builtTask.Attach(ctx, Branch("record").
		From(FrameTap("audio.frames")).
		Apply(flow).
		To(File("recording.ogg", io.Discard)))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(attachment.Spec()), "record/encode-record -> record/recording.ogg") {
		t.Fatalf("attachment spec:\n%s", specText(attachment.Spec()))
	}
	packetTap, ok := findTap(builtTask.Taps(), "audio.voice.packets")
	if !ok ||
		packetTap.Domain != shape.DomainPacket ||
		packetTap.MediaKind != av.MediaAudio ||
		packetTap.After != plan.OpEncode ||
		packetTap.Node != "record/encode-record" ||
		packetTap.Shape.Codec != customPCM ||
		packetTap.Shape.SampleRate != 16_000 ||
		packetTap.Shape.Channels != codec.Mono {
		t.Fatalf("packet tap = %+v ok=%v, want custom PCM packet tap on flow encoder", packetTap, ok)
	}
	packetMessages := 0
	packetAttachment, err := builtTask.Attach(ctx, Branch("packets").
		From(PacketTap("audio.voice.packets")).
		To(Sink(SinkFunc("packets", func(_ context.Context, msg Message) error {
			if msg.Kind != pipeline.MessagePacket {
				return errors.New("packet tap delivered non-packet message")
			}
			packetMessages++
			return nil
		}))))
	if err != nil {
		t.Fatal(err)
	}
	if err := builtTask.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || encoder.encodes != 1 || packetMessages != 1 {
		t.Fatalf("base=%d encodes=%d packetMessages=%d", base.count, encoder.encodes, packetMessages)
	}
	if encoderFactory.config.Parameters.ID != customPCM ||
		encoderFactory.config.Stream.Codec.ID != customPCM ||
		encoderFactory.config.Stream.Codec.SampleRate != 16_000 ||
		encoderFactory.config.Stream.Codec.Channels != codec.Mono {
		t.Fatalf("flow custom runtime encode config: %+v", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 ||
		muxers.muxers[0].writes != 1 ||
		muxers.muxers[0].lastStream != "record" ||
		!streamIDsEqual(muxers.muxers[0].openedStreams, []av.StreamID{"record"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := builtTask.Detach(ctx, packetAttachment); err != nil {
		t.Fatal(err)
	}
	if err := builtTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if !encoder.closed || !muxers.muxers[0].closed {
		t.Fatalf("closed encoder=%v muxer=%v", encoder.closed, muxers.muxers[0].closed)
	}
}

func TestTaskAttachRuntimeEncodeBranchFansOutToDestinations(t *testing.T) {
	ctx := context.Background()
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(format.DefaultProber()),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	codecs := withTestCodecs(
		testCodecEncoder(codec.Descriptor{ID: av.CodecOpus, Type: av.MediaAudio}, encoderFactory),
	)
	frame := av.Frame{
		StreamID: "audio",
		Type:     av.MediaAudio,
		Audio: &av.AudioFrame{
			SampleRate: 48000,
			Channels:   codec.Stereo,
			Samples:    480,
		},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	graph := Expert(New(formats, codecs)).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []snapshot.Tap{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    shape.DomainFrame,
		Shape: shape.Spec{
			Domain:     shape.DomainFrame,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   codec.Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	archive := File("archive.ogg", io.Discard)
	monitor := File("monitor.ogg", io.Discard)
	attachment, err := builtTask.Attach(ctx, Branch("fanout").
		From(FrameTap("audio.frames")).
		Encode(codec.Opus(codec.Bitrate(96_000))).
		To(archive, monitor))
	if err != nil {
		t.Fatal(err)
	}
	attachmentText := specText(attachment.Spec())
	if !strings.Contains(attachmentText, "fanout/encode-fanout -> fanout/archive.ogg") ||
		!strings.Contains(attachmentText, "fanout/encode-fanout -> fanout/monitor.ogg") {
		t.Fatalf("attachment spec:\n%s", attachmentText)
	}
	if err := builtTask.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || encoder.encodes != 1 {
		t.Fatalf("base=%d encodes=%d", base.count, encoder.encodes)
	}
	if len(muxers.muxers) != 2 {
		t.Fatalf("muxers=%d, want 2", len(muxers.muxers))
	}
	for i, muxer := range muxers.muxers {
		if muxer.writes != 1 || muxer.lastStream != "fanout" {
			t.Fatalf("muxer[%d]=%+v, want one fanout packet", i, muxer)
		}
	}
	if err := builtTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if !encoder.closed {
		t.Fatal("fanout encoder was not closed by detach")
	}
	for i, muxer := range muxers.muxers {
		if !muxer.closed {
			t.Fatalf("muxer[%d] was not closed by detach", i)
		}
	}
}

func TestFromAudioStreamRecipeResampleEncodeRuns(t *testing.T) {
	ctx := context.Background()
	customPCM := av.CodecID("x_pcm_s16")
	streams := []av.Stream{{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:           customPCM,
			Type:         av.MediaAudio,
			SampleRate:   48000,
			ClockRate:    48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
	}}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &recipePCMDecoder{}
	var decoderConfig codec.DecodeConfig
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	desc := codec.Descriptor{ID: customPCM, Name: "X PCM S16", Type: av.MediaAudio}
	runtime := New(
		formats,
		WithDecoder(desc, recipePCMDecoderFactory{decoder: decoder, config: &decoderConfig}),
		WithEncoder(desc, encoderFactory),
		WithStdFilters(),
	)
	encoded := codec.Codec(customPCM, av.MediaAudio, codec.SampleRate(16_000), codec.Channels(codec.Mono))

	task, err := From(FileInput("input.ogg", nil)).UseRuntime(runtime).
		Audio().
		Resample(16_000, codec.Mono).
		Encode(encoded).
		To(File("preview.ogg", io.Discard)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || encoder.encodes != 1 || encoder.flushes != 1 {
		t.Fatalf("decodes=%d encodes=%d flushes=%d", decoder.decodes, encoder.encodes, encoder.flushes)
	}
	if encoderFactory.config.Stream.Codec.SampleRate != 16_000 || encoderFactory.config.Stream.Codec.Channels != codec.Mono {
		t.Fatalf("encode stream after resample: %+v", encoderFactory.config.Stream)
	}
	if encoderFactory.config.Parameters.ID != customPCM || encoderFactory.config.Stream.Codec.ID != customPCM {
		t.Fatalf("encode custom codec config: %+v", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "audio" {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBranchCompositionCustomEncodeRuns(t *testing.T) {
	ctx := context.Background()
	customPCM := av.CodecID("x_pcm_s16")
	streams := []av.Stream{{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:           customPCM,
			Type:         av.MediaAudio,
			SampleRate:   48000,
			ClockRate:    48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
	}}
	demuxer := &decodeTestDemuxer{
		streams: streams,
		packets: []av.Packet{{
			StreamID: "audio",
			Payload:  av.Buffer{Bytes: []byte{1, 2, 3}},
		}},
	}
	muxers := &remuxTestMuxerFactory{}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: demuxer}),
		testFormatMuxer(av.FormatOgg, muxers),
	)
	decoder := &recipePCMDecoder{}
	var decoderConfig codec.DecodeConfig
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	desc := codec.Descriptor{ID: customPCM, Name: "X PCM S16", Type: av.MediaAudio}
	runtime := New(
		formats,
		WithDecoder(desc, recipePCMDecoderFactory{decoder: decoder, config: &decoderConfig}),
		WithEncoder(desc, encoderFactory),
		WithStdFilters(),
	)
	encoded := codec.Codec(
		customPCM,
		av.MediaAudio,
		codec.SampleRate(16_000),
		codec.Channels(codec.Mono),
		codec.Bitrate(128_000),
		codec.FPS(50),
		codec.KeyframeInterval(100),
		codec.Profile("low-delay"),
		codec.Level("1"),
		codec.Control(func(any) error { return nil }),
	)
	archive := File("archive.ogg", io.Discard)

	task, err := From(FileInput("input.ogg", nil)).UseRuntime(runtime).
		Audio().
		Decode(
			codec.Control(func(any) error { return nil }),
		).
		Branches(
			Branch("main").
				Resample(16_000, codec.Mono).
				Encode(encoded).
				To(archive),
		).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := task.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if decoder.decodes != 1 || encoder.encodes != 1 || encoder.flushes != 1 {
		t.Fatalf("decodes=%d encodes=%d flushes=%d", decoder.decodes, encoder.encodes, encoder.flushes)
	}
	if decoderConfig.Settings.Control == nil {
		t.Fatalf("decoder control callback not plumbed: %+v", decoderConfig)
	}
	if encoderFactory.config.Stream.Codec.SampleRate != 16_000 ||
		encoderFactory.config.Stream.Codec.Channels != codec.Mono ||
		encoderFactory.config.Parameters.ID != customPCM ||
		encoderFactory.config.Stream.Codec.ID != customPCM ||
		encoderFactory.config.Stream.Codec.Profile != "low-delay" ||
		encoderFactory.config.Stream.Codec.Level != "1" ||
		encoderFactory.config.Settings.Bitrate != 128_000 ||
		encoderFactory.config.Settings.Framerate != (av.Duration{Value: 1, Base: av.TimeBase{Num: 1, Den: 50}}) ||
		encoderFactory.config.Settings.KeyframeInterval != 100 ||
		encoderFactory.config.Settings.Profile != "low-delay" ||
		encoderFactory.config.Settings.Level != "1" {
		t.Fatalf("branch custom encode config: %+v", encoderFactory.config)
	}
	if encoderFactory.config.Settings.Control == nil {
		t.Fatalf("encoder control callback not plumbed: %+v", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "main" {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestBranchCompositionRejectsConflictingDecodeConfigs(t *testing.T) {
	ctx := context.Background()
	customPCM := av.CodecID("x_pcm_s16")
	streams := []av.Stream{{
		ID:       "audio",
		Type:     av.MediaAudio,
		TimeBase: av.TimeBase{Num: 1, Den: 48000},
		Codec: av.CodecParameters{
			ID:           customPCM,
			Type:         av.MediaAudio,
			SampleRate:   48000,
			ClockRate:    48000,
			Channels:     codec.Stereo,
			SampleFormat: av.SampleFormatS16,
		},
	}}
	formats := withTestFormats(
		testFormatProber(remuxTestProber{streams: streams}),
		testFormatDemuxer(av.FormatOgg, decodeTestDemuxerFactory{demuxer: &decodeTestDemuxer{streams: streams}}),
	)
	decoder := &recipePCMDecoder{}
	desc := codec.Descriptor{ID: customPCM, Name: "X PCM S16", Type: av.MediaAudio}
	runtime := New(
		formats,
		WithDecoder(desc, recipePCMDecoderFactory{decoder: decoder}),
	)

	_, err := From(FileInput("input.ogg", nil)).UseRuntime(runtime).
		Audio().
		Copy().
		Branches(
			Branch("left").
				Decode(codec.Profile("left")).
				To(Sink(SinkFunc("left", func(context.Context, Message) error {
					return nil
				}))),
			Branch("right").
				Decode(codec.Profile("right")).
				To(Sink(SinkFunc("right", func(context.Context, Message) error {
					return nil
				}))),
		).
		Build(ctx)
	if err == nil {
		t.Fatal("expected conflicting decode config error")
	}
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "decode_config_conflict" {
		t.Fatalf("err = %v, want decode_config_conflict", err)
	}
	if decoder.decodes != 0 {
		t.Fatalf("decoder ran before conflict was reported: %d", decoder.decodes)
	}
}

type recipePCMDecoderFactory struct {
	decoder *recipePCMDecoder
	config  *codec.DecodeConfig
}

func (f recipePCMDecoderFactory) NewDecoder(_ context.Context, config codec.DecodeConfig) (codec.Decoder, error) {
	if f.config != nil {
		*f.config = config
	}
	return f.decoder, nil
}

type recipePCMDecoder struct {
	decodes int
	flushes int
	closed  bool
}

func (d *recipePCMDecoder) Descriptor() codec.Descriptor {
	return codec.Descriptor{ID: av.CodecOpus}
}

func (d *recipePCMDecoder) Open(context.Context, codec.DecodeConfig) error {
	return nil
}

func (d *recipePCMDecoder) DecodeInto(_ context.Context, packet *av.Packet, out *codec.DecodeResult) error {
	if packet == nil {
		return nil
	}
	if len(out.Frames) == cap(out.Frames) {
		return codec.ErrResultFull
	}
	index := len(out.Frames)
	out.Frames = out.Frames[:index+1]
	frame := &out.Frames[index]
	frame.Reset()
	frame.StreamID = packet.StreamID
	frame.Type = av.MediaAudio
	frame.Audio = &av.AudioFrame{
		SampleRate:   48000,
		Channels:     codec.Stereo,
		SampleFormat: av.SampleFormatS16,
		Samples:      480,
	}
	if cap(frame.Planes) < 1 {
		frame.Planes = make([]av.Plane, 1)
	} else {
		frame.Planes = frame.Planes[:1]
	}
	frame.Planes[0].Buffer.Bytes = append(frame.Planes[0].Buffer.Bytes[:0], make([]byte, 480*codec.Stereo*2)...)
	frame.Planes[0].Stride = codec.Stereo * 2
	d.decodes++
	return nil
}

func (d *recipePCMDecoder) FlushInto(context.Context, *codec.DecodeResult) error {
	d.flushes++
	return nil
}

func (d *recipePCMDecoder) HandleEvent(context.Context, *av.Event) error {
	return nil
}

func (d *recipePCMDecoder) Close() error {
	d.closed = true
	return nil
}

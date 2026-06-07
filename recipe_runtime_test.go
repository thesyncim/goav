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
	"github.com/thesyncim/goav/rtpav"
	"github.com/thesyncim/goav/webrtcav"
)

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
	).Copy().To(FileOutput("recording.ivf", io.Discard))

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
	intent := job.Intent()
	if len(intent.Inputs) != 1 ||
		intent.Inputs[0].Protocol != av.ProtocolWebRTC ||
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

func TestWebRTCTrackRecipeRejectsUnknownCodecMetadata(t *testing.T) {
	_, err := From(
		webRTCRemote(webrtcav.RemoteTrack{
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
		}),
	).Copy().To(FileOutput("recording.ogg", io.Discard)).Build(context.Background())

	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "webrtc_codec_unknown" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want webrtc_codec_unknown wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), "Pion TrackRemote codec") ||
		!strings.Contains(err.Error(), "goav.RTP(reader).Codec") ||
		strings.Contains(err.Error(), "missing") {
		t.Fatalf("err = %v, want WebRTC codec guidance", err)
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
		To(FileOutput("recording.webm", io.Discard))

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
	intent := job.Intent()
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
			Channels:   Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    Stereo,
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
		RTP(receiver).Name("audio").Codec(Opus()).RTPBuffer(RTPBufferLimits{MaxPackets: 2}),
	).Copy().To(FileOutput("recording.ogg", io.Discard)).UseRuntime(runtime).Build(ctx)
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

func TestRecordRecipeCopyToTypedTargetRuns(t *testing.T) {
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
			Channels:   Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    Stereo,
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
		RTP(receiver).Name("audio").Codec(Opus()).RTPBuffer(RTPBufferLimits{MaxPackets: 2}),
	).Copy().To(Target("recording", FileOutput("recording.ogg", io.Discard).Format(av.FormatOgg))).UseRuntime(runtime)

	intent := job.Intent()
	if len(intent.Targets) != 1 || intent.Targets[0].Name != "recording" {
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
			Channels:   Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    Stereo,
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
		RTP(receiver).Codec(Opus()).RTPBuffer(RTPBufferLimits{MaxPackets: 2}),
	).Copy().To(FileOutput("recording.ogg", io.Discard)).UseRuntime(runtime).Build(ctx)
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
		RTP(receiver).Name("video").Codec(VP8()).RTPBuffer(RTPBufferLimits{MaxPackets: 2}),
	).Copy().To(FileOutput("recording.ivf", &out))

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
		FileInput("", strings.NewReader("")).MIME("audio/ogg"),
	).Copy().To(FileOutput("recording.ogg", io.Discard)).UseRuntime(runtime)
	intent := job.Intent()
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
	).Copy().To(FileOutput("", io.Discard).MIME("audio/ogg")).UseRuntime(runtime)
	intent := job.Intent()
	if len(intent.Targets) != 1 || intent.Targets[0].MIMEType != "audio/ogg" {
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
			Channels:   Stereo,
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
			Channels:    Stereo,
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
		RTP(audioReceiver).Name("audio").Codec(Opus()).RTPBuffer(RTPBufferLimits{MaxPackets: 2}),
	).UseRuntime(runtime).
		And(RTP(videoReceiver).Name("video").Codec(VP8()).RTPBuffer(RTPBufferLimits{MaxPackets: 2})).
		To(FileOutput("recording.ogg", io.Discard)).
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
		To(SinkEndpoint(&runtimeTestSink{name: "frames"})).
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
		To(SinkEndpoint(sink)).
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

func TestStreamRecipeEncodeToSinkEndpointRuns(t *testing.T) {
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
		Opus(96_000).
		To(SinkEndpoint(sink)).
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

func TestStreamRecipeEncodeFansOutToMuxAndSinkEndpoints(t *testing.T) {
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
		Opus(96_000).
		To(
			FileOutput("archive.ogg", io.Discard),
			SinkEndpoint(sink),
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

func TestStreamRecipeEncodeToTypedTargetRuns(t *testing.T) {
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
	target := Target("archive", FileOutput("archive.ogg", io.Discard).Format(av.FormatOgg))
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Opus(96_000).
		To(target)

	intent := job.Intent()
	if len(intent.Streams) != 1 ||
		len(intent.Streams[0].Targets) != 1 ||
		intent.Streams[0].Targets[0] != "archive" ||
		len(intent.Targets) != 1 ||
		intent.Targets[0].Name != "archive" {
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
		Tap("audio.packets").
		To(SinkEndpoint(base)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	taps := task.Taps()
	if len(taps) != 1 || taps[0].Name != "audio.packets" || taps[0].Domain != DomainPacket || taps[0].Node.String() != "select-audio" {
		t.Fatalf("taps = %+v", taps)
	}
	late := &runtimeTestSink{name: "late-packets"}
	attachment, err := task.Attach(ctx, Branch("late").FromTap("audio.packets").Copy().To(SinkEndpoint(late)))
	if err != nil {
		t.Fatal(err)
	}
	defer attachment.Close(ctx)
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

func TestStreamRecipeCopyFansOutToMuxAndSinkEndpoints(t *testing.T) {
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
			FileOutput("archive.ogg", io.Discard),
			SinkEndpoint(sink),
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
		Tap("audio.packets").
		Branches(
			Branch("archive").To(Target("archive", FileOutput("archive.ogg", io.Discard))),
			Branch("packets").To(Target("packets", SinkEndpoint(sink))),
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

	var packetTap TapInfo
	for _, tap := range task.Taps() {
		if tap.Name == "audio.packets" {
			packetTap = tap
			break
		}
	}
	if packetTap.Name == "" ||
		packetTap.Domain != DomainPacket ||
		packetTap.MediaKind != av.MediaAudio ||
		packetTap.Caps.Codec != av.CodecOpus ||
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

func TestStreamRecipeCopyTapCanAttachRuntimeMuxTarget(t *testing.T) {
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
			Channels:   Stereo,
		},
	}
	receiver := &runtimeRTPReceiver{
		streams: []av.Stream{stream},
		payload: rtpav.NewStaticPayloadMap(0, []rtpav.PayloadCodec{{
			PayloadType: 111,
			Parameters:  stream.Codec,
			MIMEType:    rtpav.MIMEOpus,
			ClockRate:   48000,
			Channels:    Stereo,
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

	task, err := From(RTP(receiver).Name("audio").Codec(Opus())).
		UseRuntime(runtime).
		Audio().
		Copy().
		Tap("audio.copied").
		To(FileOutput("archive.ogg", io.Discard)).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	var copiedTap TapInfo
	for _, tap := range task.Taps() {
		if tap.Name == "audio.copied" {
			copiedTap = tap
			break
		}
	}
	if copiedTap.Name == "" ||
		copiedTap.Domain != DomainPacket ||
		copiedTap.MediaKind != av.MediaAudio ||
		copiedTap.Caps.Codec != av.CodecOpus ||
		copiedTap.Caps.StreamID != "audio" ||
		copiedTap.Node != "select-audio" {
		t.Fatalf("copied tap = %+v, want packet Opus audio tap on select-audio", copiedTap)
	}

	recording, err := task.Attach(ctx, Branch("record").
		FromTap("audio.copied").
		Copy().
		To(Target("record", FileOutput("recording.ogg", io.Discard))))
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
		To(SinkEndpoint(&runtimeTestSink{name: "frames"}))

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
		Opus(96_000).
		To(FileOutput("archive.ogg", io.Discard)).
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
	if encoderFactory.config.Parameters.ID != av.CodecOpus || encoderFactory.config.Bitrate != 96_000 {
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
		Tap("audio.decoded").
		Branches(Branch("main").Opus(96_000).To(Target("archive", FileOutput("archive.ogg", io.Discard))))

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
		Tap("video.720p.frames").
		Branches(
			Branch("web").
				VP9(2_000_000).
				To(Target("web", FileOutput("web.ivf", io.Discard).Format(av.FormatIVF))),
			Branch("thumb").
				Resize(320, 180).
				To(Target("thumbnail", SinkEndpoint(&runtimeTestSink{name: "thumbnail"}))),
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
				VP9(2_000_000).
				To(Target("web", FileOutput("web.ivf", io.Discard).Format(av.FormatIVF))),
			Branch("thumb").
				Resize(320, 180).
				To(Target("thumbnail", SinkEndpoint(&runtimeTestSink{name: "thumbnail"}))),
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
		Resample(16_000, Mono).
		Branches(
			Branch("voice").
				Opus(64_000).
				To(Target("voice", FileOutput("voice.ogg", io.Discard).Format(av.FormatOgg))),
			Branch("levels").
				To(Target("levels", SinkEndpoint(levels))),
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

func TestBranchCompositionFrameSinkEndpointRuns(t *testing.T) {
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
		Branches(Branch("frames").To(Target("frames", SinkEndpoint(sink))))

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
		Branches(Branch("frames").Decode().To(Target("frames", SinkEndpoint(sink))))

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
	flow := AudioFlow("preview").
		Decode().
		Tap("audio.flow.decoded")
	job := From(FileInput("input.ogg", nil)).UseRuntime(New(formats, codecs)).
		Audio().
		Apply(flow).
		To(SinkEndpoint(sink))

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
		decodedTap.Domain != DomainFrame ||
		decodedTap.MediaKind != av.MediaAudio ||
		decodedTap.After != OpDecode ||
		decodedTap.Caps.Codec != av.CodecOpus ||
		decodedTap.Caps.SampleRate != 48000 ||
		decodedTap.Caps.Channels != Stereo ||
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
				Resample(16_000, Mono).
				Opus(64_000).
				To(Target("voice", FileOutput("voice.ogg", io.Discard).Format(av.FormatOgg))),
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
		encoderFactory.config.Bitrate != 64_000 {
		t.Fatalf("encode config: %+v", encoderFactory.config)
	}
	if resampleFactory.config.Audio == nil ||
		resampleFactory.config.Audio.SampleRate != 16_000 ||
		resampleFactory.config.Audio.Channels != Mono {
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
				Target("analysis", SinkEndpoint(analysis)),
				Target("preview", SinkEndpoint(preview)),
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

func TestBranchCompositionResizeSinkEndpointRuns(t *testing.T) {
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
				To(Target("thumbnail", SinkEndpoint(sink))),
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

func TestBranchCompositionEncodeSinkEndpointRuns(t *testing.T) {
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
		Branches(Branch("packets").Opus(96_000).To(Target("packets", SinkEndpoint(sink))))

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
		Tap("audio.decoded").
		Branches(
			Branch("archive").
				Opus(96_000).
				Tap("audio.encoded").
				To(Target("archive", FileOutput("archive.ogg", io.Discard))),
		)

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	var encodedTap TapInfo
	for _, tap := range task.Taps() {
		if tap.Name == "audio.encoded" {
			encodedTap = tap
			break
		}
	}
	if encodedTap.Name == "" ||
		encodedTap.Domain != DomainPacket ||
		encodedTap.MediaKind != av.MediaAudio ||
		encodedTap.Caps.Codec != av.CodecOpus ||
		encodedTap.Caps.StreamID != "archive" ||
		encodedTap.Node != "encode-archive" {
		t.Fatalf("encoded tap = %+v, want packet Opus archive tap on encode-archive", encodedTap)
	}

	recording, err := task.Attach(ctx, Branch("record").
		FromTap("audio.encoded").
		Copy().
		To(Target("record", FileOutput("recording.ogg", io.Discard))))
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
		Tap("video.decoded").
		Branches(
			Branch("720p").
				Resize(1280, 720).
				Tap("video.720p.frames").
				VP9(2_000_000).
				To(Target("web", FileOutput("web.ogg", io.Discard))),
		)

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	var resizeTap TapInfo
	for _, tap := range task.Taps() {
		if tap.Name == "video.720p.frames" {
			resizeTap = tap
			break
		}
	}
	if resizeTap.Name == "" ||
		resizeTap.Domain != DomainFrame ||
		resizeTap.MediaKind != av.MediaVideo ||
		resizeTap.After != OpTransform ||
		resizeTap.Caps.Width != 1280 ||
		resizeTap.Caps.Height != 720 ||
		resizeTap.Caps.PixelFormat != av.PixelFormatYUV420P ||
		resizeTap.Node == "" {
		t.Fatalf("resize tap = %+v, want frame video 1280x720 tap with graph node", resizeTap)
	}

	attachment, err := task.Attach(ctx, Branch("screenshots").
		FromTap("video.720p.frames").
		Resize(320, 180).
		Tap("video.320.frames").
		To(SinkEndpoint(SinkFunc("screenshots", func(context.Context, Message) error {
			return nil
		}))))
	if err != nil {
		t.Fatal(err)
	}
	if resizeFactory.config.Video == nil || resizeFactory.config.Video.Width != 320 || resizeFactory.config.Video.Height != 180 {
		t.Fatalf("runtime resize config = %+v, want 320x180", resizeFactory.config.Video)
	}
	var resizedTap TapInfo
	for _, tap := range task.Taps() {
		if tap.Name == "video.320.frames" {
			resizedTap = tap
			break
		}
	}
	if resizedTap.Name == "" ||
		resizedTap.Domain != DomainFrame ||
		resizedTap.MediaKind != av.MediaVideo ||
		resizedTap.After != OpTransform ||
		resizedTap.Caps.Width != 320 ||
		resizedTap.Caps.Height != 180 ||
		resizedTap.Node != "screenshots/resize-screenshots" {
		t.Fatalf("resized tap = %+v, want frame video 320x180 tap on screenshots/resize-screenshots", resizedTap)
	}
	nestedAttachment, err := task.Attach(ctx, Branch("preview").FromTap("video.320.frames").To(SinkEndpoint(SinkFunc("preview", func(context.Context, Message) error {
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
		Tap("audio.after-meter").
		Opus(96_000).
		Tap("audio.encoded").
		To(FileOutput("archive.ogg", io.Discard))

	task, err := job.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	var customTap, encodedTap TapInfo
	for _, tap := range task.Taps() {
		switch tap.Name {
		case "audio.after-meter":
			customTap = tap
		case "audio.encoded":
			encodedTap = tap
		}
	}
	if customTap.Name == "" || customTap.Domain != DomainFrame || customTap.MediaKind != av.MediaAudio || customTap.After != OpStage || customTap.Node != "meter" {
		t.Fatalf("custom tap = %+v, want frame audio tap on meter", customTap)
	}
	if encodedTap.Name == "" || encodedTap.Domain != DomainPacket || encodedTap.MediaKind != av.MediaAudio || encodedTap.After != OpEncode || encodedTap.Node != "encode-audio" {
		t.Fatalf("encoded tap = %+v, want packet audio tap on encode-audio", encodedTap)
	}

	frameAttachment, err := task.Attach(ctx, Branch("levels").FromTap("audio.after-meter").To(SinkEndpoint(SinkFunc("levels", func(context.Context, Message) error {
		return nil
	}))))
	if err != nil {
		t.Fatal(err)
	}
	packetAttachment, err := task.Attach(ctx, Branch("packets").FromTap("audio.encoded").To(SinkEndpoint(SinkFunc("packets", func(context.Context, Message) error {
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
		Tap("audio.decoded").
		To(SinkEndpoint(&runtimeTestSink{name: "frames"})).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	meter := &runtimeTestStage{name: "meter"}
	voice := AudioFlow("voice").
		Do(meter).
		Resample(16_000, Mono).
		Tap("audio.16k")
	attachment, err := task.Attach(ctx, Branch("voice").
		FromTap("audio.decoded").
		Apply(voice).
		To(SinkEndpoint(SinkFunc("voice", func(context.Context, Message) error {
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
		resampleFactory.config.Audio.Channels != Mono {
		t.Fatalf("runtime resample config = %+v, want 16k mono", resampleFactory.config.Audio)
	}
	var resampledTap TapInfo
	for _, tap := range task.Taps() {
		if tap.Name == "audio.16k" {
			resampledTap = tap
			break
		}
	}
	if resampledTap.Name == "" ||
		resampledTap.Domain != DomainFrame ||
		resampledTap.MediaKind != av.MediaAudio ||
		resampledTap.Caps.SampleRate != 16_000 ||
		resampledTap.Caps.Channels != Mono ||
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
		Tap("audio.decoded").
		To(SinkEndpoint(base)).
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
		FromTap("audio.decoded").
		Do(observer).
		Tap("audio.observed").
		To(SinkEndpoint(analysis)))
	if err != nil {
		t.Fatal(err)
	}
	parentText := specText(parent.Spec())
	if !strings.Contains(parentText, "analysis/observer -> analysis/analysis") {
		t.Fatalf("parent attachment spec:\n%s", parentText)
	}

	var observedTap TapInfo
	for _, tap := range task.Taps() {
		if tap.Name == "audio.observed" {
			observedTap = tap
			break
		}
	}
	if observedTap.Name == "" ||
		observedTap.Domain != DomainFrame ||
		observedTap.MediaKind != av.MediaAudio ||
		observedTap.Node != "analysis/observer" {
		t.Fatalf("observed tap = %+v, want frame audio tap on analysis/observer", observedTap)
	}

	dependent := &runtimeTestSink{name: "dependent"}
	child, err := task.Attach(ctx, Branch("dependent").
		FromTap("audio.observed").
		To(SinkEndpoint(dependent)))
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
		Tap("audio.decoded").
		To(SinkEndpoint(&runtimeTestSink{name: "frames"})).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	before := task.Describe()

	branch := Branch("voice").
		FromTap("audio.decoded").
		Resample(16_000, Mono).
		To(SinkEndpoint(SinkFunc("voice", func(context.Context, Message) error {
			return nil
		})))
	branch.steps[0].transform.Resample.SampleFormat = av.SampleFormatF32
	branch.transforms[0].Resample.SampleFormat = av.SampleFormatF32

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
		Tap("audio.decoded").
		To(SinkEndpoint(&runtimeTestSink{name: "frames"})).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	attachment, err := task.Attach(ctx, Branch("archive").
		FromTap("audio.decoded").
		Opus(96_000).
		To(Target("archive", FileOutput("archive.ogg", io.Discard))))
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
		encoderFactory.config.Bitrate != 96_000 {
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
				Metadata:   av.Metadata{"summary": "audioonly targets accept PCM audio only"},
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
		Tap("audio.decoded").
		To(SinkEndpoint(&runtimeTestSink{name: "frames"})).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()
	before := task.Describe()

	_, err = task.Attach(ctx, Branch("archive").
		FromTap("audio.decoded").
		Opus(96_000).
		To(Target("archive", FileOutput("archive.audioonly", io.Discard).Format(audioOnly))))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "target_mux_incompatible" ||
		!strings.Contains(err.Error(), "audioonly targets accept PCM audio only") ||
		!strings.Contains(err.Error(), "target=archive") ||
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
	graph := New(formats).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.packets",
		MediaKind: av.MediaAudio,
		Domain:    DomainPacket,
		Caps: StreamCaps{
			Domain:     DomainPacket,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	attachment, err := builtTask.Attach(ctx, Branch("record").
		FromTap("audio.packets").
		Copy().
		To(Target("record", FileOutput("recording.ogg", io.Discard))))
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

func TestTaskAttachRejectsDuplicateRuntimeBranchTargetsBeforeMutation(t *testing.T) {
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
	graph := New(formats, codecs).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    DomainFrame,
		Caps: StreamCaps{
			Domain:     DomainFrame,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()
	before := builtTask.Describe()
	tapsBefore := builtTask.Taps()

	archive := Target("archive", FileOutput("archive.ogg", io.Discard))
	_, err = builtTask.Attach(ctx, Branch("fanout").
		FromTap("audio.frames").
		Opus(96_000).
		To(archive, archive))

	var buildErr *BuildError
	if !errors.As(err, &buildErr) ||
		buildErr.Code != "target_duplicate" ||
		buildErr.Operation != "attach runtime branch" ||
		!errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want runtime target_duplicate wrapping ErrUnsupportedBuild", err)
	}
	if !strings.Contains(err.Error(), `branch routes to target "archive" more than once`) ||
		!strings.Contains(err.Error(), "second target index: 1") ||
		!strings.Contains(err.Error(), "list each target once") {
		t.Fatalf("err = %v, want duplicate runtime target guidance", err)
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
	graph := New(formats).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    DomainFrame,
		Caps:      StreamCaps{Domain: DomainFrame, MediaKind: av.MediaAudio, StreamID: "audio", Codec: av.CodecOpus},
		Node:      "source",
	}}
	defer builtTask.Close()

	_, err = builtTask.Attach(ctx, Branch("archive").
		FromTap("audio.frames").
		To(Target("archive", FileOutput("archive.ogg", io.Discard))))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "runtime_branch_encode_missing" {
		t.Fatalf("err = %v, want runtime_branch_encode_missing", err)
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
	graph := New(formats).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", &runtimeTestSink{name: "base"}).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "video.frames",
		MediaKind: av.MediaVideo,
		Domain:    DomainFrame,
		Caps: StreamCaps{
			Domain:      DomainFrame,
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
		codec  CodecSpec
		output EndpointSpec
	}{
		{name: "h264", codec: H264(Bitrate(2_000_000)), output: FileOutput("archive.h264", io.Discard)},
		{name: "av1", codec: AV1(Bitrate(2_000_000)), output: FileOutput("archive.ivf", io.Discard)},
	}
	for _, tc := range cases {
		_, err := builtTask.Attach(ctx, Branch(tc.name).
			FromTap("video.frames").
			Encode(tc.codec).
			To(Target(tc.name, tc.output)))
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
			Channels:     Stereo,
			SampleFormat: av.SampleFormatF32,
			Samples:      480,
		},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	graph := New(codecs).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    DomainFrame,
		Caps: StreamCaps{
			Domain:       DomainFrame,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecPCM,
			SampleRate:   48000,
			Channels:     Stereo,
			SampleFormat: av.SampleFormatF32,
		},
		Node: "source",
	}}
	defer builtTask.Close()
	before := builtTask.Describe()

	_, err = builtTask.Attach(ctx, Branch("record").
		FromTap("audio.frames").
		Encode(Codec(customPCM, av.MediaAudio)).
		To(SinkEndpoint(SinkFunc("record", func(context.Context, Message) error {
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
			Channels:     Stereo,
			SampleFormat: av.SampleFormatS16,
			Samples:      480,
		},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	graph := New(formats, codecs).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    DomainFrame,
		Caps: StreamCaps{
			Domain:       DomainFrame,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecPCM,
			SampleRate:   48000,
			Channels:     Stereo,
			SampleFormat: av.SampleFormatS16,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	attachment, err := builtTask.Attach(ctx, Branch("record").
		FromTap("audio.frames").
		Encode(Codec(customPCM, av.MediaAudio, SampleRate(16_000), Channels(Mono))).
		To(Target("record", FileOutput("recording.ogg", io.Discard))))
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
		encoderFactory.config.Stream.Codec.Channels != Mono {
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
	graph := New(codecs).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.packets",
		MediaKind: av.MediaAudio,
		Domain:    DomainPacket,
		Caps: StreamCaps{
			Domain:     DomainPacket,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	decoded := &runtimeTestSink{name: "decoded"}
	parent, err := builtTask.Attach(ctx, Branch("preview").
		FromTap("audio.packets").
		Decode().
		Tap("audio.decoded.late").
		To(SinkEndpoint(decoded)))
	if err != nil {
		t.Fatal(err)
	}
	decodedTap, ok := findTap(builtTask.Taps(), "audio.decoded.late")
	if !ok ||
		decodedTap.Domain != DomainFrame ||
		decodedTap.MediaKind != av.MediaAudio ||
		decodedTap.Caps.Codec != av.CodecOpus ||
		decodedTap.Caps.SampleRate != 48000 ||
		decodedTap.Node != "preview/decode-preview" {
		t.Fatalf("decoded tap = %+v ok=%v, want frame Opus tap on preview decoder", decodedTap, ok)
	}
	dependent := &runtimeTestSink{name: "dependent"}
	child, err := builtTask.Attach(ctx, Branch("dependent").
		FromTap("audio.decoded.late").
		To(SinkEndpoint(dependent)))
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
	graph := New(codecs).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.packets",
		MediaKind: av.MediaAudio,
		Domain:    DomainPacket,
		Caps: StreamCaps{
			Domain:     DomainPacket,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	flow := AudioFlow("preview").
		Decode().
		Tap("audio.flow.decoded")
	decoded := &runtimeTestSink{name: "decoded"}
	attachment, err := builtTask.Attach(ctx, Branch("preview").
		FromTap("audio.packets").
		Apply(flow).
		To(SinkEndpoint(decoded)))
	if err != nil {
		t.Fatal(err)
	}
	attachmentText := specText(attachment.Spec())
	if !strings.Contains(attachmentText, "preview/decode-preview -> preview/decoded") {
		t.Fatalf("attachment spec:\n%s", attachmentText)
	}
	decodedTap, ok := findTap(builtTask.Taps(), "audio.flow.decoded")
	if !ok ||
		decodedTap.Domain != DomainFrame ||
		decodedTap.MediaKind != av.MediaAudio ||
		decodedTap.After != OpDecode ||
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
	graph := New().Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "video.frames",
		MediaKind: av.MediaVideo,
		Domain:    DomainFrame,
		Caps: StreamCaps{
			Domain:      DomainFrame,
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
		FromTap("video.frames").
		Apply(AudioFlow("voice").Resample(16_000, Mono)).
		To(SinkEndpoint(SinkFunc("voice", func(context.Context, Message) error {
			return nil
		}))))
	var buildErr *BuildError
	if !errors.As(err, &buildErr) || buildErr.Code != "flow_media_mismatch" || !errors.Is(err, ErrUnsupportedBuild) {
		t.Fatalf("err = %v, want flow_media_mismatch wrapping ErrUnsupportedBuild", err)
	}
	if buildErr.Operation != "attach runtime branch" ||
		!strings.Contains(err.Error(), "audio flow cannot be applied to video stream") ||
		!strings.Contains(err.Error(), "AudioFlow") ||
		!strings.Contains(err.Error(), "VideoFlow") {
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
	graph := New(formats, codecs, filters).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.packets",
		MediaKind: av.MediaAudio,
		Domain:    DomainPacket,
		Caps: StreamCaps{
			Domain:       DomainPacket,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecOpus,
			SampleRate:   48000,
			Channels:     Stereo,
			SampleFormat: av.SampleFormatS16,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	attachment, err := builtTask.Attach(ctx, Branch("voice").
		FromTap("audio.packets").
		Decode().
		Resample(16_000, Mono).
		Opus(64_000).
		Tap("audio.voice.packets").
		To(Target("voice", FileOutput("voice.ogg", io.Discard).Format(av.FormatOgg))))
	if err != nil {
		t.Fatal(err)
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
		packetTap.Domain != DomainPacket ||
		packetTap.MediaKind != av.MediaAudio ||
		packetTap.Caps.Codec != av.CodecOpus ||
		packetTap.Caps.SampleRate != 16_000 ||
		packetTap.Caps.Channels != Mono ||
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
		resampleFactory.config.Audio.Channels != Mono {
		t.Fatalf("runtime resample config = %+v, want 16k mono", resampleFactory.config.Audio)
	}
	if encoderFactory.config.Stream.ID != "voice" ||
		encoderFactory.config.Stream.Codec.ID != av.CodecOpus ||
		encoderFactory.config.Stream.Codec.SampleRate != 16_000 ||
		encoderFactory.config.Stream.Codec.Channels != Mono ||
		encoderFactory.config.Bitrate != 64_000 {
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
			Channels:     Stereo,
			SampleFormat: av.SampleFormatS16,
			Samples:      480,
		},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	graph := New(formats, codecs).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    DomainFrame,
		Caps: StreamCaps{
			Domain:       DomainFrame,
			MediaKind:    av.MediaAudio,
			StreamID:     "audio",
			Codec:        av.CodecPCM,
			SampleRate:   48000,
			Channels:     Stereo,
			SampleFormat: av.SampleFormatS16,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	flow := AudioFlow("voice").Encode(Codec(customPCM, av.MediaAudio, SampleRate(16_000), Channels(Mono)))
	attachment, err := builtTask.Attach(ctx, Branch("record").
		FromTap("audio.frames").
		Apply(flow).
		To(Target("record", FileOutput("recording.ogg", io.Discard))))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(specText(attachment.Spec()), "record/encode-record -> record/recording.ogg") {
		t.Fatalf("attachment spec:\n%s", specText(attachment.Spec()))
	}
	if err := builtTask.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if base.count != 1 || encoder.encodes != 1 {
		t.Fatalf("base=%d encodes=%d", base.count, encoder.encodes)
	}
	if encoderFactory.config.Parameters.ID != customPCM ||
		encoderFactory.config.Stream.Codec.ID != customPCM ||
		encoderFactory.config.Stream.Codec.SampleRate != 16_000 ||
		encoderFactory.config.Stream.Codec.Channels != Mono {
		t.Fatalf("flow custom runtime encode config: %+v", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 ||
		muxers.muxers[0].writes != 1 ||
		muxers.muxers[0].lastStream != "record" ||
		!streamIDsEqual(muxers.muxers[0].openedStreams, []av.StreamID{"record"}) {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := builtTask.Detach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if !encoder.closed || !muxers.muxers[0].closed {
		t.Fatalf("closed encoder=%v muxer=%v", encoder.closed, muxers.muxers[0].closed)
	}
}

func TestTaskAttachRuntimeEncodeBranchFansOutToTargets(t *testing.T) {
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
			Channels:   Stereo,
			Samples:    480,
		},
	}
	source := &runtimeTestSource{
		name:    "source",
		message: pipeline.Message{Kind: pipeline.MessageFrame, Frame: &frame},
	}
	base := &runtimeTestSink{name: "base"}
	graph := New(formats, codecs).Graph()
	src := graph.Source("source", source)
	graph.Connect(src.Out(), graph.Sink("base", base).In())
	builtTask, err := graph.Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	runtimeTask := builtTask.(*task)
	runtimeTask.taps = []TapInfo{{
		Name:      "audio.frames",
		MediaKind: av.MediaAudio,
		Domain:    DomainFrame,
		Caps: StreamCaps{
			Domain:     DomainFrame,
			MediaKind:  av.MediaAudio,
			StreamID:   "audio",
			Codec:      av.CodecOpus,
			SampleRate: 48000,
			Channels:   Stereo,
		},
		Node: "source",
	}}
	defer builtTask.Close()

	archive := Target("archive", FileOutput("archive.ogg", io.Discard))
	monitor := Target("monitor", FileOutput("monitor.ogg", io.Discard))
	attachment, err := builtTask.Attach(ctx, Branch("fanout").
		FromTap("audio.frames").
		Opus(96_000).
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
			Channels:     Stereo,
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
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	desc := CodecDescriptor{ID: customPCM, Name: "X PCM S16", Type: av.MediaAudio}
	runtime := New(
		formats,
		WithDecoder(desc, recipePCMDecoderFactory{decoder: decoder}),
		WithEncoder(desc, encoderFactory),
		WithStdFilters(),
	)
	encoded := Codec(customPCM, av.MediaAudio, SampleRate(16_000), Channels(Mono))

	task, err := From(FileInput("input.ogg", nil)).UseRuntime(runtime).
		Audio().
		Resample(16_000, Mono).
		Encode(encoded).
		To(FileOutput("preview.ogg", io.Discard)).
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
	if encoderFactory.config.Stream.Codec.SampleRate != 16_000 || encoderFactory.config.Stream.Codec.Channels != Mono {
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
			Channels:     Stereo,
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
	encoder := &encodeTestEncoder{}
	encoderFactory := &encodeTestEncoderFactory{encoder: encoder}
	desc := CodecDescriptor{ID: customPCM, Name: "X PCM S16", Type: av.MediaAudio}
	runtime := New(
		formats,
		WithDecoder(desc, recipePCMDecoderFactory{decoder: decoder}),
		WithEncoder(desc, encoderFactory),
		WithStdFilters(),
	)
	encoded := Codec(customPCM, av.MediaAudio, SampleRate(16_000), Channels(Mono))
	archive := Target("archive", FileOutput("archive.ogg", io.Discard))

	task, err := From(FileInput("input.ogg", nil)).UseRuntime(runtime).
		Audio().
		Decode().
		Branches(
			Branch("main").
				Resample(16_000, Mono).
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
	if encoderFactory.config.Stream.Codec.SampleRate != 16_000 ||
		encoderFactory.config.Stream.Codec.Channels != Mono ||
		encoderFactory.config.Parameters.ID != customPCM ||
		encoderFactory.config.Stream.Codec.ID != customPCM {
		t.Fatalf("branch custom encode config: %+v", encoderFactory.config)
	}
	if len(muxers.muxers) != 1 || muxers.muxers[0].writes != 1 || muxers.muxers[0].lastStream != "main" {
		t.Fatalf("muxers=%d first=%+v", len(muxers.muxers), muxers.muxers)
	}
	if err := task.Close(); err != nil {
		t.Fatal(err)
	}
}

type recipePCMDecoderFactory struct {
	decoder *recipePCMDecoder
}

func (f recipePCMDecoderFactory) NewDecoder(context.Context, codec.DecodeConfig) (codec.Decoder, error) {
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
		Channels:     Stereo,
		SampleFormat: av.SampleFormatS16,
		Samples:      480,
	}
	if cap(frame.Planes) < 1 {
		frame.Planes = make([]av.Plane, 1)
	} else {
		frame.Planes = frame.Planes[:1]
	}
	frame.Planes[0].Buffer.Bytes = append(frame.Planes[0].Buffer.Bytes[:0], make([]byte, 480*Stereo*2)...)
	frame.Planes[0].Stride = Stereo * 2
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

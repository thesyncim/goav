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
		To(FrameSink(&runtimeTestSink{name: "frames"})).
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
		To(FrameSink(sink)).
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
		To(FrameSink(&runtimeTestSink{name: "frames"}))

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
	if resizeTap.Name == "" || resizeTap.Domain != DomainFrame || resizeTap.MediaKind != av.MediaVideo || resizeTap.Node == "" {
		t.Fatalf("resize tap = %+v, want frame video tap with graph node", resizeTap)
	}

	attachment, err := task.Attach(ctx, Branch("screenshots").
		FromTap("video.720p.frames").
		Resize(320, 180).
		Tap("video.320.frames").
		To(FrameSink(SinkFunc("screenshots", func(context.Context, Message) error {
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
		resizedTap.Caps.Width != 320 ||
		resizedTap.Caps.Height != 180 ||
		resizedTap.Node != "screenshots/resize-screenshots" {
		t.Fatalf("resized tap = %+v, want frame video 320x180 tap on screenshots/resize-screenshots", resizedTap)
	}
	nestedAttachment, err := task.Attach(ctx, Branch("preview").FromTap("video.320.frames").To(FrameSink(SinkFunc("preview", func(context.Context, Message) error {
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
	if customTap.Name == "" || customTap.Domain != DomainFrame || customTap.MediaKind != av.MediaAudio || customTap.Node != "meter" {
		t.Fatalf("custom tap = %+v, want frame audio tap on meter", customTap)
	}
	if encodedTap.Name == "" || encodedTap.Domain != DomainPacket || encodedTap.MediaKind != av.MediaAudio || encodedTap.Node != "encode-audio" {
		t.Fatalf("encoded tap = %+v, want packet audio tap on encode-audio", encodedTap)
	}

	frameAttachment, err := task.Attach(ctx, Branch("levels").FromTap("audio.after-meter").To(FrameSink(SinkFunc("levels", func(context.Context, Message) error {
		return nil
	}))))
	if err != nil {
		t.Fatal(err)
	}
	packetAttachment, err := task.Attach(ctx, Branch("packets").FromTap("audio.encoded").To(FrameSink(SinkFunc("packets", func(context.Context, Message) error {
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
		To(FrameSink(&runtimeTestSink{name: "frames"})).
		Build(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer task.Close()

	attachment, err := task.Attach(ctx, Branch("voice").
		FromTap("audio.decoded").
		Resample(16_000, Mono).
		Tap("audio.16k").
		To(FrameSink(SinkFunc("voice", func(context.Context, Message) error {
			return nil
		}))))
	if err != nil {
		t.Fatal(err)
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
